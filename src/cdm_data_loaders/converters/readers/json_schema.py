"""Read dereferenced or strict Draft 2020-12 JSON Schema into the intermediate representation."""

import copy
import json
from collections.abc import Mapping
from dataclasses import dataclass, field, replace
from typing import Any, Final, NoReturn, cast

from frozendict import frozendict
from jsonschema.validators import validator_for

from cdm_data_loaders.converters.core.errors import ConversionError
from cdm_data_loaders.converters.core.extensions import DEFAULT_EXTENSIONS, ExtensionRegistry, Extensions
from cdm_data_loaders.converters.core.guards import (
    reject_unresolved_references,
    require_object_root,
    require_schema_keyword,
)
from cdm_data_loaders.converters.core.inference import infer_implicit_type
from cdm_data_loaders.converters.core.ir import Field, NodeHints, NodeType, Provenance, SchemaDocument, TypedNode

DEFAULT_MAX_RECURSION_DEPTH: Final[int] = 2

SUPPORTED_SCHEMA_DIALECTS: Final[frozenset[str]] = frozenset(
    {
        "https://json-schema.org/draft/2020-12/schema",
        "https://json-schema.kbase.us/draft/2020-12-xsv/schema",
    }
)

_CONSTRAINING_KEYWORDS: Final[frozenset[str]] = frozenset(
    {
        "type",
        "properties",
        "items",
        "prefixItems",
        "patternProperties",
        "enum",
        "const",
        "$ref",
        "allOf",
        "anyOf",
        "oneOf",
        "dependentSchemas",
        "then",
        "else",
    }
)

_NON_EXTENDABLE_SCALAR_HINTS: Final[dict[str, tuple[NodeType, NodeHints, str]]] = {
    "boolean": ("boolean", NodeHints(logical_type="boolean"), "boolean"),
    "integer": ("integer", NodeHints(logical_type="long", bit_width=64), "integer"),
    "number": ("number", NodeHints(logical_type="double", bit_width=64), "number"),
}

TYPES: Final = frozenset({"object", "array", "string", "integer", "number", "boolean", "null"})
ANNOTATIONS: Final = frozenset(
    {
        "$schema",
        "$id",
        "$anchor",
        "$dynamicAnchor",
        "$comment",
        "$vocabulary",
        "title",
        "description",
        "default",
        "examples",
        "readOnly",
        "writeOnly",
        "deprecated",
    }
)
SCHEMA_KEYS: Final = frozenset(
    {
        "additionalItems",
        "unevaluatedItems",
        "contains",
        "unevaluatedProperties",
        "propertyNames",
        "not",
        "if",
        "then",
        "else",
        "contentSchema",
    }
)
SCHEMA_MAPS: Final = frozenset({"$defs", "definitions", "dependentSchemas"})
STRUCTURE: Final = (
    SCHEMA_KEYS
    | SCHEMA_MAPS
    | {
        "$ref",
        "allOf",
        "type",
        "properties",
        "required",
        "items",
        "prefixItems",
        "patternProperties",
        "additionalProperties",
        "anyOf",
        "oneOf",
        "dependencies",
    }
)


@dataclass(frozen=True, slots=True)
class _StrictBuildContext:
    """Immutable traversal context for strict JSON Schema IR lowering."""

    defs: Mapping[str, Any]
    ref_counts: Mapping[str, int]
    max_recursion_depth: int

    def with_ref(self, ref_name: str) -> "_StrictBuildContext":
        """Return a new context with the recursion counter for ``ref_name`` incremented by 1."""
        return _StrictBuildContext(
            defs=self.defs,
            ref_counts={**self.ref_counts, ref_name: self.ref_counts.get(ref_name, 0) + 1},
            max_recursion_depth=self.max_recursion_depth,
        )


@dataclass(frozen=True, slots=True)
class _StrictNodeMeta:
    """Node-level path, nullability, and facet payloads for strict IR lowering."""

    path: tuple[str | int, ...] = ()
    nullable: bool = False
    annotations: Mapping[str, Any] = field(default_factory=frozendict)
    extensions: Mapping[str, Any] = field(default_factory=frozendict)
    constraints: Mapping[str, Any] = field(default_factory=frozendict)


def _is_null_branch(node: Mapping[str, Any]) -> bool:
    """Return True if ``node`` represents a null-only schema."""
    node_type = node.get("type")
    if node_type in ("null", ["null"]):
        return True
    if "const" in node and node["const"] is None and "type" not in node:
        return True
    return bool(
        "enum" in node
        and isinstance(node["enum"], list)
        and all(v is None for v in node["enum"])
        and "type" not in node
    )


def _merge_property_definitions(prop_defs: list[dict[str, Any]]) -> dict[str, Any]:
    """Merge multiple schema definitions for the same property name."""
    if len(prop_defs) == 1:
        return prop_defs[0]
    first_json = json.dumps(prop_defs[0], sort_keys=True)
    if all(json.dumps(p, sort_keys=True) == first_json for p in prop_defs[1:]):
        return prop_defs[0]
    merged: dict[str, Any] = {"anyOf": prop_defs}
    for prop_schema in prop_defs:
        if "description" in prop_schema:
            merged["description"] = prop_schema["description"]
            break
    return merged


@dataclass(frozen=True, slots=True)
class JsonSchemaReader:
    """Read validated object-compatible documents using an explicit extension registry."""

    extension_registry: ExtensionRegistry = DEFAULT_EXTENSIONS
    error_type: type[ConversionError] = ConversionError
    converter_name: str = "JsonSchemaReader"
    dereference_function: str = "dereference_schema"
    strict: bool = False
    max_recursion_depth: int = DEFAULT_MAX_RECURSION_DEPTH
    error_by_code: Mapping[str, type[ConversionError]] = field(default_factory=frozendict)

    def _raise_err(self, code: str, msg: str) -> NoReturn:
        """Raise the configured exception subclass for ``code`` or fall back to ``error_type``."""
        cls = self.error_by_code.get(code, self.error_type)
        raise cls(msg)

    def read(self, source: Mapping[str, Any]) -> SchemaDocument:
        """Read a document; validation and reference resolution are upstream steps unless strict is enabled."""
        if self.strict:
            return self._read_strict(source)
        if not isinstance(source, Mapping):
            msg = "A JSON Schema document must be an object"
            raise self.error_type(msg)
        schema = dict(source)
        require_schema_keyword(schema, self.error_type, converter_name=self.converter_name)
        declaration = schema.get("type")
        if (
            declaration is not None
            and declaration != "object"
            and not (isinstance(declaration, list) and "object" in declaration)
        ):
            msg = "Root schema must be object-compatible"
            raise self.error_type(msg)
        node = self._node(schema, ())
        return SchemaDocument(
            root=node,
            dialect=schema["$schema"],
            identifier=schema.get("$id"),
            annotations=node.annotations,
            extensions=node.extensions,
            provenance=Provenance("json-schema"),
            jsonschema_validator_cls=validator_for(schema),
        )

    def read_node(self, source: Mapping[str, Any] | bool) -> TypedNode:  # noqa: FBT001
        """Read a schema fragment without document-level dialect or root guards."""
        if self.strict:
            ctx = _StrictBuildContext(defs={}, ref_counts={}, max_recursion_depth=self.max_recursion_depth)
            return self._strict_build_field_node(source, ctx, (), required_in_parent=True)
        return self._node(source, ())

    def _read_strict(self, source: Mapping[str, Any]) -> SchemaDocument:
        """Read and lower a Draft 2020-12 JSON Schema under the strict specification."""
        if not isinstance(source, Mapping):
            msg = f"Missing or unsupported $schema dialect: {None!r}; expected JSON Schema Draft 2020-12"
            self._raise_err("E1", msg)
        schema = dict(source)
        require_schema_keyword(
            schema,
            self.error_by_code.get("E1", self.error_type),
            converter_name=self.converter_name,
        )
        if schema.get("$schema") not in SUPPORTED_SCHEMA_DIALECTS:
            msg = (
                f"Missing or unsupported $schema dialect: {schema.get('$schema')!r}; expected JSON Schema Draft 2020-12"
            )
            self._raise_err("E1", msg)
        require_object_root(
            schema,
            self.error_by_code.get("E2", self.error_type),
            target="a struct schema",
        )

        defs = schema.get("$defs", {})
        if not isinstance(defs, Mapping):
            msg = "$defs must be a mapping of definition names to schema objects"
            raise self.error_type(msg)

        root_dict = self._strict_validate_node(schema)
        ref_counts: dict[str, int] = {}
        if "$ref" in root_dict and "properties" not in root_dict:
            ref_name, target = self._strict_resolve_ref(root_dict["$ref"], defs)
            ref_counts[ref_name] = 1
            root_dict = self._strict_validate_node(target)

        root_kind = self._strict_classify_node_kind(root_dict, defs)
        if root_kind == "map":
            msg = "Top-level root schema cannot be a dynamic map without fixed properties; root JSON Schema did not resolve to a struct"
            self._raise_err("E2", msg)
        if root_kind != "object":
            msg = f"Top-level root schema must resolve to a fixed-property object, got {root_kind!r}"
            self._raise_err("E2", msg)

        ctx = _StrictBuildContext(
            defs=defs,
            ref_counts=ref_counts,
            max_recursion_depth=self.max_recursion_depth,
        )
        desc = self._strict_get_inherited_description(root_dict, defs)
        root_annotations: dict[str, Any] = {"description": desc} if desc else {}
        root_extensions = Extensions(
            {k: v for k, v in schema.items() if k.startswith("x-")},
            self.extension_registry,
        )
        root_constraints = {
            k: v for k, v in schema.items() if k not in STRUCTURE and k not in ANNOTATIONS and not k.startswith("x-")
        }
        root_meta = _StrictNodeMeta(
            path=(),
            nullable=False,
            annotations=root_annotations,
            extensions=root_extensions,
            constraints=root_constraints,
        )
        node = self._strict_build_merged_object(root_dict, ctx, root_meta)
        return SchemaDocument(
            root=node,
            dialect=schema["$schema"],
            identifier=schema.get("$id"),
            annotations=node.annotations,
            extensions=node.extensions,
            provenance=Provenance("json-schema"),
            jsonschema_validator_cls=validator_for(schema),
        )

    def _strict_resolve_ref(self, ref: object, defs: Mapping[str, Any]) -> tuple[str, dict[str, Any]]:
        """Resolve a local ``#/$defs/<name>`` reference pointer against ``defs``."""
        prefix = "#/$defs/"
        if not isinstance(ref, str) or not ref.startswith(prefix):
            msg = f"Unresolved or unsupported non-local $ref: {ref!r}"
            self._raise_err("E9", msg)
        name = ref[len(prefix) :]
        if name not in defs or not isinstance(defs[name], dict):
            msg = f"Unresolvable or unresolved $ref {ref!r}: definition {name!r} does not exist in $defs"
            self._raise_err("E9", msg)
        return name, defs[name]

    def _strict_validate_node(self, node: object) -> dict[str, Any]:
        """Validate that ``node`` is a constrained JSON Schema object without unsupported keywords."""
        if isinstance(node, bool) or not isinstance(node, dict):
            msg = f"Unconstrained or boolean schema node is not supported: {node!r}"
            self._raise_err("E4", msg)
        Extensions(
            {k: v for k, v in node.items() if k.startswith("x-")},
            self.extension_registry,
        )
        if isinstance(node.get("if"), Mapping):
            Extensions(
                {k: v for k, v in node["if"].items() if k.startswith("x-")},
                self.extension_registry,
            )
        if "not" in node:
            msg = "Unsupported JSON Schema keyword 'not': negation schemas cannot be converted to PyArrow"
            self._raise_err("E7", msg)
        if "$dynamicRef" in node or "$recursiveRef" in node:
            msg = "Unresolved or unsupported dynamic reference keyword '$dynamicRef' in JSON Schema"
            self._raise_err("E9", msg)

        has_constraint = any(k in node for k in _CONSTRAINING_KEYWORDS) or isinstance(
            node.get("additionalProperties"), dict
        )
        if not has_constraint:
            msg = f"Unconstrained schema without type or structural keywords is not supported: {node!r}"
            self._raise_err("E4", msg)

        has_fixed_props = bool(node.get("properties")) or any(
            k in node for k in ("allOf", "anyOf", "oneOf", "dependentSchemas", "then", "else")
        )
        has_map_schema = isinstance(node.get("additionalProperties"), dict) or bool(node.get("patternProperties"))
        if has_fixed_props and has_map_schema:
            msg = "Mixed fixed properties and dynamic map (additionalProperties/patternProperties) is not supported"
            self._raise_err("E6", msg)
        return node

    def _strict_check_no_forbidden_types_in_recursive_cycle(
        self,
        ref_name: str,
        target: Mapping[str, Any],
        defs: Mapping[str, Any],
    ) -> None:
        """Raise ``E10`` if a recursive ``$defs`` cycle contains non-JSON-serializable types."""
        visited_refs: set[str] = {ref_name}
        stack: list[object] = [target]
        while stack:
            current = stack.pop()
            if isinstance(current, Mapping):
                if current.get("format") == "date" or current.get("contentEncoding") in {"base64", "base16"}:
                    msg = (
                        f"Non-JSON-serializable type (format={current.get('format')!r}, "
                        f"contentEncoding={current.get('contentEncoding')!r}) is not supported "
                        f"inside recursive $defs cycle {ref_name!r}"
                    )
                    self._raise_err("E10", msg)
                if "$ref" in current and isinstance(current["$ref"], str):
                    child_ref_name, child_target = self._strict_resolve_ref(current["$ref"], defs)
                    if child_ref_name not in visited_refs:
                        visited_refs.add(child_ref_name)
                        stack.append(child_target)
                stack.extend(v for k, v in current.items() if k != "$defs")
            elif isinstance(current, list):
                stack.extend(current)

    def _strict_has_null_possibility(self, node: Mapping[str, Any], defs: Mapping[str, Any]) -> bool:
        """Return True if ``node`` or any of its referenced/combinator branches allows ``null``."""
        if "$ref" in node:
            _, target = self._strict_resolve_ref(node["$ref"], defs)
            if self._strict_has_null_possibility(target, defs):
                return True
        node_type = node.get("type")
        if node_type == "null" or (isinstance(node_type, list) and "null" in node_type):
            return True
        if ("const" in node and node["const"] is None) or (
            "enum" in node and isinstance(node["enum"], list) and None in node["enum"]
        ):
            return True
        for kw in ("anyOf", "oneOf", "allOf"):
            if kw in node and isinstance(node[kw], list):
                for branch in node[kw]:
                    if isinstance(branch, Mapping) and (
                        _is_null_branch(branch) or self._strict_has_null_possibility(branch, defs)
                    ):
                        return True
        return False

    def _strict_get_inherited_description(self, node: Mapping[str, Any], defs: Mapping[str, Any]) -> str | None:
        """Extract the local or ``$ref``-inherited ``description`` string for a schema node."""
        if isinstance(node.get("description"), str) and node["description"]:
            return node["description"]
        if "$ref" in node:
            _, target = self._strict_resolve_ref(node["$ref"], defs)
            return self._strict_get_inherited_description(target, defs)
        for kw in ("anyOf", "oneOf"):
            if kw in node and isinstance(node[kw], list):
                non_null = [b for b in node[kw] if isinstance(b, Mapping) and not _is_null_branch(b)]
                if len(non_null) == 1 and "$ref" in non_null[0]:
                    return self._strict_get_inherited_description(non_null[0], defs)
        return None

    def _strict_scalar_for_type_name(self, type_name: str, node: Mapping[str, Any]) -> tuple[NodeType, NodeHints, str]:
        """Map a single JSON Schema scalar type name and format/encoding facets to IR scalar facts."""
        if type_name in _NON_EXTENDABLE_SCALAR_HINTS:
            return _NON_EXTENDABLE_SCALAR_HINTS[type_name]
        if type_name == "string":
            content_enc = node.get("contentEncoding")
            if content_enc in {"base64", "base16"}:
                return "string", NodeHints(logical_type="binary"), f"string ({content_enc})"
            fmt = node.get("format")
            if fmt == "date":
                return "string", NodeHints(logical_type="date"), "string (date)"
            if isinstance(fmt, str) and fmt:
                return "string", NodeHints(logical_type="string"), f"string ({fmt})"
            return "string", NodeHints(logical_type="string"), "string"
        msg = f"Unsupported or non-scalar type in scalar context: {type_name!r}"
        raise self.error_type(msg)

    @staticmethod
    def _strict_unify_scalar_types(
        branches: list[tuple[NodeType, NodeHints, str]],
    ) -> tuple[NodeType, NodeHints, str]:
        """Unify multiple scalar ``(NodeType, NodeHints, jsonschema_type)`` tuples."""
        seen_labels: list[str] = []
        for _, _, label in branches:
            if label not in seen_labels:
                seen_labels.append(label)
        joined_label = " | ".join(seen_labels) if len(seen_labels) > 1 else seen_labels[0]

        first_nt, first_hints, _ = branches[0]
        if all((nt, h) == (first_nt, first_hints) for nt, h, _ in branches[1:]):
            return first_nt, first_hints, joined_label
        base_types = {label.split(" ", maxsplit=1)[0] for _, _, label in branches}
        if base_types <= {"integer", "number"}:
            return "number", NodeHints(logical_type="double", bit_width=64), joined_label
        return "string", NodeHints(logical_type="string"), joined_label

    def _strict_infer_literal_type_name(self, value: object) -> str:
        """Infer the JSON Schema scalar type name of a Python literal inside ``const`` or ``enum``."""
        if isinstance(value, bool):
            return "boolean"
        if isinstance(value, int):
            return "integer"
        if isinstance(value, float):
            return "number"
        if isinstance(value, str):
            return "string"
        msg = f"Unsupported non-scalar literal in const/enum: {value!r}"
        raise self.error_type(msg)

    def _strict_resolve_scalar(self, node: Mapping[str, Any]) -> tuple[NodeType, NodeHints, str]:
        """Resolve a scalar JSON Schema node to ``(NodeType, NodeHints, jsonschema_type)``."""
        if _is_null_branch(node):
            msg = "Standalone null-only schema cannot be converted to a PyArrow field"
            self._raise_err("E11", msg)
        if "const" in node and "type" not in node:
            return self._strict_scalar_for_type_name(self._strict_infer_literal_type_name(node["const"]), {})
        if "enum" in node and "type" not in node:
            non_null_vals = [v for v in node["enum"] if v is not None]
            types_seen: list[str] = []
            for val in non_null_vals:
                t_name = self._strict_infer_literal_type_name(val)
                if t_name not in types_seen:
                    types_seen.append(t_name)
            return self._strict_unify_scalar_types(
                [self._strict_scalar_for_type_name(t_name, {}) for t_name in types_seen]
            )

        node_type = node.get("type")
        if isinstance(node_type, list):
            non_null = [t for t in node_type if t != "null"]
            if len(non_null) == 1:
                return self._strict_resolve_scalar({**node, "type": non_null[0]})
            if any(t in {"object", "array"} for t in non_null):
                msg = f"Incompatible branch types in multi-type union: {node_type!r}"
                self._raise_err("E8", msg)
            return self._strict_unify_scalar_types([self._strict_scalar_for_type_name(t, node) for t in non_null])
        if isinstance(node_type, str):
            return self._strict_scalar_for_type_name(node_type, node)
        msg = f"Cannot resolve scalar type for schema node: {node!r}"
        raise self.error_type(msg)

    def _strict_classify_resolved_node_kind(self, node: Mapping[str, Any]) -> str:
        """Classify a validated, non-``$ref`` schema node into its structural shape."""
        if any(kw in node for kw in ("anyOf", "oneOf")) and "properties" not in node and "type" not in node:
            return "union"
        if "allOf" in node and "properties" not in node and "type" not in node:
            return "object"
        node_type = node.get("type")
        if isinstance(node_type, list):
            non_null = [t for t in node_type if t != "null"]
            if len(non_null) == 1:
                node_type = non_null[0]
            elif any(t in {"object", "array"} for t in non_null):
                msg = f"Incompatible branch types in multi-type union: {node_type!r}"
                self._raise_err("E8", msg)
            else:
                return "scalar"
        if node_type == "object" or any(k in node for k in ("properties", "additionalProperties", "patternProperties")):
            has_props = bool(node.get("properties")) or any(
                k in node for k in ("allOf", "anyOf", "oneOf", "dependentSchemas", "then", "else")
            )
            has_map = isinstance(node.get("additionalProperties"), dict) or bool(node.get("patternProperties"))
            return "map" if (has_map and not has_props) else "object"
        if node_type == "array" or "items" in node or "prefixItems" in node:
            return "array"
        return "scalar"

    def _strict_classify_node_kind(self, node: object, defs: Mapping[str, Any]) -> str:
        """Validate ``node`` and return ``'null'``, ``'object'``, ``'array'``, ``'map'``, ``'scalar'``, or ``'union'``."""
        validated = self._strict_validate_node(node)
        if _is_null_branch(validated):
            return "null"
        if "$ref" in validated:
            _, target = self._strict_resolve_ref(validated["$ref"], defs)
            if len(validated) == 1:
                return self._strict_classify_node_kind(target, defs)
            merged = {**target, **{k: v for k, v in validated.items() if k != "$ref"}}
            return self._strict_classify_node_kind(merged, defs)
        return self._strict_classify_resolved_node_kind(validated)

    def _strict_extract_choice_branches_into_object(
        self,
        branches: list[Any],
        kw: str,
        ctx: _StrictBuildContext,
        raw_props: dict[str, list[tuple[dict[str, Any], _StrictBuildContext]]],
        scalar_branches: list[dict[str, Any]],
    ) -> set[str]:
        """Extract properties, common required fields, and scalar branches from an ``anyOf``/``oneOf`` list."""
        non_null = [b for b in branches if not (isinstance(b, Mapping) and _is_null_branch(b))]
        if not non_null:
            msg = f"Standalone null-only {kw} combinator cannot be converted to a PyArrow type"
            self._raise_err("E11", msg)
        branch_req_sets: list[set[str]] = []
        has_scalar = False
        for branch in non_null:
            branch_kind = self._strict_classify_node_kind(branch, ctx.defs)
            if branch_kind == "scalar":
                has_scalar = True
                scalar_branches.append(branch)
            elif branch_kind == "object":
                b_props, b_req, b_scalars = self._strict_extract_object_parts_with_refs(branch, ctx)
                for prop_name, (prop_schema, prop_ctx) in b_props.items():
                    raw_props.setdefault(prop_name, []).append((prop_schema, prop_ctx))
                branch_req_sets.append(b_req)
                scalar_branches.extend(b_scalars)
            else:
                msg = f"Incompatible branch shape {branch_kind!r} in {kw} combinator"
                self._raise_err("E8", msg)
        if branch_req_sets and not has_scalar:
            return set.intersection(*branch_req_sets)
        return set()

    def _strict_extract_object_parts_with_refs(
        self,
        node: dict[str, Any],
        ctx: _StrictBuildContext,
    ) -> tuple[dict[str, tuple[dict[str, Any], _StrictBuildContext]], set[str], list[dict[str, Any]]]:
        """Extract ordered merged property definitions, required set, and scalar union branches for an object node."""
        validated = self._strict_validate_node(node)
        if "$ref" in validated:
            ref_name, target = self._strict_resolve_ref(validated["$ref"], ctx.defs)
            if ctx.ref_counts.get(ref_name, 0) >= 1:
                self._strict_check_no_forbidden_types_in_recursive_cycle(ref_name, target, ctx.defs)
            next_ctx = ctx.with_ref(ref_name)
            merged_node = {**copy.deepcopy(target), **{k: v for k, v in validated.items() if k != "$ref"}}
            return self._strict_extract_object_parts_with_refs(merged_node, next_ctx)

        raw_props: dict[str, list[tuple[dict[str, Any], _StrictBuildContext]]] = {}
        required: set[str] = set(validated.get("required", []))
        scalar_branches: list[dict[str, Any]] = []

        for sub in validated.get("allOf", []):
            sub_kind = self._strict_classify_node_kind(sub, ctx.defs)
            if sub_kind != "object":
                msg = f"Incompatible non-object branch shape {sub_kind!r} in unmerged 'allOf' combinator"
                self._raise_err("E8", msg)
            sub_props, sub_req, sub_scalars = self._strict_extract_object_parts_with_refs(sub, ctx)
            for prop_name, item in sub_props.items():
                raw_props.setdefault(prop_name, []).append(item)
            required |= sub_req
            scalar_branches.extend(sub_scalars)

        for prop_name, prop_schema in validated.get("properties", {}).items():
            raw_props.setdefault(prop_name, []).append((self._strict_validate_node(prop_schema), ctx))

        for kw in ("anyOf", "oneOf"):
            if kw in validated:
                required |= self._strict_extract_choice_branches_into_object(
                    validated[kw], kw, ctx, raw_props, scalar_branches
                )

        conditional_subschemas = [
            *validated.get("dependentSchemas", {}).values(),
            *(validated[kw] for kw in ("then", "else") if kw in validated),
        ]
        for cond_schema in conditional_subschemas:
            c_props, _, _ = self._strict_extract_object_parts_with_refs(cond_schema, ctx)
            for prop_name, item in c_props.items():
                raw_props.setdefault(prop_name, []).append(item)

        merged_props: dict[str, tuple[dict[str, Any], _StrictBuildContext]] = {
            prop_name: (_merge_property_definitions([item[0] for item in items]), items[0][1])
            for prop_name, items in raw_props.items()
        }
        return merged_props, required, scalar_branches

    def _strict_build_merged_object(
        self,
        node: dict[str, Any],
        ctx: _StrictBuildContext,
        meta: _StrictNodeMeta,
        *,
        force_all_nullable: bool = False,
        scalar_branches: list[dict[str, Any]] | None = None,
    ) -> TypedNode:
        """Build an object ``TypedNode`` from an object schema and its combinator branches."""
        props_by_name, required, extracted_scalars = self._strict_extract_object_parts_with_refs(node, ctx)
        all_scalars = list(scalar_branches or [])
        for scalar_node in extracted_scalars:
            if scalar_node not in all_scalars:
                all_scalars.append(scalar_node)

        if all_scalars:
            force_all_nullable = True
            if "_value" in props_by_name:
                msg = "Property name '_value' in object branch collides with synthesized '_value' field in scalar+object union"
                self._raise_err("E12", msg)

        fields: list[Field] = []
        for prop_name, (merged_prop, prop_ctx) in props_by_name.items():
            is_req = (prop_name in required) and not force_all_nullable
            prop_node = self._strict_build_field_node(
                merged_prop, prop_ctx, (*meta.path, "properties", prop_name), required_in_parent=is_req
            )
            fields.append(
                Field(prop_name, prop_node, required=not bool(prop_node.nullable), provenance=prop_node.provenance)
            )

        if all_scalars:
            resolved_scalars: list[tuple[NodeType, NodeHints, str]] = []
            val_meta = _StrictNodeMeta(path=(*meta.path, "_value"))
            for scalar_branch in all_scalars:
                s_node = self._strict_build_type(scalar_branch, ctx, val_meta)
                s_label = s_node.provenance.metadata.get("jsonschema_type") if s_node.provenance else None
                if not isinstance(s_label, str):
                    msg = f"Expected scalar branch with jsonschema_type label, got {scalar_branch!r}"
                    raise self.error_type(msg)
                resolved_scalars.append((s_node.type, s_node.hints, s_label))
            val_type, val_hints, val_label = self._strict_unify_scalar_types(resolved_scalars)
            val_prov = Provenance("json-schema", (*meta.path, "_value"), {"jsonschema_type": val_label})
            val_node = TypedNode(type=val_type, nullable=True, hints=val_hints, provenance=val_prov)
            fields.append(Field("_value", val_node, required=False, provenance=val_prov))

        if not fields:
            msg = "Empty object schema with no properties cannot be converted to a PyArrow struct"
            self._raise_err("E3", msg)
        return TypedNode(
            type="object",
            nullable=meta.nullable,
            hints=NodeHints(logical_type="struct"),
            properties=tuple(fields),
            required_names=tuple(f.name for f in fields if f.required),
            additional_properties=TypedNode(type="never", nullable=False),
            annotations=meta.annotations,
            extensions=meta.extensions,
            constraints=meta.constraints,
            provenance=Provenance("json-schema", meta.path),
        )

    def _strict_build_map(
        self,
        node: dict[str, Any],
        ctx: _StrictBuildContext,
        meta: _StrictNodeMeta,
    ) -> TypedNode:
        """Build a map ``TypedNode`` from a dynamic map object schema."""
        val_schemas: list[dict[str, Any]] = []
        if isinstance(node.get("additionalProperties"), dict):
            val_schemas.append(self._strict_validate_node(node["additionalProperties"]))
        val_schemas.extend(
            self._strict_validate_node(pat_schema) for pat_schema in node.get("patternProperties", {}).values()
        )
        val_schema = _merge_property_definitions(val_schemas)
        val_node = self._strict_build_field_node(
            val_schema, ctx, (*meta.path, "additionalProperties"), required_in_parent=True
        )
        key_node = TypedNode(
            type="string",
            nullable=False,
            hints=NodeHints(logical_type="string"),
            provenance=Provenance("json-schema", meta.path, {"jsonschema_type": "string"}),
        )
        return TypedNode(
            type="map",
            nullable=meta.nullable,
            hints=NodeHints(logical_type="map"),
            key_type=key_node,
            value_type=val_node,
            annotations=meta.annotations,
            extensions=meta.extensions,
            constraints=meta.constraints,
            provenance=Provenance("json-schema", meta.path),
        )

    def _strict_build_array(
        self,
        node: dict[str, Any],
        ctx: _StrictBuildContext,
        meta: _StrictNodeMeta,
    ) -> TypedNode:
        """Build a list ``TypedNode`` (homogeneous array) or struct ``TypedNode`` (closed tuple ``prefixItems``)."""
        if "prefixItems" in node:
            if node.get("items") not in (False, None):
                msg = "Open tuple combining prefixItems with schema-valued items is not supported"
                self._raise_err("E5", msg)
            min_items = node.get("minItems", 0)
            fields: list[Field] = []
            for idx, item_schema in enumerate(node["prefixItems"]):
                item_node = self._strict_build_field_node(
                    item_schema, ctx, (*meta.path, "prefixItems", idx), required_in_parent=idx < min_items
                )
                fields.append(
                    Field(
                        f"item_{idx}",
                        item_node,
                        required=not bool(item_node.nullable),
                        provenance=item_node.provenance,
                    )
                )
            if not fields:
                msg = "Empty prefixItems tuple cannot be converted to a PyArrow struct"
                self._raise_err("E3", msg)
            return TypedNode(
                type="object",
                nullable=meta.nullable,
                hints=NodeHints(logical_type="struct"),
                properties=tuple(fields),
                required_names=tuple(f.name for f in fields if f.required),
                additional_properties=TypedNode(type="never", nullable=False),
                annotations=meta.annotations,
                extensions=meta.extensions,
                constraints=meta.constraints,
                provenance=Provenance("json-schema", meta.path),
            )

        validated_items = self._strict_validate_node(node.get("items"))
        item_node = self._strict_build_field_node(validated_items, ctx, (*meta.path, "items"), required_in_parent=True)
        return TypedNode(
            type="array",
            nullable=meta.nullable,
            hints=NodeHints(logical_type="list"),
            items=item_node,
            annotations=meta.annotations,
            extensions=meta.extensions,
            constraints=meta.constraints,
            provenance=Provenance("json-schema", meta.path),
        )

    def _strict_build_union_type(
        self,
        node: dict[str, Any],
        ctx: _StrictBuildContext,
        meta: _StrictNodeMeta,
    ) -> TypedNode:
        """Resolve an ``anyOf`` or ``oneOf`` combinator node into a ``TypedNode``."""
        kw = "anyOf" if "anyOf" in node else "oneOf"
        non_null = [
            self._strict_validate_node(b) for b in node[kw] if not (isinstance(b, Mapping) and _is_null_branch(b))
        ]
        if not non_null:
            msg = f"Standalone null-only {kw} combinator cannot be converted to a PyArrow field"
            self._raise_err("E11", msg)
        if len(non_null) == 1:
            return self._strict_build_type(non_null[0], ctx, meta)

        obj_branches: list[dict[str, Any]] = []
        scalar_branches: list[dict[str, Any]] = []
        for branch in non_null:
            kind = self._strict_classify_node_kind(branch, ctx.defs)
            if kind == "object":
                obj_branches.append(branch)
            elif kind == "scalar":
                scalar_branches.append(branch)
            else:
                msg = f"Incompatible branch shape {kind!r} in {kw} combinator"
                self._raise_err("E8", msg)

        if scalar_branches and not obj_branches:
            resolved_scalars: list[tuple[NodeType, NodeHints, str]] = []
            scalar_meta = _StrictNodeMeta(path=meta.path)
            for scalar_branch in scalar_branches:
                s_node = self._strict_build_type(scalar_branch, ctx, scalar_meta)
                s_label = s_node.provenance.metadata.get("jsonschema_type") if s_node.provenance else None
                if not isinstance(s_label, str):
                    msg = f"Expected scalar branch with jsonschema_type label, got {scalar_branch!r}"
                    raise self.error_type(msg)
                resolved_scalars.append((s_node.type, s_node.hints, s_label))
            u_type, u_hints, u_label = self._strict_unify_scalar_types(resolved_scalars)
            return TypedNode(
                type=u_type,
                nullable=meta.nullable,
                hints=u_hints,
                annotations=meta.annotations,
                extensions=meta.extensions,
                constraints=meta.constraints,
                provenance=Provenance("json-schema", meta.path, {"jsonschema_type": u_label}),
            )

        if obj_branches and not scalar_branches:
            return self._strict_build_merged_object(node, ctx, meta)

        return self._strict_build_merged_object(
            node,
            ctx,
            meta,
            force_all_nullable=True,
            scalar_branches=scalar_branches,
        )

    def _strict_build_ref_type(
        self,
        validated: dict[str, Any],
        ctx: _StrictBuildContext,
        meta: _StrictNodeMeta,
    ) -> TypedNode:
        """Resolve and build a ``$ref`` schema node with recursion depth tracking."""
        ref_name, target = self._strict_resolve_ref(validated["$ref"], ctx.defs)
        count = ctx.ref_counts.get(ref_name, 0)
        if count >= 1:
            self._strict_check_no_forbidden_types_in_recursive_cycle(ref_name, target, ctx.defs)
        if count >= ctx.max_recursion_depth:
            return TypedNode(
                type="string",
                nullable=meta.nullable,
                hints=NodeHints(logical_type="string"),
                annotations=meta.annotations,
                extensions=meta.extensions,
                constraints=meta.constraints,
                provenance=Provenance("json-schema", meta.path),
            )
        next_ctx = ctx.with_ref(ref_name)
        if len(validated) == 1:
            result = self._strict_build_type(target, next_ctx, meta)
            if result.type == "object":
                prov_meta = dict(result.provenance.metadata) if result.provenance is not None else {}
                prov_meta.setdefault("component_id", hash(("jsonschema_ref", ref_name)))
                prov_meta.setdefault("type_name", ref_name)
                if count >= 1:
                    prov_meta["inline_struct"] = True
                return replace(
                    result,
                    provenance=Provenance(
                        result.provenance.source if result.provenance is not None else "json-schema",
                        result.provenance.path if result.provenance is not None else meta.path,
                        prov_meta,
                    ),
                )
            return result
        merged = {**copy.deepcopy(target), **{k: v for k, v in validated.items() if k not in {"$ref", "description"}}}
        return self._strict_build_type(merged, next_ctx, meta)

    def _strict_build_type(
        self,
        node: object,
        ctx: _StrictBuildContext,
        meta: _StrictNodeMeta,
    ) -> TypedNode:
        """Convert any JSON Schema node into a strict ``TypedNode``."""
        validated = self._strict_validate_node(node)
        if _is_null_branch(validated):
            msg = "Standalone null-only schema cannot be converted to a PyArrow field"
            self._raise_err("E11", msg)

        if "$ref" in validated:
            return self._strict_build_ref_type(validated, ctx, meta)

        if (
            ("anyOf" in validated or "oneOf" in validated)
            and "properties" not in validated
            and "allOf" not in validated
        ):
            return self._strict_build_union_type(validated, ctx, meta)

        kind = self._strict_classify_node_kind(validated, ctx.defs)
        if kind == "object":
            return self._strict_build_merged_object(validated, ctx, meta)
        if kind == "map":
            return self._strict_build_map(validated, ctx, meta)
        if kind == "array":
            return self._strict_build_array(validated, ctx, meta)
        s_type, s_hints, s_label = self._strict_resolve_scalar(validated)
        raw_decl = validated.get("type")
        declared_type: str | tuple[str, ...] | None = (
            tuple(str(t) for t in raw_decl)
            if isinstance(raw_decl, list)
            else (raw_decl if isinstance(raw_decl, str) else None)
        )
        return TypedNode(
            type=s_type,
            declared_type=declared_type,
            nullable=meta.nullable,
            hints=s_hints,
            annotations=meta.annotations,
            extensions=meta.extensions,
            constraints=meta.constraints,
            provenance=Provenance("json-schema", meta.path, {"jsonschema_type": s_label}),
        )

    def _strict_build_field_node(
        self,
        node: object,
        ctx: _StrictBuildContext,
        path: tuple[str | int, ...],
        *,
        required_in_parent: bool,
    ) -> TypedNode:
        """Build a ``TypedNode`` with nullability, description, extensions, and constraints for a field or item."""
        validated = self._strict_validate_node(node)
        nullable = (not required_in_parent) or self._strict_has_null_possibility(validated, ctx.defs)
        desc = self._strict_get_inherited_description(validated, ctx.defs)
        annotations: dict[str, Any] = {"description": desc} if desc else {}
        extensions = Extensions(
            {k: v for k, v in validated.items() if k.startswith("x-")},
            self.extension_registry,
        )
        constraints = {
            k: v for k, v in validated.items() if k not in STRUCTURE and k not in ANNOTATIONS and not k.startswith("x-")
        }
        meta = _StrictNodeMeta(
            path=path,
            nullable=nullable,
            annotations=annotations,
            extensions=extensions,
            constraints=constraints,
        )
        return self._strict_build_type(validated, ctx, meta)

    def _node(self, schema: object, path: tuple[str | int, ...]) -> TypedNode:
        """Traverse only schema-valued keywords, leaving literal data untouched."""
        provenance = Provenance("json-schema", path)
        if isinstance(schema, bool):
            return TypedNode(type="any" if schema else "never", nullable=schema, provenance=provenance)
        if not isinstance(schema, Mapping):
            msg = f"Expected a schema object or boolean at {path}"
            raise self.error_type(msg)
        reject_unresolved_references(
            dict(schema),
            self.error_type,
            converter_name=self.converter_name,
            dereference_function=self.dereference_function,
        )
        if "$dynamicRef" in schema or "$recursiveRef" in schema:
            msg = f"Unresolved dynamic reference at {path}"
            raise self.error_type(msg)
        declaration = schema.get("type")
        inferred = infer_implicit_type(dict(schema))
        kind = declaration if isinstance(declaration, str) and declaration in TYPES else "unknown"
        nullable = None
        if declaration is not None:
            nullable = "null" in declaration if isinstance(declaration, list) else declaration == "null"
        constraints = {
            key: value
            for key, value in schema.items()
            if key not in STRUCTURE and key not in ANNOTATIONS and not key.startswith("x-")
        }
        dependencies = schema.get("dependencies", {})
        literal_dependencies = {key: value for key, value in dependencies.items() if isinstance(value, list)}
        if literal_dependencies:
            constraints["dependencies"] = literal_dependencies
        schema_maps = {
            key: {name: self._node(child, (*path, key, name)) for name, child in schema[key].items()}
            for key in SCHEMA_MAPS
            if key in schema
        }
        if "dependencies" in schema:
            schema_maps["dependencies"] = {
                name: self._node(child, (*path, "dependencies", name))
                for name, child in dependencies.items()
                if not isinstance(child, list)
            }
        required = schema.get("required", [])
        items = schema.get("items")
        return TypedNode(
            type=cast("NodeType", kind),
            nullable=nullable,
            declared_type=tuple(declaration) if isinstance(declaration, list) else declaration,
            inferred_type=inferred,
            properties=tuple(
                Field(name, self._node(child, (*path, "properties", name)), required=name in required)
                for name, child in schema.get("properties", {}).items()
            ),
            required_names=tuple(required) if "required" in schema else None,
            items=(
                tuple(self._node(child, (*path, "items", index)) for index, child in enumerate(items))
                if isinstance(items, list)
                else self._node(items, (*path, "items"))
            )
            if "items" in schema
            else None,
            prefix_items=self._branches(schema, "prefixItems", path),
            pattern_properties={
                name: self._node(child, (*path, "patternProperties", name))
                for name, child in schema.get("patternProperties", {}).items()
            },
            additional_properties=self._node(schema["additionalProperties"], (*path, "additionalProperties"))
            if "additionalProperties" in schema
            else None,
            any_of=self._branches(schema, "anyOf", path),
            one_of=self._branches(schema, "oneOf", path),
            schema_keywords={key: self._node(schema[key], (*path, key)) for key in SCHEMA_KEYS if key in schema},
            schema_maps=schema_maps,
            constraints=constraints,
            annotations={key: value for key, value in schema.items() if key in ANNOTATIONS},
            extensions=Extensions(
                {key: value for key, value in schema.items() if key.startswith("x-")},
                self.extension_registry,
            ),
            provenance=provenance,
            source_keywords=tuple(schema),
        )

    def _branches(
        self,
        schema: Mapping[str, Any],
        keyword: str,
        path: tuple[str | int, ...],
    ) -> tuple[TypedNode, ...] | None:
        """Retain branch order and distinguish an absent keyword from an empty list."""
        if keyword not in schema:
            return None
        return tuple(self._node(child, (*path, keyword, index)) for index, child in enumerate(schema[keyword]))
