"""Read strict Draft 2020-12 JSON Schema documents with ``$defs`` recursion into the IR."""

import copy
import json
from collections.abc import Mapping
from dataclasses import dataclass, field, replace
from typing import Any, Final, NoReturn

from frozendict import frozendict
from jsonschema.validators import validator_for

from cdm_data_loaders.converters.core.errors import ConversionError
from cdm_data_loaders.converters.core.extensions import DEFAULT_EXTENSIONS, ExtensionRegistry
from cdm_data_loaders.converters.core.guards import (
    DEFAULT_MAX_RECURSION_DEPTH,
    require_object_root,
    require_schema_keyword,
)
from cdm_data_loaders.converters.core.ir import (
    SPEC_BINARY,
    SPEC_BOOL,
    SPEC_DATE,
    SPEC_FLOAT64,
    SPEC_INT64,
    SPEC_STRING,
    Field,
    NodeHints,
    NodeType,
    Provenance,
    SchemaDocument,
    TypedNode,
)
from cdm_data_loaders.converters.readers.json_schema import extract_constraints, extract_extensions
from cdm_data_loaders.schemas.jsonschema import CUSTOM_META_SCHEMA_URI, register_xsv_validator

SUPPORTED_SCHEMA_DIALECTS: Final[frozenset[str]] = frozenset(
    {
        "https://json-schema.org/draft/2020-12/schema",
        CUSTOM_META_SCHEMA_URI,
    }
)

CONSTRAINING_KEYWORDS: Final[frozenset[str]] = frozenset(
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

NON_EXTENDABLE_SCALAR_HINTS: Final[frozendict[str, tuple[NodeType, NodeHints, str]]] = frozendict(
    {
        "boolean": (*SPEC_BOOL, "boolean"),
        "integer": (*SPEC_INT64, "integer"),
        "number": (*SPEC_FLOAT64, "number"),
    }
)

STRING_FORMAT_SPECS: Final[frozendict[str, tuple[NodeType, NodeHints]]] = frozendict(
    {
        "date": SPEC_DATE,
        **dict.fromkeys(("date-time", "time", "duration", "uuid", "uri", "email"), SPEC_STRING),
    }
)

STRING_CONTENT_ENCODING_SPECS: Final[frozendict[str, tuple[NodeType, NodeHints]]] = frozendict(
    {
        "base64": SPEC_BINARY,
        "base16": SPEC_BINARY,
    }
)


@dataclass(frozen=True, slots=True)
class _StrictBuildContext:
    """Immutable traversal context for strict JSON Schema IR lowering."""

    defs: Mapping[str, Any]
    ref_counts: Mapping[str, int]
    max_recursion_depth: int

    def with_ref(self, ref_name: str) -> "_StrictBuildContext":
        """Return a new context with the recursion counter for ``ref_name`` incremented by 1.

        :param ref_name: ``$defs`` definition name being entered.
        :type ref_name: str
        :return: Updated traversal context.
        :rtype: _StrictBuildContext
        """
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
    """Return True if ``node`` represents a null-only schema.

    :param node: JSON Schema node mapping.
    :type node: Mapping[str, Any]
    :return: ``True`` if ``node`` matches only ``null``.
    :rtype: bool
    """
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
    """Merge multiple schema definitions for the same property name.

    :param prop_defs: List of schema dictionaries for one property.
    :type prop_defs: list[dict[str, Any]]
    :return: Merged property schema dictionary.
    :rtype: dict[str, Any]
    """
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
class StrictJsonSchemaReader:
    """Read Draft 2020-12 JSON Schema documents under the strict tabular conversion specification."""

    extension_registry: ExtensionRegistry = DEFAULT_EXTENSIONS
    error_type: type[ConversionError] = ConversionError
    converter_name: str = "StrictJsonSchemaReader"
    max_recursion_depth: int = DEFAULT_MAX_RECURSION_DEPTH
    error_by_code: Mapping[str, type[ConversionError]] = field(default_factory=frozendict)

    def _raise_err(self, code: str, msg: str) -> NoReturn:
        """Raise the configured exception subclass for ``code`` or fall back to ``error_type``.

        :param code: Specification error code (``E1``-``E12``).
        :type code: str
        :param msg: Diagnostic error message.
        :type msg: str
        :raises ConversionError: Always raised with the resolved error class.
        """
        cls = self.error_by_code.get(code, self.error_type)
        raise cls(msg)

    def read(self, source: Mapping[str, Any]) -> SchemaDocument:
        """Read and lower a Draft 2020-12 JSON Schema under the strict specification.

        :param source: Input JSON Schema mapping.
        :type source: Mapping[str, Any]
        :return: Lowered ``SchemaDocument``.
        :rtype: SchemaDocument
        """
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
        if schema.get("$schema") == CUSTOM_META_SCHEMA_URI:
            register_xsv_validator()
        require_object_root(
            schema,
            self.error_by_code.get("E2", self.error_type),
            target="a struct schema",
        )

        defs = schema.get("$defs", {})
        if not isinstance(defs, Mapping):
            msg = "$defs must be a mapping of definition names to schema objects"
            raise self.error_type(msg)

        root_dict = self._validate_node(schema)
        ref_counts: dict[str, int] = {}
        if "$ref" in root_dict and "properties" not in root_dict:
            ref_name, target = self._resolve_ref(root_dict["$ref"], defs)
            ref_counts[ref_name] = 1
            root_dict = self._validate_node(target)

        root_kind = self._classify_node_kind(root_dict, defs)
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
        root_meta = self._build_node_meta(root_dict, defs, (), nullable=False, facet_source=schema)
        node = self._build_merged_object(root_dict, ctx, root_meta)
        return SchemaDocument(
            root=node,
            dialect=schema["$schema"],
            identifier=schema.get("$id"),
            annotations=node.annotations,
            extensions=node.extensions,
            provenance=Provenance("json-schema"),
            jsonschema_validator_cls=validator_for(schema),
        )

    def _build_node_meta(
        self,
        node: Mapping[str, Any],
        defs: Mapping[str, Any],
        path: tuple[str | int, ...],
        *,
        nullable: bool,
        facet_source: Mapping[str, Any] | None = None,
    ) -> _StrictNodeMeta:
        """Build a ``_StrictNodeMeta`` with inherited description, extensions, and constraints.

        :param node: Validated JSON Schema node used for description inheritance.
        :type node: Mapping[str, Any]
        :param defs: Root ``$defs`` mapping.
        :type defs: Mapping[str, Any]
        :param path: JSON pointer path tuple.
        :type path: tuple[str | int, ...]
        :param nullable: Whether the node is nullable.
        :type nullable: bool
        :param facet_source: Optional mapping from which to extract extensions and constraints (defaults to ``node``).
        :type facet_source: Mapping[str, Any] | None
        :return: Populated ``_StrictNodeMeta``.
        :rtype: _StrictNodeMeta
        """
        source = facet_source if facet_source is not None else node
        desc = self._get_inherited_description(node, defs)
        return _StrictNodeMeta(
            path=path,
            nullable=nullable,
            annotations={"description": desc} if desc else {},
            extensions=extract_extensions(source, self.extension_registry),
            constraints=extract_constraints(source),
        )

    def read_node(self, source: Mapping[str, Any] | bool) -> TypedNode:  # noqa: FBT001
        """Read a standalone schema fragment under strict rules without root-object guards.

        :param source: JSON Schema fragment mapping or boolean schema.
        :type source: Mapping[str, Any] | bool
        :return: Lowered ``TypedNode``.
        :rtype: TypedNode
        """
        ctx = _StrictBuildContext(defs={}, ref_counts={}, max_recursion_depth=self.max_recursion_depth)
        return self._build_field_node(source, ctx, (), required_in_parent=True)

    def _resolve_ref(self, ref: object, defs: Mapping[str, Any]) -> tuple[str, dict[str, Any]]:
        """Resolve a local ``#/$defs/<name>`` reference pointer against ``defs``.

        :param ref: Reference URI string.
        :type ref: object
        :param defs: Root ``$defs`` mapping.
        :type defs: Mapping[str, Any]
        :return: Tuple of ``(definition_name, target_schema_dict)``.
        :rtype: tuple[str, dict[str, Any]]
        """
        prefix = "#/$defs/"
        if not isinstance(ref, str) or not ref.startswith(prefix):
            msg = f"Unresolved or unsupported non-local $ref: {ref!r}"
            self._raise_err("E9", msg)
        name = ref[len(prefix) :]
        if name not in defs or not isinstance(defs[name], dict):
            msg = f"Unresolvable or unresolved $ref {ref!r}: definition {name!r} does not exist in $defs"
            self._raise_err("E9", msg)
        return name, defs[name]

    def _validate_node(self, node: object) -> dict[str, Any]:
        """Validate that ``node`` is a constrained JSON Schema object without unsupported keywords.

        :param node: Raw JSON Schema node.
        :type node: object
        :return: Validated schema dictionary.
        :rtype: dict[str, Any]
        """
        if isinstance(node, bool) or not isinstance(node, dict):
            msg = f"Unconstrained or boolean schema node is not supported: {node!r}"
            self._raise_err("E4", msg)
        extract_extensions(node, self.extension_registry)
        if isinstance(node.get("if"), Mapping):
            extract_extensions(node["if"], self.extension_registry)
        if "not" in node:
            msg = "Unsupported JSON Schema keyword 'not': negation schemas cannot be converted to PyArrow"
            self._raise_err("E7", msg)
        if "$dynamicRef" in node or "$recursiveRef" in node:
            msg = "Unresolved or unsupported dynamic reference keyword '$dynamicRef' in JSON Schema"
            self._raise_err("E9", msg)

        has_constraint = any(k in node for k in CONSTRAINING_KEYWORDS) or isinstance(
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

    def _check_no_forbidden_types_in_recursive_cycle(
        self,
        ref_name: str,
        target: Mapping[str, Any],
        defs: Mapping[str, Any],
    ) -> None:
        """Raise ``E10`` if a recursive ``$defs`` cycle contains non-JSON-serializable types.

        :param ref_name: Name of the recursive ``$defs`` entry.
        :type ref_name: str
        :param target: Target schema mapping for ``ref_name``.
        :type target: Mapping[str, Any]
        :param defs: Root ``$defs`` mapping.
        :type defs: Mapping[str, Any]
        """
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
                    child_ref_name, child_target = self._resolve_ref(current["$ref"], defs)
                    if child_ref_name not in visited_refs:
                        visited_refs.add(child_ref_name)
                        stack.append(child_target)
                stack.extend(v for k, v in current.items() if k != "$defs")
            elif isinstance(current, list):
                stack.extend(current)

    def _has_null_possibility(self, node: Mapping[str, Any], defs: Mapping[str, Any]) -> bool:
        """Return True if ``node`` or any of its referenced/combinator branches allows ``null``.

        :param node: JSON Schema node mapping.
        :type node: Mapping[str, Any]
        :param defs: Root ``$defs`` mapping.
        :type defs: Mapping[str, Any]
        :return: ``True`` if ``null`` is permitted.
        :rtype: bool
        """
        if "$ref" in node:
            _, target = self._resolve_ref(node["$ref"], defs)
            if self._has_null_possibility(target, defs):
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
                        _is_null_branch(branch) or self._has_null_possibility(branch, defs)
                    ):
                        return True
        return False

    def _get_inherited_description(self, node: Mapping[str, Any], defs: Mapping[str, Any]) -> str | None:
        """Extract the local or ``$ref``-inherited ``description`` string for a schema node.

        :param node: JSON Schema node mapping.
        :type node: Mapping[str, Any]
        :param defs: Root ``$defs`` mapping.
        :type defs: Mapping[str, Any]
        :return: Description string or ``None``.
        :rtype: str | None
        """
        if isinstance(node.get("description"), str) and node["description"]:
            return node["description"]
        if "$ref" in node:
            _, target = self._resolve_ref(node["$ref"], defs)
            return self._get_inherited_description(target, defs)
        for kw in ("anyOf", "oneOf"):
            if kw in node and isinstance(node[kw], list):
                non_null = [b for b in node[kw] if isinstance(b, Mapping) and not _is_null_branch(b)]
                if len(non_null) == 1 and "$ref" in non_null[0]:
                    return self._get_inherited_description(non_null[0], defs)
        return None

    def _scalar_for_type_name(self, type_name: str, node: Mapping[str, Any]) -> tuple[NodeType, NodeHints, str]:
        """Map a single JSON Schema scalar type name and format/encoding facets to IR scalar facts.

        :param type_name: JSON Schema scalar type name.
        :type type_name: str
        :param node: JSON Schema node mapping carrying ``format`` or ``contentEncoding``.
        :type node: Mapping[str, Any]
        :return: Tuple of ``(NodeType, NodeHints, jsonschema_type)``.
        :rtype: tuple[NodeType, NodeHints, str]
        """
        if type_name in NON_EXTENDABLE_SCALAR_HINTS:
            return NON_EXTENDABLE_SCALAR_HINTS[type_name]
        if type_name == "string":
            content_enc = node.get("contentEncoding")
            if isinstance(content_enc, str) and content_enc in STRING_CONTENT_ENCODING_SPECS:
                return *STRING_CONTENT_ENCODING_SPECS[content_enc], f"string ({content_enc})"
            fmt = node.get("format")
            if isinstance(fmt, str) and fmt in STRING_FORMAT_SPECS:
                return *STRING_FORMAT_SPECS[fmt], f"string ({fmt})"
            if isinstance(fmt, str) and fmt:
                return *SPEC_STRING, f"string ({fmt})"
            return *SPEC_STRING, "string"
        msg = f"Unsupported or non-scalar type in scalar context: {type_name!r}"
        raise self.error_type(msg)

    @staticmethod
    def _unify_scalar_types(
        branches: list[tuple[NodeType, NodeHints, str]],
    ) -> tuple[NodeType, NodeHints, str]:
        """Unify multiple scalar ``(NodeType, NodeHints, jsonschema_type)`` tuples.

        :param branches: List of resolved scalar branch tuples.
        :type branches: list[tuple[NodeType, NodeHints, str]]
        :return: Unified ``(NodeType, NodeHints, jsonschema_type)`` tuple.
        :rtype: tuple[NodeType, NodeHints, str]
        """
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
            return *SPEC_FLOAT64, joined_label
        return *SPEC_STRING, joined_label

    def _resolve_and_unify_scalar_branches(
        self,
        scalar_branches: list[dict[str, Any]],
        ctx: _StrictBuildContext,
        path: tuple[str | int, ...],
    ) -> tuple[NodeType, NodeHints, str]:
        """Build each scalar branch node and unify their scalar types and labels.

        :param scalar_branches: List of scalar branch schema dictionaries.
        :type scalar_branches: list[dict[str, Any]]
        :param ctx: Active strict build context.
        :type ctx: _StrictBuildContext
        :param path: Provenance path for scalar branch evaluation.
        :type path: tuple[str | int, ...]
        :return: Unified ``(NodeType, NodeHints, jsonschema_type)`` tuple.
        :rtype: tuple[NodeType, NodeHints, str]
        """
        resolved_scalars: list[tuple[NodeType, NodeHints, str]] = []
        branch_meta = _StrictNodeMeta(path=path)
        for scalar_branch in scalar_branches:
            s_node = self._build_type(scalar_branch, ctx, branch_meta)
            s_label = s_node.provenance.metadata.get("jsonschema_type") if s_node.provenance else None
            if not isinstance(s_label, str):
                msg = f"Expected scalar branch with jsonschema_type label, got {scalar_branch!r}"
                raise self.error_type(msg)
            resolved_scalars.append((s_node.type, s_node.hints, s_label))
        return self._unify_scalar_types(resolved_scalars)

    def _infer_literal_type_name(self, value: object) -> str:
        """Infer the JSON Schema scalar type name of a Python literal inside ``const`` or ``enum``.

        :param value: Python scalar literal.
        :type value: object
        :return: Inferred JSON Schema scalar type name.
        :rtype: str
        """
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

    def _resolve_scalar(self, node: Mapping[str, Any]) -> tuple[NodeType, NodeHints, str]:
        """Resolve a scalar JSON Schema node to ``(NodeType, NodeHints, jsonschema_type)``.

        :param node: Scalar JSON Schema node mapping.
        :type node: Mapping[str, Any]
        :return: Tuple of ``(NodeType, NodeHints, jsonschema_type)``.
        :rtype: tuple[NodeType, NodeHints, str]
        """
        if _is_null_branch(node):
            msg = "Standalone null-only schema cannot be converted to a PyArrow field"
            self._raise_err("E11", msg)
        if "const" in node and "type" not in node:
            return self._scalar_for_type_name(self._infer_literal_type_name(node["const"]), {})
        if "enum" in node and "type" not in node:
            non_null_vals = [v for v in node["enum"] if v is not None]
            types_seen: list[str] = []
            for val in non_null_vals:
                t_name = self._infer_literal_type_name(val)
                if t_name not in types_seen:
                    types_seen.append(t_name)
            return self._unify_scalar_types([self._scalar_for_type_name(t_name, {}) for t_name in types_seen])

        node_type = node.get("type")
        if isinstance(node_type, list):
            non_null = [t for t in node_type if t != "null"]
            if len(non_null) == 1:
                return self._resolve_scalar({**node, "type": non_null[0]})
            if any(t in {"object", "array"} for t in non_null):
                msg = f"Incompatible branch types in multi-type union: {node_type!r}"
                self._raise_err("E8", msg)
            return self._unify_scalar_types([self._scalar_for_type_name(t, node) for t in non_null])
        if isinstance(node_type, str):
            return self._scalar_for_type_name(node_type, node)
        msg = f"Cannot resolve scalar type for schema node: {node!r}"
        raise self.error_type(msg)

    def _classify_resolved_node_kind(self, node: Mapping[str, Any]) -> str:
        """Classify a validated, non-``$ref`` schema node into its structural shape.

        :param node: Validated non-``$ref`` JSON Schema node.
        :type node: Mapping[str, Any]
        :return: Structural shape classification string.
        :rtype: str
        """
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

    def _classify_node_kind(self, node: object, defs: Mapping[str, Any]) -> str:
        """Validate ``node`` and return ``'null'``, ``'object'``, ``'array'``, ``'map'``, ``'scalar'``, or ``'union'``.

        :param node: Raw JSON Schema node.
        :type node: object
        :param defs: Root ``$defs`` mapping.
        :type defs: Mapping[str, Any]
        :return: Structural shape classification string.
        :rtype: str
        """
        validated = self._validate_node(node)
        if _is_null_branch(validated):
            return "null"
        if "$ref" in validated:
            _, target = self._resolve_ref(validated["$ref"], defs)
            if len(validated) == 1:
                return self._classify_node_kind(target, defs)
            merged = {**target, **{k: v for k, v in validated.items() if k != "$ref"}}
            return self._classify_node_kind(merged, defs)
        return self._classify_resolved_node_kind(validated)

    def _extract_choice_branches_into_object(
        self,
        branches: list[Any],
        kw: str,
        ctx: _StrictBuildContext,
        raw_props: dict[str, list[tuple[dict[str, Any], _StrictBuildContext]]],
        scalar_branches: list[dict[str, Any]],
    ) -> set[str]:
        """Extract properties, common required fields, and scalar branches from an ``anyOf``/``oneOf`` list.

        :param branches: List of combinator branch schemas.
        :type branches: list[Any]
        :param kw: Combinator keyword (``'anyOf'`` or ``'oneOf'``).
        :type kw: str
        :param ctx: Active strict build context.
        :type ctx: _StrictBuildContext
        :param raw_props: Accumulator mapping property names to ``(schema, ctx)`` pairs.
        :type raw_props: dict[str, list[tuple[dict[str, Any], _StrictBuildContext]]]
        :param scalar_branches: Accumulator list of scalar branch schemas.
        :type scalar_branches: list[dict[str, Any]]
        :return: Set of property names required across all object branches.
        :rtype: set[str]
        """
        non_null = [b for b in branches if not (isinstance(b, Mapping) and _is_null_branch(b))]
        if not non_null:
            msg = f"Standalone null-only {kw} combinator cannot be converted to a PyArrow type"
            self._raise_err("E11", msg)
        branch_req_sets: list[set[str]] = []
        has_scalar = False
        for branch in non_null:
            branch_kind = self._classify_node_kind(branch, ctx.defs)
            if branch_kind == "scalar":
                has_scalar = True
                scalar_branches.append(branch)
            elif branch_kind == "object":
                b_props, b_req, b_scalars = self._extract_object_parts_with_refs(branch, ctx)
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

    def _extract_object_parts_with_refs(
        self,
        node: dict[str, Any],
        ctx: _StrictBuildContext,
    ) -> tuple[dict[str, tuple[dict[str, Any], _StrictBuildContext]], set[str], list[dict[str, Any]]]:
        """Extract ordered merged property definitions, required set, and scalar union branches for an object node.

        :param node: Object JSON Schema node dictionary.
        :type node: dict[str, Any]
        :param ctx: Active strict build context.
        :type ctx: _StrictBuildContext
        :return: Tuple of ``(merged_props, required_set, scalar_branches)``.
        :rtype: tuple[dict[str, tuple[dict[str, Any], _StrictBuildContext]], set[str], list[dict[str, Any]]]
        """
        validated = self._validate_node(node)
        if "$ref" in validated:
            ref_name, target = self._resolve_ref(validated["$ref"], ctx.defs)
            if ctx.ref_counts.get(ref_name, 0) >= 1:
                self._check_no_forbidden_types_in_recursive_cycle(ref_name, target, ctx.defs)
            next_ctx = ctx.with_ref(ref_name)
            merged_node = {**copy.deepcopy(target), **{k: v for k, v in validated.items() if k != "$ref"}}
            return self._extract_object_parts_with_refs(merged_node, next_ctx)

        raw_props: dict[str, list[tuple[dict[str, Any], _StrictBuildContext]]] = {}
        required: set[str] = set(validated.get("required", []))
        scalar_branches: list[dict[str, Any]] = []

        for sub in validated.get("allOf", []):
            sub_kind = self._classify_node_kind(sub, ctx.defs)
            if sub_kind != "object":
                msg = f"Incompatible non-object branch shape {sub_kind!r} in unmerged 'allOf' combinator"
                self._raise_err("E8", msg)
            sub_props, sub_req, sub_scalars = self._extract_object_parts_with_refs(sub, ctx)
            for prop_name, item in sub_props.items():
                raw_props.setdefault(prop_name, []).append(item)
            required |= sub_req
            scalar_branches.extend(sub_scalars)

        for prop_name, prop_schema in validated.get("properties", {}).items():
            raw_props.setdefault(prop_name, []).append((self._validate_node(prop_schema), ctx))

        for kw in ("anyOf", "oneOf"):
            if kw in validated:
                required |= self._extract_choice_branches_into_object(
                    validated[kw], kw, ctx, raw_props, scalar_branches
                )

        conditional_subschemas = [
            *validated.get("dependentSchemas", {}).values(),
            *(validated[kw] for kw in ("then", "else") if kw in validated),
        ]
        for cond_schema in conditional_subschemas:
            c_props, _, _ = self._extract_object_parts_with_refs(cond_schema, ctx)
            for prop_name, item in c_props.items():
                raw_props.setdefault(prop_name, []).append(item)

        merged_props: dict[str, tuple[dict[str, Any], _StrictBuildContext]] = {
            prop_name: (_merge_property_definitions([item[0] for item in items]), items[0][1])
            for prop_name, items in raw_props.items()
        }
        return merged_props, required, scalar_branches

    def _build_merged_object(
        self,
        node: dict[str, Any],
        ctx: _StrictBuildContext,
        meta: _StrictNodeMeta,
        *,
        force_all_nullable: bool = False,
        scalar_branches: list[dict[str, Any]] | None = None,
    ) -> TypedNode:
        """Build an object ``TypedNode`` from an object schema and its combinator branches.

        :param node: Object JSON Schema node dictionary.
        :type node: dict[str, Any]
        :param ctx: Active strict build context.
        :type ctx: _StrictBuildContext
        :param meta: Node metadata payload.
        :type meta: _StrictNodeMeta
        :param force_all_nullable: Whether to force all properties to be nullable.
        :type force_all_nullable: bool
        :param scalar_branches: Optional scalar union branches to synthesize into ``_value``.
        :type scalar_branches: list[dict[str, Any]] | None
        :return: Object ``TypedNode``.
        :rtype: TypedNode
        """
        props_by_name, required, extracted_scalars = self._extract_object_parts_with_refs(node, ctx)
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
            prop_node = self._build_field_node(
                merged_prop, prop_ctx, (*meta.path, "properties", prop_name), required_in_parent=is_req
            )
            fields.append(
                Field(prop_name, prop_node, required=not bool(prop_node.nullable), provenance=prop_node.provenance)
            )

        if all_scalars:
            val_path = (*meta.path, "_value")
            val_type, val_hints, val_label = self._resolve_and_unify_scalar_branches(all_scalars, ctx, val_path)
            val_prov = Provenance("json-schema", val_path, {"jsonschema_type": val_label})
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

    def _build_map(
        self,
        node: dict[str, Any],
        ctx: _StrictBuildContext,
        meta: _StrictNodeMeta,
    ) -> TypedNode:
        """Build a map ``TypedNode`` from a dynamic map object schema.

        :param node: Map JSON Schema node dictionary.
        :type node: dict[str, Any]
        :param ctx: Active strict build context.
        :type ctx: _StrictBuildContext
        :param meta: Node metadata payload.
        :type meta: _StrictNodeMeta
        :return: Map ``TypedNode``.
        :rtype: TypedNode
        """
        val_schemas: list[dict[str, Any]] = []
        if isinstance(node.get("additionalProperties"), dict):
            val_schemas.append(self._validate_node(node["additionalProperties"]))
        val_schemas.extend(self._validate_node(pat_schema) for pat_schema in node.get("patternProperties", {}).values())
        val_schema = _merge_property_definitions(val_schemas)
        val_node = self._build_field_node(
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

    def _build_array(
        self,
        node: dict[str, Any],
        ctx: _StrictBuildContext,
        meta: _StrictNodeMeta,
    ) -> TypedNode:
        """Build a list ``TypedNode`` (homogeneous array) or struct ``TypedNode`` (closed tuple ``prefixItems``).

        :param node: Array JSON Schema node dictionary.
        :type node: dict[str, Any]
        :param ctx: Active strict build context.
        :type ctx: _StrictBuildContext
        :param meta: Node metadata payload.
        :type meta: _StrictNodeMeta
        :return: Array or tuple-struct ``TypedNode``.
        :rtype: TypedNode
        """
        if "prefixItems" in node:
            if node.get("items") not in (False, None):
                msg = "Open tuple combining prefixItems with schema-valued items is not supported"
                self._raise_err("E5", msg)
            min_items = node.get("minItems", 0)
            fields: list[Field] = []
            for idx, item_schema in enumerate(node["prefixItems"]):
                item_node = self._build_field_node(
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

        validated_items = self._validate_node(node.get("items"))
        item_node = self._build_field_node(validated_items, ctx, (*meta.path, "items"), required_in_parent=True)
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

    def _build_union_type(
        self,
        node: dict[str, Any],
        ctx: _StrictBuildContext,
        meta: _StrictNodeMeta,
    ) -> TypedNode:
        """Resolve an ``anyOf`` or ``oneOf`` combinator node into a ``TypedNode``.

        :param node: Union JSON Schema node dictionary.
        :type node: dict[str, Any]
        :param ctx: Active strict build context.
        :type ctx: _StrictBuildContext
        :param meta: Node metadata payload.
        :type meta: _StrictNodeMeta
        :return: Unified ``TypedNode``.
        :rtype: TypedNode
        """
        kw = "anyOf" if "anyOf" in node else "oneOf"
        non_null = [self._validate_node(b) for b in node[kw] if not (isinstance(b, Mapping) and _is_null_branch(b))]
        if not non_null:
            msg = f"Standalone null-only {kw} combinator cannot be converted to a PyArrow field"
            self._raise_err("E11", msg)
        if len(non_null) == 1:
            return self._build_type(non_null[0], ctx, meta)

        obj_branches: list[dict[str, Any]] = []
        scalar_branches: list[dict[str, Any]] = []
        for branch in non_null:
            kind = self._classify_node_kind(branch, ctx.defs)
            if kind == "object":
                obj_branches.append(branch)
            elif kind == "scalar":
                scalar_branches.append(branch)
            else:
                msg = f"Incompatible branch shape {kind!r} in {kw} combinator"
                self._raise_err("E8", msg)

        if scalar_branches and not obj_branches:
            u_type, u_hints, u_label = self._resolve_and_unify_scalar_branches(scalar_branches, ctx, meta.path)
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
            return self._build_merged_object(node, ctx, meta)

        return self._build_merged_object(
            node,
            ctx,
            meta,
            force_all_nullable=True,
            scalar_branches=scalar_branches,
        )

    def _build_ref_type(
        self,
        validated: dict[str, Any],
        ctx: _StrictBuildContext,
        meta: _StrictNodeMeta,
    ) -> TypedNode:
        """Resolve and build a ``$ref`` schema node with recursion depth tracking.

        :param validated: Validated schema dictionary containing ``$ref``.
        :type validated: dict[str, Any]
        :param ctx: Active strict build context.
        :type ctx: _StrictBuildContext
        :param meta: Node metadata payload.
        :type meta: _StrictNodeMeta
        :return: Resolved ``TypedNode``.
        :rtype: TypedNode
        """
        ref_name, target = self._resolve_ref(validated["$ref"], ctx.defs)
        count = ctx.ref_counts.get(ref_name, 0)
        if count >= 1:
            self._check_no_forbidden_types_in_recursive_cycle(ref_name, target, ctx.defs)
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
            result = self._build_type(target, next_ctx, meta)
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
        return self._build_type(merged, next_ctx, meta)

    def _build_type(
        self,
        node: object,
        ctx: _StrictBuildContext,
        meta: _StrictNodeMeta,
    ) -> TypedNode:
        """Convert any JSON Schema node into a strict ``TypedNode``.

        :param node: Raw JSON Schema node.
        :type node: object
        :param ctx: Active strict build context.
        :type ctx: _StrictBuildContext
        :param meta: Node metadata payload.
        :type meta: _StrictNodeMeta
        :return: Lowered ``TypedNode``.
        :rtype: TypedNode
        """
        validated = self._validate_node(node)
        if _is_null_branch(validated):
            msg = "Standalone null-only schema cannot be converted to a PyArrow field"
            self._raise_err("E11", msg)

        if "$ref" in validated:
            return self._build_ref_type(validated, ctx, meta)

        if (
            ("anyOf" in validated or "oneOf" in validated)
            and "properties" not in validated
            and "allOf" not in validated
        ):
            return self._build_union_type(validated, ctx, meta)

        kind = self._classify_node_kind(validated, ctx.defs)
        if kind == "object":
            return self._build_merged_object(validated, ctx, meta)
        if kind == "map":
            return self._build_map(validated, ctx, meta)
        if kind == "array":
            return self._build_array(validated, ctx, meta)
        s_type, s_hints, s_label = self._resolve_scalar(validated)
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

    def _build_field_node(
        self,
        node: object,
        ctx: _StrictBuildContext,
        path: tuple[str | int, ...],
        *,
        required_in_parent: bool,
    ) -> TypedNode:
        """Build a ``TypedNode`` with nullability, description, extensions, and constraints for a field or item.

        :param node: Raw field or item JSON Schema node.
        :type node: object
        :param ctx: Active strict build context.
        :type ctx: _StrictBuildContext
        :param path: JSON pointer path tuple.
        :type path: tuple[str | int, ...]
        :param required_in_parent: Whether the field is required in its parent object.
        :type required_in_parent: bool
        :return: Lowered ``TypedNode``.
        :rtype: TypedNode
        """
        validated = self._validate_node(node)
        nullable = (not required_in_parent) or self._has_null_possibility(validated, ctx.defs)
        meta = self._build_node_meta(validated, ctx.defs, path, nullable=nullable)
        return self._build_type(validated, ctx, meta)
