"""Convert JSON Schema through the shared reader and PyArrow emitter."""

import base64
import copy
import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime, time
from decimal import Decimal
from typing import Any, ClassVar, Final

import pyarrow as pa
from frozendict import frozendict
from pydantic import Field, field_validator

from cdm_data_loaders.converters.core.errors import ConversionError
from cdm_data_loaders.converters.core.guards import require_object_root, require_schema_keyword
from cdm_data_loaders.converters.core.io import JsonSchemaConverterBase, coerce_extra_metadata_keywords
from cdm_data_loaders.converters.emitters.pyarrow import DEFAULT_FORMAT_MAP, PyArrowEmitter, merge_format_map
from cdm_data_loaders.converters.readers.json_schema import JsonSchemaReader

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

_NON_EXTENDABLE_SCALAR_MAP: Final[dict[str, tuple[pa.DataType, str]]] = {
    "boolean": (pa.bool_(), "boolean"),
    "integer": (pa.int64(), "integer"),
    "number": (pa.float64(), "number"),
}


class JSONSchemaToPyArrowError(ConversionError):
    """Raised when a JSON Schema construct cannot be converted.

    Subclasses raised by :func:`generate_pyarrow_schema` carry a stable specification ``code`` and a short
    ``title``; their docstrings state why the construct is rejected. The converter specification document is
    generated from these attributes.
    """

    code: ClassVar[str] = ""
    title: ClassVar[str] = ""


class InvalidJSONSchemaError(JSONSchemaToPyArrowError):
    """Raised when the input document is not a valid JSON Schema."""


class UnsupportedDialectError(InvalidJSONSchemaError):
    """Only JSON Schema Draft 2020-12 and the KBase Draft 2020-12 XSV metaschema are supported."""

    code = "E1"
    title = "Missing or unsupported `$schema` dialect"


class NonStructRootError(JSONSchemaToPyArrowError):
    """A `pa.Schema` needs top-level columns, so the root must resolve to a fixed-property object."""

    code = "E2"
    title = "Non-struct top-level schema"


class EmptyObjectError(JSONSchemaToPyArrowError):
    """Parquet cannot store a `pa.struct([])` with no child fields."""

    code = "E3"
    title = "Empty object or tuple (zero fields)"


class UnconstrainedSchemaError(JSONSchemaToPyArrowError):
    """An untyped JSON value cannot be assigned a concrete Arrow `DataType`."""

    code = "E4"
    title = "Unconstrained or boolean schema"


class OpenTupleError(JSONSchemaToPyArrowError):
    """A field cannot be both a positional `pa.struct` and an unbounded `pa.list_`."""

    code = "E5"
    title = "Open tuple (`prefixItems` with schema-valued `items`)"


class MixedPropertiesAndMapError(JSONSchemaToPyArrowError):
    """An object cannot be both a fixed-field `pa.struct` and a dynamic `pa.map_`."""

    code = "E6"
    title = "Fixed `properties` mixed with a dynamic map"


class NegationError(JSONSchemaToPyArrowError):
    """A complement type does not define a positive storage `DataType`."""

    code = "E7"
    title = "Negation (`not`)"


class IncompatibleBranchShapesError(JSONSchemaToPyArrowError):
    """Branches with different structural shapes (object vs array, array vs scalar, object vs map) cannot share one column."""

    code = "E8"
    title = "Incompatible structural union branches"


class UnsupportedRefError(JSONSchemaToPyArrowError):
    """Every `$ref` must resolve to a local `#/$defs/...` definition; `$dynamicRef` and `$recursiveRef` are not supported."""

    code = "E9"
    title = "Unresolvable or unsupported reference"


class RecursiveNonJsonTypeError(JSONSchemaToPyArrowError):
    """Truncated recursive subtrees are serialized with `json.dumps`, which cannot encode `datetime.date` or `bytes`."""

    code = "E10"
    title = "Non-JSON-serializable type in a recursive `$ref` cycle"


class NullOnlySchemaError(JSONSchemaToPyArrowError):
    """Parquet has no physical storage type for a `pa.null()` column."""

    code = "E11"
    title = "Null-only schema"


class ValueFieldCollisionError(JSONSchemaToPyArrowError):
    """The synthesized `_value` field of a scalar + object union would collide with a declared `_value` property."""

    code = "E12"
    title = "`_value` collision in a scalar + object union"


JSONSCHEMA_TO_PYARROW_ERRORS: Final[tuple[type[JSONSchemaToPyArrowError], ...]] = (
    UnsupportedDialectError,
    NonStructRootError,
    EmptyObjectError,
    UnconstrainedSchemaError,
    OpenTupleError,
    MixedPropertiesAndMapError,
    NegationError,
    IncompatibleBranchShapesError,
    UnsupportedRefError,
    RecursiveNonJsonTypeError,
    NullOnlySchemaError,
    ValueFieldCollisionError,
)


class JSONSchemaToPyArrow(JsonSchemaConverterBase[pa.Schema]):
    """Read dereferenced JSON Schema and emit PyArrow schemas and empty tables."""

    format_map: frozendict[str, pa.DataType] = Field(default_factory=lambda: DEFAULT_FORMAT_MAP)
    treat_unknown_as_string: bool = True
    emit_unions_as_json: bool = False
    extra_metadata_keywords: frozenset[str] = Field(default_factory=frozenset)
    max_nesting: int = Field(default=10, ge=1)

    @field_validator("format_map", mode="before")
    @classmethod
    def _merge_with_builtin_format_map(cls, value: object) -> object:
        """Delegate nonempty format overrides to the emitter."""
        return merge_format_map(value) if value else value

    @field_validator("extra_metadata_keywords", mode="before")
    @classmethod
    def _coerce_extra_metadata_keywords(cls, value: object) -> object:
        """Normalize supported metadata keyword collections."""
        return coerce_extra_metadata_keywords(value)

    def _reader(self) -> JsonSchemaReader:
        """Configure reader diagnostics for this public facade."""
        return JsonSchemaReader(
            self.extension_registry,
            error_type=JSONSchemaToPyArrowError,
            converter_name="JSONSchemaToPyArrow",
            dereference_function="cdm_data_loaders.utils.jsonschema.dereferencer.dereference_schema",
        )

    def _emitter(self) -> PyArrowEmitter:
        """Pass target policy to the emitter."""
        return PyArrowEmitter(
            format_map=self.format_map,
            treat_unknown_as_string=self.treat_unknown_as_string,
            emit_unions_as_json=self.emit_unions_as_json,
            extra_metadata_keywords=self.extra_metadata_keywords,
            max_nesting=self.max_nesting,
        )

    def convert(self, schema: dict[str, Any]) -> pa.Schema:
        """Convert a dereferenced JSON Schema object document to an Arrow schema.

        :param schema: the JSON Schema, which must declare ``$schema`` and have an object root
        :type schema: dict[str, Any]
        :return: the Arrow schema
        :rtype: pa.Schema
        :raises InvalidJSONSchemaError: if ``$schema`` is missing
        :raises JSONSchemaToPyArrowError: if the root is not an object or a construct cannot be converted
        """
        require_schema_keyword(schema, InvalidJSONSchemaError, converter_name="JSONSchemaToPyArrow")
        require_object_root(schema, JSONSchemaToPyArrowError, target="a struct schema")
        try:
            return self._emitter().emit(self._reader().read(schema))
        except ConversionError as error:
            raise JSONSchemaToPyArrowError(str(error)) from error

    def convert_to_table(self, schema: dict[str, Any]) -> pa.Table:
        """Convert a dereferenced JSON Schema object document to an empty Arrow table.

        :param schema: the JSON Schema, which must declare ``$schema`` and have an object root
        :type schema: dict[str, Any]
        :return: a zero-row table with the converted schema
        :rtype: pa.Table
        :raises InvalidJSONSchemaError: if ``$schema`` is missing
        :raises JSONSchemaToPyArrowError: if the root is not an object or a construct cannot be converted
        """
        return self.convert(schema).empty_table()


@dataclass(frozen=True, slots=True)
class _SchemaBuildContext:
    """Immutable traversal context for JSON Schema to PyArrow schema generation."""

    defs: Mapping[str, Any]
    ref_counts: Mapping[str, int]
    max_recursion_depth: int

    def with_ref(self, ref_name: str) -> "_SchemaBuildContext":
        """Return a new context with the recursion counter for ``ref_name`` incremented by 1."""
        return _SchemaBuildContext(
            defs=self.defs,
            ref_counts={**self.ref_counts, ref_name: self.ref_counts.get(ref_name, 0) + 1},
            max_recursion_depth=self.max_recursion_depth,
        )


def _resolve_ref(ref: object, defs: Mapping[str, Any]) -> tuple[str, dict[str, Any]]:
    """Resolve a local ``#/$defs/<name>`` reference pointer against ``defs``."""
    prefix = "#/$defs/"
    if not isinstance(ref, str) or not ref.startswith(prefix):
        msg = f"Unresolvable or unsupported non-local $ref: {ref!r}"
        raise UnsupportedRefError(msg)
    name = ref[len(prefix) :]
    if name not in defs or not isinstance(defs[name], dict):
        msg = f"Unresolvable $ref {ref!r}: definition {name!r} does not exist in $defs"
        raise UnsupportedRefError(msg)
    return name, defs[name]


def _validate_node_constraints(node: object) -> dict[str, Any]:
    """Validate that ``node`` is a constrained JSON Schema object without unsupported keywords."""
    if isinstance(node, bool) or not isinstance(node, dict):
        msg = f"Unconstrained or boolean schema node is not supported: {node!r}"
        raise UnconstrainedSchemaError(msg)
    if "not" in node:
        msg = "Unsupported JSON Schema keyword 'not': negation schemas cannot be converted to PyArrow"
        raise NegationError(msg)
    if "$dynamicRef" in node or "$recursiveRef" in node:
        msg = "Unsupported dynamic reference keyword '$dynamicRef' in JSON Schema"
        raise UnsupportedRefError(msg)

    has_constraint = any(k in node for k in _CONSTRAINING_KEYWORDS) or isinstance(
        node.get("additionalProperties"), dict
    )
    if not has_constraint:
        msg = f"Unconstrained schema without type or structural keywords is not supported: {node!r}"
        raise UnconstrainedSchemaError(msg)

    has_fixed_props = bool(node.get("properties")) or any(
        k in node for k in ("allOf", "anyOf", "oneOf", "dependentSchemas", "then", "else")
    )
    has_map_schema = isinstance(node.get("additionalProperties"), dict) or bool(node.get("patternProperties"))
    if has_fixed_props and has_map_schema:
        msg = "Mixed fixed properties and dynamic map (additionalProperties/patternProperties) is not supported"
        raise MixedPropertiesAndMapError(msg)
    return node


def _check_no_forbidden_types_in_recursive_cycle(
    ref_name: str,
    target: Mapping[str, Any],
    defs: Mapping[str, Any],
) -> None:
    """Raise ``JSONSchemaToPyArrowError`` if a recursive ``$defs`` cycle contains non-JSON-serializable types."""
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
                raise RecursiveNonJsonTypeError(msg)
            if "$ref" in current and isinstance(current["$ref"], str):
                child_ref_name, child_target = _resolve_ref(current["$ref"], defs)
                if child_ref_name not in visited_refs:
                    visited_refs.add(child_ref_name)
                    stack.append(child_target)
            stack.extend(v for k, v in current.items() if k != "$defs")
        elif isinstance(current, list):
            stack.extend(current)


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


def _has_null_possibility(node: Mapping[str, Any], defs: Mapping[str, Any]) -> bool:
    """Return True if ``node`` or any of its referenced/combinator branches allows ``null``."""
    if "$ref" in node:
        _, target = _resolve_ref(node["$ref"], defs)
        if _has_null_possibility(target, defs):
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
                if isinstance(branch, Mapping) and (_is_null_branch(branch) or _has_null_possibility(branch, defs)):
                    return True
    return False


def _resolve_string_scalar(node: Mapping[str, Any]) -> tuple[pa.DataType, str]:
    """Resolve a ``type: 'string'`` node with optional ``format`` or ``contentEncoding``."""
    content_enc = node.get("contentEncoding")
    if content_enc in {"base64", "base16"}:
        return pa.binary(), f"string ({content_enc})"
    fmt = node.get("format")
    if fmt == "date":
        return pa.date32(), "string (date)"
    if isinstance(fmt, str) and fmt:
        return pa.string(), f"string ({fmt})"
    return pa.string(), "string"


def _scalar_for_type_name(type_name: str, node: Mapping[str, Any]) -> tuple[pa.DataType, str]:
    """Map a single JSON Schema scalar type name and format/encoding facets to ``(pa.DataType, label)``."""
    if type_name in _NON_EXTENDABLE_SCALAR_MAP:
        return _NON_EXTENDABLE_SCALAR_MAP[type_name]
    if type_name == "string":
        return _resolve_string_scalar(node)
    msg = f"Unsupported or non-scalar type in scalar context: {type_name!r}"
    raise JSONSchemaToPyArrowError(msg)


def _unify_scalar_types(branches: list[tuple[pa.DataType, str]]) -> tuple[pa.DataType, str]:
    """Unify multiple scalar ``(pa.DataType, jsonschema_type)`` pairs according to the scalar union rules."""
    seen_labels: list[str] = []
    for _, label in branches:
        if label not in seen_labels:
            seen_labels.append(label)
    joined_label = " | ".join(seen_labels) if len(seen_labels) > 1 else seen_labels[0]

    first_type = branches[0][0]
    if all(dt == first_type for dt, _ in branches[1:]):
        return first_type, joined_label
    base_types = {label.split(" ", maxsplit=1)[0] for _, label in branches}
    if base_types <= {"integer", "number"}:
        return pa.float64(), joined_label
    return pa.string(), joined_label


def _infer_literal_type_name(value: object) -> str:
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
    raise JSONSchemaToPyArrowError(msg)


def _resolve_scalar(node: Mapping[str, Any]) -> tuple[pa.DataType, str]:
    """Resolve a scalar JSON Schema node to ``(pa.DataType, jsonschema_type)``."""
    if _is_null_branch(node):
        msg = "Standalone null-only schema cannot be converted to a PyArrow field"
        raise NullOnlySchemaError(msg)
    if "const" in node and "type" not in node:
        return _scalar_for_type_name(_infer_literal_type_name(node["const"]), {})
    if "enum" in node and "type" not in node:
        non_null_vals = [v for v in node["enum"] if v is not None]
        types_seen: list[str] = []
        for val in non_null_vals:
            t_name = _infer_literal_type_name(val)
            if t_name not in types_seen:
                types_seen.append(t_name)
        return _unify_scalar_types([_scalar_for_type_name(t_name, {}) for t_name in types_seen])

    node_type = node.get("type")
    if isinstance(node_type, list):
        non_null = [t for t in node_type if t != "null"]
        if len(non_null) == 1:
            single_node = {**node, "type": non_null[0]}
            return _resolve_scalar(single_node)
        if any(t in {"object", "array"} for t in non_null):
            msg = f"Incompatible branch types in multi-type union: {node_type!r}"
            raise IncompatibleBranchShapesError(msg)
        return _unify_scalar_types([_scalar_for_type_name(t, node) for t in non_null])
    if isinstance(node_type, str):
        return _scalar_for_type_name(node_type, node)
    msg = f"Cannot resolve scalar type for schema node: {node!r}"
    raise JSONSchemaToPyArrowError(msg)


def resolve_scalar_type(node: Mapping[str, Any]) -> tuple[pa.DataType, str]:
    """Map a scalar JSON Schema node to its PyArrow type and ``jsonschema_type`` label.

    :param node: scalar schema node (``type``, ``format``, ``contentEncoding``, ``enum`` or ``const``)
    :type node: Mapping[str, Any]
    :return: ``(pa.DataType, jsonschema_type)``
    :rtype: tuple[pa.DataType, str]
    :raises JSONSchemaToPyArrowError: if ``node`` is not a convertible scalar schema
    """
    return _resolve_scalar(node)


def _classify_resolved_node_kind(node: Mapping[str, Any]) -> str:
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
            raise IncompatibleBranchShapesError(msg)
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


def _classify_node_kind(node: object, defs: Mapping[str, Any]) -> str:
    """Validate ``node`` and return ``'null'``, ``'object'``, ``'array'``, ``'map'``, ``'scalar'``, or ``'union'``."""
    validated = _validate_node_constraints(node)
    if _is_null_branch(validated):
        return "null"
    if "$ref" in validated:
        _, target = _resolve_ref(validated["$ref"], defs)
        if len(validated) == 1:
            return _classify_node_kind(target, defs)
        merged = {**target, **{k: v for k, v in validated.items() if k != "$ref"}}
        return _classify_node_kind(merged, defs)
    return _classify_resolved_node_kind(validated)


def _get_inherited_description(node: Mapping[str, Any], defs: Mapping[str, Any]) -> str | None:
    """Extract the local or ``$ref``-inherited ``description`` string for a schema node."""
    if isinstance(node.get("description"), str) and node["description"]:
        return node["description"]
    if "$ref" in node:
        _, target = _resolve_ref(node["$ref"], defs)
        return _get_inherited_description(target, defs)
    for kw in ("anyOf", "oneOf"):
        if kw in node and isinstance(node[kw], list):
            non_null = [b for b in node[kw] if isinstance(b, Mapping) and not _is_null_branch(b)]
            if len(non_null) == 1 and "$ref" in non_null[0]:
                return _get_inherited_description(non_null[0], defs)
    return None


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


def _extract_choice_branches_into_object(
    branches: list[Any],
    kw: str,
    ctx: _SchemaBuildContext,
    raw_props: dict[str, list[tuple[dict[str, Any], _SchemaBuildContext]]],
    scalar_branches: list[dict[str, Any]],
) -> set[str]:
    """Extract properties, common required fields, and scalar branches from an ``anyOf``/``oneOf`` list."""
    non_null = [b for b in branches if not (isinstance(b, Mapping) and _is_null_branch(b))]
    if not non_null:
        msg = f"Standalone null-only {kw} combinator cannot be converted to a PyArrow type"
        raise NullOnlySchemaError(msg)
    branch_req_sets: list[set[str]] = []
    has_scalar = False
    for branch in non_null:
        branch_kind = _classify_node_kind(branch, ctx.defs)
        if branch_kind == "scalar":
            has_scalar = True
            scalar_branches.append(branch)
        elif branch_kind == "object":
            b_props, b_req, b_scalars = _extract_object_parts_with_refs(branch, ctx)
            for prop_name, (prop_schema, prop_ctx) in b_props.items():
                raw_props.setdefault(prop_name, []).append((prop_schema, prop_ctx))
            branch_req_sets.append(b_req)
            scalar_branches.extend(b_scalars)
        else:
            msg = f"Incompatible branch shape {branch_kind!r} in {kw} combinator"
            raise IncompatibleBranchShapesError(msg)
    if branch_req_sets and not has_scalar:
        return set.intersection(*branch_req_sets)
    return set()


def _extract_object_parts_with_refs(
    node: dict[str, Any],
    ctx: _SchemaBuildContext,
) -> tuple[dict[str, tuple[dict[str, Any], _SchemaBuildContext]], set[str], list[dict[str, Any]]]:
    """Extract ordered merged property definitions, required set, and scalar union branches for an object node."""
    validated = _validate_node_constraints(node)
    if "$ref" in validated:
        ref_name, target = _resolve_ref(validated["$ref"], ctx.defs)
        if ctx.ref_counts.get(ref_name, 0) >= 1:
            _check_no_forbidden_types_in_recursive_cycle(ref_name, target, ctx.defs)
        next_ctx = ctx.with_ref(ref_name)
        merged_node = {**copy.deepcopy(target), **{k: v for k, v in validated.items() if k != "$ref"}}
        return _extract_object_parts_with_refs(merged_node, next_ctx)

    raw_props: dict[str, list[tuple[dict[str, Any], _SchemaBuildContext]]] = {}
    required: set[str] = set(validated.get("required", []))
    scalar_branches: list[dict[str, Any]] = []

    for sub in validated.get("allOf", []):
        sub_props, sub_req, sub_scalars = _extract_object_parts_with_refs(sub, ctx)
        for prop_name, item in sub_props.items():
            raw_props.setdefault(prop_name, []).append(item)
        required |= sub_req
        scalar_branches.extend(sub_scalars)

    for prop_name, prop_schema in validated.get("properties", {}).items():
        raw_props.setdefault(prop_name, []).append((_validate_node_constraints(prop_schema), ctx))

    for kw in ("anyOf", "oneOf"):
        if kw in validated:
            required |= _extract_choice_branches_into_object(validated[kw], kw, ctx, raw_props, scalar_branches)

    for dep_schema in validated.get("dependentSchemas", {}).values():
        d_props, _, _ = _extract_object_parts_with_refs(dep_schema, ctx)
        for prop_name, item in d_props.items():
            raw_props.setdefault(prop_name, []).append(item)

    for kw in ("then", "else"):
        if kw in validated:
            c_props, _, _ = _extract_object_parts_with_refs(validated[kw], ctx)
            for prop_name, item in c_props.items():
                raw_props.setdefault(prop_name, []).append(item)

    merged_props: dict[str, tuple[dict[str, Any], _SchemaBuildContext]] = {
        prop_name: (_merge_property_definitions([item[0] for item in items]), items[0][1])
        for prop_name, items in raw_props.items()
    }
    return merged_props, required, scalar_branches


def _build_merged_object(
    node: dict[str, Any],
    ctx: _SchemaBuildContext,
    *,
    force_all_nullable: bool = False,
    scalar_branches: list[dict[str, Any]] | None = None,
) -> pa.StructType:
    """Build a ``pa.StructType`` from an object schema and its combinator branches."""
    props_by_name, required, extracted_scalars = _extract_object_parts_with_refs(node, ctx)
    all_scalars = list(scalar_branches or [])
    for scalar_node in extracted_scalars:
        if scalar_node not in all_scalars:
            all_scalars.append(scalar_node)

    if all_scalars:
        force_all_nullable = True
        if "_value" in props_by_name:
            msg = "Property name '_value' in object branch collides with synthesized '_value' field in scalar+object union"
            raise ValueFieldCollisionError(msg)

    fields: list[pa.Field] = []
    for prop_name, (merged_prop, prop_ctx) in props_by_name.items():
        is_req = (prop_name in required) and not force_all_nullable
        fields.append(_build_field(prop_name, merged_prop, prop_ctx, required_in_parent=is_req))

    if all_scalars:
        resolved_scalars: list[tuple[pa.DataType, str]] = []
        for scalar_branch in all_scalars:
            s_type, s_label = _build_type(scalar_branch, ctx)
            if s_label is None:
                msg = f"Expected scalar branch with jsonschema_type label, got {scalar_branch!r}"
                raise JSONSchemaToPyArrowError(msg)
            resolved_scalars.append((s_type, s_label))
        val_type, val_label = _unify_scalar_types(resolved_scalars)
        fields.append(pa.field("_value", val_type, nullable=True, metadata={"jsonschema_type": val_label}))

    if not fields:
        msg = "Empty object schema with no properties cannot be converted to a PyArrow struct"
        raise EmptyObjectError(msg)
    return pa.struct(fields)


def _build_map(node: dict[str, Any], ctx: _SchemaBuildContext) -> pa.MapType:
    """Build a ``pa.MapType`` from a dynamic map object schema (``additionalProperties`` / ``patternProperties``)."""
    val_schemas: list[dict[str, Any]] = []
    if isinstance(node.get("additionalProperties"), dict):
        val_schemas.append(_validate_node_constraints(node["additionalProperties"]))
    val_schemas.extend(
        _validate_node_constraints(pat_schema) for pat_schema in node.get("patternProperties", {}).values()
    )
    val_schema = _merge_property_definitions(val_schemas)
    val_field = _build_field("value", val_schema, ctx, required_in_parent=True)
    return pa.map_(pa.string(), val_field)


def _build_array(node: dict[str, Any], ctx: _SchemaBuildContext) -> pa.DataType:
    """Build a ``pa.ListType`` (homogeneous array) or ``pa.StructType`` (closed tuple ``prefixItems``)."""
    if "prefixItems" in node:
        if node.get("items") not in (False, None):
            msg = "Open tuple combining prefixItems with schema-valued items is not supported"
            raise OpenTupleError(msg)
        min_items = node.get("minItems", 0)
        fields = [
            _build_field(f"item_{idx}", item_schema, ctx, required_in_parent=idx < min_items)
            for idx, item_schema in enumerate(node["prefixItems"])
        ]
        if not fields:
            msg = "Empty prefixItems tuple cannot be converted to a PyArrow struct"
            raise EmptyObjectError(msg)
        return pa.struct(fields)

    items_schema = node.get("items")
    validated_items = _validate_node_constraints(items_schema)
    item_field = _build_field("item", validated_items, ctx, required_in_parent=True)
    return pa.list_(item_field)


def _build_union_type(node: dict[str, Any], ctx: _SchemaBuildContext) -> tuple[pa.DataType, str | None]:
    """Resolve an ``anyOf`` or ``oneOf`` combinator node to ``(pa.DataType, jsonschema_type)``."""
    kw = "anyOf" if "anyOf" in node else "oneOf"
    non_null = [_validate_node_constraints(b) for b in node[kw] if not (isinstance(b, Mapping) and _is_null_branch(b))]
    if not non_null:
        msg = f"Standalone null-only {kw} combinator cannot be converted to a PyArrow field"
        raise NullOnlySchemaError(msg)
    if len(non_null) == 1:
        return _build_type(non_null[0], ctx)

    obj_branches: list[dict[str, Any]] = []
    scalar_branches: list[dict[str, Any]] = []
    for branch in non_null:
        kind = _classify_node_kind(branch, ctx.defs)
        if kind == "object":
            obj_branches.append(branch)
        elif kind == "scalar":
            scalar_branches.append(branch)
        else:
            msg = f"Incompatible branch shape {kind!r} in {kw} combinator"
            raise IncompatibleBranchShapesError(msg)

    if scalar_branches and not obj_branches:
        resolved_scalars: list[tuple[pa.DataType, str]] = []
        for scalar_branch in scalar_branches:
            s_type, s_label = _build_type(scalar_branch, ctx)
            if s_label is None:
                msg = f"Expected scalar branch with jsonschema_type label, got {scalar_branch!r}"
                raise JSONSchemaToPyArrowError(msg)
            resolved_scalars.append((s_type, s_label))
        return _unify_scalar_types(resolved_scalars)

    if obj_branches and not scalar_branches:
        return _build_merged_object(node, ctx), None

    return (
        _build_merged_object(
            node,
            ctx,
            force_all_nullable=True,
            scalar_branches=scalar_branches,
        ),
        None,
    )


def _build_ref_type(validated: dict[str, Any], ctx: _SchemaBuildContext) -> tuple[pa.DataType, str | None]:
    """Resolve and build a ``$ref`` schema node with recursion depth tracking."""
    ref_name, target = _resolve_ref(validated["$ref"], ctx.defs)
    count = ctx.ref_counts.get(ref_name, 0)
    if count >= 1:
        _check_no_forbidden_types_in_recursive_cycle(ref_name, target, ctx.defs)
    if count >= ctx.max_recursion_depth:
        return pa.string(), None
    next_ctx = ctx.with_ref(ref_name)
    if len(validated) == 1:
        return _build_type(target, next_ctx)
    merged = {**copy.deepcopy(target), **{k: v for k, v in validated.items() if k not in {"$ref", "description"}}}
    return _build_type(merged, next_ctx)


def _build_type(node: object, ctx: _SchemaBuildContext) -> tuple[pa.DataType, str | None]:
    """Convert any JSON Schema node into ``(pa.DataType, jsonschema_type | None)``."""
    validated = _validate_node_constraints(node)
    if _is_null_branch(validated):
        msg = "Standalone null-only schema cannot be converted to a PyArrow field"
        raise NullOnlySchemaError(msg)

    if "$ref" in validated:
        return _build_ref_type(validated, ctx)

    if ("anyOf" in validated or "oneOf" in validated) and "properties" not in validated and "allOf" not in validated:
        return _build_union_type(validated, ctx)

    kind = _classify_node_kind(validated, ctx.defs)
    if kind == "object":
        return _build_merged_object(validated, ctx), None
    if kind == "map":
        return _build_map(validated, ctx), None
    if kind == "array":
        return _build_array(validated, ctx), None
    return _resolve_scalar(validated)


def _build_field(
    name: str,
    node: object,
    ctx: _SchemaBuildContext,
    *,
    required_in_parent: bool,
) -> pa.Field:
    """Build a ``pa.Field`` with nullability and metadata from a JSON Schema property/item node."""
    validated = _validate_node_constraints(node)
    nullable = (not required_in_parent) or _has_null_possibility(validated, ctx.defs)
    desc = _get_inherited_description(validated, ctx.defs)
    data_type, js_type = _build_type(validated, ctx)
    metadata: dict[str, str] = {}
    if desc:
        metadata["comment"] = desc
    if js_type:
        metadata["jsonschema_type"] = js_type
    return pa.field(name, data_type, nullable=nullable, metadata=metadata or None)


def generate_pyarrow_schema(
    schema: dict[str, Any],
    *,
    max_recursion_depth: int = DEFAULT_MAX_RECURSION_DEPTH,
) -> pa.Schema:
    """Generate a deterministic PyArrow Schema from a Draft 2020-12 JSON Schema.

    :param schema: JSON Schema Draft 2020-12 dictionary
    :type schema: dict[str, Any]
    :param max_recursion_depth: maximum expansion depth for recursive ``$defs`` references
    :type max_recursion_depth: int
    :return: PyArrow schema with top-level fields corresponding to root object properties
    :rtype: pa.Schema
    :raises JSONSchemaToPyArrowError: if ``schema`` uses an unsupported dialect or construct
    """
    if not isinstance(schema, dict) or schema.get("$schema") not in SUPPORTED_SCHEMA_DIALECTS:
        msg = f"Missing or unsupported $schema dialect: {schema.get('$schema') if isinstance(schema, dict) else None!r}; expected JSON Schema Draft 2020-12"
        raise UnsupportedDialectError(msg)

    defs = schema.get("$defs", {})
    if not isinstance(defs, Mapping):
        msg = "$defs must be a mapping of definition names to schema objects"
        raise JSONSchemaToPyArrowError(msg)

    root: dict[str, Any] = _validate_node_constraints(schema)
    ref_counts: dict[str, int] = {}
    if "$ref" in root and "properties" not in root:
        ref_name, target = _resolve_ref(root["$ref"], defs)
        ref_counts[ref_name] = 1
        root = _validate_node_constraints(target)

    root_kind = _classify_node_kind(root, defs)
    if root_kind == "map":
        msg = "Top-level root schema cannot be a dynamic map without fixed properties"
        raise NonStructRootError(msg)
    if root_kind != "object":
        msg = f"Top-level root schema must resolve to a fixed-property object, got {root_kind!r}"
        raise NonStructRootError(msg)

    ctx = _SchemaBuildContext(
        defs=defs,
        ref_counts=ref_counts,
        max_recursion_depth=max_recursion_depth,
    )
    struct_type = _build_merged_object(root, ctx)
    return pa.schema(list(struct_type))


def _parse_iso_timestamp(value: str, data_type: pa.TimestampType) -> datetime:
    """Parse an ISO-8601 timestamp string, attaching UTC when target type is tz-aware."""
    parsed_dt = datetime.fromisoformat(value)
    if parsed_dt.tzinfo is None and data_type.tz is not None:
        return parsed_dt.replace(tzinfo=UTC)
    return parsed_dt


def _decode_binary_scalar(value: str, metadata: dict[bytes, bytes] | None) -> bytes:
    """Decode a Base64 or Base16 (hex) string into raw bytes based on field metadata."""
    if metadata and b"jsonschema_type" in metadata and b"base16" in metadata[b"jsonschema_type"]:
        return bytes.fromhex(value)
    return base64.b64decode(value)


def _coerce_string_input_scalar(
    value: str,
    data_type: pa.DataType,
    metadata: dict[bytes, bytes] | None,
) -> Any:  # noqa: ANN401
    """Coerce a string value when the target PyArrow type is binary, temporal, or decimal."""
    if pa.types.is_binary(data_type):
        return _decode_binary_scalar(value, metadata)
    if pa.types.is_date(data_type):
        return date.fromisoformat(value)
    if pa.types.is_timestamp(data_type):
        return _parse_iso_timestamp(value, data_type)
    if pa.types.is_time(data_type):
        return time.fromisoformat(value)
    if pa.types.is_decimal(data_type):
        return Decimal(value)
    return value


def _coerce_scalar_for_pyarrow(
    value: Any,  # noqa: ANN401
    data_type: pa.DataType,
    metadata: dict[bytes, bytes] | None = None,
) -> Any:  # noqa: ANN401
    """Coerce a JSON-parsed value to match a PyArrow scalar type."""
    if isinstance(value, str):
        return _coerce_string_input_scalar(value, data_type, metadata)
    if pa.types.is_decimal(data_type) and isinstance(value, int | float) and not isinstance(value, bool):
        return Decimal(str(value))
    if pa.types.is_floating(data_type) and isinstance(value, int) and not isinstance(value, bool):
        return float(value)
    if pa.types.is_integer(data_type) and isinstance(value, float) and value.is_integer():
        return int(value)
    if pa.types.is_string(data_type) or pa.types.is_large_string(data_type):
        return json.dumps(value) if isinstance(value, Mapping | list | bool) else str(value)
    return value


def _coerce_struct_for_pyarrow(value: Any, data_type: pa.StructType) -> Any:  # noqa: ANN401
    """Coerce a mapping, closed tuple list, or scalar union value to match a PyArrow StructType."""
    if isinstance(value, Mapping):
        return {
            field.name: _coerce_value_for_pyarrow(value[field.name], field.type, field.metadata)
            for field in data_type
            if field.name in value
        }
    if isinstance(value, list) and len(data_type) > 0 and data_type[0].name == "item_0":
        return {
            field.name: _coerce_value_for_pyarrow(value[idx], field.type, field.metadata)
            for idx, field in enumerate(data_type)
            if idx < len(value) and value[idx] is not None
        }
    value_field = next((field for field in data_type if field.name == "_value"), None)
    if value_field is not None:
        return {"_value": _coerce_value_for_pyarrow(value, value_field.type, value_field.metadata)}
    return value


def _coerce_value_for_pyarrow(
    value: Any,  # noqa: ANN401
    data_type: pa.DataType,
    metadata: dict[bytes, bytes] | None = None,
) -> Any:  # noqa: ANN401
    """Recursively coerce a JSON-parsed value to match the target PyArrow DataType."""
    if value is None:
        return None
    if pa.types.is_struct(data_type):
        return _coerce_struct_for_pyarrow(value, data_type)
    if (pa.types.is_list(data_type) or pa.types.is_large_list(data_type)) and isinstance(value, list):
        item_field = data_type.value_field
        return [_coerce_value_for_pyarrow(item, item_field.type, item_field.metadata) for item in value]
    if pa.types.is_map(data_type) and isinstance(value, Mapping):
        key_field = data_type.key_field
        item_field = data_type.item_field
        return {
            _coerce_value_for_pyarrow(k, key_field.type, key_field.metadata): _coerce_value_for_pyarrow(
                v, item_field.type, item_field.metadata
            )
            for k, v in value.items()
        }
    return _coerce_scalar_for_pyarrow(value, data_type, metadata)


def coerce_record_for_pyarrow(
    record: dict[str, Any],
    schema: pa.Schema,
    *,
    max_recursion_depth: int | None = DEFAULT_MAX_RECURSION_DEPTH,
) -> dict[str, Any]:
    """Coerce a JSON-validated record dictionary to match a PyArrow schema.

    Only fields declared in ``schema`` are retained and coerced recursively so
    ``pyarrow.Table.from_pylist`` can construct a table matching ``schema``.

    :param record: JSON-parsed and schema-validated record dictionary
    :type record: dict[str, Any]
    :param schema: target PyArrow schema for the record's table
    :type schema: pa.Schema
    :param max_recursion_depth: optional recursion depth parameter accepted for caller compatibility
    :type max_recursion_depth: int | None
    :return: coerced record dictionary ready for ``ArrowListBuffer`` or ``pa.Table.from_pylist``
    :rtype: dict[str, Any]
    """
    _ = max_recursion_depth
    return {
        field.name: _coerce_value_for_pyarrow(record[field.name], field.type, field.metadata)
        for field in schema
        if field.name in record
    }


convert_json_record = coerce_record_for_pyarrow
