"""Scalar type, format, and content-encoding resolution for strict JSON Schema."""

from collections.abc import Mapping
from typing import Any, Final

from frozendict import frozendict

from cdm_data_loaders.converters.core.errors import ConversionError, raise_spec_error
from cdm_data_loaders.converters.core.inference import unify_scalar_specs
from cdm_data_loaders.converters.core.ir import (
    SPEC_BINARY,
    SPEC_BOOL,
    SPEC_DATE,
    SPEC_FLOAT64,
    SPEC_INT64,
    SPEC_STRING,
    NodeHints,
    NodeType,
)
from cdm_data_loaders.converters.sources.json_schema.strict_refs import is_null_branch

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


def scalar_for_type_name(
    type_name: str,
    node: Mapping[str, Any],
    *,
    error_type: type[ConversionError],
) -> tuple[NodeType, NodeHints, str]:
    """Map a JSON Schema scalar type name and format or encoding facets to IR scalar facts.

    :param type_name: JSON Schema scalar type name.
    :type type_name: str
    :param node: JSON Schema node mapping carrying ``format`` or ``contentEncoding``.
    :type node: Mapping[str, Any]
    :param error_type: Fallback error class.
    :type error_type: type[ConversionError]
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
    raise error_type(msg)


def infer_literal_type_name(value: object, *, error_type: type[ConversionError]) -> str:
    """Infer the JSON Schema scalar type name of a Python literal inside ``const`` or ``enum``.

    :param value: Python scalar literal.
    :type value: object
    :param error_type: Fallback error class.
    :type error_type: type[ConversionError]
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
    raise error_type(msg)


def resolve_scalar(
    node: Mapping[str, Any],
    *,
    error_type: type[ConversionError],
    error_by_code: Mapping[str, type[ConversionError]],
) -> tuple[NodeType, NodeHints, str]:
    """Resolve a scalar JSON Schema node to ``(NodeType, NodeHints, jsonschema_type)``.

    :param node: Scalar JSON Schema node mapping.
    :type node: Mapping[str, Any]
    :param error_type: Fallback error class.
    :type error_type: type[ConversionError]
    :param error_by_code: Mapping from specification error code to exception class.
    :type error_by_code: Mapping[str, type[ConversionError]]
    :return: Tuple of ``(NodeType, NodeHints, jsonschema_type)``.
    :rtype: tuple[NodeType, NodeHints, str]
    """
    if is_null_branch(node):
        msg = "Standalone null-only schema cannot be converted to a PyArrow field"
        raise_spec_error("E11", msg, error_type=error_type, error_by_code=error_by_code)
    if "const" in node and "type" not in node:
        return scalar_for_type_name(
            infer_literal_type_name(node["const"], error_type=error_type),
            {},
            error_type=error_type,
        )
    if "enum" in node and "type" not in node:
        non_null_vals = [v for v in node["enum"] if v is not None]
        types_seen: list[str] = []
        for val in non_null_vals:
            t_name = infer_literal_type_name(val, error_type=error_type)
            if t_name not in types_seen:
                types_seen.append(t_name)
        return unify_scalar_specs([scalar_for_type_name(t_name, {}, error_type=error_type) for t_name in types_seen])

    node_type = node.get("type")
    if isinstance(node_type, list):
        non_null = [t for t in node_type if t != "null"]
        if len(non_null) == 1:
            return resolve_scalar(
                {**node, "type": non_null[0]},
                error_type=error_type,
                error_by_code=error_by_code,
            )
        if any(t in {"object", "array"} for t in non_null):
            msg = f"Incompatible branch types in multi-type union: {node_type!r}"
            raise_spec_error("E8", msg, error_type=error_type, error_by_code=error_by_code)
        return unify_scalar_specs([scalar_for_type_name(t, node, error_type=error_type) for t in non_null])
    if isinstance(node_type, str):
        return scalar_for_type_name(node_type, node, error_type=error_type)
    msg = f"Cannot resolve scalar type for schema node: {node!r}"
    raise error_type(msg)
