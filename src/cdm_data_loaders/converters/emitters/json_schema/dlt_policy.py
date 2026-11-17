"""dlt-sourced IR to JSON Schema emission policy."""

from copy import deepcopy
from typing import Any, Final

from frozendict import frozendict

from cdm_data_loaders.converters.core.errors import ConversionError
from cdm_data_loaders.converters.core.ir import TypedNode
from cdm_data_loaders.converters.core.ir_values import mutable_value
from cdm_data_loaders.converters.emitters.json_schema.common import closed_object_schema, nullable_schema

DLT_TYPES: Final[frozendict[str, dict[str, Any]]] = frozendict(
    {
        "text": {"type": "string"},
        "bigint": {"type": "integer"},
        "double": {"type": "number"},
        "decimal": {"type": "string", "pattern": r"^-?\d+(\.\d+)?$", "x-dlt": {"data_type": "decimal"}},
        "bool": {"type": "boolean"},
        "timestamp": {"type": "string", "format": "date-time", "x-dlt": {"data_type": "timestamp"}},
        "date": {"type": "string", "format": "date"},
        "time": {"type": "string", "format": "time"},
        "binary": {"type": "string", "contentEncoding": "base64"},
        "json": {},
        "wei": {"type": "integer", "x-dlt": {"data_type": "wei"}},
    }
)
DLT_KNOWN: Final[frozenset[str]] = frozenset({"name", "data_type", "nullable", "description", "precision", "scale"})


def emit_dlt_node(node: TypedNode, *, preserve_unknown_hints: bool = True) -> dict[str, Any]:
    """Render a dlt-sourced IR node to a JSON Schema dictionary.

    :param node: dlt IR node to emit.
    :type node: TypedNode
    :param preserve_unknown_hints: Whether to retain unknown ``x-dlt`` column hints.
    :type preserve_unknown_hints: bool
    :return: Rendered JSON Schema dictionary.
    :rtype: dict[str, Any]
    """
    if node.type == "object":
        result = closed_object_schema(
            {
                field.name: emit_dlt_node(field.node, preserve_unknown_hints=preserve_unknown_hints)
                for field in node.properties
            },
            node,
        )
    elif node.type == "array" and isinstance(node.items, TypedNode):
        result = {
            "type": "array",
            "items": emit_dlt_node(node.items, preserve_unknown_hints=preserve_unknown_hints),
        }
    else:
        return emit_dlt_scalar(node, preserve_unknown_hints=preserve_unknown_hints)
    result.update(mutable_value(node.annotations))
    result.update(mutable_value(node.extensions))
    return result


def emit_dlt_scalar(node: TypedNode, *, preserve_unknown_hints: bool = True) -> dict[str, Any]:
    """Select logical templates and keep column metadata inside null unions.

    :param node: Scalar dlt IR node.
    :type node: TypedNode
    :param preserve_unknown_hints: Whether to retain unknown ``x-dlt`` column hints.
    :type preserve_unknown_hints: bool
    :return: Rendered JSON Schema dictionary.
    :rtype: dict[str, Any]
    :raises ConversionError: If the node has an unknown dlt ``data_type``.
    """
    logical = node.hints.logical_type
    if node.type == "unknown" or (logical is not None and logical not in DLT_TYPES):
        path = node.provenance.path if node.provenance is not None else ()
        name = path[-1] if path else "<unknown>"
        msg = f"Column {name!r} has unknown dlt data_type {logical!r}."
        raise ConversionError(msg)
    result = deepcopy(DLT_TYPES[logical]) if logical is not None else {}
    if node.hints.precision is not None or node.hints.scale is not None:
        hints = result.setdefault("x-dlt", {})
        hints["precision"] = node.hints.precision
        if node.hints.scale is not None:
            hints["scale"] = node.hints.scale
    result.update(mutable_value(node.constraints))
    result.update(mutable_value(node.annotations))
    metadata = mutable_value(node.extensions)
    source_hints = metadata.pop("x-dlt", {})
    if preserve_unknown_hints:
        hints = {key: value for key, value in source_hints.items() if key not in DLT_KNOWN and value is not None}
        if hints:
            result.setdefault("x-dlt", {}).update(hints)
    result.update(metadata)
    return nullable_schema(result, nullable=bool(node.nullable and result))
