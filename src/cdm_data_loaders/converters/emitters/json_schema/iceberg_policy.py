"""Iceberg-sourced IR to JSON Schema emission policy."""

from typing import Any, Final

from cdm_data_loaders.converters.core.errors import ConversionError
from cdm_data_loaders.converters.core.inference import decimal_pattern
from cdm_data_loaders.converters.core.ir import Field, TypedNode
from cdm_data_loaders.converters.core.ir_values import mutable_value
from cdm_data_loaders.converters.emitters.json_schema.common import (
    closed_object_schema,
    kv_array_schema,
    nullable_schema,
)

ICEBERG_LOGICAL_TYPES: Final[frozenset[str]] = frozenset(
    {
        "boolean",
        "int",
        "long",
        "float",
        "double",
        "string",
        "date",
        "time",
        "uuid",
        "timestamp-without-tz",
        "timestamp-with-tz",
        "decimal",
        "fixed",
    }
)
ICEBERG_ELEMENT_KEYS: Final[frozenset[str]] = frozenset(
    {"element_id", "element_required", "key_id", "value_id", "value_required"}
)


def emit_iceberg_node(node: TypedNode) -> dict[str, Any]:
    """Wrap nullable Iceberg types and elements before applying field metadata.

    :param node: Iceberg IR node to emit.
    :type node: TypedNode
    :return: Rendered JSON Schema dictionary.
    :rtype: dict[str, Any]
    """
    result = emit_iceberg_type(node)
    result.update(mutable_value(node.constraints))
    result.update(mutable_value(node.annotations))
    extensions = mutable_value(node.extensions)
    metadata = extensions.pop("x-iceberg", {})
    metadata = {key: value for key, value in metadata.items() if key not in ICEBERG_ELEMENT_KEYS}
    if metadata:
        result.setdefault("x-iceberg", {}).update(metadata)
    result.update(extensions)
    return nullable_schema(result, nullable=node.nullable is True)


def emit_iceberg_type(node: TypedNode) -> dict[str, Any]:
    """Render structural and scalar Iceberg nodes from their typed children.

    :param node: Iceberg IR node to emit.
    :type node: TypedNode
    :return: Rendered JSON Schema dictionary.
    :rtype: dict[str, Any]
    """
    if node.type == "object":
        return closed_object_schema(
            {field.name: emit_iceberg_field(field) for field in node.properties},
            node,
        )
    if node.type == "array" and isinstance(node.items, TypedNode):
        return {"type": "array", "items": emit_iceberg_node(node.items)}
    if node.type == "map" and node.key_type is not None and node.value_type is not None:
        return kv_array_schema(
            key_schema=emit_iceberg_node(node.key_type),
            value_schema=emit_iceberg_node(node.value_type),
            description="Iceberg map rendered as key/value pairs (JSON object keys must be strings).",
        )
    return emit_iceberg_scalar(node)


def emit_iceberg_scalar(node: TypedNode) -> dict[str, Any]:
    """Apply physical bounds and source logical encodings to Iceberg scalar nodes.

    :param node: Scalar Iceberg IR node.
    :type node: TypedNode
    :return: Rendered JSON Schema dictionary.
    :rtype: dict[str, Any]
    :raises ConversionError: If the logical type is unsupported or missing required parameters.
    """
    hints = node.hints
    logical = hints.logical_type
    if logical not in ICEBERG_LOGICAL_TYPES:
        msg = f"No JSON Schema mapping for Iceberg type: {logical or node.type}"
        raise ConversionError(msg)
    result: dict[str, Any] = {"type": node.type}
    if node.type == "integer" and hints.bit_width:
        bound = 2 ** (hints.bit_width - 1)
        result.update(minimum=-bound, maximum=bound - 1)
    if logical in {"date", "time", "uuid", "timestamp-with-tz"}:
        result["format"] = "date-time" if logical == "timestamp-with-tz" else logical
    elif logical == "timestamp-without-tz":
        result.update(
            description="ISO 8601 timestamp with no timezone attached.",
            **{"x-iceberg": {"logical_type": logical}},
        )
    elif logical == "decimal":
        if hints.precision is None or hints.scale is None:
            msg = "Iceberg decimals require precision and scale"
            raise ConversionError(msg)
        result = {
            "type": "string",
            "pattern": decimal_pattern(hints.precision, hints.scale),
            "x-iceberg": {"logical_type": logical, "precision": hints.precision, "scale": hints.scale},
        }
    elif logical == "fixed":
        if hints.length is None:
            msg = "Iceberg fixed types require length"
            raise ConversionError(msg)
        result = {
            "type": "string",
            "contentEncoding": "base64",
            "x-iceberg": {"logical_type": logical, "length": hints.length},
        }
    return result


def emit_iceberg_field(field: Field) -> dict[str, Any]:
    """Place descriptions, IDs, and non-null defaults outside optional types.

    :param field: Iceberg IR field to emit.
    :type field: Field
    :return: Rendered JSON Schema dictionary.
    :rtype: dict[str, Any]
    """
    result = emit_iceberg_node(field.node)
    annotations = mutable_value(field.annotations)
    if not annotations.get("description"):
        annotations.pop("description", None)
    result.update(annotations)
    extensions = mutable_value(field.extensions)
    metadata = extensions.pop("x-iceberg", {})
    metadata = {
        key: value
        for key, value in metadata.items()
        if key not in {"initial_default", "write_default"} or value is not None
    }
    if metadata:
        result.setdefault("x-iceberg", {}).update(metadata)
    result.update(extensions)
    return result
