"""PyArrow-sourced IR to JSON Schema emission policy."""

from typing import Any, Final

from cdm_data_loaders.converters.core.errors import ConversionError
from cdm_data_loaders.converters.core.inference import decimal_pattern
from cdm_data_loaders.converters.core.ir import Field, SchemaDocument, TypedNode
from cdm_data_loaders.converters.core.ir_values import mutable_value
from cdm_data_loaders.converters.emitters.json_schema.common import (
    JSON_SCHEMA_DIALECT,
    closed_object_schema,
    kv_array_schema,
    nullable_schema,
)

PYARROW_SIGNED_INT_TYPES: Final[frozenset[str]] = frozenset({"int8", "int16", "int32", "long"})
PYARROW_UNSIGNED_INT_TYPES: Final[frozenset[str]] = frozenset({"uint8", "uint16", "uint32", "uint64"})
PYARROW_LOGICAL_TYPES: Final[frozenset[str]] = frozenset(
    {
        "boolean",
        "int8",
        "int16",
        "int32",
        "long",
        "uint8",
        "uint16",
        "uint32",
        "uint64",
        "float16",
        "float",
        "double",
        "string",
        "binary",
        "fixed",
        "date",
        "time",
        "timestamp-without-tz",
        "timestamp-with-tz",
        "decimal",
    }
)


def emit_pyarrow_document(document: SchemaDocument) -> dict[str, Any]:
    """Emit a Draft 2020-12 JSON Schema document from a PyArrow IR document.

    :param document: PyArrow-sourced IR schema document.
    :type document: SchemaDocument
    :return: Rendered JSON Schema dictionary with canonical top-level keyword ordering.
    :rtype: dict[str, Any]
    """
    body = emit_pyarrow_node(document.root)
    annotations = mutable_value(document.annotations)
    if not annotations.get("description"):
        annotations.pop("description", None)
    envelope: dict[str, Any] = {"$schema": document.dialect or JSON_SCHEMA_DIALECT}
    if document.identifier is not None:
        envelope["$id"] = document.identifier
    if document.name is not None:
        envelope["title"] = document.name
    if "description" in annotations:
        envelope["description"] = annotations["description"]
    envelope.update({key: val for key, val in body.items() if key != "description"})
    envelope.update({key: val for key, val in annotations.items() if key != "description"})
    extensions = mutable_value(document.extensions)
    pa_meta = extensions.pop("x-pyarrow", {})
    if pa_meta:
        envelope.setdefault("x-pyarrow", {}).update(pa_meta)
    envelope.update(extensions)
    return envelope


def emit_pyarrow_node(node: TypedNode) -> dict[str, Any]:
    """Wrap nullable PyArrow types and elements before applying outer field metadata.

    :param node: PyArrow IR node to emit.
    :type node: TypedNode
    :return: Rendered JSON Schema dictionary.
    :rtype: dict[str, Any]
    """
    result = emit_pyarrow_type(node)
    result.update(mutable_value(node.constraints))
    annotations = mutable_value(node.annotations)
    if not annotations.get("description"):
        annotations.pop("description", None)
    result.update(annotations)
    extensions = mutable_value(node.extensions)
    pa_meta = extensions.pop("x-pyarrow", {})
    if pa_meta:
        result.setdefault("x-pyarrow", {}).update(pa_meta)
    result.update(extensions)
    return nullable_schema(result, nullable=node.nullable is True)


def emit_pyarrow_type(node: TypedNode) -> dict[str, Any]:
    """Render structural and scalar PyArrow nodes from their typed children.

    :param node: PyArrow IR node to emit.
    :type node: TypedNode
    :return: Rendered JSON Schema dictionary.
    :rtype: dict[str, Any]
    """
    if node.type == "object":
        return closed_object_schema(
            {field.name: emit_pyarrow_field(field) for field in node.properties},
            node,
        )
    if node.type == "array" and isinstance(node.items, TypedNode):
        result: dict[str, Any] = {"type": "array", "items": emit_pyarrow_node(node.items)}
        if node.hints.logical_type == "fixed-size-list" and node.hints.length is not None:
            result["minItems"] = node.hints.length
            result["maxItems"] = node.hints.length
        return result
    if node.type == "map" and node.key_type is not None and node.value_type is not None:
        return kv_array_schema(
            key_schema=emit_pyarrow_node(node.key_type),
            value_schema=emit_pyarrow_node(node.value_type),
            description="PyArrow map rendered as key/value pairs (JSON object keys must be strings).",
        )
    return emit_pyarrow_scalar(node)


def emit_pyarrow_scalar(node: TypedNode) -> dict[str, Any]:
    """Apply physical bounds and logical encodings to PyArrow scalar nodes.

    :param node: Scalar PyArrow IR node.
    :type node: TypedNode
    :return: Rendered JSON Schema dictionary.
    :rtype: dict[str, Any]
    :raises ConversionError: If the logical type is unsupported or missing required parameters.
    """
    hints = node.hints
    logical = hints.logical_type
    if logical not in PYARROW_LOGICAL_TYPES:
        msg = f"No JSON Schema mapping for PyArrow type: {logical or node.type}"
        raise ConversionError(msg)
    result: dict[str, Any] = {"type": node.type}
    if logical in PYARROW_SIGNED_INT_TYPES and hints.bit_width:
        bound = 2 ** (hints.bit_width - 1)
        result.update(minimum=-bound, maximum=bound - 1)
    elif logical in PYARROW_UNSIGNED_INT_TYPES and hints.bit_width:
        result.update(minimum=0, maximum=(2**hints.bit_width) - 1)
    if logical in {"date", "time", "timestamp-with-tz"}:
        result["format"] = "date-time" if logical == "timestamp-with-tz" else logical
    elif logical == "timestamp-without-tz":
        result["description"] = "ISO 8601 timestamp with no timezone attached."
    elif logical in {"binary", "fixed"}:
        result["contentEncoding"] = "base64"
    elif logical == "decimal":
        if hints.precision is None or hints.scale is None:
            msg = "PyArrow decimals require precision and scale"
            raise ConversionError(msg)
        result = {
            "type": "string",
            "pattern": decimal_pattern(hints.precision, hints.scale),
        }
    return result


def emit_pyarrow_field(field: Field) -> dict[str, Any]:
    """Place field descriptions and custom Arrow metadata outside nullable wrappers.

    :param field: PyArrow IR field to emit.
    :type field: Field
    :return: Rendered JSON Schema dictionary.
    :rtype: dict[str, Any]
    """
    result = emit_pyarrow_node(field.node)
    annotations = mutable_value(field.annotations)
    if not annotations.get("description"):
        annotations.pop("description", None)
    result.update(annotations)
    extensions = mutable_value(field.extensions)
    pa_meta = extensions.pop("x-pyarrow", {})
    if pa_meta:
        result.setdefault("x-pyarrow", {}).update(pa_meta)
    result.update(extensions)
    return result
