"""Generic and JSON-Schema-sourced IR emission policy."""

from typing import Any

from cdm_data_loaders.converters.core.errors import ConversionError
from cdm_data_loaders.converters.core.ir import Field, SchemaDocument, TypedNode
from cdm_data_loaders.converters.core.ir_values import mutable_value
from cdm_data_loaders.converters.emitters.json_schema.common import kv_array_schema, nullable_schema, schema_object


def emit_json_document(document: SchemaDocument) -> dict[str, Any]:
    """Emit structural JSON facts while keeping the source dialect.

    :param document: IR schema document to emit.
    :type document: SchemaDocument
    :return: Rendered JSON Schema dictionary.
    :rtype: dict[str, Any]
    """
    result = schema_object(schema=emit_json_node(document.root))
    result.update(mutable_value(document.annotations))
    result.update(mutable_value(document.extensions))
    if document.dialect is not None:
        result["$schema"] = document.dialect
    if document.identifier is not None:
        result["$id"] = document.identifier
    if document.name is not None:
        result.setdefault("title", document.name)
    return result


def emit_json_node(node: TypedNode) -> dict[str, Any] | bool:
    """Emit a generic or JSON-Schema-sourced IR node.

    :param node: IR node to emit.
    :type node: TypedNode
    :return: Rendered JSON Schema dictionary or boolean schema.
    :rtype: dict[str, Any] | bool
    :raises ConversionError: If ``node.type`` is ``unknown`` without a declaration.
    """
    from_json = node.provenance is not None and node.provenance.source == "json-schema"
    result = mutable_value(node.constraints)
    result.update(mutable_value(node.annotations))
    result.update(mutable_value(node.extensions))
    if node.declared_type is not None:
        result["type"] = list(node.declared_type) if isinstance(node.declared_type, tuple) else node.declared_type
    elif not from_json and node.type not in {"any", "never", "unknown", "map"}:
        result["type"] = node.type
    elif not from_json and node.type == "unknown":
        msg = "Cannot emit an unknown type without a JSON Schema declaration"
        raise ConversionError(msg)
    emit_json_children(node, result)
    if node.type == "never":
        result["not"] = {}
    if from_json:
        if not node.source_keywords and (
            (node.type == "any" and not result) or (node.type == "never" and result == {"not": {}})
        ):
            return node.type == "any"
        result = {**{key: result[key] for key in node.source_keywords if key in result}, **result}
    else:
        result = nullable_schema(
            result,
            nullable=node.nullable is True and node.type not in {"any", "never", "null"},
        )
    return result


def emit_json_children(node: TypedNode, result: dict[str, Any]) -> None:
    """Render child schema positions without interpreting literal keyword payloads.

    :param node: IR node whose child schemas are being rendered.
    :type node: TypedNode
    :param result: Target JSON Schema dictionary to populate in place.
    :type result: dict[str, Any]
    """
    if node.properties or "properties" in node.source_keywords:
        result["properties"] = {field.name: emit_json_field(field) for field in node.properties}
    if node.required_names is not None:
        result["required"] = list(node.required_names)
    elif any(field.required for field in node.properties):
        result["required"] = [field.name for field in node.properties if field.required]
    if node.pattern_properties or "patternProperties" in node.source_keywords:
        result["patternProperties"] = {key: emit_json_node(child) for key, child in node.pattern_properties.items()}
    if node.items is not None:
        result["items"] = (
            [emit_json_node(child) for child in node.items]
            if isinstance(node.items, tuple)
            else emit_json_node(node.items)
        )
    if node.additional_properties is not None:
        result["additionalProperties"] = emit_json_node(node.additional_properties)
    for keyword, children in (("prefixItems", node.prefix_items), ("anyOf", node.any_of), ("oneOf", node.one_of)):
        if children is not None:
            result[keyword] = [emit_json_node(child) for child in children]
    result.update({key: emit_json_node(child) for key, child in node.schema_keywords.items()})
    for keyword, children in node.schema_maps.items():
        result.setdefault(keyword, {}).update({key: emit_json_node(child) for key, child in children.items()})
    if node.type == "map":
        result.update(emit_json_map(node))


def emit_json_field(field: Field) -> dict[str, Any] | bool:
    """Attach field metadata without altering an unannotated boolean schema.

    :param field: IR field to emit.
    :type field: Field
    :return: Rendered JSON Schema dictionary or boolean schema.
    :rtype: dict[str, Any] | bool
    """
    schema = emit_json_node(field.node)
    if not field.annotations and not field.extensions:
        return schema
    result = schema_object(schema=schema)
    result.update(mutable_value(field.annotations))
    result.update(mutable_value(field.extensions))
    return result


def emit_json_map(node: TypedNode) -> dict[str, Any]:
    """Represent map nodes with typed keys as key/value entry arrays.

    :param node: Map IR node.
    :type node: TypedNode
    :return: Array-of-entries JSON Schema dictionary.
    :rtype: dict[str, Any]
    :raises ConversionError: If ``node.key_type`` or ``node.value_type`` is ``None``.
    """
    if node.key_type is None or node.value_type is None:
        msg = "Map nodes require key and value types"
        raise ConversionError(msg)
    return kv_array_schema(
        key_schema=emit_json_node(node.key_type),
        value_schema=emit_json_node(node.value_type),
    )
