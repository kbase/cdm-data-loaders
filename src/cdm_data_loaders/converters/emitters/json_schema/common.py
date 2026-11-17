"""Shared JSON Schema builders and dialect constants."""

from typing import Any, Final

from cdm_data_loaders.converters.core.ir import TypedNode

JSON_SCHEMA_DIALECT: Final[str] = "https://json-schema.org/draft/2020-12/schema"


def nullable_schema(schema: dict[str, Any], *, nullable: bool) -> dict[str, Any]:
    """Wrap ``schema`` in an ``anyOf`` null union when ``nullable`` is ``True``.

    :param schema: Base JSON Schema object.
    :type schema: dict[str, Any]
    :param nullable: Whether to wrap ``schema`` in a null union.
    :type nullable: bool
    :return: Resulting JSON Schema object.
    :rtype: dict[str, Any]
    """
    return {"anyOf": [schema, {"type": "null"}]} if nullable else schema


def schema_object(*, schema: dict[str, Any] | bool) -> dict[str, Any]:
    """Represent a boolean schema as an object when metadata must be attached.

    :param schema: JSON Schema dictionary or boolean schema.
    :type schema: dict[str, Any] | bool
    :return: Equivalent JSON Schema dictionary.
    :rtype: dict[str, Any]
    """
    if isinstance(schema, bool):
        return {} if schema else {"not": {}}
    return schema


def closed_object_schema(properties: dict[str, Any], node: TypedNode) -> dict[str, Any]:
    """Build a closed JSON Schema object from rendered properties and required names.

    :param properties: Rendered property subschemas.
    :type properties: dict[str, Any]
    :param node: Source object IR node.
    :type node: TypedNode
    :return: Closed JSON Schema object definition.
    :rtype: dict[str, Any]
    """
    result: dict[str, Any] = {
        "type": "object",
        "properties": properties,
        "additionalProperties": False,
    }
    required = node.required_names
    if required is None:
        required = tuple(field.name for field in node.properties if field.required)
    if required:
        result["required"] = list(required)
    return result


def kv_array_schema(
    *,
    key_schema: dict[str, Any] | bool,
    value_schema: dict[str, Any] | bool,
    description: str | None = None,
) -> dict[str, Any]:
    """Render a map node as an array of closed key/value object items.

    :param key_schema: Rendered key JSON Schema.
    :type key_schema: dict[str, Any] | bool
    :param value_schema: Rendered value JSON Schema.
    :type value_schema: dict[str, Any] | bool
    :param description: Optional description string.
    :type description: str | None
    :return: Array-of-entries JSON Schema object.
    :rtype: dict[str, Any]
    """
    result: dict[str, Any] = {"type": "array"}
    if description is not None:
        result["description"] = description
    result["items"] = {
        "type": "object",
        "properties": {"key": key_schema, "value": value_schema},
        "required": ["key", "value"],
        "additionalProperties": False,
    }
    return result
