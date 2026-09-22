"""Converter to translate a table schema from an Iceberg catalog into JSONschema."""

from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from frozendict import frozendict
from pyiceberg.table import Table
from pyiceberg.types import (
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    FixedType,
    FloatType,
    IcebergType,
    IntegerType,
    ListType,
    LongType,
    MapType,
    NestedField,
    StringType,
    StructType,
    TimestampType,
    TimestamptzType,
    TimeType,
    UUIDType,
)

if TYPE_CHECKING:
    from pyiceberg.schema import Schema

JSON_SCHEMA_DIALECT = "https://json-schema.org/draft/2020-12/schema"


def decimal_pattern(precision: int, scale: int) -> str:
    """Build a regex for a JSONSchema pattern to constrain decimal fields to the appropriate form.

    :param precision: Decimal() class precision
    :type precision: int
    :param scale: Decimal() class scale
    :type scale: int
    :return: corresponding regex for the decimal field
    :rtype: str
    """
    digits_before = precision - scale
    if scale > 0:
        if digits_before == 0:
            return rf"^-?0(\.\d{{1,{scale}}})?$"
        return rf"^-?\d{{1,{digits_before}}}(\.\d{{1,{scale}}})?$"
    return rf"^-?\d{{1,{digits_before}}}$"


def _convert_list(field_type: ListType) -> dict[str, Any]:
    """Convert an Iceberg ListType field into the JSONSchema equivalent.

    :param field_type: a list type field
    :type field_type: ListType
    :return: the JSONSchema equivalent
    :rtype: dict[str, Any]
    """
    item_schema = convert_type(field_type.element_type)
    if not field_type.element_required:
        item_schema = {"anyOf": [item_schema, {"type": "null"}]}
    return {"type": "array", "items": item_schema}


def _convert_map(field_type: MapType) -> dict[str, Any]:
    """Convert an Iceberg MapType field into the JSONSchema equivalent.

    :param field_type: a map type field
    :type field_type: MapType
    :return: the JSONSchema equivalent
    :rtype: dict[str, Any]
    """
    key_schema = convert_type(field_type.key_type)
    value_schema = convert_type(field_type.value_type)
    if not field_type.value_required:
        value_schema = {"anyOf": [value_schema, {"type": "null"}]}
    return {
        "type": "array",
        "description": "Iceberg map rendered as key/value pairs (JSON object keys must be strings).",
        "items": {
            "type": "object",
            "properties": {"key": key_schema, "value": value_schema},
            "required": ["key", "value"],
            "additionalProperties": False,
        },
    }


TYPE_CONVERTER: frozendict[type[IcebergType], Callable] = frozendict(
    {
        BooleanType: lambda _: {"type": "boolean"},
        IntegerType: lambda _: {"type": "integer", "minimum": -2147483648, "maximum": 2147483647},
        LongType: lambda _: {"type": "integer", "minimum": -9223372036854775808, "maximum": 9223372036854775807},
        FloatType: lambda _: {"type": "number"},
        DoubleType: lambda _: {"type": "number"},
        StringType: lambda _: {"type": "string"},
        DateType: lambda _: {"type": "string", "format": "date"},
        TimeType: lambda _: {"type": "string", "format": "time"},
        TimestampType: lambda _: {
            "type": "string",
            "description": "ISO 8601 timestamp with no timezone attached.",
            "x-iceberg": {"logical_type": "timestamp-without-tz"},
        },
        TimestamptzType: lambda _: {"type": "string", "format": "date-time"},
        UUIDType: lambda _: {"type": "string", "format": "uuid"},
        DecimalType: lambda field_type: {
            "type": "string",
            "pattern": decimal_pattern(field_type.precision, field_type.scale),  # pyright: ignore[reportAttributeAccessIssue]
            "x-iceberg": {"logical_type": "decimal", "precision": field_type.precision, "scale": field_type.scale},  # pyright: ignore[reportAttributeAccessIssue]
        },
        FixedType: lambda field_type: {
            "type": "string",
            "contentEncoding": "base64",
            "x-iceberg": {"logical_type": "fixed", "length": field_type.root},  # pyright: ignore[reportAttributeAccessIssue]
        },
        StructType: lambda field_type: convert_struct(field_type.fields),  # pyright: ignore[reportAttributeAccessIssue]
        ListType: _convert_list,
        MapType: _convert_map,
    }
)


def convert_type(field_type: IcebergType) -> dict[str, Any]:
    """Convert a PyIceberg type into the JSONSchema equivalent.

    :param field_type: pyiceberg field type
    :type field_type: IcebergType
    :raises NotImplementedError: when the type has no JSONschema mapping
    :return: JSONSchema dictionary specifying type
    :rtype: dict[str, Any]
    """
    field_type_type = type(field_type)
    if field_type_type in TYPE_CONVERTER:
        return TYPE_CONVERTER[field_type_type](field_type)

    err_msg = f"No JSON Schema mapping for Iceberg type: {field_type}"
    raise NotImplementedError(err_msg)


def convert_field(field: NestedField) -> dict[str, Any]:
    """Convert a nested field into the JSONSchema equivalent.

    :param field: a nested field!
    :type field: NestedField
    :return: JSONSchema equivalent
    :rtype: dict[str, Any]
    """
    node = convert_type(field.field_type)
    if not field.required:
        node: dict[str, Any] = {"anyOf": [node, {"type": "null"}]}
    if field.doc:
        node["description"] = field.doc
    iceberg_meta = {"field_id": field.field_id, "required": field.required}
    if field.initial_default is not None:
        iceberg_meta["initial_default"] = field.initial_default
    if field.write_default is not None:
        iceberg_meta["write_default"] = field.write_default
    node.setdefault("x-iceberg", {}).update(iceberg_meta)
    return node


def convert_struct(fields: tuple[NestedField, ...]) -> dict[str, Any]:
    """Convert a struct datatype into the JSONSchema equivalent.

    :param fields: a struct of fields
    :type field: tuple[NestedField, ...]
    :return: JSONSchema equivalent
    :rtype: dict[str, Any]
    """
    properties = {f.name: convert_field(f) for f in fields}
    required = [f.name for f in fields if f.required]
    result = {"type": "object", "properties": properties, "additionalProperties": False}
    if required:
        result["required"] = required
    return result


def table_to_json_schema(table: Table, identifier: tuple[str, ...]) -> dict[str, Any]:
    """Convert a PyIceberg Table object into the JSONSchema equivalent.

    :param table: pyiceberg table
    :type table: Table
    :param identifier: identifier for the table -- e.g. namespace.table_name
    :type identifier: tuple[str, ...]
    :return: JSONSchema version
    :rtype: dict[str, Any]
    """
    metadata = table.metadata
    schema: Schema = table.schema()
    body = convert_struct(schema.fields)
    body["$schema"] = JSON_SCHEMA_DIALECT
    body["$id"] = f"urn:iceberg:{'.'.join(identifier)}"
    body["title"] = identifier[-1]

    comment = table.properties.get("comment")
    if comment:
        body["description"] = comment

    partition_fields = [
        {"source_id": pf.source_id, "name": pf.name, "transform": str(pf.transform)} for pf in table.spec().fields
    ]
    body["x-iceberg"] = {
        "identifier": list(identifier),
        "schema_id": schema.schema_id,
        "format_version": metadata.format_version,
        "current_snapshot_id": metadata.current_snapshot_id,
        "location": metadata.location,
        "properties": dict(table.properties),
        "partition_spec": partition_fields,
        "identifier_field_ids": sorted(schema.identifier_field_ids),
    }
    return body
