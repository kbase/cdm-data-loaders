"""Coerce JSON-parsed records and scalar values to match a target PyArrow schema."""

import base64
import json
from collections.abc import Mapping
from datetime import UTC, date, datetime, time
from decimal import Decimal
from enum import Enum
from typing import Any

import pyarrow as pa


def _parse_iso_timestamp(value: str, data_type: pa.TimestampType) -> datetime:
    """Parse an ISO-8601 timestamp string, attaching UTC when target type is tz-aware.

    :param value: ISO-8601 timestamp string.
    :type value: str
    :param data_type: Target PyArrow timestamp type.
    :type data_type: pa.TimestampType
    :return: Parsed ``datetime`` instance.
    :rtype: datetime
    """
    parsed_dt = datetime.fromisoformat(value)
    if parsed_dt.tzinfo is None and data_type.tz is not None:
        return parsed_dt.replace(tzinfo=UTC)
    return parsed_dt


def _decode_binary_scalar(value: str, metadata: dict[bytes, bytes] | None) -> bytes:
    """Decode a Base64 or Base16 (hex) string into raw bytes based on field metadata.

    :param value: Encoded binary string.
    :type value: str
    :param metadata: Optional PyArrow field metadata dictionary.
    :type metadata: dict[bytes, bytes] | None
    :return: Decoded raw bytes.
    :rtype: bytes
    """
    if metadata and b"jsonschema_type" in metadata and b"base16" in metadata[b"jsonschema_type"]:
        return bytes.fromhex(value)
    return base64.b64decode(value)


def _coerce_string_input_scalar(
    value: str,
    data_type: pa.DataType,
    metadata: dict[bytes, bytes] | None,
) -> bytes | date | datetime | time | Decimal | str:
    """Coerce a string value when the target PyArrow type is binary, temporal, or decimal.

    :param value: Input string scalar.
    :type value: str
    :param data_type: Target PyArrow scalar data type.
    :type data_type: pa.DataType
    :param metadata: Optional PyArrow field metadata dictionary.
    :type metadata: dict[bytes, bytes] | None
    :return: Coerced scalar value matching ``data_type``.
    :rtype: bytes | date | datetime | time | Decimal | str
    """
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
    value: object,
    data_type: pa.DataType,
    metadata: dict[bytes, bytes] | None = None,
) -> object:
    """Coerce a JSON-parsed value to match a PyArrow scalar type.

    :param value: Input JSON-parsed scalar value.
    :type value: object
    :param data_type: Target PyArrow scalar data type.
    :type data_type: pa.DataType
    :param metadata: Optional PyArrow field metadata dictionary.
    :type metadata: dict[bytes, bytes] | None
    :return: Coerced scalar value matching ``data_type``.
    :rtype: object
    """
    if isinstance(value, Enum):
        value = value.value
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


def _coerce_struct_for_pyarrow(value: object, data_type: pa.StructType) -> object:
    """Coerce a mapping, closed tuple list, or scalar union value to match a PyArrow StructType.

    :param value: Input mapping, tuple list, or scalar value.
    :type value: object
    :param data_type: Target PyArrow struct data type.
    :type data_type: pa.StructType
    :return: Coerced struct dictionary or pass-through value.
    :rtype: object
    """
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
    value: object,
    data_type: pa.DataType,
    metadata: dict[bytes, bytes] | None = None,
) -> object:
    """Recursively coerce a JSON-parsed value to match the target PyArrow DataType.

    :param value: Input JSON-parsed value.
    :type value: object
    :param data_type: Target PyArrow data type.
    :type data_type: pa.DataType
    :param metadata: Optional PyArrow field metadata dictionary.
    :type metadata: dict[bytes, bytes] | None
    :return: Coerced value matching ``data_type``.
    :rtype: object
    """
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
) -> dict[str, Any]:
    """Coerce a JSON-validated record dictionary to match a PyArrow schema.

    Only fields declared in ``schema`` are retained and coerced recursively so
    ``pyarrow.Table.from_pylist`` can construct a table matching ``schema``.

    :param record: JSON-parsed and schema-validated record dictionary.
    :type record: dict[str, Any]
    :param schema: Target PyArrow schema for the record's table.
    :type schema: pa.Schema
    :return: Coerced record dictionary ready for ``DictBuffer`` or ``pa.Table.from_pylist``.
    :rtype: dict[str, Any]
    """
    return {
        field.name: _coerce_value_for_pyarrow(record[field.name], field.type, field.metadata)
        for field in schema
        if field.name in record
    }


convert_json_record = coerce_record_for_pyarrow
