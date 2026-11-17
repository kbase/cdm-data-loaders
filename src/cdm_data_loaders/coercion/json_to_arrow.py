"""Coerce JSON-decoded record dictionaries to match a target PyArrow schema."""

import base64
import json
from collections.abc import Mapping
from datetime import date, datetime, time
from decimal import Decimal
from enum import Enum
from typing import Any

import pyarrow as pa

from cdm_data_loaders.coercion._arrow_core import (
    coerce_map_entries,
    coerce_struct_from_mapping_or_tuple,
    parse_iso_timestamp,
)
from cdm_data_loaders.utils.arrow_schema import decode_field_metadata


def _decode_binary_scalar(value: str, metadata: Mapping[bytes, bytes] | None) -> bytes:
    """Decode a Base64 or Base16 (hex) string into raw bytes based on field metadata.

    :param value: Encoded binary string.
    :type value: str
    :param metadata: Optional PyArrow field metadata mapping.
    :type metadata: Mapping[bytes, bytes] | None
    :return: Decoded raw bytes.
    :rtype: bytes
    """
    decoded_meta = decode_field_metadata(metadata)
    if "base16" in decoded_meta.get("jsonschema_type", ""):
        return bytes.fromhex(value)
    return base64.b64decode(value)


def _coerce_string_input_scalar(
    value: str,
    data_type: pa.DataType,
    metadata: Mapping[bytes, bytes] | None,
) -> bytes | date | datetime | time | Decimal | str:
    """Coerce a string value when ``data_type`` is binary, temporal, or decimal.

    :param value: Input string scalar.
    :type value: str
    :param data_type: Target PyArrow scalar data type.
    :type data_type: pa.DataType
    :param metadata: Optional PyArrow field metadata mapping.
    :type metadata: Mapping[bytes, bytes] | None
    :return: Coerced scalar value matching ``data_type``.
    :rtype: bytes | date | datetime | time | Decimal | str
    """
    if pa.types.is_binary(data_type):
        return _decode_binary_scalar(value, metadata)
    if pa.types.is_date(data_type):
        return date.fromisoformat(value)
    if pa.types.is_timestamp(data_type):
        return parse_iso_timestamp(value, data_type)
    if pa.types.is_time(data_type):
        return time.fromisoformat(value)
    if pa.types.is_decimal(data_type):
        return Decimal(value)
    return value


def _coerce_scalar_for_pyarrow(
    value: object,
    data_type: pa.DataType,
    metadata: Mapping[bytes, bytes] | None = None,
) -> object:
    """Coerce a JSON-parsed value to match a PyArrow scalar type.

    :param value: Input JSON-parsed scalar value.
    :type value: object
    :param data_type: Target PyArrow scalar data type.
    :type data_type: pa.DataType
    :param metadata: Optional PyArrow field metadata mapping.
    :type metadata: Mapping[bytes, bytes] | None
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


def _coerce_value_for_pyarrow(
    value: object,
    data_type: pa.DataType,
    metadata: Mapping[bytes, bytes] | None = None,
) -> object:
    """Coerce a JSON-parsed value across nested structs, lists, maps, and scalars.

    :param value: Input JSON-parsed value.
    :type value: object
    :param data_type: Target PyArrow data type.
    :type data_type: pa.DataType
    :param metadata: Optional PyArrow field metadata mapping.
    :type metadata: Mapping[bytes, bytes] | None
    :return: Coerced value matching ``data_type``.
    :rtype: object
    """
    if value is None:
        return None
    if pa.types.is_struct(data_type):
        return coerce_struct_from_mapping_or_tuple(value, data_type, _coerce_value_for_pyarrow)
    if (pa.types.is_list(data_type) or pa.types.is_large_list(data_type)) and isinstance(value, list):
        item_field = data_type.value_field
        return [_coerce_value_for_pyarrow(item, item_field.type, item_field.metadata) for item in value]
    if pa.types.is_map(data_type) and isinstance(value, Mapping):
        return coerce_map_entries(value, data_type, _coerce_value_for_pyarrow)
    return _coerce_scalar_for_pyarrow(value, data_type, metadata)


def coerce_json_record_for_pyarrow(
    record: dict[str, Any],
    schema: pa.Schema,
) -> dict[str, Any]:
    """Coerce a JSON-validated record dictionary to match a PyArrow schema.

    Retains and coerces fields declared in ``schema`` so ``pyarrow.Table.from_pylist``
    can construct a table matching ``schema``.

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
