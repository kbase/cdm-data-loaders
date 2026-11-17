"""Shared PyArrow structural and scalar coercion helpers."""

from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from typing import Any

import pyarrow as pa

type ValueCoercer = Callable[[object, pa.DataType, Mapping[bytes, bytes] | None], object]


def comparable_sort_key(value: object) -> tuple[bool, int, int | float | bytes | str]:
    """Return a deterministic sort key for set elements during record coercion.

    :param value: Set element to sort.
    :type value: object
    :return: Sort key tuple ``(is_none, type_rank, comparable_value)``.
    :rtype: tuple[bool, int, int | float | bytes | str]
    """
    if value is None:
        return (True, 0, "")
    if isinstance(value, bool):
        return (False, 0, int(value))
    if isinstance(value, int | float):
        return (False, 1, value)
    if isinstance(value, bytes):
        return (False, 2, value)
    if isinstance(value, str):
        return (False, 3, value)
    return (False, 4, str(value))


def parse_iso_timestamp(value: str, data_type: pa.TimestampType) -> datetime:
    """Parse an ISO-8601 timestamp string, attaching UTC when ``data_type`` is timezone-aware.

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


def coerce_struct_from_mapping_or_tuple(
    value: object,
    data_type: pa.StructType,
    coerce_child: ValueCoercer,
    *,
    allow_tuple: bool = False,
) -> object:
    """Coerce a mapping, positional ``item_0`` tuple, or ``_value`` scalar union into a struct dict.

    :param value: Input mapping, sequence, or scalar value.
    :type value: object
    :param data_type: Target PyArrow struct data type.
    :type data_type: pa.StructType
    :param coerce_child: Child value coercion callback.
    :type coerce_child: ValueCoercer
    :param allow_tuple: Whether Python ``tuple`` instances match ``item_0`` positional structs.
    :type allow_tuple: bool
    :return: Coerced struct dictionary or pass-through value.
    :rtype: object
    """
    if isinstance(value, Mapping):
        return {
            field.name: coerce_child(value[field.name], field.type, field.metadata)
            for field in data_type
            if field.name in value
        }
    is_tuple_seq = isinstance(value, list | tuple) if allow_tuple else isinstance(value, list)
    if is_tuple_seq and len(data_type) > 0 and data_type[0].name == "item_0":
        return {
            field.name: coerce_child(value[idx], field.type, field.metadata)
            for idx, field in enumerate(data_type)
            if idx < len(value) and value[idx] is not None
        }
    value_field = next((field for field in data_type if field.name == "_value"), None)
    if value_field is not None:
        return {"_value": coerce_child(value, value_field.type, value_field.metadata)}
    return value


def coerce_map_entries(
    value: Mapping[Any, Any],
    data_type: pa.MapType,
    coerce_child: ValueCoercer,
) -> dict[object, object]:
    """Coerce all keys and values of a mapping for a ``pa.MapType`` column.

    :param value: Input mapping.
    :type value: Mapping[Any, Any]
    :param data_type: Target PyArrow map data type.
    :type data_type: pa.MapType
    :param coerce_child: Child value coercion callback.
    :type coerce_child: ValueCoercer
    :return: Coerced dictionary of keys and values.
    :rtype: dict[object, object]
    """
    key_field = data_type.key_field
    item_field = data_type.item_field
    return {
        coerce_child(k, key_field.type, key_field.metadata): coerce_child(v, item_field.type, item_field.metadata)
        for k, v in value.items()
    }
