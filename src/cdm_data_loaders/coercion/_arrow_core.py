"""Shared PyArrow structural and scalar coercion helpers."""

import base64
import functools
import json
from collections import deque
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime, time
from decimal import Context, Decimal
from enum import Enum
from typing import Any, Final, Literal

import pyarrow as pa

from cdm_data_loaders.utils.arrow_schema import (
    ARROW_LIST_TYPES,
    ArrowListType,
    decode_field_metadata,
    is_any_binary_type,
    is_any_list_type,
    is_any_string_type,
)
from cdm_data_loaders.utils.json_utils import json_default_scalar

type ValueCoercer = Callable[[object, pa.DataType, Mapping[bytes, bytes] | None], object]
type DefaultStrEncoding = Literal["base64", "utf-8"]
type ArrowDecimalType = pa.Decimal128Type | pa.Decimal256Type

MIN_DECIMAL_QUANTIZE_PRECISION: Final[int] = 76
PYTHON_SEQUENCE_OR_SET_TYPES: Final[tuple[type[Any], ...]] = (list, tuple, set, frozenset, deque)


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


def coerce_date(value: object) -> object:
    """Coerce a ``date``, ``datetime``, or ISO-8601 string into a ``datetime.date``.

    :param value: Input ``date``, ``datetime``, or ISO-8601 date string.
    :type value: object
    :return: Coerced ``datetime.date`` or pass-through value.
    :rtype: object
    """
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, str):
        return date.fromisoformat(value)
    return value


def coerce_timestamp(value: object, data_type: pa.TimestampType) -> object:
    """Coerce a ``datetime`` or ISO-8601 string to match a tz-aware or tz-naive ``pa.TimestampType``.

    :param value: Input ``datetime`` or ISO-8601 timestamp string.
    :type value: object
    :param data_type: Target PyArrow timestamp data type.
    :type data_type: pa.TimestampType
    :return: Coerced ``datetime`` with matching timezone awareness, or pass-through value.
    :rtype: object
    """
    parsed = datetime.fromisoformat(value) if isinstance(value, str) else value
    if not isinstance(parsed, datetime):
        return value
    if data_type.tz is not None:
        return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed.astimezone(UTC)
    return parsed.astimezone(UTC).replace(tzinfo=None) if parsed.tzinfo is not None else parsed


def coerce_time(value: object) -> object:
    """Coerce a ``time`` or ISO-8601 string into a naive ``datetime.time``.

    :param value: Input ``time`` or ISO-8601 time string.
    :type value: object
    :return: Naive ``datetime.time`` or pass-through value.
    :rtype: object
    """
    parsed = time.fromisoformat(value) if isinstance(value, str) else value
    if isinstance(parsed, time) and parsed.tzinfo is not None:
        return parsed.replace(tzinfo=None)
    return parsed


def coerce_temporal(value: object, data_type: pa.DataType) -> object:
    """Coerce a date, time, or timestamp value to match a temporal PyArrow data type.

    :param value: Input temporal object or ISO-8601 string.
    :type value: object
    :param data_type: Target PyArrow temporal data type.
    :type data_type: pa.DataType
    :return: Coerced temporal Python object.
    :rtype: object
    """
    if pa.types.is_date(data_type):
        return coerce_date(value)
    if isinstance(data_type, pa.TimestampType):
        return coerce_timestamp(value, data_type)
    return coerce_time(value)


def coerce_decimal(value: object, data_type: ArrowDecimalType) -> object:
    """Coerce a ``Decimal``, ``int``, ``float``, or ``str`` into a quantized ``Decimal``.

    :param value: Input numeric or string value.
    :type value: object
    :param data_type: Target PyArrow decimal data type defining precision and scale.
    :type data_type: ArrowDecimalType
    :return: Quantized ``Decimal`` value, or pass-through value for non-numeric inputs.
    :rtype: object
    """
    if isinstance(value, bool) or not isinstance(value, Decimal | int | float | str):
        return value
    dec = value if isinstance(value, Decimal) else Decimal(str(value))
    quantum = Decimal(1).scaleb(-data_type.scale)
    ctx = Context(prec=max(data_type.precision, MIN_DECIMAL_QUANTIZE_PRECISION))
    return dec.quantize(quantum, context=ctx)


def _decode_binary_string(
    value: str,
    metadata: Mapping[bytes, bytes] | None,
    *,
    default_str_encoding: DefaultStrEncoding,
) -> bytes:
    """Decode a string into ``bytes`` using field metadata or ``default_str_encoding``.

    :param value: Input string scalar.
    :type value: str
    :param metadata: Optional PyArrow field metadata mapping.
    :type metadata: Mapping[bytes, bytes] | None
    :param default_str_encoding: Fallback encoding when field metadata carries no binary hint.
    :type default_str_encoding: DefaultStrEncoding
    :return: Decoded raw bytes.
    :rtype: bytes
    """
    decoded_meta = decode_field_metadata(metadata)
    jsonschema_type = decoded_meta.get("jsonschema_type", "")
    pydantic_type = decoded_meta.get("pydantic_type", "")
    xsd_type = decoded_meta.get("xsd_type", "")
    if "base16" in jsonschema_type or xsd_type == "xs:hexBinary":
        return bytes.fromhex(value)
    if "base64" in jsonschema_type or "base64" in pydantic_type or xsd_type == "xs:base64Binary":
        return base64.b64decode(value)
    if pydantic_type == "binary":
        return value.encode("utf-8")
    return base64.b64decode(value) if default_str_encoding == "base64" else value.encode("utf-8")


def coerce_binary(
    value: object,
    metadata: Mapping[bytes, bytes] | None = None,
    *,
    default_str_encoding: DefaultStrEncoding = "base64",
) -> object:
    """Coerce ``bytes``, ``bytearray``, ``memoryview``, or ``str`` into ``bytes``.

    :param value: Input binary or string value.
    :type value: object
    :param metadata: Optional PyArrow field metadata mapping.
    :type metadata: Mapping[bytes, bytes] | None
    :param default_str_encoding: Fallback encoding when ``value`` is an unannotated ``str``.
    :type default_str_encoding: DefaultStrEncoding
    :return: Coerced ``bytes`` or pass-through value.
    :rtype: object
    """
    if isinstance(value, bytes):
        return value
    if isinstance(value, bytearray | memoryview):
        return bytes(value)
    if isinstance(value, str):
        return _decode_binary_string(value, metadata, default_str_encoding=default_str_encoding)
    return value


def coerce_numeric(value: object, data_type: pa.DataType) -> object:
    """Coerce an ``int``, ``float``, ``Decimal``, or numeric ``str`` to match a numeric PyArrow type.

    :param value: Input numeric or string value.
    :type value: object
    :param data_type: Target PyArrow integer or floating data type.
    :type data_type: pa.DataType
    :return: Coerced ``int`` or ``float``, or pass-through value.
    :rtype: object
    """
    if isinstance(value, bool):
        return value
    if pa.types.is_floating(data_type):
        return float(value) if isinstance(value, Decimal | int | str) else value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    if isinstance(value, Decimal) and value == value.to_integral_value():
        return int(value)
    if isinstance(value, int | str):
        return int(value)
    return value


def coerce_string(value: object) -> str:
    """Coerce a Python or JSON-parsed value into a UTF-8 string for string Arrow columns.

    :param value: Input Python or JSON-parsed value.
    :type value: object
    :return: String representation suitable for ``pa.string()``, ``pa.large_string()``, or ``pa.string_view()``.
    :rtype: str
    """
    if isinstance(value, str):
        return value
    if isinstance(value, Mapping | list | tuple | bool):
        return json.dumps(value, default=json_default_scalar)
    if isinstance(value, date | datetime | time):
        return value.isoformat()
    if isinstance(value, bytes | bytearray):
        return bytes(value).decode("utf-8")
    return str(value)


@dataclass(frozen=True, slots=True)
class ArrowCoercionPolicy:
    """Configuration policy controlling scalar and container PyArrow coercion."""

    default_str_encoding: DefaultStrEncoding = "base64"
    allow_tuple: bool = False
    string_coercer: Callable[[object], str] = coerce_string
    preprocess_value: Callable[[object, pa.DataType], object] | None = None


JSON_COERCION_POLICY: Final[ArrowCoercionPolicy] = ArrowCoercionPolicy()


def coerce_scalar_for_pyarrow(
    value: object,
    data_type: pa.DataType,
    metadata: Mapping[bytes, bytes] | None = None,
    *,
    policy: ArrowCoercionPolicy = JSON_COERCION_POLICY,
) -> object:
    """Coerce a scalar value to match ``data_type`` under ``policy``.

    :param value: Input scalar value.
    :type value: object
    :param data_type: Target PyArrow scalar data type.
    :type data_type: pa.DataType
    :param metadata: Optional PyArrow field metadata mapping.
    :type metadata: Mapping[bytes, bytes] | None
    :param policy: Scalar and container coercion policy.
    :type policy: ArrowCoercionPolicy
    :return: Coerced scalar value matching ``data_type``.
    :rtype: object
    """
    if isinstance(value, Enum):
        value = value.value
    if pa.types.is_date(data_type) or pa.types.is_timestamp(data_type) or pa.types.is_time(data_type):
        return coerce_temporal(value, data_type)
    if isinstance(data_type, pa.Decimal128Type | pa.Decimal256Type):
        return coerce_decimal(value, data_type)
    if is_any_binary_type(data_type):
        return coerce_binary(value, metadata, default_str_encoding=policy.default_str_encoding)
    if pa.types.is_floating(data_type) or pa.types.is_integer(data_type):
        return coerce_numeric(value, data_type)
    if is_any_string_type(data_type):
        return policy.string_coercer(value)
    return value


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


def coerce_list_items(
    value: Iterable[Any],
    data_type: ArrowListType,
    coerce_child: ValueCoercer,
    *,
    sort_items: bool = False,
) -> list[object]:
    """Coerce an iterable of elements for any PyArrow list data type.

    :param value: Input sequence or set of items.
    :type value: Iterable[Any]
    :param data_type: Target PyArrow list data type variant.
    :type data_type: ArrowListType
    :param coerce_child: Child value coercion callback.
    :type coerce_child: ValueCoercer
    :param sort_items: Whether to sort coerced elements with ``comparable_sort_key``.
    :type sort_items: bool
    :return: Coerced list of elements.
    :rtype: list[object]
    """
    item_field = data_type.value_field
    coerced = [coerce_child(item, item_field.type, item_field.metadata) for item in value]
    return sorted(coerced, key=comparable_sort_key) if sort_items else coerced


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


def coerce_value_for_pyarrow(
    value: object,
    data_type: pa.DataType,
    metadata: Mapping[bytes, bytes] | None = None,
    *,
    policy: ArrowCoercionPolicy = JSON_COERCION_POLICY,
) -> object:
    """Coerce a value across nested structs, lists, maps, and scalars under ``policy``.

    :param value: Input Python or JSON-parsed value.
    :type value: object
    :param data_type: Target PyArrow data type.
    :type data_type: pa.DataType
    :param metadata: Optional PyArrow field metadata mapping.
    :type metadata: Mapping[bytes, bytes] | None
    :param policy: Scalar and container coercion policy.
    :type policy: ArrowCoercionPolicy
    :return: Coerced value matching ``data_type``.
    :rtype: object
    """
    if policy.preprocess_value is not None:
        value = policy.preprocess_value(value, data_type)
    if value is None:
        return None
    child_coercer = functools.partial(coerce_value_for_pyarrow, policy=policy)
    if isinstance(data_type, pa.StructType):
        return coerce_struct_from_mapping_or_tuple(value, data_type, child_coercer, allow_tuple=policy.allow_tuple)
    list_input_types = PYTHON_SEQUENCE_OR_SET_TYPES if policy.allow_tuple else list
    if is_any_list_type(data_type) and isinstance(data_type, ARROW_LIST_TYPES) and isinstance(value, list_input_types):
        return coerce_list_items(value, data_type, child_coercer, sort_items=isinstance(value, set | frozenset))
    if isinstance(data_type, pa.MapType) and isinstance(value, Mapping):
        return coerce_map_entries(value, data_type, child_coercer)
    return coerce_scalar_for_pyarrow(value, data_type, metadata, policy=policy)
