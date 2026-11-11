"""Coerce Pydantic v2 BaseModel instances and record dictionaries to match a target PyArrow schema."""

import base64
import json
from collections import deque
from collections.abc import Iterable, Mapping
from dataclasses import fields as dc_fields
from dataclasses import is_dataclass
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal
from enum import Enum
from ipaddress import (
    IPv4Address,
    IPv4Interface,
    IPv4Network,
    IPv6Address,
    IPv6Interface,
    IPv6Network,
)
from pathlib import PurePath
from typing import Any, Final
from uuid import UUID

import pyarrow as pa
from pydantic import (
    AnyUrl,
    BaseModel,
    NameEmail,
    RootModel,
    SecretBytes,
    SecretStr,
    TypeAdapter,
)
from pydantic_core import Url

IP_CLASSES: Final[tuple[type, ...]] = (
    IPv4Address,
    IPv4Interface,
    IPv4Network,
    IPv6Address,
    IPv6Interface,
    IPv6Network,
)
TIMEDELTA_ADAPTER: Final[TypeAdapter[timedelta]] = TypeAdapter(timedelta)


def _extract_model_dict(instance: BaseModel, *, by_alias: bool = False) -> dict[str, Any]:
    """Extract active fields and computed fields from a ``BaseModel`` instance into a dictionary.

    :param instance: Pydantic ``BaseModel`` instance to extract fields from.
    :type instance: BaseModel
    :param by_alias: Whether to use ``serialization_alias`` or ``alias`` for field names.
    :type by_alias: bool
    :return: Dictionary of extracted field values.
    :rtype: dict[str, Any]
    """
    if isinstance(instance, RootModel):
        inner = instance.root
        if isinstance(inner, BaseModel):
            return _extract_model_dict(inner, by_alias=by_alias)
        if isinstance(inner, Mapping):
            return dict(inner)
        return {"_value": inner}

    model_cls = type(instance)
    result: dict[str, Any] = {}
    fields_set = instance.model_fields_set

    for field_name, finfo in model_cls.model_fields.items():
        if finfo.exclude is True:
            continue
        val = getattr(instance, field_name)
        if field_name in fields_set or val is not None:
            out_name = (finfo.serialization_alias or finfo.alias or field_name) if by_alias else field_name
            result[out_name] = val

    for comp_name, comp_info in model_cls.model_computed_fields.items():
        if comp_info.repr is False and getattr(comp_info, "exclude", False):
            continue
        val = getattr(instance, comp_name)
        if val is not None:
            out_name = (comp_info.alias or comp_name) if by_alias else comp_name
            result[out_name] = val

    return result


def _extract_dataclass_dict(instance: object) -> dict[str, Any]:
    """Extract non-None fields from a stdlib dataclass instance into a dictionary.

    :param instance: Dataclass instance to extract fields from.
    :type instance: object
    :return: Dictionary of non-None field values.
    :rtype: dict[str, Any]
    """
    result: dict[str, Any] = {}
    for dc_f in dc_fields(instance):
        val = getattr(instance, dc_f.name)
        if val is not None:
            result[dc_f.name] = val
    return result


def _comparable_sort_key(value: object) -> tuple[bool, int, int | float | bytes | str]:
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


def _to_jsonable_scalar(value: object) -> object:
    """Convert non-container Python scalar objects to JSON-serializable scalars.

    :param value: Scalar Python object.
    :type value: object
    :return: JSON-serializable scalar value.
    :rtype: object
    """
    if isinstance(value, date | datetime | time):
        return value.isoformat()
    if isinstance(value, timedelta):
        return TIMEDELTA_ADAPTER.dump_python(value, mode="json")
    if isinstance(value, bytes | bytearray):
        return bytes(value).decode("utf-8", errors="replace")
    if isinstance(value, (Decimal, UUID, PurePath, AnyUrl, Url, NameEmail, *IP_CLASSES)):
        return str(value)
    return value


def _to_jsonable_recursive(value: object, *, by_alias: bool = False) -> object:
    """Convert nested BaseModels and Python objects into JSON-serializable structures for recursion cutoffs.

    :param value: Input Python object or nested ``BaseModel``.
    :type value: object
    :param by_alias: Whether to use ``serialization_alias`` or ``alias`` when extracting models.
    :type by_alias: bool
    :return: JSON-serializable representation of ``value``.
    :rtype: object
    """
    while isinstance(value, RootModel | Enum | SecretStr | SecretBytes):
        if isinstance(value, RootModel):
            value = value.root
        elif isinstance(value, Enum):
            value = value.value
        else:
            value = value.get_secret_value()

    if isinstance(value, BaseModel) or (is_dataclass(value) and not isinstance(value, type)):
        raw_dict = (
            _extract_model_dict(value, by_alias=by_alias)
            if isinstance(value, BaseModel)
            else _extract_dataclass_dict(value)
        )
        return {k: _to_jsonable_recursive(v, by_alias=by_alias) for k, v in raw_dict.items()}
    if isinstance(value, Mapping):
        return {str(k): _to_jsonable_recursive(v, by_alias=by_alias) for k, v in value.items()}
    if isinstance(value, set | frozenset):
        items = [_to_jsonable_recursive(v, by_alias=by_alias) for v in value]
        return sorted(items, key=_comparable_sort_key)
    if isinstance(value, list | tuple | deque):
        return [_to_jsonable_recursive(v, by_alias=by_alias) for v in value]
    return _to_jsonable_scalar(value)


def _coerce_timestamp(value: object, data_type: pa.TimestampType) -> object:
    """Coerce a ``datetime`` or ISO-8601 string to match a tz-aware or tz-naive ``pa.TimestampType``.

    :param value: Input ``datetime`` or ISO-8601 timestamp string.
    :type value: object
    :param data_type: Target PyArrow timestamp type.
    :type data_type: pa.TimestampType
    :return: Coerced ``datetime`` with matching timezone awareness.
    :rtype: object
    """
    parsed = datetime.fromisoformat(value) if isinstance(value, str) else value
    if not isinstance(parsed, datetime):
        return value
    if data_type.tz is not None:
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=UTC)
        return parsed.astimezone(UTC)
    if parsed.tzinfo is not None:
        return parsed.astimezone(UTC).replace(tzinfo=None)
    return parsed


def _coerce_time(value: object) -> object:
    """Coerce a ``time`` or ISO-8601 string to a naive ``datetime.time`` for ``pa.time64('us')``.

    :param value: Input ``time`` or ISO-8601 time string.
    :type value: object
    :return: Naive ``datetime.time`` instance.
    :rtype: object
    """
    parsed = time.fromisoformat(value) if isinstance(value, str) else value
    if isinstance(parsed, time) and parsed.tzinfo is not None:
        return parsed.replace(tzinfo=None)
    return parsed


def _coerce_decimal(value: object, data_type: pa.Decimal128Type) -> Decimal:
    """Coerce a ``Decimal``, ``int``, ``float``, or ``str`` to a quantized ``Decimal``.

    :param value: Input numeric or string value.
    :type value: object
    :param data_type: Target PyArrow ``Decimal128Type`` defining scale.
    :type data_type: pa.Decimal128Type
    :return: Quantized ``Decimal`` value.
    :rtype: Decimal
    """
    dec = value if isinstance(value, Decimal) else Decimal(str(value))
    quantum = Decimal(10) ** (-data_type.scale)
    return dec.quantize(quantum)


def _coerce_binary(value: object, metadata: dict[bytes, bytes] | None) -> bytes:
    """Coerce ``bytes``, ``bytearray``, or ``str`` into ``bytes``.

    :param value: Input binary or string value.
    :type value: object
    :param metadata: Optional PyArrow field metadata dictionary.
    :type metadata: dict[bytes, bytes] | None
    :return: Coerced raw ``bytes``.
    :rtype: bytes
    """
    if isinstance(value, bytes):
        return value
    if isinstance(value, bytearray | memoryview):
        return bytes(value)
    if isinstance(value, str):
        if metadata and b"pydantic_type" in metadata and b"base64" in metadata[b"pydantic_type"]:
            return base64.b64decode(value)
        return value.encode("utf-8")
    return bytes(value)  # type: ignore[arg-type]


def _coerce_date(value: object) -> object:
    """Coerce a ``date``, ``datetime``, or ISO-8601 string to a ``datetime.date``.

    :param value: Input ``date``, ``datetime``, or ISO-8601 date string.
    :type value: object
    :return: Coerced ``datetime.date`` instance.
    :rtype: object
    """
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, str):
        return date.fromisoformat(value)
    return value


def _coerce_numeric(value: object, data_type: pa.DataType) -> object:
    """Coerce an int, float, Decimal, or numeric string to match an integer or floating PyArrow DataType.

    :param value: Input numeric or string value.
    :type value: object
    :param data_type: Target PyArrow numeric data type.
    :type data_type: pa.DataType
    :return: Coerced ``int`` or ``float``.
    :rtype: object
    """
    if pa.types.is_floating(data_type):
        if isinstance(value, Decimal | int | str) and not isinstance(value, bool):
            return float(value)
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    if isinstance(value, int | str) and not isinstance(value, bool):
        return int(value)
    return value


def _coerce_string(value: object, *, by_alias: bool = False) -> str:
    """Coerce any Python value into its ``pa.string()`` representation.

    :param value: Input Python value.
    :type value: object
    :param by_alias: Whether to serialize nested models using field aliases.
    :type by_alias: bool
    :return: String representation suitable for ``pa.string()``.
    :rtype: str
    """
    if isinstance(value, str):
        return str(value)
    if isinstance(value, timedelta):
        return str(TIMEDELTA_ADAPTER.dump_python(value, mode="json"))
    if isinstance(value, BaseModel | Mapping | list | tuple | set | frozenset | deque | bool) or (
        is_dataclass(value) and not isinstance(value, type)
    ):
        return json.dumps(_to_jsonable_recursive(value, by_alias=by_alias))
    if isinstance(value, date | datetime | time):
        return value.isoformat()
    if isinstance(value, bytes | bytearray):
        return bytes(value).decode("utf-8")
    return str(value)


def _coerce_temporal(value: object, data_type: pa.DataType) -> object:
    """Coerce a date, time, or timestamp value to match a temporal PyArrow DataType.

    :param value: Input temporal or string value.
    :type value: object
    :param data_type: Target PyArrow temporal data type.
    :type data_type: pa.DataType
    :return: Coerced temporal Python object.
    :rtype: object
    """
    if pa.types.is_date(data_type):
        return _coerce_date(value)
    if pa.types.is_timestamp(data_type):
        return _coerce_timestamp(value, data_type)
    return _coerce_time(value)


def _coerce_scalar_for_pyarrow(
    value: object,
    data_type: pa.DataType,
    metadata: dict[bytes, bytes] | None = None,
    *,
    by_alias: bool = False,
) -> object:
    """Coerce a Python or JSON-parsed scalar value to match a PyArrow scalar DataType.

    :param value: Input scalar value.
    :type value: object
    :param data_type: Target PyArrow scalar data type.
    :type data_type: pa.DataType
    :param metadata: Optional PyArrow field metadata dictionary.
    :type metadata: dict[bytes, bytes] | None
    :param by_alias: Whether to serialize nested models using field aliases.
    :type by_alias: bool
    :return: Coerced scalar value matching ``data_type``.
    :rtype: object
    """
    if isinstance(value, Enum):
        value = value.value
    if isinstance(value, SecretStr | SecretBytes):
        value = value.get_secret_value()

    if pa.types.is_date(data_type) or pa.types.is_timestamp(data_type) or pa.types.is_time(data_type):
        return _coerce_temporal(value, data_type)
    if pa.types.is_decimal(data_type):
        return _coerce_decimal(value, data_type)
    if pa.types.is_binary(data_type):
        return _coerce_binary(value, metadata)
    if pa.types.is_floating(data_type) or pa.types.is_integer(data_type):
        return _coerce_numeric(value, data_type)
    if pa.types.is_string(data_type) or pa.types.is_large_string(data_type):
        return _coerce_string(value, by_alias=by_alias)
    return value


def _coerce_struct_for_pyarrow(
    value: object,
    data_type: pa.StructType,
    *,
    by_alias: bool = False,
) -> object:
    """Coerce a ``BaseModel``, ``Mapping``, positional tuple, or scalar union value to match a ``pa.StructType``.

    :param value: Input ``BaseModel``, mapping, sequence, or scalar value.
    :type value: object
    :param data_type: Target PyArrow struct data type.
    :type data_type: pa.StructType
    :param by_alias: Whether to extract model fields using aliases.
    :type by_alias: bool
    :return: Coerced struct dictionary or pass-through value.
    :rtype: object
    """
    if isinstance(value, RootModel):
        value = value.root
    if isinstance(value, BaseModel):
        value = _extract_model_dict(value, by_alias=by_alias)
    elif is_dataclass(value) and not isinstance(value, type):
        value = _extract_dataclass_dict(value)
    if isinstance(value, Mapping):
        return {
            field.name: _coerce_value_for_pyarrow(
                value[field.name],
                field.type,
                field.metadata,
                by_alias=by_alias,
            )
            for field in data_type
            if field.name in value
        }
    if isinstance(value, list | tuple) and len(data_type) > 0 and data_type[0].name == "item_0":
        return {
            field.name: _coerce_value_for_pyarrow(
                value[idx],
                field.type,
                field.metadata,
                by_alias=by_alias,
            )
            for idx, field in enumerate(data_type)
            if idx < len(value) and value[idx] is not None
        }
    value_field = next((field for field in data_type if field.name == "_value"), None)
    if value_field is not None:
        return {
            "_value": _coerce_value_for_pyarrow(
                value,
                value_field.type,
                value_field.metadata,
                by_alias=by_alias,
            )
        }
    return value


def _coerce_list_for_pyarrow(
    value: Iterable[Any],
    data_type: pa.ListType | pa.LargeListType,
    *,
    by_alias: bool = False,
) -> list[Any]:
    """Coerce a sequence or set into a list matching ``data_type``.

    :param value: Input iterable or set.
    :type value: Iterable[Any]
    :param data_type: Target PyArrow list data type.
    :type data_type: pa.ListType | pa.LargeListType
    :param by_alias: Whether to extract nested model fields using aliases.
    :type by_alias: bool
    :return: Coerced list of items.
    :rtype: list[Any]
    """
    item_field = data_type.value_field
    coerced = [
        _coerce_value_for_pyarrow(item, item_field.type, item_field.metadata, by_alias=by_alias) for item in value
    ]
    if isinstance(value, set | frozenset):
        return sorted(coerced, key=_comparable_sort_key)
    return coerced


def _coerce_value_for_pyarrow(
    value: object,
    data_type: pa.DataType,
    metadata: dict[bytes, bytes] | None = None,
    *,
    by_alias: bool = False,
) -> object:
    """Recursively coerce a Python or JSON-parsed value to match the target PyArrow DataType.

    :param value: Input Python or JSON-parsed value.
    :type value: object
    :param data_type: Target PyArrow data type.
    :type data_type: pa.DataType
    :param metadata: Optional PyArrow field metadata dictionary.
    :type metadata: dict[bytes, bytes] | None
    :param by_alias: Whether to extract nested model fields using aliases.
    :type by_alias: bool
    :return: Coerced value matching ``data_type``.
    :rtype: object
    """
    if isinstance(value, RootModel):
        value = value.root
    if value is None:
        return None
    if pa.types.is_struct(data_type):
        return _coerce_struct_for_pyarrow(value, data_type, by_alias=by_alias)
    if (pa.types.is_list(data_type) or pa.types.is_large_list(data_type)) and isinstance(
        value, list | tuple | set | frozenset | deque
    ):
        return _coerce_list_for_pyarrow(value, data_type, by_alias=by_alias)
    if pa.types.is_map(data_type) and isinstance(value, Mapping):
        key_field = data_type.key_field
        item_field = data_type.item_field
        return {
            _coerce_value_for_pyarrow(k, key_field.type, key_field.metadata, by_alias=by_alias): (
                _coerce_value_for_pyarrow(v, item_field.type, item_field.metadata, by_alias=by_alias)
            )
            for k, v in value.items()
        }
    return _coerce_scalar_for_pyarrow(value, data_type, metadata, by_alias=by_alias)


def coerce_record_for_pyarrow(
    record: BaseModel | Mapping[str, Any],
    schema: pa.Schema,
    *,
    model: type[BaseModel] | None = None,
    by_alias: bool = False,
) -> dict[str, Any]:
    """Coerce a Pydantic ``BaseModel`` instance or record mapping to match a PyArrow schema.

    When ``record`` is a ``BaseModel`` instance (or ``model`` is supplied to validate a mapping),
    non-null default values and ``@computed_field`` properties are populated on the output dictionary.
    Only fields declared in ``schema`` are retained and coerced recursively so
    ``pyarrow.Table.from_pylist`` can construct a table matching ``schema``.

    :param record: Pydantic ``BaseModel`` instance or record dictionary.
    :type record: BaseModel | Mapping[str, Any]
    :param schema: Target PyArrow schema for the record's table.
    :type schema: pa.Schema
    :param model: Optional ``BaseModel`` class used to validate ``record`` when ``record`` is a mapping.
    :type model: type[BaseModel] | None
    :param by_alias: When ``True``, extract model fields using ``serialization_alias`` or ``alias``.
    :type by_alias: bool
    :return: Coerced record dictionary ready for ``DictBuffer`` or ``pa.Table.from_pylist``.
    :rtype: dict[str, Any]
    """
    if isinstance(record, BaseModel):
        source_map: Mapping[str, Any] = _extract_model_dict(record, by_alias=by_alias)
    elif model is not None:
        validated = model.model_validate(dict(record))
        source_map = _extract_model_dict(validated, by_alias=by_alias)
    else:
        source_map = record

    return {
        field.name: _coerce_value_for_pyarrow(
            source_map[field.name],
            field.type,
            field.metadata,
            by_alias=by_alias,
        )
        for field in schema
        if field.name in source_map
    }
