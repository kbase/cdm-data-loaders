"""Coerce Pydantic v2 BaseModel instances and record mappings to match a target PyArrow schema."""

import functools
import json
from collections import deque
from collections.abc import Mapping
from dataclasses import fields as dc_fields
from dataclasses import is_dataclass
from datetime import date, datetime, time, timedelta
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

from cdm_data_loaders.coercion._arrow_core import (
    ArrowCoercionPolicy,
    coerce_string,
    coerce_value_for_pyarrow,
    comparable_sort_key,
)
from cdm_data_loaders.utils.json_utils import json_default_scalar

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


def _to_jsonable_scalar(value: object) -> object:
    """Convert non-container Python scalar objects to JSON-serializable scalars.

    :param value: Scalar Python object.
    :type value: object
    :return: JSON-serializable scalar value.
    :rtype: object
    """
    if isinstance(value, timedelta):
        return TIMEDELTA_ADAPTER.dump_python(value, mode="json")
    if isinstance(value, (AnyUrl, Url, NameEmail, *IP_CLASSES)):
        return str(value)
    if isinstance(value, date | datetime | time | Decimal | UUID | PurePath | bytes | bytearray):
        return json_default_scalar(value, bytes_encoding="utf-8-replace")
    return value


def to_jsonable_recursive(value: object, *, by_alias: bool = False) -> object:
    """Convert nested ``BaseModel`` instances and Python objects into JSON-serializable structures.

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
        return {k: to_jsonable_recursive(v, by_alias=by_alias) for k, v in raw_dict.items()}
    if isinstance(value, Mapping):
        return {str(k): to_jsonable_recursive(v, by_alias=by_alias) for k, v in value.items()}
    if isinstance(value, set | frozenset):
        items = [to_jsonable_recursive(v, by_alias=by_alias) for v in value]
        return sorted(items, key=comparable_sort_key)
    if isinstance(value, list | tuple | deque):
        return [to_jsonable_recursive(v, by_alias=by_alias) for v in value]
    return _to_jsonable_scalar(value)


def _coerce_pydantic_string(value: object, *, by_alias: bool = False) -> str:
    """Coerce a Pydantic or Python value into a string for string Arrow columns.

    :param value: Input Python or Pydantic value.
    :type value: object
    :param by_alias: Whether to serialize nested models using field aliases.
    :type by_alias: bool
    :return: String representation suitable for string Arrow columns.
    :rtype: str
    """
    if isinstance(value, timedelta):
        return str(TIMEDELTA_ADAPTER.dump_python(value, mode="json"))
    if isinstance(value, BaseModel | Mapping | list | tuple | set | frozenset | deque | bool) or (
        is_dataclass(value) and not isinstance(value, type)
    ):
        return json.dumps(to_jsonable_recursive(value, by_alias=by_alias))
    return coerce_string(value)


def _preprocess_pydantic_value(
    value: object,
    data_type: pa.DataType,
    *,
    by_alias: bool = False,
) -> object:
    """Unwrap ``RootModel``, ``BaseModel``, dataclass, ``Enum``, and secret wrappers before Arrow coercion.

    :param value: Input Python or Pydantic value.
    :type value: object
    :param data_type: Target PyArrow data type.
    :type data_type: pa.DataType
    :param by_alias: Whether to extract model fields using aliases.
    :type by_alias: bool
    :return: Unwrapped value ready for structural or scalar Arrow coercion.
    :rtype: object
    """
    while isinstance(value, RootModel):
        value = value.root
    if value is None:
        return None
    if isinstance(data_type, pa.StructType):
        if isinstance(value, BaseModel):
            return _extract_model_dict(value, by_alias=by_alias)
        if is_dataclass(value) and not isinstance(value, type):
            return _extract_dataclass_dict(value)
        return value
    while isinstance(value, Enum | SecretStr | SecretBytes):
        value = value.value if isinstance(value, Enum) else value.get_secret_value()
    return value


PYDANTIC_DEFAULT_POLICY: Final[ArrowCoercionPolicy] = ArrowCoercionPolicy(
    default_str_encoding="utf-8",
    allow_tuple=True,
    string_coercer=_coerce_pydantic_string,
    preprocess_value=_preprocess_pydantic_value,
)
PYDANTIC_BY_ALIAS_POLICY: Final[ArrowCoercionPolicy] = ArrowCoercionPolicy(
    default_str_encoding="utf-8",
    allow_tuple=True,
    string_coercer=functools.partial(_coerce_pydantic_string, by_alias=True),
    preprocess_value=functools.partial(_preprocess_pydantic_value, by_alias=True),
)


def coerce_pydantic_record_for_pyarrow(
    record: BaseModel | Mapping[str, Any],
    schema: pa.Schema,
    *,
    model: type[BaseModel] | None = None,
    by_alias: bool = False,
) -> dict[str, Any]:
    """Coerce a Pydantic ``BaseModel`` instance or record mapping to match a PyArrow schema.

    When ``record`` is a ``BaseModel`` instance (or ``model`` is supplied to validate a mapping),
    non-null default values and ``@computed_field`` properties are populated on the output dictionary.
    Retains and coerces fields declared in ``schema`` so ``pyarrow.Table.from_pylist``
    can construct a table matching ``schema``.

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

    policy = PYDANTIC_BY_ALIAS_POLICY if by_alias else PYDANTIC_DEFAULT_POLICY
    return {
        field.name: coerce_value_for_pyarrow(
            source_map[field.name],
            field.type,
            field.metadata,
            policy=policy,
        )
        for field in schema
        if field.name in source_map
    }
