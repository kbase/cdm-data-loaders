"""Convert Pydantic v2 BaseModel schemas to PyArrow schemas and coerce records for Arrow/Parquet."""

import base64
import json
from collections import deque
from collections.abc import Iterable, Mapping
from dataclasses import fields as dc_fields
from dataclasses import is_dataclass
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal
from enum import Enum
from pathlib import Path, PurePath
from typing import Any, Final
from uuid import UUID

import pyarrow as pa
from pydantic import (
    AnyUrl,
    BaseModel,
    ConfigDict,
    Field,
    NameEmail,
    PositiveInt,
    RootModel,
    SecretBytes,
    SecretStr,
    TypeAdapter,
)
from pydantic_core import Url

from cdm_data_loaders.converters.emitters.pyarrow import PyArrowEmitter
from cdm_data_loaders.converters.readers.pydantic import (
    DEFAULT_MAX_RECURSION_DEPTH,
    IP_CLASSES,
    PYDANTIC_TO_PYARROW_ERRORS,
    CyclicTypeAliasError,
    EmptyModelError,
    IncompatibleUnionBranchesError,
    InvalidDecimalConstraintsError,
    InvalidPydanticModelError,
    NonStringMapKeyError,
    NonStructRootError,
    NullOnlyFieldError,
    OpenTupleError,
    PydanticReader,
    PydanticToPyArrowError,
    RecursiveNonJsonTypeError,
    UnconstrainedFieldError,
    UnresolvedForwardRefError,
    UnsupportedFieldTypeError,
    ValueFieldCollisionError,
)

TIMEDELTA_ADAPTER: Final[TypeAdapter[timedelta]] = TypeAdapter(timedelta)
_DEFAULT_EMITTER: Final[PyArrowEmitter] = PyArrowEmitter(max_nesting=100)


def resolve_scalar_type(annotation: Any) -> tuple[pa.DataType, str]:  # noqa: ANN401
    """Map a scalar Python / Pydantic type annotation to its PyArrow DataType and ``pydantic_type`` label.

    :param annotation: scalar type annotation (e.g. ``int``, ``AwareDatetime``, ``Annotated[Decimal, Field(...)]``)
    :type annotation: Any
    :return: ``(pa.DataType, pydantic_type)``
    :rtype: tuple[pa.DataType, str]
    :raises PydanticToPyArrowError: if ``annotation`` is not a supported scalar type
    """
    node = PydanticReader().read_scalar_annotation(annotation)
    dtype = _DEFAULT_EMITTER.emit_node(node)
    pyd_type = str(node.provenance.metadata["pydantic_type"]) if node.provenance else ""
    return dtype, pyd_type


def generate_pyarrow_schema(
    model: type[BaseModel],
    *,
    max_recursion_depth: int = DEFAULT_MAX_RECURSION_DEPTH,
    by_alias: bool = False,
) -> pa.Schema:
    """Generate a deterministic PyArrow Schema from a Pydantic v2 ``BaseModel`` subclass.

    :param model: concrete Pydantic ``BaseModel`` subclass
    :type model: type[BaseModel]
    :param max_recursion_depth: maximum expansion depth for recursive ``BaseModel`` references
    :type max_recursion_depth: int
    :param by_alias: when ``True``, use ``serialization_alias`` or ``alias`` for field names
    :type by_alias: bool
    :return: PyArrow schema with top-level fields corresponding to model fields
    :rtype: pa.Schema
    :raises PydanticToPyArrowError: if ``model`` is invalid or uses an unsupported construct
    """
    doc = PydanticReader(max_recursion_depth=max_recursion_depth, by_alias=by_alias).read(model)
    return _DEFAULT_EMITTER.emit(doc)


def _extract_model_dict(instance: BaseModel, *, by_alias: bool = False) -> dict[str, Any]:
    """Extract active fields and computed fields from a ``BaseModel`` instance into a dictionary."""
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


def _extract_dataclass_dict(instance: Any) -> dict[str, Any]:  # noqa: ANN401
    """Extract non-None fields from a stdlib dataclass instance into a dictionary."""
    result: dict[str, Any] = {}
    for dc_f in dc_fields(instance):
        val = getattr(instance, dc_f.name)
        if val is not None:
            result[dc_f.name] = val
    return result


def _comparable_sort_key(value: Any) -> tuple[bool, Any]:  # noqa: ANN401
    """Return a deterministic sort key for set elements during record coercion."""
    return (value is None, value if isinstance(value, int | float | str | bytes) else str(value))


def _to_jsonable_scalar(value: Any) -> Any:  # noqa: ANN401
    """Convert non-container Python scalar objects to JSON-serializable scalars."""
    if isinstance(value, date | datetime | time):
        return value.isoformat()
    if isinstance(value, timedelta):
        return TIMEDELTA_ADAPTER.dump_python(value, mode="json")
    if isinstance(value, bytes | bytearray):
        return bytes(value).decode("utf-8", errors="replace")
    if isinstance(value, (Decimal, UUID, PurePath, AnyUrl, Url, NameEmail, *IP_CLASSES)):
        return str(value)
    return value


def _to_jsonable_recursive(value: Any, *, by_alias: bool = False) -> Any:  # noqa: ANN401
    """Convert nested BaseModels and Python objects into JSON-serializable structures for recursion cutoffs."""
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


def _coerce_timestamp(value: Any, data_type: pa.TimestampType) -> Any:  # noqa: ANN401
    """Coerce a ``datetime`` or ISO-8601 string to match a tz-aware or tz-naive ``pa.TimestampType``."""
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


def _coerce_time(value: Any) -> Any:  # noqa: ANN401
    """Coerce a ``time`` or ISO-8601 string to a naive ``datetime.time`` for ``pa.time64('us')``."""
    parsed = time.fromisoformat(value) if isinstance(value, str) else value
    if isinstance(parsed, time) and parsed.tzinfo is not None:
        return parsed.replace(tzinfo=None)
    return parsed


def _coerce_decimal(value: Any, data_type: pa.Decimal128Type) -> Decimal:  # noqa: ANN401
    """Coerce a ``Decimal``, ``int``, ``float``, or ``str`` to a quantized ``Decimal``."""
    dec = value if isinstance(value, Decimal) else Decimal(str(value))
    quantum = Decimal(10) ** (-data_type.scale)
    return dec.quantize(quantum)


def _coerce_binary(value: Any, metadata: dict[bytes, bytes] | None) -> bytes:  # noqa: ANN401
    """Coerce ``bytes``, ``bytearray``, or ``str`` into ``bytes``."""
    if isinstance(value, bytes):
        return value
    if isinstance(value, bytearray | memoryview):
        return bytes(value)
    if isinstance(value, str):
        if metadata and b"pydantic_type" in metadata and b"base64" in metadata[b"pydantic_type"]:
            return base64.b64decode(value)
        return value.encode("utf-8")
    return bytes(value)


def _coerce_date(value: Any) -> Any:  # noqa: ANN401
    """Coerce a ``date``, ``datetime``, or ISO-8601 string to a ``datetime.date``."""
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, str):
        return date.fromisoformat(value)
    return value


def _coerce_numeric(value: Any, data_type: pa.DataType) -> Any:  # noqa: ANN401
    """Coerce an int, float, Decimal, or numeric string to match an integer or floating PyArrow DataType."""
    if pa.types.is_floating(data_type):
        if isinstance(value, Decimal | int | str) and not isinstance(value, bool):
            return float(value)
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    if isinstance(value, int | str) and not isinstance(value, bool):
        return int(value)
    return value


def _coerce_string(value: Any, *, by_alias: bool = False) -> str:  # noqa: ANN401
    """Coerce any Python value into its ``pa.string()`` representation."""
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


def _coerce_temporal(value: Any, data_type: pa.DataType) -> Any:  # noqa: ANN401
    """Coerce a date, time, or timestamp value to match a temporal PyArrow DataType."""
    if pa.types.is_date(data_type):
        return _coerce_date(value)
    if pa.types.is_timestamp(data_type):
        return _coerce_timestamp(value, data_type)
    return _coerce_time(value)


def _coerce_scalar_for_pyarrow(
    value: Any,  # noqa: ANN401
    data_type: pa.DataType,
    metadata: dict[bytes, bytes] | None = None,
    *,
    by_alias: bool = False,
) -> Any:  # noqa: ANN401
    """Coerce a Python or JSON-parsed scalar value to match a PyArrow scalar DataType."""
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
    value: Any,  # noqa: ANN401
    data_type: pa.StructType,
    *,
    by_alias: bool = False,
) -> Any:  # noqa: ANN401
    """Coerce a ``BaseModel``, ``Mapping``, positional tuple, or scalar union value to match a ``pa.StructType``."""
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
    """Coerce a sequence or set into a list matching ``data_type``."""
    item_field = data_type.value_field
    coerced = [
        _coerce_value_for_pyarrow(item, item_field.type, item_field.metadata, by_alias=by_alias) for item in value
    ]
    if isinstance(value, set | frozenset):
        return sorted(coerced, key=_comparable_sort_key)
    return coerced


def _coerce_value_for_pyarrow(
    value: Any,  # noqa: ANN401
    data_type: pa.DataType,
    metadata: dict[bytes, bytes] | None = None,
    *,
    by_alias: bool = False,
) -> Any:  # noqa: ANN401
    """Recursively coerce a Python or JSON-parsed value to match the target PyArrow DataType."""
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

    :param record: Pydantic ``BaseModel`` instance or record dictionary
    :type record: BaseModel | Mapping[str, Any]
    :param schema: target PyArrow schema for the record's table
    :type schema: pa.Schema
    :param model: optional ``BaseModel`` class used to validate ``record`` when ``record`` is a mapping
    :type model: type[BaseModel] | None
    :param by_alias: when ``True``, extract model fields using ``serialization_alias`` or ``alias``
    :type by_alias: bool
    :return: coerced record dictionary ready for ``DictBuffer`` or ``pa.Table.from_pylist``
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


coerce_model_for_pyarrow = coerce_record_for_pyarrow


class PydanticToPyArrow(BaseModel):
    """Facade converting Pydantic v2 ``BaseModel`` classes to PyArrow schemas and empty tables."""

    model_config = ConfigDict(arbitrary_types_allowed=True, frozen=True)

    max_recursion_depth: PositiveInt = Field(default=DEFAULT_MAX_RECURSION_DEPTH)
    by_alias: bool = False

    def convert(self, model: type[BaseModel]) -> pa.Schema:
        """Convert a Pydantic ``BaseModel`` subclass to a ``pa.Schema``.

        :param model: concrete Pydantic ``BaseModel`` subclass
        :type model: type[BaseModel]
        :return: converted PyArrow schema
        :rtype: pa.Schema
        """
        doc = PydanticReader(max_recursion_depth=self.max_recursion_depth, by_alias=self.by_alias).read(model)
        return _DEFAULT_EMITTER.emit(doc)

    def convert_to_table(self, model: type[BaseModel]) -> pa.Table:
        """Convert a Pydantic ``BaseModel`` subclass to a zero-row ``pa.Table``.

        :param model: concrete Pydantic ``BaseModel`` subclass
        :type model: type[BaseModel]
        :return: zero-row PyArrow table with the converted schema
        :rtype: pa.Table
        """
        doc = PydanticReader(max_recursion_depth=self.max_recursion_depth, by_alias=self.by_alias).read(model)
        return _DEFAULT_EMITTER.emit_table(doc)

    def convert_to_python(self, model: type[BaseModel], *, docstring: str | None = None) -> str:
        """Convert a Pydantic ``BaseModel`` subclass to a static Python schema module.

        :param model: concrete Pydantic ``BaseModel`` subclass
        :type model: type[BaseModel]
        :param docstring: optional module docstring header
        :type docstring: str | None
        :return: formatted Python source code defining ``*_STRUCT``, ``*_SCHEMA``, ``SCHEMA``, and ``SCHEMAS``
        :rtype: str
        """
        doc = PydanticReader(max_recursion_depth=self.max_recursion_depth, by_alias=self.by_alias).read(model)
        return _DEFAULT_EMITTER.emit_python(doc, docstring=docstring)

    def convert_to_python_file(
        self,
        model: type[BaseModel],
        output: str | Path,
        *,
        docstring: str | None = None,
    ) -> Path:
        """Convert a Pydantic ``BaseModel`` subclass and write a static Python schema module to ``output``.

        :param model: concrete Pydantic ``BaseModel`` subclass
        :type model: type[BaseModel]
        :param output: destination file path for the generated Python module
        :type output: str | Path
        :param docstring: optional module docstring header
        :type docstring: str | None
        :return: resolved output ``Path``
        :rtype: Path
        """
        doc = PydanticReader(max_recursion_depth=self.max_recursion_depth, by_alias=self.by_alias).read(model)
        return _DEFAULT_EMITTER.emit_python_file(doc, output, docstring=docstring)


__all__ = [
    "DEFAULT_MAX_RECURSION_DEPTH",
    "PYDANTIC_TO_PYARROW_ERRORS",
    "CyclicTypeAliasError",
    "EmptyModelError",
    "IncompatibleUnionBranchesError",
    "InvalidDecimalConstraintsError",
    "InvalidPydanticModelError",
    "NonStringMapKeyError",
    "NonStructRootError",
    "NullOnlyFieldError",
    "OpenTupleError",
    "PydanticToPyArrow",
    "PydanticToPyArrowError",
    "RecursiveNonJsonTypeError",
    "UnconstrainedFieldError",
    "UnresolvedForwardRefError",
    "UnsupportedFieldTypeError",
    "ValueFieldCollisionError",
    "coerce_model_for_pyarrow",
    "coerce_record_for_pyarrow",
    "generate_pyarrow_schema",
    "resolve_scalar_type",
]
