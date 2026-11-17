"""Serialize PyArrow tables and row mappings into JSON Schema and LinkML dictionaries."""

import base64
import math
from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any, Final

import numpy as np
import pyarrow as pa

from cdm_data_loaders.utils.arrow_schema import is_any_binary_type, is_any_string_type

if TYPE_CHECKING:
    from datetime import date, time
    from decimal import Decimal

NANOS_PER_MICRO: Final[int] = 1_000
NANOS_PER_SECOND: Final[int] = 1_000_000_000
SECONDS_PER_MINUTE: Final[int] = 60
SECONDS_PER_HOUR: Final[int] = 3_600
PAIR_LENGTH: Final[int] = 2


def _format_time_scalar(scalar: pa.Scalar, dtype: pa.DataType) -> str:
    """Format a PyArrow ``Time32Scalar`` or ``Time64Scalar`` as an ISO-8601 time string.

    Preserves 9-digit nanosecond resolution when ``time64('ns')`` has sub-microsecond ticks.

    :param scalar: Non-null PyArrow time scalar.
    :type scalar: pa.Scalar
    :param dtype: PyArrow time data type.
    :type dtype: pa.DataType
    :return: ISO-8601 time string (``HH:MM:SS`` or ``HH:MM:SS.fffffffff``).
    :rtype: str
    """
    if isinstance(dtype, pa.Time64Type) and dtype.unit == "ns":
        raw_ns = int(scalar.value)
        if raw_ns % NANOS_PER_MICRO != 0:
            total_sec, frac_ns = divmod(raw_ns, NANOS_PER_SECOND)
            hours, rem = divmod(total_sec, SECONDS_PER_HOUR)
            minutes, seconds = divmod(rem, SECONDS_PER_MINUTE)
            frac_str = f"{frac_ns:09d}".rstrip("0")
            return f"{hours:02d}:{minutes:02d}:{seconds:02d}.{frac_str}"
    py_time: time = scalar.as_py()
    return py_time.isoformat()


def _serialize_list_scalar(
    scalar: pa.Scalar,
    dtype: pa.ListType | pa.LargeListType | pa.ListViewType | pa.LargeListViewType | pa.FixedSizeListType,
    *,
    for_linkml: bool,
) -> list[Any] | None:
    """Serialize a PyArrow list scalar into a JSON-compatible Python list.

    :param scalar: Non-null PyArrow list scalar.
    :type scalar: pa.Scalar
    :param dtype: List data type variant.
    :type dtype: pa.ListType | pa.LargeListType | pa.ListViewType | pa.LargeListViewType | pa.FixedSizeListType
    :param for_linkml: Whether to enforce LinkML non-null and flat-list constraints.
    :type for_linkml: bool
    :return: List of serialized items, or ``None`` if the array is null.
    :rtype: list[Any] | None
    :raises ValueError: On nested list or map items or null list elements when ``for_linkml`` is ``True``.
    """
    item_type = dtype.value_field.type
    if for_linkml and isinstance(
        item_type,
        pa.ListType | pa.LargeListType | pa.ListViewType | pa.LargeListViewType | pa.FixedSizeListType | pa.MapType,
    ):
        msg = f"LinkML multivalued slots do not support nested list or map items ({dtype})."
        raise ValueError(msg)
    items_array = scalar.values
    if items_array is None:
        return None
    out_items: list[Any] = []
    for idx in range(len(items_array)):
        item_scalar = items_array[idx]
        if for_linkml and not item_scalar.is_valid:
            msg = "LinkML multivalued slots do not permit null list elements."
            raise ValueError(msg)
        out_items.append(_serialize_arrow_scalar(item_scalar, item_type, for_linkml=for_linkml))
    return out_items


def _serialize_map_scalar(
    scalar: pa.Scalar,
    dtype: pa.MapType,
    *,
    for_linkml: bool,
) -> list[dict[str, object]] | None:
    """Serialize a PyArrow ``MapScalar`` into an array of ``{"key": ..., "value": ...}`` objects.

    :param scalar: Non-null PyArrow map scalar.
    :type scalar: pa.Scalar
    :param dtype: PyArrow map data type.
    :type dtype: pa.MapType
    :param for_linkml: Whether to enforce LinkML constraints inside map values.
    :type for_linkml: bool
    :return: List of key/value entry dictionaries, or ``None`` if null.
    :rtype: list[dict[str, object]] | None
    """
    entries_array = scalar.values
    if entries_array is None:
        return None
    key_type = dtype.key_field.type
    val_type = dtype.item_field.type
    return [
        {
            "key": _serialize_arrow_scalar(entries_array[idx]["key"], key_type, for_linkml=for_linkml),
            "value": _serialize_arrow_scalar(entries_array[idx]["value"], val_type, for_linkml=for_linkml),
        }
        for idx in range(len(entries_array))
    ]


def _serialize_temporal_or_text_scalar(scalar: pa.Scalar, dtype: pa.DataType) -> str:
    """Serialize a string, binary, or temporal PyArrow scalar to its canonical JSON string.

    :param scalar: Non-null PyArrow scalar.
    :type scalar: pa.Scalar
    :param dtype: PyArrow data type.
    :type dtype: pa.DataType
    :return: Canonical JSON string representation.
    :rtype: str
    :raises TypeError: If ``dtype`` is not a supported string, binary, or temporal type.
    """
    if is_any_string_type(dtype):
        return str(scalar.as_py())
    if is_any_binary_type(dtype):
        raw_bytes: bytes = scalar.as_py()
        return base64.b64encode(raw_bytes).decode("ascii")
    if pa.types.is_date(dtype):
        py_date: date = scalar.as_py()
        return py_date.isoformat()
    if pa.types.is_time(dtype):
        return _format_time_scalar(scalar, dtype)
    if pa.types.is_timestamp(dtype):
        return str(scalar.as_py().isoformat())
    msg = f"Unsupported PyArrow type during record serialization: {dtype}"
    raise TypeError(msg)


def _serialize_leaf_scalar(scalar: pa.Scalar, dtype: pa.DataType) -> object:
    """Serialize a non-null primitive PyArrow scalar to its JSON-compatible value.

    :param scalar: Non-null primitive PyArrow scalar.
    :type scalar: pa.Scalar
    :param dtype: Primitive PyArrow data type.
    :type dtype: pa.DataType
    :return: JSON-compatible primitive Python value.
    :rtype: object
    :raises ValueError: If a floating-point scalar is ``NaN`` or ``±Infinity``.
    """
    if pa.types.is_boolean(dtype):
        return bool(scalar.as_py())
    if pa.types.is_integer(dtype):
        return int(scalar.as_py())
    if pa.types.is_floating(dtype):
        fval = float(scalar.as_py())
        if math.isnan(fval) or math.isinf(fval):
            msg = f"Non-finite floating-point value {fval!r} cannot be serialized to JSON."
            raise ValueError(msg)
        return fval
    if pa.types.is_decimal(dtype):
        dec_val: Decimal = scalar.as_py()
        return format(dec_val, "f")
    return _serialize_temporal_or_text_scalar(scalar, dtype)


def _serialize_arrow_scalar(
    scalar: pa.Scalar,
    dtype: pa.DataType,
    *,
    for_linkml: bool,
) -> object:
    """Serialize a PyArrow ``Scalar`` to its JSON Schema or LinkML JSON-compatible representation.

    :param scalar: PyArrow scalar value to serialize.
    :type scalar: pa.Scalar
    :param dtype: Declared PyArrow data type of ``scalar``.
    :type dtype: pa.DataType
    :param for_linkml: When ``True``, enforce LinkML multivalued non-null list item rules.
    :type for_linkml: bool
    :return: JSON-compatible Python value.
    :rtype: object
    :raises ValueError: On non-finite floats or null list items in LinkML mode.
    """
    if not scalar.is_valid:
        return None
    if isinstance(dtype, pa.DictionaryType):
        dict_val: pa.Scalar | None = scalar.value
        return (
            None
            if dict_val is None or not dict_val.is_valid
            else _serialize_arrow_scalar(dict_val, dtype.value_type, for_linkml=for_linkml)
        )
    if isinstance(dtype, pa.StructType):
        return {
            child_field.name: _serialize_arrow_scalar(
                scalar[child_field.name],
                child_field.type,
                for_linkml=for_linkml,
            )
            for child_field in dtype
        }
    if isinstance(
        dtype,
        pa.ListType | pa.LargeListType | pa.ListViewType | pa.LargeListViewType | pa.FixedSizeListType,
    ):
        return _serialize_list_scalar(scalar, dtype, for_linkml=for_linkml)
    if isinstance(dtype, pa.MapType):
        return _serialize_map_scalar(scalar, dtype, for_linkml=for_linkml)
    return _serialize_leaf_scalar(scalar, dtype)


def _normalize_map_for_arrow_table(value: object, dtype: pa.MapType) -> object:
    """Normalize a Python dict or sequence of map entries into ``(key, value)`` pairs for PyArrow.

    :param value: Input map value (mapping or sequence of pairs or entry dicts).
    :type value: object
    :param dtype: Target PyArrow ``MapType``.
    :type dtype: pa.MapType
    :return: List of normalized ``(key, value)`` tuples, or ``value`` unchanged.
    :rtype: object
    :raises ValueError: If a sequence entry is neither a 2-tuple or 2-list nor a ``{"key", "value"}`` mapping.
    """
    k_type = dtype.key_field.type
    v_type = dtype.item_field.type
    if isinstance(value, Mapping):
        return [
            (_normalize_python_for_arrow_table(k, k_type), _normalize_python_for_arrow_table(v, v_type))
            for k, v in value.items()
        ]
    if isinstance(value, Sequence) and not isinstance(value, str | bytes | bytearray):
        pairs: list[tuple[object, object]] = []
        for entry in value:
            if isinstance(entry, Mapping) and "key" in entry and "value" in entry:
                k_raw, v_raw = entry["key"], entry["value"]
            elif isinstance(entry, tuple | list) and len(entry) == PAIR_LENGTH:
                k_raw, v_raw = entry[0], entry[1]
            else:
                msg = f"Invalid map entry {entry!r}; expected (key, value) tuple or {{'key', 'value'}} mapping."
                raise ValueError(msg)
            pairs.append(
                (
                    _normalize_python_for_arrow_table(k_raw, k_type),
                    _normalize_python_for_arrow_table(v_raw, v_type),
                )
            )
        return pairs
    return value


def _normalize_python_for_arrow_table(value: object, dtype: pa.DataType) -> object:
    """Normalize Python mappings and sequences for ``pa.Table.from_arrays`` construction.

    :param value: Input Python value for an Arrow field.
    :type value: object
    :param dtype: Target PyArrow ``DataType``.
    :type dtype: pa.DataType
    :return: Normalized Python value.
    :rtype: object
    """
    if value is None:
        return None
    if pa.types.is_float16(dtype) and isinstance(value, int | float):
        return np.float16(value)
    if isinstance(dtype, pa.DictionaryType):
        return _normalize_python_for_arrow_table(value, dtype.value_type)
    if isinstance(dtype, pa.StructType) and isinstance(value, Mapping):
        return {f.name: _normalize_python_for_arrow_table(value.get(f.name), f.type) for f in dtype if f.name in value}
    if (
        isinstance(
            dtype,
            pa.ListType | pa.LargeListType | pa.ListViewType | pa.LargeListViewType | pa.FixedSizeListType,
        )
        and isinstance(value, Sequence)
        and not isinstance(value, str | bytes | bytearray)
    ):
        return [_normalize_python_for_arrow_table(item, dtype.value_field.type) for item in value]
    return _normalize_map_for_arrow_table(value, dtype) if isinstance(dtype, pa.MapType) else value


def _serialize_table(table: pa.Table, *, for_linkml: bool) -> list[dict[str, Any]]:
    """Serialize all rows of a ``pa.Table`` into JSON-compatible dictionaries.

    :param table: Input PyArrow table.
    :type table: pa.Table
    :param for_linkml: Whether to enforce LinkML list-element constraints.
    :type for_linkml: bool
    :return: List of serialized row dictionaries in schema column order.
    :rtype: list[dict[str, Any]]
    """
    if not isinstance(table, pa.Table):
        msg = f"Expected a pyarrow.Table instance, got {type(table).__name__}."
        raise TypeError(msg)
    schema = table.schema
    columns = [(field.name, field.type, table.column(idx)) for idx, field in enumerate(schema)]
    return [
        {
            col_name: _serialize_arrow_scalar(col[row_idx], col_type, for_linkml=for_linkml)
            for col_name, col_type, col in columns
        }
        for row_idx in range(table.num_rows)
    ]


def _build_single_row_table(record: Mapping[str, Any], schema: pa.Schema) -> pa.Table:
    """Construct a one-row ``pa.Table`` from a Python mapping matching ``schema``.

    :param record: Input Python row dictionary.
    :type record: Mapping[str, Any]
    :param schema: Target PyArrow schema.
    :type schema: pa.Schema
    :return: Single-row ``pa.Table``.
    :rtype: pa.Table
    """
    arrays: list[pa.Array] = []
    for field in schema:
        norm = _normalize_python_for_arrow_table(record.get(field.name), field.type)
        if isinstance(field.type, pa.DictionaryType):
            if norm is None:
                indices = pa.array([None], type=field.type.index_type)
                dictionary = pa.array([], type=field.type.value_type)
            else:
                indices = pa.array([0], type=field.type.index_type)
                dictionary = pa.array([norm], type=field.type.value_type)
            arrays.append(pa.DictionaryArray.from_arrays(indices, dictionary, ordered=field.type.ordered))
        else:
            arrays.append(pa.array([norm], type=field.type))
    return pa.Table.from_arrays(arrays, schema=schema)


def serialize_table_for_jsonschema(table: pa.Table) -> list[dict[str, Any]]:
    """Serialize a ``pa.Table`` into JSON-compatible row dictionaries matching ``generate_json_schema``.

    Encodes ``Decimal`` values as fixed-point strings, binary columns as ASCII Base64 strings,
    temporal columns as ISO-8601 strings, and ``pa.map_`` columns as ``[{"key": ..., "value": ...}]`` arrays.

    :param table: Input PyArrow table.
    :type table: pa.Table
    :return: List of JSON-compatible row dictionaries ready for JSON Schema validation.
    :rtype: list[dict[str, Any]]
    """
    return _serialize_table(table, for_linkml=False)


def serialize_record_for_jsonschema(record: Mapping[str, Any], schema: pa.Schema) -> dict[str, Any]:
    """Serialize a single Python row mapping matching ``schema`` for JSON Schema validation.

    :param record: Input Python row dictionary matching ``schema``.
    :type record: Mapping[str, Any]
    :param schema: PyArrow schema describing ``record``.
    :type schema: pa.Schema
    :return: JSON-compatible dictionary matching ``generate_json_schema(schema)``.
    :rtype: dict[str, Any]
    """
    return serialize_table_for_jsonschema(_build_single_row_table(record, schema))[0]


def serialize_table_for_linkml(table: pa.Table) -> list[dict[str, Any]]:
    """Serialize a ``pa.Table`` into JSON-compatible row dictionaries matching ``generate_linkml_schema``.

    Encodes ``Decimal`` values as fixed-point strings, binary columns as ASCII Base64 strings,
    temporal columns as ISO-8601 strings, and ``pa.map_`` columns as ``[{"key": ..., "value": ...}]`` entry
    lists while rejecting null list items and nested multivalued list or map items unsupported by LinkML.

    :param table: Input PyArrow table.
    :type table: pa.Table
    :return: List of JSON-compatible row dictionaries ready for LinkML validation.
    :rtype: list[dict[str, Any]]
    """
    return _serialize_table(table, for_linkml=True)


def serialize_record_for_linkml(record: Mapping[str, Any], schema: pa.Schema) -> dict[str, Any]:
    """Serialize a single Python row mapping matching ``schema`` for LinkML validation.

    :param record: Input Python row dictionary matching ``schema``.
    :type record: Mapping[str, Any]
    :param schema: PyArrow schema describing ``record``.
    :type schema: pa.Schema
    :return: JSON-compatible dictionary matching ``generate_linkml_schema(schema)``.
    :rtype: dict[str, Any]
    """
    return serialize_table_for_linkml(_build_single_row_table(record, schema))[0]
