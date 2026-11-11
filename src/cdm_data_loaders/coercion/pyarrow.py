"""Coerce JSON-parsed records and scalar values to match a target PyArrow schema."""

import base64
import json
import math
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime, time
from decimal import Decimal
from enum import Enum
from typing import Any, Final

import numpy as np
import pyarrow as pa

NANOS_PER_MICRO: Final[int] = 1_000
NANOS_PER_SECOND: Final[int] = 1_000_000_000
SECONDS_PER_MINUTE: Final[int] = 60
SECONDS_PER_HOUR: Final[int] = 3_600
PAIR_LENGTH: Final[int] = 2


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


def _is_any_binary_type(dtype: pa.DataType) -> bool:
    """Return ``True`` when ``dtype`` is any raw or fixed-size PyArrow binary type.

    :param dtype: PyArrow data type to inspect.
    :type dtype: pa.DataType
    :return: ``True`` for binary, large_binary, binary_view, or fixed_size_binary.
    :rtype: bool
    """
    return (
        pa.types.is_binary(dtype)
        or pa.types.is_large_binary(dtype)
        or pa.types.is_binary_view(dtype)
        or pa.types.is_fixed_size_binary(dtype)
    )


def _is_any_string_type(dtype: pa.DataType) -> bool:
    """Return ``True`` when ``dtype`` is any UTF-8 PyArrow string type.

    :param dtype: PyArrow data type to inspect.
    :type dtype: pa.DataType
    :return: ``True`` for string, large_string, or string_view.
    :rtype: bool
    """
    return pa.types.is_string(dtype) or pa.types.is_large_string(dtype) or pa.types.is_string_view(dtype)


def pyarrow_to_python_type_label(data_type: pa.DataType) -> str:
    """Return the Markdown-formatted Python runtime type label corresponding to a coerced PyArrow ``DataType``.

    Mirrors the scalar and list coercion rules enforced by :func:`coerce_record_for_pyarrow`.

    :param data_type: Target PyArrow data type.
    :type data_type: pa.DataType
    :return: Backtick-wrapped Python runtime type label (e.g. ``"`int`"``, ``"`datetime.datetime` (UTC)"``).
    :rtype: str
    """
    if isinstance(data_type, pa.DictionaryType):
        return pyarrow_to_python_type_label(data_type.value_type)
    if isinstance(
        data_type,
        pa.ListType | pa.LargeListType | pa.ListViewType | pa.LargeListViewType | pa.FixedSizeListType,
    ):
        inner = pyarrow_to_python_type_label(data_type.value_field.type).strip("`")
        return f"`list[{inner}]`"
    if isinstance(data_type, pa.TimestampType):
        tz_suffix = f"({data_type.tz})" if data_type.tz is not None else "(naive)"
        return f"`datetime.datetime` {tz_suffix}"
    for predicate, label in (
        (pa.types.is_boolean, "`bool`"),
        (pa.types.is_integer, "`int`"),
        (pa.types.is_floating, "`float`"),
        (pa.types.is_decimal, "`decimal.Decimal`"),
        (pa.types.is_date, "`datetime.date`"),
        (pa.types.is_time, "`datetime.time`"),
        (_is_any_binary_type, "`bytes`"),
        (_is_any_string_type, "`str`"),
    ):
        if predicate(data_type):
            return label
    return f"`{data_type}`"


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


def _format_time_scalar(scalar: pa.Scalar, dtype: pa.DataType) -> str:
    """Format a PyArrow ``Time32Scalar`` or ``Time64Scalar`` as an ISO-8601 time string.

    Preserves full 9-digit nanosecond resolution when ``time64('ns')`` has sub-microsecond ticks.

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
    :return: List of serialized items, or ``None`` if the underlying array is null.
    :rtype: list[Any] | None
    :raises ValueError: On nested list/map items or null list elements when ``for_linkml`` is ``True``.
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
    if _is_any_string_type(dtype):
        return str(scalar.as_py())
    if _is_any_binary_type(dtype):
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

    :param value: Input map value (mapping or sequence of pairs/entry dicts).
    :type value: object
    :param dtype: Target PyArrow ``MapType``.
    :type dtype: pa.MapType
    :return: List of normalized ``(key, value)`` tuples, or ``value`` unchanged.
    :rtype: object
    :raises ValueError: If a sequence entry is neither a 2-tuple/list nor a ``{"key", "value"}`` mapping.
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
    """Normalize Python mappings/dicts for ``pa.Table.from_pylist`` construction.

    :param value: Input Python value for an Arrow field.
    :type value: object
    :param dtype: Target PyArrow ``DataType``.
    :type dtype: pa.DataType
    :return: Value normalized for ``pa.Table.from_pylist``.
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


def serialize_table_for_jsonschema(table: pa.Table) -> list[dict[str, Any]]:
    """Serialize a ``pa.Table`` into JSON-compatible row dictionaries matching ``generate_json_schema``.

    Encodes ``Decimal`` values as plain fixed-point strings, binary columns as ASCII Base64 strings,
    temporal columns as ISO-8601 strings, and ``pa.map_`` columns as ``[{"key": ..., "value": ...}]`` arrays.

    :param table: Input PyArrow table.
    :type table: pa.Table
    :return: List of JSON-compatible row dictionaries ready for JSON Schema validation.
    :rtype: list[dict[str, Any]]
    """
    return _serialize_table(table, for_linkml=False)


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

    Encodes ``Decimal`` values as plain fixed-point strings, binary columns as ASCII Base64 strings,
    temporal columns as ISO-8601 strings, and ``pa.map_`` columns as ``[{"key": ..., "value": ...}]`` entry
    lists while rejecting null list items and nested multivalued list/map items unsupported by LinkML.

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
