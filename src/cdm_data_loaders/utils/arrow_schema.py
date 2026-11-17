"""Pure PyArrow schema assembly and byte-metadata decoding helpers."""

from collections.abc import Mapping, Sequence
from typing import Final

import pyarrow as pa

ARROW_LIST_TYPES: Final[tuple[type[pa.DataType], ...]] = (
    pa.ListType,
    pa.LargeListType,
    pa.FixedSizeListType,
    pa.ListViewType,
    pa.LargeListViewType,
)

type ArrowListType = pa.ListType | pa.LargeListType | pa.FixedSizeListType | pa.ListViewType | pa.LargeListViewType


def assemble_pyarrow_schema(
    component: pa.StructType | Sequence[pa.Field],
    *,
    metadata: Mapping[str, str | bytes] | None = None,
) -> pa.Schema:
    """Assemble a PyArrow table schema from a ``pa.StructType`` component or sequence of ``pa.Field`` instances.

    :param component: struct component or sequence of PyArrow fields
    :type component: pa.StructType | Sequence[pa.Field]
    :param metadata: optional schema-level metadata
    :type metadata: Mapping[str, str | bytes] | None
    :return: assembled PyArrow schema
    :rtype: pa.Schema
    :raises ValueError: if ``component`` contains no fields
    """
    fields = list(component)
    if not fields:
        msg = "Cannot assemble a PyArrow schema from an empty component"
        raise ValueError(msg)
    return pa.schema(fields, metadata=metadata)


def decode_field_metadata(metadata: Mapping[bytes, bytes] | None) -> dict[str, str]:
    """Decode PyArrow UTF-8 byte key/value metadata into string key/value pairs.

    :param metadata: optional raw PyArrow field metadata mapping
    :type metadata: Mapping[bytes, bytes] | None
    :return: decoded string-to-string metadata dictionary
    :rtype: dict[str, str]
    """
    if not metadata:
        return {}
    return {k.decode("utf-8"): v.decode("utf-8") for k, v in metadata.items()}


def is_any_binary_type(dtype: pa.DataType) -> bool:
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


def is_any_string_type(dtype: pa.DataType) -> bool:
    """Return ``True`` when ``dtype`` is any UTF-8 PyArrow string type.

    :param dtype: PyArrow data type to inspect.
    :type dtype: pa.DataType
    :return: ``True`` for string, large_string, or string_view.
    :rtype: bool
    """
    return pa.types.is_string(dtype) or pa.types.is_large_string(dtype) or pa.types.is_string_view(dtype)


def is_any_list_type(dtype: pa.DataType) -> bool:
    """Return ``True`` when ``dtype`` is any variable-size, fixed-size, or view PyArrow list type.

    :param dtype: PyArrow data type to inspect.
    :type dtype: pa.DataType
    :return: ``True`` for list, large_list, fixed_size_list, list_view, or large_list_view.
    :rtype: bool
    """
    return isinstance(dtype, ARROW_LIST_TYPES)


def is_non_json_arrow_type(dtype: pa.DataType) -> bool:
    """Return ``True`` when ``dtype`` cannot be round-tripped directly via ``json.dumps`` at recursion cutoff.

    :param dtype: PyArrow data type to inspect.
    :type dtype: pa.DataType
    :return: ``True`` for date, timestamp, time, duration, decimal, or binary types.
    :rtype: bool
    """
    return (
        pa.types.is_date(dtype)
        or pa.types.is_timestamp(dtype)
        or pa.types.is_time(dtype)
        or pa.types.is_duration(dtype)
        or pa.types.is_decimal(dtype)
        or is_any_binary_type(dtype)
    )
