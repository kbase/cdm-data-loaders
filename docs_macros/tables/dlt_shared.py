"""Shared PyArrow and Iceberg slot formatters and parallel child-slot decomposition for dlt tables."""

from collections.abc import Iterator, Mapping
from enum import StrEnum
from typing import Any, Final

import pyarrow as pa
from pyiceberg.schema import Schema as IcebergSchema
from pyiceberg.types import DecimalType, IcebergType, ListType, MapType, StructType

from docs_macros.formatting import escape_table_cell, pa_ctor_expr
from specs._loader.dlt_contracts import decode_arrow_metadata

DLT_COLUMN_KEY_ORDER: Final[tuple[str, ...]] = ("data_type", "precision", "scale", "nullable")


class SlotKind(StrEnum):
    """Structural position of a PyArrow or Iceberg schema node."""

    ROOT_COLUMN = "root column"
    STRUCT_FIELD = "struct field"
    LIST_ELEMENT = "list element"
    MAP_KEY = "map key"
    MAP_VALUE = "map value"


def _format_arrow_type(dtype: pa.DataType, *, expand_children: bool) -> str:
    """Format a scalar or nested PyArrow ``DataType`` with concise or child-expanded containers.

    :param dtype: PyArrow data type to format.
    :type dtype: pa.DataType
    :param expand_children: When ``True``, include child field names or recursive inner types.
    :type expand_children: bool
    :return: Formatted ``pa.*`` expression string.
    :rtype: str
    """
    if pa.types.is_struct(dtype):
        inner = ", ".join(child.name for child in dtype) if expand_children else "..."
        return f"pa.struct([{inner}])"
    if pa.types.is_list(dtype):
        inner = _format_arrow_type(dtype.value_type, expand_children=True) if expand_children else "..."
        return f"pa.list_({inner})"
    if pa.types.is_large_list(dtype):
        inner = _format_arrow_type(dtype.value_type, expand_children=True) if expand_children else "..."
        return f"pa.large_list({inner})"
    if pa.types.is_map(dtype):
        inner = (
            f"{_format_arrow_type(dtype.key_type, expand_children=True)}, "
            f"{_format_arrow_type(dtype.item_type, expand_children=True)}"
            if expand_children
            else "..."
        )
        return f"pa.map_({inner})"
    return pa_ctor_expr(dtype)


def format_arrow_type_label(dtype: pa.DataType) -> str:
    """Format a scalar or nested PyArrow ``DataType`` as a concise ``pa.*`` expression.

    :param dtype: PyArrow data type to format.
    :type dtype: pa.DataType
    :return: Concise ``pa.*`` constructor label.
    :rtype: str
    """
    return _format_arrow_type(dtype, expand_children=False)


def format_compact_arrow_type(dtype: pa.DataType) -> str:
    """Format a scalar or nested PyArrow ``DataType`` with concise child field summaries.

    :param dtype: PyArrow data type to summarize.
    :type dtype: pa.DataType
    :return: Concise ``pa.*`` expression string.
    :rtype: str
    """
    return _format_arrow_type(dtype, expand_children=True)


def _format_iceberg_type(iceberg_type: IcebergType, *, expand_children: bool) -> str:
    """Format an Iceberg ``IcebergType`` with concise or child-expanded containers.

    :param iceberg_type: PyIceberg field, element, key, or value type.
    :type iceberg_type: IcebergType
    :param expand_children: When ``True``, include child field names or recursive inner types.
    :type expand_children: bool
    :return: Formatted Iceberg type constructor string.
    :rtype: str
    """
    if isinstance(iceberg_type, StructType):
        inner = ", ".join(field.name for field in iceberg_type.fields) if expand_children else "..."
        return f"StructType({inner})"
    if isinstance(iceberg_type, ListType):
        inner = _format_iceberg_type(iceberg_type.element_type, expand_children=True) if expand_children else "..."
        return f"ListType({inner})"
    if isinstance(iceberg_type, MapType):
        inner = (
            f"{_format_iceberg_type(iceberg_type.key_type, expand_children=True)}, "
            f"{_format_iceberg_type(iceberg_type.value_type, expand_children=True)}"
            if expand_children
            else "..."
        )
        return f"MapType({inner})"
    if isinstance(iceberg_type, DecimalType):
        return f"DecimalType({iceberg_type.precision}, {iceberg_type.scale})"
    return f"{type(iceberg_type).__name__}()"


def format_iceberg_type_label(iceberg_type: IcebergType) -> str:
    """Format an Iceberg ``IcebergType`` as a concise constructor expression.

    :param iceberg_type: PyIceberg field, element, key, or value type.
    :type iceberg_type: IcebergType
    :return: Concise Iceberg type constructor string.
    :rtype: str
    """
    return _format_iceberg_type(iceberg_type, expand_children=False)


def format_compact_iceberg_type(iceberg_type: IcebergType) -> str:
    """Format a scalar or nested PyIceberg ``IcebergType`` with concise child summaries.

    :param iceberg_type: Iceberg type to summarize.
    :type iceberg_type: IcebergType
    :return: Concise Iceberg type label.
    :rtype: str
    """
    return _format_iceberg_type(iceberg_type, expand_children=True)


def format_metadata_keys(metadata: Mapping[str, str]) -> str:
    """Format decoded ``pa.Field.metadata`` keys as comma-separated inline code spans.

    :param metadata: Decoded UTF-8 key/value metadata mapping.
    :type metadata: Mapping[str, str]
    :return: Formatted key list or em-dash when empty.
    :rtype: str
    """
    if not metadata:
        return "—"
    return ", ".join(f"`{escape_table_cell(key)}`" for key in metadata)


def format_dlt_column_def(col_def: Mapping[str, Any], *, is_nested: bool = False) -> str:
    """Format a ``dlt`` column dictionary into ordered ``key=value`` inline code spans.

    :param col_def: Expected ``dlt`` column metadata dictionary.
    :type col_def: Mapping[str, Any]
    :param is_nested: Whether ``dlt`` attaches an ``x-nested-type`` Arrow IPC hint.
    :type is_nested: bool
    :return: Comma-separated inline code summary.
    :rtype: str
    """
    ordered_keys = [
        *(key for key in DLT_COLUMN_KEY_ORDER if key in col_def),
        *(key for key in col_def if key != "name" and key not in DLT_COLUMN_KEY_ORDER),
    ]
    parts = [f"`{escape_table_cell(f'{key}={col_def[key]!r}')}`" for key in ordered_keys]
    if is_nested:
        parts.append("`x-nested-type='arrow-ipc:...'`")
    return ", ".join(parts)


def format_arrow_field_summary(field: pa.Field, *, include_metadata: bool = True) -> str:
    """Format a ``pa.Field`` type, nullability, and decoded metadata key list.

    :param field: PyArrow field to summarize.
    :type field: pa.Field
    :param include_metadata: Whether to append decoded metadata keys in parentheses.
    :type include_metadata: bool
    :return: Markdown summary string for ``field``.
    :rtype: str
    """
    type_label = escape_table_cell(format_arrow_type_label(field.type))
    base = f"`{type_label}`, `nullable={field.nullable}`"
    if not include_metadata:
        return base
    meta_keys = format_metadata_keys(decode_arrow_metadata(field.metadata))
    return f"{base} ({meta_keys})"


def format_iceberg_slot_summary(
    iceberg_type: IcebergType,
    *,
    slot_kind: SlotKind,
    required: bool,
    doc: str | None,
) -> str:
    """Format an Iceberg ``NestedField`` or container element/key/value slot summary.

    :param iceberg_type: Iceberg type at this slot.
    :type iceberg_type: IcebergType
    :param slot_kind: Structural position of the slot.
    :type slot_kind: SlotKind
    :param required: Whether the field, element, key, or value is required.
    :type required: bool
    :param doc: ``NestedField.doc`` string when ``slot_kind`` supports ``doc``.
    :type doc: str | None
    :return: Markdown summary string for the Iceberg slot.
    :rtype: str
    """
    type_cell = f"`{escape_table_cell(format_iceberg_type_label(iceberg_type))}`"
    if slot_kind in {SlotKind.ROOT_COLUMN, SlotKind.STRUCT_FIELD}:
        doc_repr = escape_table_cell(repr(doc))
        return f"{type_cell}, `required={required}`, `doc={doc_repr}`"
    if slot_kind == SlotKind.LIST_ELEMENT:
        return f"{type_cell}, `element_required={required}` *(no `doc` slot)*"
    if slot_kind == SlotKind.MAP_KEY:
        return f"{type_cell}, `key` *(no `doc` slot)*"
    return f"{type_cell}, `value_required={required}` *(no `doc` slot)*"


def format_arrow_schema_fields(schema: pa.Schema) -> str:
    """Format all fields of ``schema`` as comma-separated ``name: type`` code spans.

    :param schema: PyArrow table schema.
    :type schema: pa.Schema
    :return: Markdown summary of field names, types, and non-null flags.
    :rtype: str
    """
    parts: list[str] = []
    for field in schema:
        type_str = format_compact_arrow_type(field.type)
        suffix = " (not null)" if not field.nullable else ""
        parts.append(f"`{escape_table_cell(f'{field.name}: {type_str}')}`{suffix}")
    return ", ".join(parts)


def format_iceberg_schema_fields(schema: IcebergSchema) -> str:
    """Format all root fields of ``schema`` as comma-separated ``name: Type`` code spans.

    :param schema: PyIceberg table schema.
    :type schema: IcebergSchema
    :return: Markdown summary of field names, types, and required flags.
    :rtype: str
    """
    parts: list[str] = []
    for field in schema.fields:
        type_str = format_compact_iceberg_type(field.field_type)
        suffix = " (required)" if field.required else ""
        parts.append(f"`{escape_table_cell(f'{field.name}: {type_str}')}`{suffix}")
    return ", ".join(parts)


def iter_parallel_arrow_children(
    fields: tuple[pa.Field, ...],
    paths: tuple[str, ...],
) -> Iterator[tuple[SlotKind, tuple[pa.Field, ...], tuple[str, ...]]]:
    """Yield immediate ``(slot_kind, child_fields, child_paths)`` steps across aligned PyArrow fields.

    :param fields: Tuple of parallel ``pa.Field`` nodes with matching container structure.
    :type fields: tuple[pa.Field, ...]
    :param paths: Tuple of dotted path prefixes corresponding to ``fields``.
    :type paths: tuple[str, ...]
    :return: Iterator of child slot steps for ``struct``, ``list``/``large_list``, or ``map`` containers.
    :rtype: Iterator[tuple[SlotKind, tuple[pa.Field, ...], tuple[str, ...]]]
    """
    lead_type = fields[0].type
    dtypes = [f.type for f in fields]
    if pa.types.is_struct(lead_type):
        for child_group in zip(*dtypes, strict=True):
            child_fields = tuple(child_group)
            child_paths = tuple(f"{prefix}.{child.name}" for prefix, child in zip(paths, child_fields, strict=True))
            yield SlotKind.STRUCT_FIELD, child_fields, child_paths
    elif pa.types.is_list(lead_type) or pa.types.is_large_list(lead_type):
        elem_fields = tuple(dt.value_field for dt in dtypes)
        elem_paths = tuple(f"{prefix}.{elem.name}" for prefix, elem in zip(paths, elem_fields, strict=True))
        yield SlotKind.LIST_ELEMENT, elem_fields, elem_paths
    elif pa.types.is_map(lead_type):
        key_fields = tuple(dt.key_field for dt in dtypes)
        key_paths = tuple(f"{prefix}.{kf.name}" for prefix, kf in zip(paths, key_fields, strict=True))
        yield SlotKind.MAP_KEY, key_fields, key_paths
        val_fields = tuple(dt.item_field for dt in dtypes)
        val_paths = tuple(f"{prefix}.{vf.name}" for prefix, vf in zip(paths, val_fields, strict=True))
        yield SlotKind.MAP_VALUE, val_fields, val_paths
