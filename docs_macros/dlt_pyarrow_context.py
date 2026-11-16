"""Structured row models and live schema-introspection builders for dlt PyArrow definition tables."""

from collections.abc import Mapping
from enum import StrEnum
from typing import Any, Final

import pyarrow as pa
from pydantic import ConfigDict
from pyiceberg.types import DecimalType, IcebergType, ListType, MapType, StructType

from docs_macros.formatting import FrozenDocModel, escape_table_cell, pa_ctor_expr
from tests.dependencies.test_dlt_pyarrow_table_definitions import (
    CAMEL_FLAT_ARROW_SCHEMA,
    CAMEL_NESTED_ARROW_SCHEMA,
    EXPECTED_CAMEL_FLAT_PARQUET_SCHEMA,
    EXPECTED_CAMEL_NESTED_PARQUET_SCHEMA,
    SHAPE_EXPECTATIONS,
    ShapeExpectation,
    TableShape,
    decode_arrow_metadata,
    with_parquet_compliant_list_elements,
)

DLT_COLUMN_KEY_ORDER: Final[tuple[str, ...]] = ("data_type", "precision", "scale", "nullable")


class SlotKind(StrEnum):
    """Structural position of a PyArrow or Iceberg schema node."""

    ROOT_COLUMN = "root column"
    STRUCT_FIELD = "struct field"
    LIST_ELEMENT = "list element"
    MAP_KEY = "map key"
    MAP_VALUE = "map value"


class NestedSlotCursor(FrozenDocModel):
    """Parallel cursor across input PyArrow, Parquet, Iceberg, and Iceberg scan nodes."""

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    input_path: str
    output_path: str
    slot_kind: SlotKind
    input_field: pa.Field
    parquet_field: pa.Field
    iceberg_type: IcebergType
    iceberg_required: bool
    iceberg_doc: str | None
    scan_field: pa.Field


class IdentifierSlotCursor(FrozenDocModel):
    """Parallel cursor across input, dlt, and Parquet PyArrow schemas for identifier checks."""

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    fixture_name: str
    input_path: str
    dlt_path: str
    parquet_path: str
    slot_kind: SlotKind
    input_field: pa.Field
    dlt_field: pa.Field
    parquet_field: pa.Field


class DltPyArrowFlatColumnRow(FrozenDocModel):
    """Row model for the flat PyArrow-through-dlt column definitions table."""

    column_name: str
    input_field_summary: str
    input_metadata_keys: str
    dlt_column_summary: str
    parquet_field_summary: str
    iceberg_field_summary: str
    iceberg_scan_summary: str

    @property
    def cells(self) -> tuple[str, ...]:
        """Return formatted Markdown table cells for this row.

        :return: Tuple of Markdown table cell strings.
        :rtype: tuple[str, ...]
        """
        return (
            f"`{escape_table_cell(self.column_name)}`",
            self.input_field_summary,
            self.input_metadata_keys,
            self.dlt_column_summary,
            self.parquet_field_summary,
            self.iceberg_field_summary,
            self.iceberg_scan_summary,
        )


class DltPyArrowNestedDltColumnRow(FrozenDocModel):
    """Row model for root dlt column representations on the deep-nested schema."""

    column_name: str
    input_field_summary: str
    dlt_column_summary: str
    nested_hint_summary: str

    @property
    def cells(self) -> tuple[str, ...]:
        """Return formatted Markdown table cells for this row.

        :return: Tuple of Markdown table cell strings.
        :rtype: tuple[str, ...]
        """
        return (
            f"`{escape_table_cell(self.column_name)}`",
            self.input_field_summary,
            self.dlt_column_summary,
            self.nested_hint_summary,
        )


class DltPyArrowNestedPathRow(FrozenDocModel):
    """Row model for a root or nested path across PyArrow, Parquet, and Iceberg."""

    input_path: str
    slot_kind: SlotKind
    input_summary: str
    parquet_path: str
    parquet_summary: str
    iceberg_summary: str
    iceberg_scan_summary: str

    @property
    def cells(self) -> tuple[str, ...]:
        """Return formatted Markdown table cells for this row.

        :return: Tuple of Markdown table cell strings.
        :rtype: tuple[str, ...]
        """
        pq_cell = (
            self.parquet_summary
            if self.parquet_path == self.input_path
            else f"`{escape_table_cell(self.parquet_path)}`: {self.parquet_summary}"
        )
        return (
            f"`{escape_table_cell(self.input_path)}`",
            str(self.slot_kind),
            self.input_summary,
            pq_cell,
            self.iceberg_summary,
            self.iceberg_scan_summary,
        )


class DltPyArrowIdentifierRow(FrozenDocModel):
    """Row model for snake_case root normalization and nested field name preservation."""

    fixture_name: str
    input_path: str
    slot_kind: SlotKind
    dlt_path: str
    parquet_path: str
    iceberg_path_and_doc: str

    @property
    def cells(self) -> tuple[str, ...]:
        """Return formatted Markdown table cells for this row.

        :return: Tuple of Markdown table cell strings.
        :rtype: tuple[str, ...]
        """
        return (
            f"`{escape_table_cell(self.fixture_name)}`",
            f"`{escape_table_cell(self.input_path)}`",
            str(self.slot_kind),
            f"`{escape_table_cell(self.dlt_path)}`",
            f"`{escape_table_cell(self.parquet_path)}`",
            self.iceberg_path_and_doc,
        )


def format_arrow_type_label(dtype: pa.DataType) -> str:
    """Format a scalar or nested PyArrow ``DataType`` as a concise ``pa.*`` expression.

    :param dtype: PyArrow data type to format.
    :type dtype: pa.DataType
    :return: Concise ``pa.*`` constructor label.
    :rtype: str
    """
    if pa.types.is_struct(dtype):
        return "pa.struct([...])"
    if pa.types.is_list(dtype):
        return "pa.list_(...)"
    if pa.types.is_large_list(dtype):
        return "pa.large_list(...)"
    if pa.types.is_map(dtype):
        return "pa.map_(...)"
    return pa_ctor_expr(dtype)


def format_iceberg_type_label(iceberg_type: IcebergType) -> str:
    """Format an Iceberg ``IcebergType`` as a concise constructor expression.

    :param iceberg_type: PyIceberg field, element, key, or value type.
    :type iceberg_type: IcebergType
    :return: Concise Iceberg type constructor string.
    :rtype: str
    """
    if isinstance(iceberg_type, StructType):
        return "StructType(...)"
    if isinstance(iceberg_type, ListType):
        return "ListType(...)"
    if isinstance(iceberg_type, MapType):
        return "MapType(...)"
    if isinstance(iceberg_type, DecimalType):
        return f"DecimalType({iceberg_type.precision}, {iceberg_type.scale})"
    return f"{type(iceberg_type).__name__}()"


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


def build_dlt_pyarrow_flat_column_rows(
    spec: ShapeExpectation | None = None,
) -> tuple[DltPyArrowFlatColumnRow, ...]:
    """Extract per-column expectations across all layers for ``TableShape.FLAT``.

    :param spec: Optional flat ``ShapeExpectation`` override.
    :type spec: ShapeExpectation | None
    :return: Tuple of ``DltPyArrowFlatColumnRow`` records.
    :rtype: tuple[DltPyArrowFlatColumnRow, ...]
    """
    resolved = spec if spec is not None else SHAPE_EXPECTATIONS[TableShape.FLAT]
    pq_schema = with_parquet_compliant_list_elements(resolved.input_arrow_schema)
    rows: list[DltPyArrowFlatColumnRow] = []
    for in_field in resolved.input_arrow_schema:
        col_name = in_field.name
        pq_field = pq_schema.field(col_name)
        ice_field = resolved.expected_iceberg_schema.find_field(col_name)
        scan_field = resolved.expected_iceberg_scan_arrow_schema.field(col_name)
        dlt_col = resolved.expected_dlt_columns[col_name]
        rows.append(
            DltPyArrowFlatColumnRow(
                column_name=col_name,
                input_field_summary=format_arrow_field_summary(in_field, include_metadata=False),
                input_metadata_keys=format_metadata_keys(decode_arrow_metadata(in_field.metadata)),
                dlt_column_summary=format_dlt_column_def(dlt_col, is_nested=pa.types.is_nested(in_field.type)),
                parquet_field_summary=format_arrow_field_summary(pq_field),
                iceberg_field_summary=format_iceberg_slot_summary(
                    ice_field.field_type,
                    slot_kind=SlotKind.ROOT_COLUMN,
                    required=ice_field.required,
                    doc=ice_field.doc,
                ),
                iceberg_scan_summary=format_arrow_field_summary(scan_field),
            )
        )
    return tuple(rows)


def build_dlt_pyarrow_nested_dlt_column_rows(
    spec: ShapeExpectation | None = None,
) -> tuple[DltPyArrowNestedDltColumnRow, ...]:
    """Extract root ``dlt`` column and ``x-nested-type`` expectations for ``TableShape.DEEP_NESTED``.

    :param spec: Optional deep-nested ``ShapeExpectation`` override.
    :type spec: ShapeExpectation | None
    :return: Tuple of ``DltPyArrowNestedDltColumnRow`` records.
    :rtype: tuple[DltPyArrowNestedDltColumnRow, ...]
    """
    resolved = spec if spec is not None else SHAPE_EXPECTATIONS[TableShape.DEEP_NESTED]
    rows: list[DltPyArrowNestedDltColumnRow] = []
    for in_field in resolved.input_arrow_schema:
        is_nested = pa.types.is_nested(in_field.type)
        dlt_col = resolved.expected_dlt_columns[in_field.name]
        hint_summary = (
            "Roundtrips full `pa.DataType` (inner names, types, nullability, and `pa.Field.metadata`)"
            if is_nested
            else "—"
        )
        rows.append(
            DltPyArrowNestedDltColumnRow(
                column_name=in_field.name,
                input_field_summary=format_arrow_field_summary(in_field),
                dlt_column_summary=format_dlt_column_def(dlt_col, is_nested=is_nested),
                nested_hint_summary=hint_summary,
            )
        )
    return tuple(rows)


def collect_nested_slot_rows(cursor: NestedSlotCursor, rows: list[DltPyArrowNestedPathRow]) -> None:
    """Append ``cursor`` and all descendant struct, list, and map slots to ``rows``.

    :param cursor: Current parallel schema node cursor.
    :type cursor: NestedSlotCursor
    :param rows: Output accumulator list.
    :type rows: list[DltPyArrowNestedPathRow]
    :return: None.
    :rtype: None
    """
    rows.append(
        DltPyArrowNestedPathRow(
            input_path=cursor.input_path,
            slot_kind=cursor.slot_kind,
            input_summary=format_arrow_field_summary(cursor.input_field),
            parquet_path=cursor.output_path,
            parquet_summary=format_arrow_field_summary(cursor.parquet_field),
            iceberg_summary=format_iceberg_slot_summary(
                cursor.iceberg_type,
                slot_kind=cursor.slot_kind,
                required=cursor.iceberg_required,
                doc=cursor.iceberg_doc,
            ),
            iceberg_scan_summary=format_arrow_field_summary(cursor.scan_field),
        )
    )

    in_type = cursor.input_field.type
    pq_type = cursor.parquet_field.type
    ice_type = cursor.iceberg_type
    sc_type = cursor.scan_field.type

    if pa.types.is_struct(in_type) and isinstance(ice_type, StructType):
        for in_child, pq_child, ice_child, sc_child in zip(in_type, pq_type, ice_type.fields, sc_type, strict=True):
            collect_nested_slot_rows(
                NestedSlotCursor(
                    input_path=f"{cursor.input_path}.{in_child.name}",
                    output_path=f"{cursor.output_path}.{pq_child.name}",
                    slot_kind=SlotKind.STRUCT_FIELD,
                    input_field=in_child,
                    parquet_field=pq_child,
                    iceberg_type=ice_child.field_type,
                    iceberg_required=ice_child.required,
                    iceberg_doc=ice_child.doc,
                    scan_field=sc_child,
                ),
                rows,
            )
    elif (pa.types.is_list(in_type) or pa.types.is_large_list(in_type)) and isinstance(ice_type, ListType):
        in_elem = in_type.value_field
        pq_elem = pq_type.value_field
        collect_nested_slot_rows(
            NestedSlotCursor(
                input_path=f"{cursor.input_path}.{in_elem.name}",
                output_path=f"{cursor.output_path}.{pq_elem.name}",
                slot_kind=SlotKind.LIST_ELEMENT,
                input_field=in_elem,
                parquet_field=pq_elem,
                iceberg_type=ice_type.element_type,
                iceberg_required=ice_type.element_required,
                iceberg_doc=None,
                scan_field=sc_type.value_field,
            ),
            rows,
        )
    elif pa.types.is_map(in_type) and isinstance(ice_type, MapType):
        in_key = in_type.key_field
        pq_key = pq_type.key_field
        collect_nested_slot_rows(
            NestedSlotCursor(
                input_path=f"{cursor.input_path}.{in_key.name}",
                output_path=f"{cursor.output_path}.{pq_key.name}",
                slot_kind=SlotKind.MAP_KEY,
                input_field=in_key,
                parquet_field=pq_key,
                iceberg_type=ice_type.key_type,
                iceberg_required=True,
                iceberg_doc=None,
                scan_field=sc_type.key_field,
            ),
            rows,
        )
        in_val = in_type.item_field
        pq_val = pq_type.item_field
        collect_nested_slot_rows(
            NestedSlotCursor(
                input_path=f"{cursor.input_path}.{in_val.name}",
                output_path=f"{cursor.output_path}.{pq_val.name}",
                slot_kind=SlotKind.MAP_VALUE,
                input_field=in_val,
                parquet_field=pq_val,
                iceberg_type=ice_type.value_type,
                iceberg_required=ice_type.value_required,
                iceberg_doc=None,
                scan_field=sc_type.item_field,
            ),
            rows,
        )


def build_dlt_pyarrow_nested_path_rows(
    spec: ShapeExpectation | None = None,
) -> tuple[DltPyArrowNestedPathRow, ...]:
    """Extract all root and nested field path expectations for ``TableShape.DEEP_NESTED``.

    :param spec: Optional deep-nested ``ShapeExpectation`` override.
    :type spec: ShapeExpectation | None
    :return: Tuple of ``DltPyArrowNestedPathRow`` records in depth-first schema order.
    :rtype: tuple[DltPyArrowNestedPathRow, ...]
    """
    resolved = spec if spec is not None else SHAPE_EXPECTATIONS[TableShape.DEEP_NESTED]
    pq_schema = with_parquet_compliant_list_elements(resolved.input_arrow_schema)
    ice_root = resolved.expected_iceberg_schema.as_struct()
    rows: list[DltPyArrowNestedPathRow] = []
    for in_top, pq_top, ice_top, sc_top in zip(
        resolved.input_arrow_schema,
        pq_schema,
        ice_root.fields,
        resolved.expected_iceberg_scan_arrow_schema,
        strict=True,
    ):
        collect_nested_slot_rows(
            NestedSlotCursor(
                input_path=in_top.name,
                output_path=pq_top.name,
                slot_kind=SlotKind.ROOT_COLUMN,
                input_field=in_top,
                parquet_field=pq_top,
                iceberg_type=ice_top.field_type,
                iceberg_required=ice_top.required,
                iceberg_doc=ice_top.doc,
                scan_field=sc_top,
            ),
            rows,
        )
    return tuple(rows)


def collect_identifier_slot_rows(cursor: IdentifierSlotCursor, rows: list[DltPyArrowIdentifierRow]) -> None:
    """Append ``cursor`` and all descendant slots for identifier normalization checks.

    :param cursor: Current parallel identifier cursor.
    :type cursor: IdentifierSlotCursor
    :param rows: Output accumulator list.
    :type rows: list[DltPyArrowIdentifierRow]
    :return: None.
    :rtype: None
    """
    escaped_pq = escape_table_cell(cursor.parquet_path)
    if cursor.slot_kind in {SlotKind.ROOT_COLUMN, SlotKind.STRUCT_FIELD}:
        doc_val = decode_arrow_metadata(cursor.input_field.metadata).get("doc")
        doc_repr = escape_table_cell(repr(doc_val))
        iceberg_cell = f"`{escaped_pq}` (`doc={doc_repr}`)"
    else:
        iceberg_cell = f"`{escaped_pq}` *(no `doc` slot)*"

    rows.append(
        DltPyArrowIdentifierRow(
            fixture_name=cursor.fixture_name,
            input_path=cursor.input_path,
            slot_kind=cursor.slot_kind,
            dlt_path=cursor.dlt_path,
            parquet_path=cursor.parquet_path,
            iceberg_path_and_doc=iceberg_cell,
        )
    )

    in_type = cursor.input_field.type
    dlt_type = cursor.dlt_field.type
    pq_type = cursor.parquet_field.type

    if pa.types.is_struct(in_type):
        for in_child, dlt_child, pq_child in zip(in_type, dlt_type, pq_type, strict=True):
            collect_identifier_slot_rows(
                IdentifierSlotCursor(
                    fixture_name=cursor.fixture_name,
                    input_path=f"{cursor.input_path}.{in_child.name}",
                    dlt_path=f"{cursor.dlt_path}.{dlt_child.name}",
                    parquet_path=f"{cursor.parquet_path}.{pq_child.name}",
                    slot_kind=SlotKind.STRUCT_FIELD,
                    input_field=in_child,
                    dlt_field=dlt_child,
                    parquet_field=pq_child,
                ),
                rows,
            )
    elif pa.types.is_list(in_type) or pa.types.is_large_list(in_type):
        in_elem = in_type.value_field
        dlt_elem = dlt_type.value_field
        pq_elem = pq_type.value_field
        collect_identifier_slot_rows(
            IdentifierSlotCursor(
                fixture_name=cursor.fixture_name,
                input_path=f"{cursor.input_path}.{in_elem.name}",
                dlt_path=f"{cursor.dlt_path}.{dlt_elem.name}",
                parquet_path=f"{cursor.parquet_path}.{pq_elem.name}",
                slot_kind=SlotKind.LIST_ELEMENT,
                input_field=in_elem,
                dlt_field=dlt_elem,
                parquet_field=pq_elem,
            ),
            rows,
        )
    elif pa.types.is_map(in_type):
        for slot_kind, in_sub, dlt_sub, pq_sub in (
            (SlotKind.MAP_KEY, in_type.key_field, dlt_type.key_field, pq_type.key_field),
            (SlotKind.MAP_VALUE, in_type.item_field, dlt_type.item_field, pq_type.item_field),
        ):
            collect_identifier_slot_rows(
                IdentifierSlotCursor(
                    fixture_name=cursor.fixture_name,
                    input_path=f"{cursor.input_path}.{in_sub.name}",
                    dlt_path=f"{cursor.dlt_path}.{dlt_sub.name}",
                    parquet_path=f"{cursor.parquet_path}.{pq_sub.name}",
                    slot_kind=slot_kind,
                    input_field=in_sub,
                    dlt_field=dlt_sub,
                    parquet_field=pq_sub,
                ),
                rows,
            )


def build_dlt_pyarrow_identifier_rows() -> tuple[DltPyArrowIdentifierRow, ...]:
    """Extract identifier normalization rows from ``CAMEL_FLAT`` and ``CAMEL_NESTED`` fixtures.

    :return: Tuple of ``DltPyArrowIdentifierRow`` records.
    :rtype: tuple[DltPyArrowIdentifierRow, ...]
    """
    fixtures = (
        ("flat", CAMEL_FLAT_ARROW_SCHEMA, EXPECTED_CAMEL_FLAT_PARQUET_SCHEMA),
        ("deep_nested", CAMEL_NESTED_ARROW_SCHEMA, EXPECTED_CAMEL_NESTED_PARQUET_SCHEMA),
    )
    rows: list[DltPyArrowIdentifierRow] = []
    for fixture_name, in_schema, dlt_arrow_schema in fixtures:
        pq_schema = with_parquet_compliant_list_elements(dlt_arrow_schema)
        for in_top, dlt_top, pq_top in zip(in_schema, dlt_arrow_schema, pq_schema, strict=True):
            collect_identifier_slot_rows(
                IdentifierSlotCursor(
                    fixture_name=fixture_name,
                    input_path=in_top.name,
                    dlt_path=dlt_top.name,
                    parquet_path=pq_top.name,
                    slot_kind=SlotKind.ROOT_COLUMN,
                    input_field=in_top,
                    dlt_field=dlt_top,
                    parquet_field=pq_top,
                ),
                rows,
            )
    return tuple(rows)
