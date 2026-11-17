"""Row models and schema-introspection builders for dlt PyArrow definition tables."""

import pyarrow as pa
from pydantic import ConfigDict
from pyiceberg.types import IcebergType, ListType, MapType, StructType

from docs_macros.formatting import FrozenDocModel, escape_table_cell
from docs_macros.tables.dlt_shared import (
    SlotKind,
    format_arrow_field_summary,
    format_dlt_column_def,
    format_iceberg_slot_summary,
    format_metadata_keys,
    iter_parallel_arrow_children,
)
from specs._loader.dlt_contracts import (
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


def _iceberg_child_slots(ice_type: IcebergType) -> tuple[tuple[IcebergType, bool, str | None], ...]:
    """Return immediate child ``(iceberg_type, required, doc)`` slots for ``ice_type``.

    :param iceberg_type: Container or scalar Iceberg type.
    :type ice_type: IcebergType
    :return: Ordered child slot tuples matching ``iter_parallel_arrow_children``.
    :rtype: tuple[tuple[IcebergType, bool, str | None], ...]
    """
    if isinstance(ice_type, StructType):
        return tuple((child.field_type, child.required, child.doc) for child in ice_type.fields)
    if isinstance(ice_type, ListType):
        return ((ice_type.element_type, ice_type.element_required, None),)
    if isinstance(ice_type, MapType):
        return (
            (ice_type.key_type, True, None),
            (ice_type.value_type, ice_type.value_required, None),
        )
    return ()


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

    arrow_steps = tuple(
        iter_parallel_arrow_children(
            (cursor.input_field, cursor.parquet_field, cursor.scan_field),
            (cursor.input_path, cursor.output_path, cursor.output_path),
        )
    )
    ice_children = _iceberg_child_slots(cursor.iceberg_type)
    if not arrow_steps or not ice_children:
        return

    for (slot_kind, (in_child, pq_child, sc_child), (in_path, pq_path, _)), (
        child_ice_type,
        child_req,
        child_doc,
    ) in zip(arrow_steps, ice_children, strict=True):
        collect_nested_slot_rows(
            NestedSlotCursor(
                input_path=in_path,
                output_path=pq_path,
                slot_kind=slot_kind,
                input_field=in_child,
                parquet_field=pq_child,
                iceberg_type=child_ice_type,
                iceberg_required=child_req,
                iceberg_doc=child_doc,
                scan_field=sc_child,
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

    for slot_kind, (in_sub, dlt_sub, pq_sub), (in_path, dlt_path, pq_path) in iter_parallel_arrow_children(
        (cursor.input_field, cursor.dlt_field, cursor.parquet_field),
        (cursor.input_path, cursor.dlt_path, cursor.parquet_path),
    ):
        collect_identifier_slot_rows(
            IdentifierSlotCursor(
                fixture_name=cursor.fixture_name,
                input_path=in_path,
                dlt_path=dlt_path,
                parquet_path=pq_path,
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
