"""Unit and rendering tests for ``docs_macros.tables.dlt_pyarrow`` and ``docs/dlt_pyarrow_definitions.md``."""

import markdown
import pyarrow as pa
import pytest
from pyiceberg.schema import Schema as IcebergSchema
from pyiceberg.types import (
    DecimalType,
    IcebergType,
    IntegerType,
    ListType,
    LongType,
    MapType,
    NestedField,
    StringType,
    StructType,
)

from docs_macros.constants import DOCS_DIR
from docs_macros.tables.dlt_pyarrow import (
    DltPyArrowIdentifierRow,
    DltPyArrowNestedPathRow,
    IdentifierSlotCursor,
    NestedSlotCursor,
    build_dlt_pyarrow_flat_column_rows,
    build_dlt_pyarrow_identifier_rows,
    build_dlt_pyarrow_nested_dlt_column_rows,
    build_dlt_pyarrow_nested_path_rows,
    collect_identifier_slot_rows,
    collect_nested_slot_rows,
)
from docs_macros.tables.dlt_shared import (
    SlotKind,
    format_arrow_field_summary,
    format_arrow_type_label,
    format_dlt_column_def,
    format_iceberg_slot_summary,
    format_iceberg_type_label,
    format_metadata_keys,
)
from docs_macros.tables.registry import TABLE_TEMPLATE_REGISTRY
from specs._loader.dlt_contracts import (
    DEEP_NESTED_ARROW_SCHEMA,
    FLAT_ARROW_SCHEMA,
    ShapeExpectation,
    extract_arrow_field_metadata,
    with_parquet_compliant_list_elements,
)
from tests.docs.helpers import materialize_markdown_file

pytestmark = pytest.mark.docs


@pytest.mark.parametrize(
    ("dtype", "expected"),
    [
        pytest.param(pa.int64(), "pa.int64()", id="scalar_int64"),
        pytest.param(pa.decimal128(10, 2), "pa.decimal128(10, 2)", id="scalar_decimal128"),
        pytest.param(pa.struct([pa.field("x", pa.int32())]), "pa.struct([...])", id="nested_struct"),
        pytest.param(pa.list_(pa.string()), "pa.list_(...)", id="nested_list"),
        pytest.param(pa.large_list(pa.string()), "pa.large_list(...)", id="nested_large_list"),
        pytest.param(pa.map_(pa.string(), pa.int64()), "pa.map_(...)", id="nested_map"),
    ],
)
def test_format_arrow_type_label_formats_scalar_and_nested_types(dtype: pa.DataType, expected: str) -> None:
    """format_arrow_type_label returns pa.* expressions for scalars and concise labels for nested types."""
    assert format_arrow_type_label(dtype) == expected


@pytest.mark.parametrize(
    ("iceberg_type", "expected"),
    [
        pytest.param(LongType(), "LongType()", id="long_type"),
        pytest.param(IntegerType(), "IntegerType()", id="integer_type"),
        pytest.param(DecimalType(10, 2), "DecimalType(10, 2)", id="decimal_type"),
        pytest.param(
            StructType(NestedField(field_id=1, name="x", field_type=StringType(), required=True)),
            "StructType(...)",
            id="struct_type",
        ),
        pytest.param(
            ListType(element_id=2, element_type=StringType(), element_required=True),
            "ListType(...)",
            id="list_type",
        ),
        pytest.param(
            MapType(key_id=3, key_type=StringType(), value_id=4, value_type=LongType(), value_required=False),
            "MapType(...)",
            id="map_type",
        ),
    ],
)
def test_format_iceberg_type_label_formats_primitives_and_containers(
    iceberg_type: IcebergType,
    expected: str,
) -> None:
    """format_iceberg_type_label formats primitive, decimal, and container Iceberg types."""
    assert format_iceberg_type_label(iceberg_type) == expected


@pytest.mark.parametrize(
    ("metadata", "expected"),
    [
        pytest.param({}, "—", id="empty_metadata"),
        pytest.param({"doc": "Primary key"}, "`doc`", id="single_key"),
        pytest.param({"comment": "c", "doc": "d", "a|b": "v"}, r"`comment`, `doc`, `a\|b`", id="multiple_and_pipe"),
    ],
)
def test_format_metadata_keys_handles_empty_and_escaped_keys(metadata: dict[str, str], expected: str) -> None:
    """format_metadata_keys returns em-dash when empty and escapes Markdown table pipes."""
    assert format_metadata_keys(metadata) == expected


@pytest.mark.parametrize(
    ("col_def", "is_nested", "expected"),
    [
        pytest.param(
            {"name": "id", "nullable": False, "data_type": "bigint"},
            False,
            "`data_type='bigint'`, `nullable=False`",
            id="scalar_ordered_keys",
        ),
        pytest.param(
            {
                "name": "amount",
                "scale": 2,
                "nullable": False,
                "precision": 10,
                "data_type": "decimal",
                "timezone": True,
            },
            False,
            "`data_type='decimal'`, `precision=10`, `scale=2`, `nullable=False`, `timezone=True`",
            id="decimal_and_extra_key",
        ),
        pytest.param(
            {"name": "organism", "data_type": "json", "nullable": False},
            True,
            "`data_type='json'`, `nullable=False`, `x-nested-type='arrow-ipc:...'`",
            id="nested_with_arrow_ipc_hint",
        ),
    ],
)
def test_format_dlt_column_def_orders_keys_and_nested_hint(
    col_def: dict[str, object],
    is_nested: bool,
    expected: str,
) -> None:
    """format_dlt_column_def orders standard dlt keys, keeps extra keys, and appends x-nested-type when nested."""
    assert format_dlt_column_def(col_def, is_nested=is_nested) == expected


def test_format_arrow_field_summary_and_iceberg_slot_summary_variants() -> None:
    """format_arrow_field_summary and format_iceberg_slot_summary format metadata, nullability, and slot kinds."""
    plain_field = pa.field("code", pa.int16(), nullable=True)
    meta_field = pa.field("id", pa.int64(), nullable=False, metadata={"comment": "c", "doc": "d"})
    assert format_arrow_field_summary(plain_field, include_metadata=False) == "`pa.int16()`, `nullable=True`"
    assert format_arrow_field_summary(plain_field, include_metadata=True) == "`pa.int16()`, `nullable=True` (—)"
    assert (
        format_arrow_field_summary(meta_field, include_metadata=True)
        == "`pa.int64()`, `nullable=False` (`comment`, `doc`)"
    )

    assert (
        format_iceberg_slot_summary(LongType(), slot_kind=SlotKind.ROOT_COLUMN, required=True, doc="a|b")
        == r"`LongType()`, `required=True`, `doc='a\|b'`"
    )
    assert (
        format_iceberg_slot_summary(IntegerType(), slot_kind=SlotKind.STRUCT_FIELD, required=False, doc=None)
        == "`IntegerType()`, `required=False`, `doc=None`"
    )
    assert (
        format_iceberg_slot_summary(StringType(), slot_kind=SlotKind.LIST_ELEMENT, required=True, doc=None)
        == "`StringType()`, `element_required=True` *(no `doc` slot)*"
    )
    assert (
        format_iceberg_slot_summary(StringType(), slot_kind=SlotKind.MAP_KEY, required=True, doc=None)
        == "`StringType()`, `key` *(no `doc` slot)*"
    )
    assert (
        format_iceberg_slot_summary(LongType(), slot_kind=SlotKind.MAP_VALUE, required=False, doc=None)
        == "`LongType()`, `value_required=False` *(no `doc` slot)*"
    )


def test_build_dlt_pyarrow_flat_and_nested_dlt_column_rows_match_fixtures() -> None:
    """Flat and nested dlt column builders extract exact expectations from SHAPE_EXPECTATIONS."""
    flat_rows = build_dlt_pyarrow_flat_column_rows()
    assert [row.column_name for row in flat_rows] == list(FLAT_ARROW_SCHEMA.names)
    by_name = {row.column_name: row for row in flat_rows}

    # small_code widens from pa.int16() to IntegerType() / pa.int32() in Iceberg while keeping doc
    assert "`pa.int16()`, `nullable=True`" in by_name["small_code"].input_field_summary
    assert "`precision=16`" in by_name["small_code"].dlt_column_summary
    assert "`pa.int16()`" in by_name["small_code"].parquet_field_summary
    assert (
        by_name["small_code"].iceberg_field_summary == "`IntegerType()`, `required=False`, `doc='16-bit status code'`"
    )
    assert by_name["small_code"].iceberg_scan_summary == "`pa.int32()`, `nullable=True` (`doc`)"
    assert len(by_name["small_code"].cells) == len(TABLE_TEMPLATE_REGISTRY["dlt_pyarrow_flat_columns_table"][0])

    # count_32 has comment, description, and xsd_type without doc -> Iceberg doc=None
    assert by_name["count_32"].input_metadata_keys == "`comment`, `description`, `xsd_type`"
    assert "`doc=None`" in by_name["count_32"].iceberg_field_summary

    nested_dlt_rows = build_dlt_pyarrow_nested_dlt_column_rows()
    assert [row.column_name for row in nested_dlt_rows] == list(DEEP_NESTED_ARROW_SCHEMA.names)
    nested_by_name = {row.column_name: row for row in nested_dlt_rows}
    assert nested_by_name["id"].nested_hint_summary == "—"
    assert "`x-nested-type='arrow-ipc:...'`" in nested_by_name["organism"].dlt_column_summary
    assert "Roundtrips full `pa.DataType`" in nested_by_name["organism"].nested_hint_summary
    assert len(nested_by_name["organism"].cells) == len(
        TABLE_TEMPLATE_REGISTRY["dlt_pyarrow_nested_dlt_columns_table"][0]
    )


def test_build_dlt_pyarrow_nested_path_rows_covers_all_deep_nested_nodes() -> None:
    """build_dlt_pyarrow_nested_path_rows visits all 30 nodes in DEEP_NESTED_ARROW_SCHEMA in order."""
    rows = build_dlt_pyarrow_nested_path_rows()
    expected_paths = list(extract_arrow_field_metadata(DEEP_NESTED_ARROW_SCHEMA))
    assert [row.input_path for row in rows] == expected_paths

    by_path = {row.input_path: row for row in rows}
    # list element path renames synonym_item -> element in Parquet and drops doc in Iceberg
    synonym_elem = by_path["organism.lineage.detail.synonyms.synonym_item"]
    assert synonym_elem.slot_kind == SlotKind.LIST_ELEMENT
    assert synonym_elem.parquet_path == "organism.lineage.detail.synonyms.element"
    assert "`organism.lineage.detail.synonyms.element`:" in synonym_elem.cells[3]
    assert "*(no `doc` slot)*" in synonym_elem.iceberg_summary

    # struct field inside map value keeps doc in Iceberg
    note_row = by_path["annotations.value.note"]
    assert note_row.slot_kind == SlotKind.STRUCT_FIELD
    assert note_row.parquet_path == "annotations.value.note"
    assert "`doc='Annotation text'`" in note_row.iceberg_summary


def test_collect_nested_and_identifier_cursors_support_large_list_and_map_branches() -> None:
    """collect_nested_slot_rows handles large_list and collect_identifier_slot_rows handles large_list and map."""
    in_schema = pa.schema(
        [
            pa.field(
                "bigItems",
                pa.large_list(pa.field("bigElem", pa.string(), nullable=False, metadata={"doc": "Elem doc"})),
                nullable=False,
                metadata={"doc": "Big list doc"},
            ),
            pa.field(
                "scoreMap",
                pa.map_(
                    pa.field("mapKey", pa.string(), nullable=False, metadata={"doc": "Key doc"}),
                    pa.field("mapVal", pa.int64(), nullable=True, metadata={"doc": "Val doc"}),
                ),
                nullable=True,
                metadata={"doc": "Map doc"},
            ),
        ]
    )
    dlt_schema = pa.schema([in_schema.field(0).with_name("big_items"), in_schema.field(1).with_name("score_map")])
    pq_schema = with_parquet_compliant_list_elements(dlt_schema)
    ice_schema = IcebergSchema(
        NestedField(
            field_id=1,
            name="big_items",
            field_type=ListType(element_id=2, element_type=StringType(), element_required=True),
            required=True,
            doc="Big list doc",
        ),
        NestedField(
            field_id=3,
            name="score_map",
            field_type=MapType(
                key_id=4,
                key_type=StringType(),
                value_id=5,
                value_type=LongType(),
                value_required=False,
            ),
            required=False,
            doc="Map doc",
        ),
    )
    scan_schema = pa.schema(
        [
            pa.field(
                "big_items",
                pa.large_list(pa.field("element", pa.string(), nullable=False)),
                nullable=False,
                metadata={"doc": "Big list doc"},
            ),
            pa.field(
                "score_map",
                pa.map_(pa.field("key", pa.string(), nullable=False), pa.field("value", pa.int64(), nullable=True)),
                nullable=True,
                metadata={"doc": "Map doc"},
            ),
        ]
    )

    synthetic_spec = ShapeExpectation(
        input_arrow_schema=dlt_schema,
        table_metadata={},
        rows=(),
        expected_dlt_columns={},
        expected_iceberg_schema=ice_schema,
        expected_iceberg_docs={},
        expected_iceberg_scan_arrow_schema=scan_schema,
    )
    nested_rows: list[DltPyArrowNestedPathRow] = []
    collect_nested_slot_rows(
        NestedSlotCursor(
            input_path="big_items",
            output_path="big_items",
            slot_kind=SlotKind.ROOT_COLUMN,
            input_field=dlt_schema.field("big_items"),
            parquet_field=pq_schema.field("big_items"),
            iceberg_type=ice_schema.find_field("big_items").field_type,
            iceberg_required=True,
            iceberg_doc="Big list doc",
            scan_field=scan_schema.field("big_items"),
        ),
        nested_rows,
    )
    assert [r.input_path for r in nested_rows] == ["big_items", "big_items.bigElem"]
    assert [r.input_path for r in build_dlt_pyarrow_nested_path_rows(synthetic_spec)] == [
        "big_items",
        "big_items.bigElem",
        "score_map",
        "score_map.mapKey",
        "score_map.mapVal",
    ]

    ident_rows: list[DltPyArrowIdentifierRow] = []
    for in_top, dlt_top, pq_top in zip(in_schema, dlt_schema, pq_schema, strict=True):
        collect_identifier_slot_rows(
            IdentifierSlotCursor(
                fixture_name="synthetic",
                input_path=in_top.name,
                dlt_path=dlt_top.name,
                parquet_path=pq_top.name,
                slot_kind=SlotKind.ROOT_COLUMN,
                input_field=in_top,
                dlt_field=dlt_top,
                parquet_field=pq_top,
            ),
            ident_rows,
        )
    assert [r.parquet_path for r in ident_rows] == [
        "big_items",
        "big_items.element",
        "score_map",
        "score_map.mapKey",
        "score_map.mapVal",
    ]


def test_build_dlt_pyarrow_identifier_rows_and_page_materialization_pass() -> None:
    """Identifier rows match camelCase fixtures and docs/dlt_pyarrow_definitions.md renders clean HTML tables."""
    ident_rows = build_dlt_pyarrow_identifier_rows()
    by_key = {(r.fixture_name, r.input_path): r for r in ident_rows}
    assert by_key[("flat", "entryId")].dlt_path == "entry_id"
    assert by_key[("flat", "_rawCode")].dlt_path == "_raw_code"
    assert by_key[("deep_nested", "outerBlock.camelChild")].parquet_path == "outer_block.camelChild"
    assert by_key[("deep_nested", "outerBlock.deepList.listElem")].dlt_path == "outer_block.deepList.listElem"
    assert by_key[("deep_nested", "outerBlock.deepList.listElem")].parquet_path == "outer_block.deepList.element"
    assert (
        by_key[("deep_nested", "outerBlock.deepList.listElem._innerCode")].parquet_path
        == "outer_block.deepList.element._innerCode"
    )
    assert len(by_key[("flat", "entryId")].cells) == len(
        TABLE_TEMPLATE_REGISTRY["dlt_pyarrow_identifier_normalization_table"][0]
    )

    doc_path = DOCS_DIR / "dlt_pyarrow_definitions.md"
    rendered = materialize_markdown_file(doc_path)
    assert "{{" not in rendered
    assert "`small_code`" in rendered
    assert "`organism.lineage.detail.synonyms.synonym_item`" in rendered
    assert "`outerBlock.deepList.listElem._innerCode`" in rendered

    rendered_html = markdown.markdown(rendered, extensions=["tables", "fenced_code"])
    expected_html_tables = 5
    assert rendered_html.count("<table>") == expected_html_tables
    assert r"\|" not in rendered_html
