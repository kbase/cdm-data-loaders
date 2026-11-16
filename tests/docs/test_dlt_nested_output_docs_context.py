"""Unit and rendering tests for ``docs_macros.dlt_nested_output_context`` and ``docs/dlt_nested_output_behaviour.md``."""

import markdown
import pyarrow as pa
import pytest
from pyiceberg.schema import Schema as IcebergSchema
from pyiceberg.types import (
    DoubleType,
    IcebergType,
    ListType,
    LongType,
    MapType,
    NestedField,
    StringType,
    StructType,
)

from docs_macros.constants import DOCS_DIR
from docs_macros.dlt_nested_output_context import (
    build_dlt_nested_placement_rows,
    build_dlt_nested_single_table_column_rows,
    build_dlt_unnested_table_rows,
    format_arrow_schema_fields,
    format_compact_arrow_type,
    format_compact_iceberg_type,
    format_iceberg_schema_fields,
    summarize_json_value_sample,
)
from docs_macros.tables_context import TABLE_TEMPLATE_REGISTRY
from tests.dependencies.test_dlt_nested_output_behaviour import (
    ALL_NESTING_PLACEMENTS,
    EXPECTED_ARROW_SCHEMA_DICT_NESTING_0,
    EXPECTED_DLT_COLUMNS_DICT_UNNESTED,
    NestingPlacement,
)
from tests.docs.helpers import materialize_markdown_file

pytestmark = pytest.mark.docs


@pytest.mark.parametrize(
    ("dtype", "expected"),
    [
        pytest.param(pa.int64(), "pa.int64()", id="scalar_int64"),
        pytest.param(
            pa.struct([("city", pa.string()), ("zip", pa.string())]),
            "pa.struct([city, zip])",
            id="struct_fields",
        ),
        pytest.param(pa.list_(pa.string()), "pa.list_(pa.string())", id="list_of_strings"),
        pytest.param(
            pa.map_(pa.string(), pa.float64()),
            "pa.map_(pa.string(), pa.float64())",
            id="map_string_float64",
        ),
    ],
)
def test_format_compact_arrow_type_formats_scalars_and_containers(dtype: pa.DataType, expected: str) -> None:
    """format_compact_arrow_type summarizes scalar, struct, list, and map PyArrow types."""
    assert format_compact_arrow_type(dtype) == expected


@pytest.mark.parametrize(
    ("iceberg_type", "expected"),
    [
        pytest.param(LongType(), "LongType()", id="primitive_long"),
        pytest.param(
            StructType(
                NestedField(field_id=1, name="city", field_type=StringType(), required=False),
                NestedField(field_id=2, name="zip", field_type=StringType(), required=False),
            ),
            "StructType(city, zip)",
            id="struct_fields",
        ),
        pytest.param(
            ListType(element_id=3, element_type=StringType(), element_required=False),
            "ListType(StringType())",
            id="list_of_strings",
        ),
        pytest.param(
            MapType(key_id=4, key_type=StringType(), value_id=5, value_type=DoubleType(), value_required=False),
            "MapType(StringType(), DoubleType())",
            id="map_string_double",
        ),
    ],
)
def test_format_compact_iceberg_type_formats_primitives_and_containers(
    iceberg_type: IcebergType,
    expected: str,
) -> None:
    """format_compact_iceberg_type summarizes primitive, StructType, ListType, and MapType instances."""
    assert format_compact_iceberg_type(iceberg_type) == expected


@pytest.mark.parametrize(
    ("col_name", "parsed_row", "is_dlt_meta", "expected"),
    [
        pytest.param("_dlt_id", {}, True, "Non-empty `str` (`_dlt` metadata)", id="dlt_metadata_column"),
        pytest.param("address", {"address": {"city": "Berlin"}}, False, "Native JSON `object`", id="dict_object"),
        pytest.param("tags", {"tags": ["a", "b"]}, False, "Native JSON `array`", id="plain_list"),
        pytest.param(
            "metrics",
            {"metrics": [["score", 98.5]]},
            False,
            "JSON `array` of `[key, value]` pairs (`[['score', 98.5]]`)",
            id="metrics_pairs",
        ),
        pytest.param("id", {"id": 1}, False, "JSON `int` (`1`)", id="scalar_int"),
        pytest.param("name", {"name": "a|b"}, False, r"JSON `str` (`'a\|b'`)", id="scalar_str_escaped_pipe"),
    ],
)
def test_summarize_json_value_sample_handles_metadata_containers_and_scalars(
    col_name: str,
    parsed_row: dict[str, object],
    is_dlt_meta: bool,
    expected: str,
) -> None:
    """summarize_json_value_sample formats _dlt_* metadata, JSON objects, arrays, pairs, and scalars."""
    assert summarize_json_value_sample(col_name, parsed_row, is_dlt_meta=is_dlt_meta) == expected


def test_format_arrow_and_iceberg_schema_fields_include_non_null_and_required_flags() -> None:
    """format_arrow_schema_fields and format_iceberg_schema_fields tag non-null and required fields."""
    arrow_schema = pa.schema(
        [
            pa.field("value", pa.string(), nullable=True),
            pa.field("_dlt_id", pa.string(), nullable=False),
        ]
    )
    assert format_arrow_schema_fields(arrow_schema) == "`value: pa.string()`, `_dlt_id: pa.string()` (not null)"

    iceberg_schema = IcebergSchema(
        NestedField(field_id=1, name="value", field_type=StringType(), required=False),
        NestedField(field_id=2, name="_dlt_id", field_type=StringType(), required=True),
    )
    assert format_iceberg_schema_fields(iceberg_schema) == "`value: StringType()`, `_dlt_id: StringType()` (required)"


def test_build_dlt_nested_placement_rows_captures_propagation_and_pyarrow_invariance() -> None:
    """Placement rows cover all five NestingPlacement members and highlight resource_only non-propagation."""
    rows = build_dlt_nested_placement_rows()
    assert [row.placement for row in rows] == [p.value for p in ALL_NESTING_PLACEMENTS]
    by_placement = {row.placement: row for row in rows}

    assert "`1` table (`widget`)" in by_placement[NestingPlacement.TRANSFORMER_ONLY.value].dict_outcome
    assert "`1` table (`widget`)" in by_placement[NestingPlacement.DESTINATION.value].dict_outcome
    assert "`6` tables" in by_placement[NestingPlacement.RESOURCE_ONLY.value].dict_outcome
    assert "does not propagate" in by_placement[NestingPlacement.RESOURCE_ONLY.value].dict_outcome
    assert "`6` tables" in by_placement[NestingPlacement.UNSET.value].dict_outcome
    assert "`max_table_nesting` ignored" in by_placement[NestingPlacement.UNSET.value].pyarrow_outcome
    assert len(by_placement[NestingPlacement.UNSET.value].cells) == len(
        TABLE_TEMPLATE_REGISTRY["dlt_nested_placement_matrix_table"][0]
    )


def test_build_dlt_nested_single_table_column_rows_compares_dict_and_pyarrow_columns() -> None:
    """Single-table column rows contrast JSON stringification and _dlt_* columns against native Arrow types."""
    rows = build_dlt_nested_single_table_column_rows()
    assert [row.column_name for row in rows] == list(EXPECTED_ARROW_SCHEMA_DICT_NESTING_0.names)
    by_col = {row.column_name: row for row in rows}

    assert by_col["id"].dict_parquet_iceberg == "`bigint` / `pa.int64()` / `LongType()`"
    assert by_col["id"].pyarrow_parquet_iceberg == "`bigint` / `pa.int64()` / `LongType()`"

    assert "JSON string" in by_col["metrics"].dict_parquet_iceberg
    assert "`pa.map_(pa.string(), pa.float64())`" in by_col["metrics"].pyarrow_parquet_iceberg
    assert "`MapType(StringType(), DoubleType())`" in by_col["metrics"].pyarrow_parquet_iceberg
    assert "`[['score', 98.5]]`" in by_col["metrics"].dict_jsonl
    assert "`[['score', 98.5]]`" in by_col["metrics"].pyarrow_jsonl

    assert "`nullable=False`" in by_col["_dlt_id"].dict_parquet_iceberg
    assert "`required=True`" in by_col["_dlt_id"].dict_parquet_iceberg
    assert by_col["_dlt_id"].pyarrow_parquet_iceberg == "*(not added)*"
    assert by_col["_dlt_id"].pyarrow_jsonl == "*(not added)*"
    assert len(by_col["_dlt_id"].cells) == len(TABLE_TEMPLATE_REGISTRY["dlt_nested_single_table_columns_table"][0])


def test_build_dlt_unnested_table_rows_and_page_materialization_pass() -> None:
    """Unnested table rows cover all six tables and docs/dlt_nested_output_behaviour.md renders clean HTML tables."""
    rows = build_dlt_unnested_table_rows()
    assert [row.table_name for row in rows] == list(EXPECTED_DLT_COLUMNS_DICT_UNNESTED)
    by_table = {row.table_name: row for row in rows}

    assert "`address__city: text`" in by_table["widget"].dlt_columns_summary
    assert "coerced to `text`" in by_table["widget__metrics__list"].role_summary
    assert "`_dlt_parent_id: pa.string()` (not null)" in by_table["widget__tags"].parquet_schema_summary
    assert "`_dlt_parent_id: StringType()` (required)" in by_table["widget__tags"].iceberg_schema_summary
    assert len(by_table["widget"].cells) == len(TABLE_TEMPLATE_REGISTRY["dlt_nested_unnested_tables_table"][0])

    doc_path = DOCS_DIR / "dlt_nested_output_behaviour.md"
    rendered = materialize_markdown_file(doc_path)
    assert "{{" not in rendered
    assert "`resource_only`" in rendered
    assert "`widget__metrics__list`" in rendered
    assert "`ArrowToJsonlWriter`" in rendered

    rendered_html = markdown.markdown(rendered, extensions=["tables", "fenced_code"])
    expected_html_tables = 4
    assert rendered_html.count("<table>") == expected_html_tables
    assert r"\|" not in rendered_html
