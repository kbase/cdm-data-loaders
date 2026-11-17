"""Characterisation tests for how dlt normalizes and writes nested data across formats and nesting configs.

Covers the combinations of:
- Input item type: plain Python ``dict`` vs ``pyarrow.Table`` with an explicit nested schema.
- ``max_table_nesting`` configuration: ``0`` set on ``@dlt.resource`` + ``@dlt.transformer``, ``0`` set on
  ``@dlt.transformer`` alone, ``0`` set on ``dlt.destinations.filesystem``, ``0`` set on ``@dlt.resource``
  alone (which does not propagate to a downstream ``@dlt.transformer``), and unset (``None``).
- Output format on the ``filesystem`` destination: ``parquet``, ``jsonl``, and ``iceberg`` table format.
"""

import json
from collections.abc import Generator
from operator import itemgetter
from pathlib import Path
from typing import Any

import dlt
import pyarrow as pa
import pytest
from dlt.common.pipeline import LoadInfo
from dlt.extract import DltResource
from dlt.extract.items import DataItemWithMeta

from cdm_data_loaders.utils.iceberg.cleanup import install_iceberg_catalog_cleanup
from specs._loader.dlt_contracts import (
    ALL_NESTING_PLACEMENTS,
    COMPLEX_COLUMNS,
    EXPECTED_ARROW_SCHEMA_DICT_NESTING_0,
    EXPECTED_ARROW_SCHEMAS_DICT_UNNESTED,
    EXPECTED_DLT_COLUMNS_DICT_NESTING_0,
    EXPECTED_DLT_COLUMNS_DICT_UNNESTED,
    EXPECTED_DLT_COLUMNS_PYARROW,
    EXPECTED_ICEBERG_SCHEMA_DICT_NESTING_0,
    EXPECTED_ICEBERG_SCHEMA_PYARROW,
    EXPECTED_ICEBERG_SCHEMAS_DICT_UNNESTED,
    EXPECTED_JSON_STRING_ROWS,
    EXPECTED_PARSED_JSON_ROWS,
    NESTED_ARROW_SCHEMA,
    NESTED_DATASET_NAME,
    NESTED_ROWS,
    NESTED_TABLE_NAME,
    NESTING_ZERO_PLACEMENTS,
    UNNESTED_PLACEMENTS,
    NestingPlacement,
)
from tests.helpers.table_readers import (
    is_metadata_table,
    read_iceberg_dataset,
    read_jsonl_dataset,
    read_parquet_dataset,
    strip_metadata_columns,
)


def build_routed_resource(
    *,
    use_pyarrow: bool,
    placement: NestingPlacement,
) -> DltResource:
    """Build a ``source -> transformer`` resource pair with ``dlt.mark.with_table_name`` routing.

    :param use_pyarrow: Yield a ``pyarrow.Table`` with ``NESTED_ARROW_SCHEMA`` when ``True``, or ``NESTED_ROWS`` dicts when ``False``.
    :type use_pyarrow: bool
    :param placement: Where ``max_table_nesting=0`` is applied.
    :type placement: NestingPlacement
    :return: Routed ``DltResource`` transformer.
    :rtype: DltResource
    """
    resource_kwargs: dict[str, Any] = {"name": f"{NESTED_TABLE_NAME}_source", "parallelized": True}
    transformer_kwargs: dict[str, Any] = {"name": f"{NESTED_TABLE_NAME}_routed", "parallelized": True}

    if placement in (NestingPlacement.RESOURCE_AND_TRANSFORMER, NestingPlacement.RESOURCE_ONLY):
        resource_kwargs["max_table_nesting"] = 0
    if placement in (NestingPlacement.RESOURCE_AND_TRANSFORMER, NestingPlacement.TRANSFORMER_ONLY):
        transformer_kwargs["max_table_nesting"] = 0

    @dlt.resource(**resource_kwargs)
    def _source() -> Generator[list[dict[str, Any]] | pa.Table]:
        if use_pyarrow:
            yield pa.Table.from_pylist(list(NESTED_ROWS), schema=NESTED_ARROW_SCHEMA)
        else:
            yield [dict(row) for row in NESTED_ROWS]

    @dlt.transformer(data_from=_source, **transformer_kwargs)
    def _route(page: list[dict[str, Any]] | pa.Table) -> Generator[DataItemWithMeta]:
        yield dlt.mark.with_table_name(page, NESTED_TABLE_NAME)

    return _route


def run_filesystem_pipeline(
    tmp_path: Path,
    *,
    use_pyarrow: bool,
    placement: NestingPlacement,
    loader_file_format: str,
    table_format: str | None = None,
) -> tuple[dlt.Pipeline, LoadInfo, Path]:
    """Run a ``dlt`` filesystem pipeline for the given input kind, nesting placement, and format.

    :param tmp_path: Temporary directory for pipeline metadata and destination bucket.
    :type tmp_path: Path
    :param use_pyarrow: Whether to yield a ``pyarrow.Table`` instead of Python dicts.
    :type use_pyarrow: bool
    :param placement: Where ``max_table_nesting=0`` is configured.
    :type placement: NestingPlacement
    :param loader_file_format: ``"parquet"`` or ``"jsonl"``.
    :type loader_file_format: str
    :param table_format: Optional table format such as ``"iceberg"``.
    :type table_format: str | None
    :return: Tuple of ``(pipeline, load_info, bucket_dir)``.
    :rtype: tuple[dlt.Pipeline, LoadInfo, Path]
    """
    pipelines_dir = tmp_path / "pipelines"
    bucket_dir = tmp_path / "bucket"
    bucket_dir.mkdir(parents=True, exist_ok=True)

    dest_kwargs: dict[str, Any] = {"bucket_url": str(bucket_dir)}
    if placement == NestingPlacement.DESTINATION:
        dest_kwargs["max_table_nesting"] = 0

    pipeline = dlt.pipeline(
        pipeline_name=f"nested_{'arrow' if use_pyarrow else 'dict'}_{placement}_{loader_file_format}_{table_format}",
        destination=dlt.destinations.filesystem(**dest_kwargs),
        dataset_name=NESTED_DATASET_NAME,
        pipelines_dir=str(pipelines_dir),
    )

    resource = build_routed_resource(use_pyarrow=use_pyarrow, placement=placement)
    run_kwargs: dict[str, Any] = {"loader_file_format": loader_file_format}
    if table_format is not None:
        if table_format == "iceberg":
            install_iceberg_catalog_cleanup()
        run_kwargs["table_format"] = table_format

    load_info = pipeline.run(resource, **run_kwargs)
    assert not load_info.has_failed_jobs
    assert len(load_info.loads_ids) == 1
    return pipeline, load_info, bucket_dir


def extract_dlt_column_types(pipeline: dlt.Pipeline) -> dict[str, dict[str, str | None]]:
    """Extract ``{table_name: {column_name: data_type}}`` for all non-metadata tables in the default schema.

    :param pipeline: Executed ``dlt.Pipeline``.
    :type pipeline: dlt.Pipeline
    :return: Mapping of data table names to column ``data_type`` mappings.
    :rtype: dict[str, dict[str, str | None]]
    """
    return {
        table_name: {col_name: col.get("data_type") for col_name, col in table.get("columns", {}).items()}
        for table_name, table in pipeline.default_schema.tables.items()
        if not is_metadata_table(table_name)
    }


def assert_dlt_metadata_and_strip(rows: list[dict[str, Any]], expected_load_id: str) -> list[dict[str, Any]]:
    """Verify every row carries ``_dlt_load_id`` and a unique ``_dlt_id``, then return rows without metadata.

    :param rows: Loaded rows from the root table.
    :type rows: list[dict[str, Any]]
    :param expected_load_id: Load package ID from ``LoadInfo``.
    :type expected_load_id: str
    :return: Rows with ``_dlt_*`` keys removed.
    :rtype: list[dict[str, Any]]
    """
    dlt_ids: list[str] = []
    for row in rows:
        assert row["_dlt_load_id"] == expected_load_id
        assert isinstance(row["_dlt_id"], str)
        assert row["_dlt_id"]
        dlt_ids.append(row["_dlt_id"])
    assert len(set(dlt_ids)) == len(rows)
    return [strip_metadata_columns(row) for row in rows]


def assert_unnested_table_rows(
    tables: dict[str, list[dict[str, Any]]],
    expected_load_id: str,
) -> None:
    """Assert the contents and parent-child key links across all six unnested dict tables.

    :param tables: Mapping from table name to loaded row dicts.
    :type tables: dict[str, list[dict[str, Any]]]
    :param expected_load_id: Load package ID from ``LoadInfo``.
    :type expected_load_id: str
    """
    assert set(tables) == set(EXPECTED_DLT_COLUMNS_DICT_UNNESTED)

    widget_rows = sorted(tables["widget"], key=itemgetter("id"))
    assert assert_dlt_metadata_and_strip(widget_rows, expected_load_id) == [
        {"id": 1, "name": "alpha", "address__city": "Berlin", "address__zip": "10115"},
        {"id": 2, "name": "beta", "address__city": "Turin", "address__zip": "10121"},
    ]
    widget_dlt_id = {row["id"]: row["_dlt_id"] for row in widget_rows}

    def _strip_dlt_id(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        ids = [row["_dlt_id"] for row in rows]
        assert all(isinstance(i, str) and i for i in ids)
        assert len(set(ids)) == len(rows)
        return [{k: v for k, v in row.items() if k != "_dlt_id"} for row in rows]

    assert _strip_dlt_id(tables["widget__address__street"]) == [
        {"value": "Kaiser", "_dlt_parent_id": widget_dlt_id[1], "_dlt_list_idx": 0},
        {"value": "Permanente", "_dlt_parent_id": widget_dlt_id[1], "_dlt_list_idx": 1},
        {"value": "Marco Polo", "_dlt_parent_id": widget_dlt_id[2], "_dlt_list_idx": 0},
    ]

    assert _strip_dlt_id(tables["widget__tags"]) == [
        {"value": "a", "_dlt_parent_id": widget_dlt_id[1], "_dlt_list_idx": 0},
        {"value": "b", "_dlt_parent_id": widget_dlt_id[1], "_dlt_list_idx": 1},
        {"value": "c", "_dlt_parent_id": widget_dlt_id[1], "_dlt_list_idx": 2},
        {"value": "d", "_dlt_parent_id": widget_dlt_id[2], "_dlt_list_idx": 0},
    ]

    assert _strip_dlt_id(tables["widget__children"]) == [
        {"k": "x", "v": 1, "_dlt_parent_id": widget_dlt_id[1], "_dlt_list_idx": 0},
        {"k": "y", "v": 2, "_dlt_parent_id": widget_dlt_id[1], "_dlt_list_idx": 1},
    ]

    metrics_rows = tables["widget__metrics"]
    assert _strip_dlt_id(metrics_rows) == [
        {"_dlt_parent_id": widget_dlt_id[1], "_dlt_list_idx": 0},
        {"_dlt_parent_id": widget_dlt_id[2], "_dlt_list_idx": 0},
    ]
    metrics_dlt_id = {row["_dlt_parent_id"]: row["_dlt_id"] for row in metrics_rows}

    assert _strip_dlt_id(tables["widget__metrics__list"]) == [
        {"value": "score", "_dlt_parent_id": metrics_dlt_id[widget_dlt_id[1]], "_dlt_list_idx": 0},
        {"value": "98.5", "_dlt_parent_id": metrics_dlt_id[widget_dlt_id[1]], "_dlt_list_idx": 1},
        {"value": "score", "_dlt_parent_id": metrics_dlt_id[widget_dlt_id[2]], "_dlt_list_idx": 0},
        {"value": "72.0", "_dlt_parent_id": metrics_dlt_id[widget_dlt_id[2]], "_dlt_list_idx": 1},
    ]


@pytest.mark.parametrize("placement", NESTING_ZERO_PLACEMENTS, ids=[p.value for p in NESTING_ZERO_PLACEMENTS])
def test_dlt_dict_nesting_zero_parquet(tmp_path: Path, placement: NestingPlacement) -> None:
    """Verify dict items with max_table_nesting=0 write nested fields to Parquet as JSON strings."""
    pipeline, load_info, bucket_dir = run_filesystem_pipeline(
        tmp_path,
        use_pyarrow=False,
        placement=placement,
        loader_file_format="parquet",
    )

    assert extract_dlt_column_types(pipeline) == EXPECTED_DLT_COLUMNS_DICT_NESTING_0

    schemas, tables = read_parquet_dataset(bucket_dir / NESTED_DATASET_NAME)
    assert schemas == {NESTED_TABLE_NAME: EXPECTED_ARROW_SCHEMA_DICT_NESTING_0}

    rows = assert_dlt_metadata_and_strip(tables[NESTED_TABLE_NAME], load_info.loads_ids[0])
    assert rows == EXPECTED_JSON_STRING_ROWS
    assert [
        {k: json.loads(v) if k in COMPLEX_COLUMNS else v for k, v in row.items()} for row in rows
    ] == EXPECTED_PARSED_JSON_ROWS


@pytest.mark.parametrize("placement", NESTING_ZERO_PLACEMENTS, ids=[p.value for p in NESTING_ZERO_PLACEMENTS])
def test_dlt_dict_nesting_zero_iceberg(tmp_path: Path, placement: NestingPlacement) -> None:
    """Verify dict items with max_table_nesting=0 write nested fields to Iceberg as string columns holding JSON."""
    pipeline, load_info, _ = run_filesystem_pipeline(
        tmp_path,
        use_pyarrow=False,
        placement=placement,
        loader_file_format="parquet",
        table_format="iceberg",
    )

    assert extract_dlt_column_types(pipeline) == EXPECTED_DLT_COLUMNS_DICT_NESTING_0

    iceberg_schemas, arrow_schemas, tables = read_iceberg_dataset(pipeline)
    assert iceberg_schemas == {NESTED_TABLE_NAME: EXPECTED_ICEBERG_SCHEMA_DICT_NESTING_0}
    assert arrow_schemas == {NESTED_TABLE_NAME: EXPECTED_ARROW_SCHEMA_DICT_NESTING_0}

    rows = assert_dlt_metadata_and_strip(tables[NESTED_TABLE_NAME], load_info.loads_ids[0])
    assert rows == EXPECTED_JSON_STRING_ROWS
    assert [
        {k: json.loads(v) if k in COMPLEX_COLUMNS else v for k, v in row.items()} for row in rows
    ] == EXPECTED_PARSED_JSON_ROWS


@pytest.mark.parametrize("placement", NESTING_ZERO_PLACEMENTS, ids=[p.value for p in NESTING_ZERO_PLACEMENTS])
def test_dlt_dict_nesting_zero_jsonl(tmp_path: Path, placement: NestingPlacement) -> None:
    """Verify dict items with max_table_nesting=0 write nested fields to JSONL as native JSON objects and arrays."""
    pipeline, load_info, bucket_dir = run_filesystem_pipeline(
        tmp_path,
        use_pyarrow=False,
        placement=placement,
        loader_file_format="jsonl",
    )

    assert extract_dlt_column_types(pipeline) == EXPECTED_DLT_COLUMNS_DICT_NESTING_0

    tables = read_jsonl_dataset(bucket_dir / NESTED_DATASET_NAME)
    assert set(tables) == {NESTED_TABLE_NAME}
    rows = assert_dlt_metadata_and_strip(tables[NESTED_TABLE_NAME], load_info.loads_ids[0])
    assert rows == EXPECTED_PARSED_JSON_ROWS


@pytest.mark.parametrize("placement", UNNESTED_PLACEMENTS, ids=[p.value for p in UNNESTED_PLACEMENTS])
def test_dlt_dict_unnested_parquet(tmp_path: Path, placement: NestingPlacement) -> None:
    """Verify dict items without effective max_table_nesting=0 flatten structs and split lists into Parquet child tables."""
    pipeline, load_info, bucket_dir = run_filesystem_pipeline(
        tmp_path,
        use_pyarrow=False,
        placement=placement,
        loader_file_format="parquet",
    )

    assert extract_dlt_column_types(pipeline) == EXPECTED_DLT_COLUMNS_DICT_UNNESTED

    schemas, tables = read_parquet_dataset(bucket_dir / NESTED_DATASET_NAME)
    assert schemas == dict(EXPECTED_ARROW_SCHEMAS_DICT_UNNESTED)
    assert_unnested_table_rows(tables, load_info.loads_ids[0])


@pytest.mark.parametrize("placement", UNNESTED_PLACEMENTS, ids=[p.value for p in UNNESTED_PLACEMENTS])
def test_dlt_dict_unnested_iceberg(tmp_path: Path, placement: NestingPlacement) -> None:
    """Verify dict items without effective max_table_nesting=0 flatten structs and split lists into Iceberg child tables."""
    pipeline, load_info, _ = run_filesystem_pipeline(
        tmp_path,
        use_pyarrow=False,
        placement=placement,
        loader_file_format="parquet",
        table_format="iceberg",
    )

    assert extract_dlt_column_types(pipeline) == EXPECTED_DLT_COLUMNS_DICT_UNNESTED

    iceberg_schemas, arrow_schemas, tables = read_iceberg_dataset(pipeline)
    assert iceberg_schemas == dict(EXPECTED_ICEBERG_SCHEMAS_DICT_UNNESTED)
    assert arrow_schemas == dict(EXPECTED_ARROW_SCHEMAS_DICT_UNNESTED)
    assert_unnested_table_rows(tables, load_info.loads_ids[0])


@pytest.mark.parametrize("placement", UNNESTED_PLACEMENTS, ids=[p.value for p in UNNESTED_PLACEMENTS])
def test_dlt_dict_unnested_jsonl(tmp_path: Path, placement: NestingPlacement) -> None:
    """Verify dict items without effective max_table_nesting=0 flatten structs and split lists into JSONL child tables."""
    pipeline, load_info, bucket_dir = run_filesystem_pipeline(
        tmp_path,
        use_pyarrow=False,
        placement=placement,
        loader_file_format="jsonl",
    )

    assert extract_dlt_column_types(pipeline) == EXPECTED_DLT_COLUMNS_DICT_UNNESTED

    tables = read_jsonl_dataset(bucket_dir / NESTED_DATASET_NAME)
    assert_unnested_table_rows(tables, load_info.loads_ids[0])


@pytest.mark.parametrize("placement", ALL_NESTING_PLACEMENTS, ids=[p.value for p in ALL_NESTING_PLACEMENTS])
def test_dlt_pyarrow_nested_parquet(tmp_path: Path, placement: NestingPlacement) -> None:
    """Verify pyarrow.Table items preserve native struct, list, and map types in Parquet regardless of max_table_nesting."""
    pipeline, _, bucket_dir = run_filesystem_pipeline(
        tmp_path,
        use_pyarrow=True,
        placement=placement,
        loader_file_format="parquet",
    )

    assert extract_dlt_column_types(pipeline) == EXPECTED_DLT_COLUMNS_PYARROW

    schemas, tables = read_parquet_dataset(bucket_dir / NESTED_DATASET_NAME)
    assert schemas == {NESTED_TABLE_NAME: NESTED_ARROW_SCHEMA}
    assert tables == {NESTED_TABLE_NAME: list(NESTED_ROWS)}


@pytest.mark.parametrize("placement", ALL_NESTING_PLACEMENTS, ids=[p.value for p in ALL_NESTING_PLACEMENTS])
def test_dlt_pyarrow_nested_iceberg(tmp_path: Path, placement: NestingPlacement) -> None:
    """Verify pyarrow.Table items preserve native struct, list, and map types in Iceberg regardless of max_table_nesting."""
    pipeline, _, _ = run_filesystem_pipeline(
        tmp_path,
        use_pyarrow=True,
        placement=placement,
        loader_file_format="parquet",
        table_format="iceberg",
    )

    assert extract_dlt_column_types(pipeline) == EXPECTED_DLT_COLUMNS_PYARROW

    iceberg_schemas, arrow_schemas, tables = read_iceberg_dataset(pipeline)
    assert iceberg_schemas == {NESTED_TABLE_NAME: EXPECTED_ICEBERG_SCHEMA_PYARROW}
    assert arrow_schemas == {NESTED_TABLE_NAME: NESTED_ARROW_SCHEMA}
    assert tables == {NESTED_TABLE_NAME: list(NESTED_ROWS)}


@pytest.mark.parametrize("placement", ALL_NESTING_PLACEMENTS, ids=[p.value for p in ALL_NESTING_PLACEMENTS])
def test_dlt_pyarrow_nested_jsonl(
    tmp_path: Path,
    placement: NestingPlacement,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Verify pyarrow.Table items written as JSONL keep a single unflattened table without _dlt metadata columns."""
    pipeline, _, bucket_dir = run_filesystem_pipeline(
        tmp_path,
        use_pyarrow=True,
        placement=placement,
        loader_file_format="jsonl",
    )

    assert "ArrowToJsonlWriter" in caplog.text
    assert extract_dlt_column_types(pipeline) == EXPECTED_DLT_COLUMNS_PYARROW

    tables = read_jsonl_dataset(bucket_dir / NESTED_DATASET_NAME)
    assert tables == {NESTED_TABLE_NAME: EXPECTED_PARSED_JSON_ROWS}
