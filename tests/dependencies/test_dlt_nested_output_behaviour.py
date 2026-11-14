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
from enum import StrEnum
from pathlib import Path
from typing import Any, Final

import dlt
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from dlt.common.libs.pyiceberg import get_iceberg_tables
from dlt.common.pipeline import LoadInfo
from dlt.extract import DltResource
from frozendict import frozendict
from pyiceberg.schema import Schema as IcebergSchema
from pyiceberg.types import (
    DoubleType,
    ListType,
    LongType,
    MapType,
    NestedField,
    StringType,
    StructType,
)

from cdm_data_loaders.pipelines.iceberg_cleanup import install_iceberg_catalog_cleanup
from tests.cdm_data_loaders.pipelines.helpers import is_metadata_table, read_jsonl_file, strip_metadata_columns

TABLE_NAME: Final[str] = "widget"
DATASET_NAME: Final[str] = "nested_check_data"

ROWS: Final[tuple[dict[str, Any], ...]] = (
    {
        "id": 1,
        "name": "alpha",
        "address": {"street": ["Kaiser", "Permanente"], "city": "Berlin", "zip": "10115"},
        "tags": ["a", "b", "c"],
        "children": [{"k": "x", "v": 1}, {"k": "y", "v": 2}],
        "metrics": [("score", 98.5)],
    },
    {
        "id": 2,
        "name": "beta",
        "address": {"street": ["Marco Polo"], "city": "Turin", "zip": "10121"},
        "tags": ["d"],
        "children": [],
        "metrics": [("score", 72.0)],
    },
)

ARROW_SCHEMA: Final[pa.Schema] = pa.schema(
    [
        pa.field("id", pa.int64()),
        pa.field("name", pa.string()),
        pa.field(
            "address",
            pa.struct([("street", pa.list_(pa.string())), ("city", pa.string()), ("zip", pa.string())]),
        ),
        pa.field("tags", pa.list_(pa.string())),
        pa.field("children", pa.list_(pa.struct([("k", pa.string()), ("v", pa.int64())]))),
        pa.field("metrics", pa.map_(pa.string(), pa.float64())),
    ]
)

COMPLEX_COLUMNS: Final[tuple[str, ...]] = ("address", "tags", "children", "metrics")

EXPECTED_JSON_STRING_ROWS: Final[list[dict[str, Any]]] = [
    {
        "id": 1,
        "name": "alpha",
        "address": '{"street":["Kaiser","Permanente"],"city":"Berlin","zip":"10115"}',
        "tags": '["a","b","c"]',
        "children": '[{"k":"x","v":1},{"k":"y","v":2}]',
        "metrics": '[["score",98.5]]',
    },
    {
        "id": 2,
        "name": "beta",
        "address": '{"street":["Marco Polo"],"city":"Turin","zip":"10121"}',
        "tags": '["d"]',
        "children": "[]",
        "metrics": '[["score",72.0]]',
    },
]

EXPECTED_PARSED_JSON_ROWS: Final[list[dict[str, Any]]] = [
    {
        "id": 1,
        "name": "alpha",
        "address": {"street": ["Kaiser", "Permanente"], "city": "Berlin", "zip": "10115"},
        "tags": ["a", "b", "c"],
        "children": [{"k": "x", "v": 1}, {"k": "y", "v": 2}],
        "metrics": [["score", 98.5]],
    },
    {
        "id": 2,
        "name": "beta",
        "address": {"street": ["Marco Polo"], "city": "Turin", "zip": "10121"},
        "tags": ["d"],
        "children": [],
        "metrics": [["score", 72.0]],
    },
]

EXPECTED_DLT_COLUMNS_DICT_NESTING_0: Final[dict[str, dict[str, str]]] = {
    TABLE_NAME: {
        "id": "bigint",
        "name": "text",
        "address": "json",
        "tags": "json",
        "children": "json",
        "metrics": "json",
        "_dlt_load_id": "text",
        "_dlt_id": "text",
    }
}

EXPECTED_DLT_COLUMNS_PYARROW: Final[dict[str, dict[str, str]]] = {
    TABLE_NAME: {
        "id": "bigint",
        "name": "text",
        "address": "json",
        "tags": "json",
        "children": "json",
        "metrics": "json",
    }
}

EXPECTED_DLT_COLUMNS_DICT_UNNESTED: Final[dict[str, dict[str, str]]] = {
    "widget": {
        "id": "bigint",
        "name": "text",
        "address__city": "text",
        "address__zip": "text",
        "_dlt_load_id": "text",
        "_dlt_id": "text",
    },
    "widget__address__street": {
        "value": "text",
        "_dlt_parent_id": "text",
        "_dlt_list_idx": "bigint",
        "_dlt_id": "text",
    },
    "widget__children": {
        "k": "text",
        "v": "bigint",
        "_dlt_parent_id": "text",
        "_dlt_list_idx": "bigint",
        "_dlt_id": "text",
    },
    "widget__metrics": {
        "_dlt_parent_id": "text",
        "_dlt_list_idx": "bigint",
        "_dlt_id": "text",
    },
    "widget__metrics__list": {
        "value": "text",
        "_dlt_parent_id": "text",
        "_dlt_list_idx": "bigint",
        "_dlt_id": "text",
    },
    "widget__tags": {
        "value": "text",
        "_dlt_parent_id": "text",
        "_dlt_list_idx": "bigint",
        "_dlt_id": "text",
    },
}

EXPECTED_ARROW_SCHEMA_DICT_NESTING_0: Final[pa.Schema] = pa.schema(
    [
        pa.field("id", pa.int64()),
        pa.field("name", pa.string()),
        pa.field("address", pa.string()),
        pa.field("tags", pa.string()),
        pa.field("children", pa.string()),
        pa.field("metrics", pa.string()),
        pa.field("_dlt_load_id", pa.string(), nullable=False),
        pa.field("_dlt_id", pa.string(), nullable=False),
    ]
)

CHILD_VALUE_ARROW_SCHEMA: Final[pa.Schema] = pa.schema(
    [
        pa.field("value", pa.string()),
        pa.field("_dlt_parent_id", pa.string(), nullable=False),
        pa.field("_dlt_list_idx", pa.int64(), nullable=False),
        pa.field("_dlt_id", pa.string(), nullable=False),
    ]
)

EXPECTED_ARROW_SCHEMAS_DICT_UNNESTED: Final[frozendict[str, pa.Schema]] = frozendict(
    {
        "widget": pa.schema(
            [
                pa.field("id", pa.int64()),
                pa.field("name", pa.string()),
                pa.field("address__city", pa.string()),
                pa.field("address__zip", pa.string()),
                pa.field("_dlt_load_id", pa.string(), nullable=False),
                pa.field("_dlt_id", pa.string(), nullable=False),
            ]
        ),
        "widget__address__street": CHILD_VALUE_ARROW_SCHEMA,
        "widget__children": pa.schema(
            [
                pa.field("k", pa.string()),
                pa.field("v", pa.int64()),
                pa.field("_dlt_parent_id", pa.string(), nullable=False),
                pa.field("_dlt_list_idx", pa.int64(), nullable=False),
                pa.field("_dlt_id", pa.string(), nullable=False),
            ]
        ),
        "widget__metrics": pa.schema(
            [
                pa.field("_dlt_parent_id", pa.string(), nullable=False),
                pa.field("_dlt_list_idx", pa.int64(), nullable=False),
                pa.field("_dlt_id", pa.string(), nullable=False),
            ]
        ),
        "widget__metrics__list": CHILD_VALUE_ARROW_SCHEMA,
        "widget__tags": CHILD_VALUE_ARROW_SCHEMA,
    }
)

EXPECTED_ICEBERG_SCHEMA_DICT_NESTING_0: Final[IcebergSchema] = IcebergSchema(
    NestedField(field_id=1, name="id", field_type=LongType(), required=False),
    NestedField(field_id=2, name="name", field_type=StringType(), required=False),
    NestedField(field_id=3, name="address", field_type=StringType(), required=False),
    NestedField(field_id=4, name="tags", field_type=StringType(), required=False),
    NestedField(field_id=5, name="children", field_type=StringType(), required=False),
    NestedField(field_id=6, name="metrics", field_type=StringType(), required=False),
    NestedField(field_id=7, name="_dlt_load_id", field_type=StringType(), required=True),
    NestedField(field_id=8, name="_dlt_id", field_type=StringType(), required=True),
)

CHILD_VALUE_ICEBERG_SCHEMA: Final[IcebergSchema] = IcebergSchema(
    NestedField(field_id=1, name="value", field_type=StringType(), required=False),
    NestedField(field_id=2, name="_dlt_parent_id", field_type=StringType(), required=True),
    NestedField(field_id=3, name="_dlt_list_idx", field_type=LongType(), required=True),
    NestedField(field_id=4, name="_dlt_id", field_type=StringType(), required=True),
)

EXPECTED_ICEBERG_SCHEMAS_DICT_UNNESTED: Final[frozendict[str, IcebergSchema]] = frozendict(
    {
        "widget": IcebergSchema(
            NestedField(field_id=1, name="id", field_type=LongType(), required=False),
            NestedField(field_id=2, name="name", field_type=StringType(), required=False),
            NestedField(field_id=3, name="address__city", field_type=StringType(), required=False),
            NestedField(field_id=4, name="address__zip", field_type=StringType(), required=False),
            NestedField(field_id=5, name="_dlt_load_id", field_type=StringType(), required=True),
            NestedField(field_id=6, name="_dlt_id", field_type=StringType(), required=True),
        ),
        "widget__address__street": CHILD_VALUE_ICEBERG_SCHEMA,
        "widget__children": IcebergSchema(
            NestedField(field_id=1, name="k", field_type=StringType(), required=False),
            NestedField(field_id=2, name="v", field_type=LongType(), required=False),
            NestedField(field_id=3, name="_dlt_parent_id", field_type=StringType(), required=True),
            NestedField(field_id=4, name="_dlt_list_idx", field_type=LongType(), required=True),
            NestedField(field_id=5, name="_dlt_id", field_type=StringType(), required=True),
        ),
        "widget__metrics": IcebergSchema(
            NestedField(field_id=1, name="_dlt_parent_id", field_type=StringType(), required=True),
            NestedField(field_id=2, name="_dlt_list_idx", field_type=LongType(), required=True),
            NestedField(field_id=3, name="_dlt_id", field_type=StringType(), required=True),
        ),
        "widget__metrics__list": CHILD_VALUE_ICEBERG_SCHEMA,
        "widget__tags": CHILD_VALUE_ICEBERG_SCHEMA,
    }
)

EXPECTED_ICEBERG_SCHEMA_PYARROW: Final[IcebergSchema] = IcebergSchema(
    NestedField(field_id=1, name="id", field_type=LongType(), required=False),
    NestedField(field_id=2, name="name", field_type=StringType(), required=False),
    NestedField(
        field_id=3,
        name="address",
        field_type=StructType(
            NestedField(
                field_id=7,
                name="street",
                field_type=ListType(element_id=10, element_type=StringType(), element_required=False),
                required=False,
            ),
            NestedField(field_id=8, name="city", field_type=StringType(), required=False),
            NestedField(field_id=9, name="zip", field_type=StringType(), required=False),
        ),
        required=False,
    ),
    NestedField(
        field_id=4,
        name="tags",
        field_type=ListType(element_id=11, element_type=StringType(), element_required=False),
        required=False,
    ),
    NestedField(
        field_id=5,
        name="children",
        field_type=ListType(
            element_id=12,
            element_type=StructType(
                NestedField(field_id=13, name="k", field_type=StringType(), required=False),
                NestedField(field_id=14, name="v", field_type=LongType(), required=False),
            ),
            element_required=False,
        ),
        required=False,
    ),
    NestedField(
        field_id=6,
        name="metrics",
        field_type=MapType(
            key_id=15,
            key_type=StringType(),
            value_id=16,
            value_type=DoubleType(),
            value_required=False,
        ),
        required=False,
    ),
)


class NestingPlacement(StrEnum):
    """Where ``max_table_nesting=0`` is configured (or ``UNSET`` when omitted everywhere)."""

    RESOURCE_AND_TRANSFORMER = "resource_and_transformer"
    TRANSFORMER_ONLY = "transformer_only"
    DESTINATION = "destination"
    RESOURCE_ONLY = "resource_only"
    UNSET = "unset"


NESTING_ZERO_PLACEMENTS: Final[tuple[NestingPlacement, ...]] = (
    NestingPlacement.RESOURCE_AND_TRANSFORMER,
    NestingPlacement.TRANSFORMER_ONLY,
    NestingPlacement.DESTINATION,
)

UNNESTED_PLACEMENTS: Final[tuple[NestingPlacement, ...]] = (
    NestingPlacement.UNSET,
    NestingPlacement.RESOURCE_ONLY,
)

ALL_NESTING_PLACEMENTS: Final[tuple[NestingPlacement, ...]] = (
    *NESTING_ZERO_PLACEMENTS,
    *UNNESTED_PLACEMENTS,
)


def build_routed_resource(
    *,
    use_pyarrow: bool,
    placement: NestingPlacement,
) -> DltResource:
    """Build a ``source -> transformer`` resource pair with ``dlt.mark.with_table_name`` routing.

    :param use_pyarrow: yield a ``pyarrow.Table`` with ``ARROW_SCHEMA`` when True, or ``ROWS`` dicts when False
    :type use_pyarrow: bool
    :param placement: where ``max_table_nesting=0`` is applied
    :type placement: NestingPlacement
    :return: routed ``DltResource`` transformer
    :rtype: DltResource
    """
    resource_kwargs: dict[str, Any] = {"name": f"{TABLE_NAME}_source", "parallelized": True}
    transformer_kwargs: dict[str, Any] = {"name": f"{TABLE_NAME}_routed", "parallelized": True}

    if placement in (NestingPlacement.RESOURCE_AND_TRANSFORMER, NestingPlacement.RESOURCE_ONLY):
        resource_kwargs["max_table_nesting"] = 0
    if placement in (NestingPlacement.RESOURCE_AND_TRANSFORMER, NestingPlacement.TRANSFORMER_ONLY):
        transformer_kwargs["max_table_nesting"] = 0

    @dlt.resource(**resource_kwargs)
    def _source() -> Generator[list[dict[str, Any]] | pa.Table]:
        if use_pyarrow:
            yield pa.Table.from_pylist(list(ROWS), schema=ARROW_SCHEMA)
        else:
            yield [dict(row) for row in ROWS]

    @dlt.transformer(data_from=_source, **transformer_kwargs)
    def _route(page: list[dict[str, Any]] | pa.Table) -> Generator[Any]:
        yield dlt.mark.with_table_name(page, TABLE_NAME)

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

    :param tmp_path: temporary directory for pipeline metadata and destination bucket
    :type tmp_path: Path
    :param use_pyarrow: whether to yield a ``pyarrow.Table`` instead of Python dicts
    :type use_pyarrow: bool
    :param placement: where ``max_table_nesting=0`` is configured
    :type placement: NestingPlacement
    :param loader_file_format: ``"parquet"`` or ``"jsonl"``
    :type loader_file_format: str
    :param table_format: optional table format such as ``"iceberg"``
    :type table_format: str | None
    :return: tuple of ``(pipeline, load_info, bucket_dir)``
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
        dataset_name=DATASET_NAME,
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

    :param pipeline: executed ``dlt.Pipeline``
    :type pipeline: dlt.Pipeline
    :return: mapping of data table names to column ``data_type`` mappings
    :rtype: dict[str, dict[str, str | None]]
    """
    return {
        table_name: {col_name: col.get("data_type") for col_name, col in table.get("columns", {}).items()}
        for table_name, table in pipeline.default_schema.tables.items()
        if not is_metadata_table(table_name)
    }


def assert_dlt_metadata_and_strip(rows: list[dict[str, Any]], expected_load_id: str) -> list[dict[str, Any]]:
    """Verify every row carries ``_dlt_load_id`` and a unique ``_dlt_id``, then return rows without metadata.

    :param rows: loaded rows from the root table
    :type rows: list[dict[str, Any]]
    :param expected_load_id: load package ID from ``LoadInfo``
    :type expected_load_id: str
    :return: rows with ``_dlt_*`` keys removed
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

    :param tables: mapping from table name to loaded row dicts
    :type tables: dict[str, list[dict[str, Any]]]
    :param expected_load_id: load package ID from ``LoadInfo``
    :type expected_load_id: str
    """
    assert set(tables) == set(EXPECTED_DLT_COLUMNS_DICT_UNNESTED)

    widget_rows = sorted(tables["widget"], key=lambda r: r["id"])
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


def read_parquet_dataset(bucket_dir: Path) -> tuple[dict[str, pa.Schema], dict[str, list[dict[str, Any]]]]:
    """Read all non-metadata Parquet schemas and table rows under ``bucket_dir / DATASET_NAME``.

    :param bucket_dir: filesystem destination root directory
    :type bucket_dir: Path
    :return: tuple of ``({table_name: pa.Schema}, {table_name: rows})``
    :rtype: tuple[dict[str, pa.Schema], dict[str, list[dict[str, Any]]]]
    """
    dataset_dir = bucket_dir / DATASET_NAME
    schemas: dict[str, pa.Schema] = {}
    tables: dict[str, list[dict[str, Any]]] = {}
    for table_dir in sorted(p for p in dataset_dir.iterdir() if p.is_dir() and not is_metadata_table(p.name)):
        parquet_files = sorted(table_dir.glob("*.parquet"))
        assert len(parquet_files) == 1
        schemas[table_dir.name] = pq.read_schema(parquet_files[0])
        tables[table_dir.name] = pq.read_table(parquet_files[0]).to_pylist()
    return schemas, tables


def read_jsonl_dataset(bucket_dir: Path) -> dict[str, list[dict[str, Any]]]:
    """Read all non-metadata JSONL table rows under ``bucket_dir / DATASET_NAME``.

    :param bucket_dir: filesystem destination root directory
    :type bucket_dir: Path
    :return: mapping of ``{table_name: rows}``
    :rtype: dict[str, list[dict[str, Any]]]
    """
    dataset_dir = bucket_dir / DATASET_NAME
    tables: dict[str, list[dict[str, Any]]] = {}
    for table_dir in sorted(p for p in dataset_dir.iterdir() if p.is_dir() and not is_metadata_table(p.name)):
        jsonl_files = sorted(table_dir.glob("*.jsonl*"))
        assert len(jsonl_files) == 1
        tables[table_dir.name] = read_jsonl_file(jsonl_files[0])
    return tables


def read_iceberg_dataset(
    pipeline: dlt.Pipeline,
) -> tuple[dict[str, IcebergSchema], dict[str, pa.Schema], dict[str, list[dict[str, Any]]]]:
    """Read Iceberg schemas, materialized Arrow schemas, and rows for all open tables in ``pipeline``.

    :param pipeline: executed ``dlt.Pipeline``
    :type pipeline: dlt.Pipeline
    :return: tuple of ``({table: IcebergSchema}, {table: pa.Schema}, {table: rows})``
    :rtype: tuple[dict[str, IcebergSchema], dict[str, pa.Schema], dict[str, list[dict[str, Any]]]]
    """
    iceberg_tables = get_iceberg_tables(pipeline)
    iceberg_schemas: dict[str, IcebergSchema] = {}
    arrow_schemas: dict[str, pa.Schema] = {}
    rows_by_table: dict[str, list[dict[str, Any]]] = {}
    try:
        for name, table in iceberg_tables.items():
            iceberg_schemas[name] = table.schema()
            arrow_table = table.scan().to_arrow()
            arrow_schemas[name] = arrow_table.schema
            rows_by_table[name] = arrow_table.to_pylist()
    finally:
        for table in iceberg_tables.values():
            table.catalog.close()
    return iceberg_schemas, arrow_schemas, rows_by_table


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

    schemas, tables = read_parquet_dataset(bucket_dir)
    assert schemas == {TABLE_NAME: EXPECTED_ARROW_SCHEMA_DICT_NESTING_0}

    rows = assert_dlt_metadata_and_strip(tables[TABLE_NAME], load_info.loads_ids[0])
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
    assert iceberg_schemas == {TABLE_NAME: EXPECTED_ICEBERG_SCHEMA_DICT_NESTING_0}
    assert arrow_schemas == {TABLE_NAME: EXPECTED_ARROW_SCHEMA_DICT_NESTING_0}

    rows = assert_dlt_metadata_and_strip(tables[TABLE_NAME], load_info.loads_ids[0])
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

    tables = read_jsonl_dataset(bucket_dir)
    assert set(tables) == {TABLE_NAME}
    rows = assert_dlt_metadata_and_strip(tables[TABLE_NAME], load_info.loads_ids[0])
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

    schemas, tables = read_parquet_dataset(bucket_dir)
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

    tables = read_jsonl_dataset(bucket_dir)
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

    schemas, tables = read_parquet_dataset(bucket_dir)
    assert schemas == {TABLE_NAME: ARROW_SCHEMA}
    assert tables == {TABLE_NAME: list(ROWS)}


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
    assert iceberg_schemas == {TABLE_NAME: EXPECTED_ICEBERG_SCHEMA_PYARROW}
    assert arrow_schemas == {TABLE_NAME: ARROW_SCHEMA}
    assert tables == {TABLE_NAME: list(ROWS)}


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

    tables = read_jsonl_dataset(bucket_dir)
    assert tables == {TABLE_NAME: EXPECTED_PARSED_JSON_ROWS}
