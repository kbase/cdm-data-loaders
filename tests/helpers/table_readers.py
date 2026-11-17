"""Shared Parquet, JSONL, Iceberg, DuckDB, and pipeline report reader/assertion helpers."""

import gzip
import io
import json
import posixpath
from collections import defaultdict
from collections.abc import Callable, Generator, Mapping
from contextlib import contextmanager
from functools import partial
from logging import Logger, getLogger
from operator import itemgetter
from pathlib import Path
from types import ModuleType
from typing import Any, Final
from unittest.mock import patch
from uuid import uuid4

import dlt
import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import yaml
from dlt.common.libs.pyiceberg import get_iceberg_tables
from dlt.common.normalizers.naming.snake_case import NamingConvention as SnakeCaseNamingConvention
from dlt.common.pipeline import LoadInfo
from dlt.common.storages.fsspec_filesystem import FileItemDict
from dlt.extract import DltResource
from fsspec import AbstractFileSystem
from pandas import DataFrame
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.schema import Schema as IcebergSchema

from cdm_data_loaders.core.fields import ICEBERG, PARQUET, PIPELINE_DIR_NAME, LoaderFileFormatEnum
from cdm_data_loaders.core.file_items import as_file_item, open_binary
from cdm_data_loaders.core.reporting import LOAD_INFO_TABLE_NAME
from cdm_data_loaders.core.settings import CtsSettings, FileFormatIngestSettings
from specs._loader.pipeline_spec import PipelineRun, load_expected_json_tables, load_expected_tables
from tests.helpers.cli_assertions import set_nested, strip_none_values
from tests.helpers.pyarrow_spec import json_default

LOGGER: Final[Logger] = getLogger(__name__)

WORKER_CONFIGS: Final[tuple[tuple[int, int, int], ...]] = ((1, 1, 1), (3, 2, 2), (5, 3, 4))
BUFFER_SIZES: Final[tuple[int, ...]] = (1, 5, 100, 500)
LOADER_FILE_FORMATS: Final[list[LoaderFileFormatEnum]] = [
    fmt for fmt in LoaderFileFormatEnum if fmt != LoaderFileFormatEnum.FILE_LISTING
]

DLT_METADATA_PREFIX: Final[str] = "_dlt"
XMLNS_COLUMNS: Final[frozenset[str]] = frozenset({"entry__axmlns", "entry__axmlns_xsi"})
MIN_DATASETS_TO_COMPARE: Final[int] = 2

DLT_TO_XML_NAMES: Final[dict[str, str]] = {
    "aid": "@id",
    "aupdated": "@updated",
    "atype": "@type",
    "avalue": "@value",
    "alength": "@length",
    "achecksum": "@checksum",
    "_text": "#text",
    "representative_member": "representativeMember",
    "db_reference": "dbReference",
}


def is_metadata_table(table_name: str) -> bool:
    """Return whether ``table_name`` contains dlt pipeline metadata rather than loaded records.

    :param table_name: Destination table name.
    :type table_name: str
    :return: ``True`` for ``_dlt*`` or ``_load_info`` tables.
    :rtype: bool
    """
    return table_name.startswith("_dlt") or table_name == LOAD_INFO_TABLE_NAME


def strip_metadata_columns(row: dict[str, Any]) -> dict[str, Any]:
    """Drop dlt metadata columns (``_dlt*``) from a row dict.

    :param row: Input row dictionary.
    :type row: dict[str, Any]
    :return: Copy without ``_dlt*`` keys.
    :rtype: dict[str, Any]
    """
    return {key: value for key, value in row.items() if not key.startswith(DLT_METADATA_PREFIX)}


def read_jsonl_file(file_path: Path) -> list[dict[str, Any]]:
    """Read a plain or gzipped JSON Lines file from disk.

    :param file_path: Path to ``.jsonl`` or ``.jsonl.gz`` file.
    :type file_path: Path
    :return: Parsed list of row dictionaries.
    :rtype: list[dict[str, Any]]
    """
    rows: list[dict[str, Any]] = []
    with open_binary(file_path) as fh:
        for raw_line in fh:
            stripped = raw_line.decode("utf-8").strip()
            if not stripped:
                continue
            try:
                rows.append(json.loads(stripped))
            except json.JSONDecodeError:
                LOGGER.warning("Skipping unparseable line in %s: %r", file_path, stripped[:200])
    return rows


def read_parquet_tables(output_dir: Path) -> dict[str, list[dict[str, Any]]]:
    """Read all Parquet files under a pipeline output dataset dir, grouped by table name.

    :param output_dir: Dataset directory path.
    :type output_dir: Path
    :return: Mapping from table name to loaded row dicts.
    :rtype: dict[str, list[dict[str, Any]]]
    """
    tables: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for parquet_file in sorted(output_dir.rglob("*.parquet")):
        parent = parquet_file.parent
        table_name = (
            parent.parent.name if parent.name == "data" and (parent.parent / "metadata").is_dir() else parent.name
        )
        tables[table_name].extend(pq.read_table(parquet_file).to_pylist())
    return dict(tables)


def read_parquet_dataset(dataset_dir: Path) -> tuple[dict[str, pa.Schema], dict[str, list[dict[str, Any]]]]:
    """Read all non-metadata single-file Parquet schemas and rows under ``dataset_dir``.

    :param dataset_dir: Dataset directory path (``bucket_dir / dataset_name``).
    :type dataset_dir: Path
    :return: Pair of ``({table_name: pa.Schema}, {table_name: rows})``.
    :rtype: tuple[dict[str, pa.Schema], dict[str, list[dict[str, Any]]]]
    """
    schemas: dict[str, pa.Schema] = {}
    tables: dict[str, list[dict[str, Any]]] = {}
    for table_dir in sorted(p for p in dataset_dir.iterdir() if p.is_dir() and not is_metadata_table(p.name)):
        parquet_files = sorted(table_dir.glob("*.parquet"))
        assert len(parquet_files) == 1
        schemas[table_dir.name] = pq.read_schema(parquet_files[0])
        tables[table_dir.name] = pq.read_table(parquet_files[0]).to_pylist()
    return schemas, tables


def read_jsonl_dataset(dataset_dir: Path) -> dict[str, list[dict[str, Any]]]:
    """Read all non-metadata single-file JSONL table rows under ``dataset_dir``.

    :param dataset_dir: Dataset directory path (``bucket_dir / dataset_name``).
    :type dataset_dir: Path
    :return: Mapping from table name to rows.
    :rtype: dict[str, list[dict[str, Any]]]
    """
    tables: dict[str, list[dict[str, Any]]] = {}
    for table_dir in sorted(p for p in dataset_dir.iterdir() if p.is_dir() and not is_metadata_table(p.name)):
        jsonl_files = sorted(table_dir.glob("*.jsonl*"))
        assert len(jsonl_files) == 1
        tables[table_dir.name] = read_jsonl_file(jsonl_files[0])
    return tables


def read_iceberg_dataset(
    pipeline: dlt.Pipeline,
) -> tuple[dict[str, IcebergSchema], dict[str, pa.Schema], dict[str, list[dict[str, Any]]]]:
    """Read Iceberg schemas, Arrow scan schemas, and rows for all open tables in ``pipeline``.

    :param pipeline: Executed ``dlt.Pipeline``.
    :type pipeline: dlt.Pipeline
    :return: Tuple of ``({table: IcebergSchema}, {table: pa.Schema}, {table: rows})``.
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


def read_pipeline_tables(output_dir: Path, output_format: str | None = None) -> dict[str, list[dict[str, Any]]]:
    """Read all data files under a pipeline output dataset dir, grouped by table name.

    :param output_dir: Dataset directory path.
    :type output_dir: Path
    :param output_format: Optional format override (``"parquet"``, ``"iceberg"``, or ``"jsonl"``).
    :type output_format: str | None
    :return: Mapping from table name to loaded row dicts.
    :rtype: dict[str, list[dict[str, Any]]]
    """
    if output_format and output_format in {PARQUET, ICEBERG}:
        return read_parquet_tables(output_dir)

    tables: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for json_file in sorted(output_dir.rglob("*.json*")):
        if not json_file.is_file():
            continue
        if json_file.parent.name == "metadata" or is_metadata_table(json_file.parent.name):
            continue
        tables[json_file.parent.name].extend(read_jsonl_file(json_file))
    return dict(tables)


def read_destination_rows(
    base_dir: Path,
    table_name: str,
    *,
    strip_metadata: bool = True,
) -> list[dict[str, Any]]:
    """Read rows written by the filesystem destination for ``table_name`` under ``base_dir``.

    :param base_dir: Destination root or dataset directory.
    :type base_dir: Path
    :param table_name: Target table name.
    :type table_name: str
    :param strip_metadata: Whether to strip ``_dlt*`` columns from returned rows.
    :type strip_metadata: bool
    :return: Loaded row dictionaries.
    :rtype: list[dict[str, Any]]
    """
    rows: list[dict[str, Any]] = []
    candidate_files = sorted({*base_dir.glob(f"**/{table_name}/*"), *base_dir.glob(f"**/{table_name}/data/*.parquet")})
    for data_file in candidate_files:
        if not data_file.is_file():
            continue
        if data_file.name.endswith(".parquet"):
            rows.extend(pq.read_table(data_file).to_pylist())
        elif ".jsonl" in data_file.name:
            rows.extend(read_jsonl_file(data_file))
    if strip_metadata:
        return [strip_metadata_columns(row) for row in rows]
    return rows


def assert_iceberg_table_metadata(dataset_dir: Path, table_name: str) -> None:
    """Assert that ``dataset_dir / table_name`` has Iceberg metadata and data subdirectories.

    :param dataset_dir: Dataset directory path.
    :type dataset_dir: Path
    :param table_name: Target Iceberg table name.
    :type table_name: str
    """
    table_dir = dataset_dir / table_name
    metadata_dir = table_dir / "metadata"
    data_dir = table_dir / "data"
    assert metadata_dir.is_dir(), f"Missing Iceberg metadata directory for table {table_name!r}"
    assert data_dir.is_dir(), f"Missing Iceberg data directory for table {table_name!r}"
    assert list(metadata_dir.glob("*.metadata.json")), f"Missing *.metadata.json for table {table_name!r}"
    assert list(metadata_dir.glob("snap-*.avro")), f"Missing Iceberg snapshot manifest list for table {table_name!r}"
    assert list(data_dir.glob("*.parquet")), f"Missing Iceberg data parquet files for table {table_name!r}"


def duckdb_pipeline(tmp_path: Path, pipeline_name: str, dataset_name: str, db_name: str | None = None) -> dlt.Pipeline:
    """Return a dlt pipeline backed by a DuckDB file in ``tmp_path`` distinct from ``dataset_name``.

    :param tmp_path: Per-test temporary directory.
    :type tmp_path: Path
    :param pipeline_name: dlt pipeline name.
    :type pipeline_name: str
    :param dataset_name: dlt dataset name.
    :type dataset_name: str
    :param db_name: Optional DuckDB file stem prefix.
    :type db_name: str | None
    :return: Configured ``dlt.Pipeline``.
    :rtype: dlt.Pipeline
    """
    db_file = f"{db_name or pipeline_name}_db.duckdb"
    return dlt.pipeline(
        pipeline_name=pipeline_name,
        dataset_name=dataset_name,
        destination=dlt.destinations.duckdb(str(tmp_path / db_file)),
        pipelines_dir=str(tmp_path / "pipelines"),
    )


def assert_core_run_pipeline_args(
    pipeline_module: ModuleType,
    run_fn: Callable[[Any], Any],
    settings: FileFormatIngestSettings,
    pipeline_name: str,
) -> None:
    """Assert that a pipeline runner delegates to ``run_pipeline`` with standard ingest kwargs.

    :param pipeline_module: Pipeline module containing ``run_pipeline``.
    :type pipeline_module: ModuleType
    :param run_fn: Pipeline entrypoint function accepting ``settings``.
    :type run_fn: Callable[[Any], Any]
    :param settings: Pipeline settings instance.
    :type settings: FileFormatIngestSettings
    :param pipeline_name: Expected ``pipeline_name`` passed to ``run_pipeline``.
    :type pipeline_name: str
    """
    with patch.object(pipeline_module, "run_pipeline") as mock_run_pipeline:
        run_fn(settings)

    assert mock_run_pipeline.call_count == 1
    _, kwargs = mock_run_pipeline.call_args
    assert kwargs.keys() == {"settings", "resource", "pipeline_kwargs", "pipeline_run_kwargs"}
    assert kwargs["settings"] == settings
    assert kwargs["pipeline_kwargs"] == {
        "pipeline_name": pipeline_name,
        "dataset_name": settings.dataset_name,
    }
    expected_run_kwargs = (
        {"loader_file_format": str(PARQUET), "table_format": str(ICEBERG)}
        if settings.loader_file_format == LoaderFileFormatEnum.ICEBERG
        else {"loader_file_format": str(settings.loader_file_format)}
    )
    assert kwargs["pipeline_run_kwargs"] == expected_run_kwargs
    assert isinstance(kwargs["resource"], DltResource)


def assert_binds_filesystem_reader(
    pipeline_module: ModuleType,
    run_fn: Callable[[Any], Any],
    settings: CtsSettings,
    expected_reader_name: str | None = None,
) -> DltResource:
    """Assert that ``run_fn`` passes a ``DltResource`` piped from ``filesystem`` to ``run_pipeline``.

    :param pipeline_module: Pipeline module containing ``run_pipeline``.
    :type pipeline_module: ModuleType
    :param run_fn: Pipeline entrypoint function accepting ``settings``.
    :type run_fn: Callable[[Any], Any]
    :param settings: Pipeline settings instance.
    :type settings: CtsSettings
    :param expected_reader_name: Optional expected resource name.
    :type expected_reader_name: str | None
    :return: Captured ``DltResource``.
    :rtype: DltResource
    """
    with patch.object(pipeline_module, "run_pipeline") as mock_run_pipeline:
        run_fn(settings)

    mock_run_pipeline.assert_called_once()
    resource = mock_run_pipeline.call_args.kwargs["resource"]
    assert isinstance(resource, dlt.sources.DltResource)
    if expected_reader_name is not None:
        assert resource.name == expected_reader_name
        pipe = getattr(resource, "_pipe", None)
        assert pipe is not None
        assert pipe.parent.name == "filesystem"
    return resource


def assert_run_ok(load_info: LoadInfo | None) -> LoadInfo:
    """Assert a pipeline run succeeded without failed jobs and return ``load_info``.

    :param load_info: Returned ``LoadInfo`` from a pipeline run.
    :type load_info: LoadInfo | None
    :return: Non-null ``LoadInfo``.
    :rtype: LoadInfo
    """
    assert load_info is not None
    assert not load_info.has_failed_jobs
    return load_info


def run_with_duckdb_pipeline(
    tmp_path: Path,
    pipeline_module: ModuleType,
    invoke_fn: Callable[[], Any],
    *,
    expected_pipeline_name: str,
    expected_dataset_name: str,
    pipeline_name_prefix: str = "test_cli_pipeline",
) -> LoadInfo:
    """Patch ``pipeline_module.run_pipeline`` to run against a DuckDB destination and return ``LoadInfo``.

    :param tmp_path: Per-test temporary directory.
    :type tmp_path: Path
    :param pipeline_module: Module binding ``run_pipeline``.
    :type pipeline_module: ModuleType
    :param invoke_fn: Zero-arg callable invoking the pipeline or CLI.
    :type invoke_fn: Callable[[], Any]
    :param expected_pipeline_name: Expected ``pipeline_name`` passed in ``pipeline_kwargs``.
    :type expected_pipeline_name: str
    :param expected_dataset_name: Expected ``dataset_name`` passed in ``pipeline_kwargs``.
    :type expected_dataset_name: str
    :param pipeline_name_prefix: Prefix for the disposable DuckDB pipeline name.
    :type pipeline_name_prefix: str
    :return: Completed ``LoadInfo``.
    :rtype: LoadInfo
    """
    db_pipeline_name = f"{pipeline_name_prefix}_{uuid4().hex}"
    captured: dict[str, LoadInfo] = {}

    def fake_run_pipeline(
        *,
        resource: DltResource,
        pipeline_kwargs: dict[str, Any],
        **_extra: object,
    ) -> LoadInfo:
        assert pipeline_kwargs == {
            "pipeline_name": expected_pipeline_name,
            "dataset_name": expected_dataset_name,
        }
        pipeline = duckdb_pipeline(tmp_path, db_pipeline_name, expected_dataset_name)
        load_info = pipeline.run(resource)
        captured["load_info"] = load_info
        return load_info

    with patch.object(pipeline_module, "run_pipeline", fake_run_pipeline):
        invoke_fn()

    return assert_run_ok(captured.get("load_info"))


def check_book_list_results(
    load_info: LoadInfo | None,
    table_name: str | None = None,
    *,
    expected_columns: set[str] | None = None,
) -> DataFrame:
    """Check the output of parsing ``SIMPLE_LIBRARY_XML`` into a single ``book`` table.

    :param load_info: Pipeline ``LoadInfo``.
    :type load_info: LoadInfo | None
    :param table_name: Optional overridden table name (defaults to ``"book"``).
    :type table_name: str | None
    :param expected_columns: Optional expected column subset.
    :type expected_columns: set[str] | None
    :return: Loaded ``book`` rows as a pandas ``DataFrame``.
    :rtype: DataFrame
    """
    verified_info = assert_run_ok(load_info)
    with verified_info.pipeline.destination_client() as client:
        dataset_dir = Path(client.dataset_path)  # pyright: ignore[reportAttributeAccessIssue]
    resolved_table = table_name or "book"
    assert {p.name for p in dataset_dir.iterdir() if p.is_dir()} == {
        resolved_table,
        "_dlt_version",
        "_dlt_loads",
        "_dlt_pipeline_state",
        LOAD_INFO_TABLE_NAME,
    }
    book_df = DataFrame(read_destination_rows(dataset_dir, resolved_table, strip_metadata=False))
    required_cols = expected_columns or {"book___id", "book__title", "_dlt_id", "_dlt_load_id"}
    assert set(book_df.columns.tolist()) >= required_cols
    return book_df


def make_file_item(file_path: Path) -> FileItemDict:
    """Build a dlt ``FileItemDict`` for a local file.

    :param file_path: Local file path.
    :type file_path: Path
    :return: Constructed ``FileItemDict``.
    :rtype: FileItemDict
    """
    return as_file_item(file_path)


def fetch_all_duckdb(pipeline: dlt.Pipeline, query: str) -> list[tuple[Any, ...]]:
    """Run ``query`` against ``pipeline``'s DuckDB destination and return all rows.

    :param pipeline: Executed ``dlt.Pipeline``.
    :type pipeline: dlt.Pipeline
    :param query: SQL query string.
    :type query: str
    :return: Fetched tuple rows.
    :rtype: list[tuple[Any, ...]]
    """
    with (
        pipeline.sql_client() as client,
        client.execute_query(query) as cur,
    ):
        return list(cur.fetchall())


def assert_duckdb_table_counts(pipeline: dlt.Pipeline, expected_counts: Mapping[str, int]) -> None:
    """Assert that each table in ``expected_counts`` has the expected row count in DuckDB.

    :param pipeline: Executed ``dlt.Pipeline``.
    :type pipeline: dlt.Pipeline
    :param expected_counts: Expected ``{table_name: row_count}`` mapping.
    :type expected_counts: Mapping[str, int]
    """
    with pipeline.sql_client() as client:
        for table_name, expected_count in expected_counts.items():
            qualified = client.make_qualified_table_name(table_name)
            with client.execute_query(f"SELECT COUNT(*) FROM {qualified}") as cur:  # noqa: S608
                ((count,),) = list(cur.fetchall())
            assert count == expected_count, f"expected {expected_count} rows in '{table_name}', got {count}"


def assert_destination_table_counts(dataset_dir: Path, expected_counts: Mapping[str, int]) -> None:
    """Assert that each table in ``expected_counts`` has the expected row count on the filesystem destination.

    :param dataset_dir: Dataset directory path.
    :type dataset_dir: Path
    :param expected_counts: Expected ``{table_name: row_count}`` mapping.
    :type expected_counts: Mapping[str, int]
    """
    for table_name, expected_count in expected_counts.items():
        rows = read_destination_rows(dataset_dir, table_name)
        assert len(rows) == expected_count, f"expected {expected_count} rows in '{table_name}', got {len(rows)}"


@contextmanager
def duckdb_reference_from_jsonl(jsonl_path: Path) -> Generator[duckdb.DuckDBPyConnection]:
    """Load a reference JSONL file into an in-memory DuckDB connection with a ``reference`` table.

    :param jsonl_path: Path to the reference JSONL file.
    :type jsonl_path: Path
    :return: Context manager yielding the open ``DuckDBPyConnection``.
    :rtype: Generator[duckdb.DuckDBPyConnection]
    """
    connection = duckdb.connect()
    connection.execute(
        "CREATE TABLE reference AS SELECT * FROM read_json_auto(?, sample_size=200)",
        [str(jsonl_path)],
    )
    try:
        yield connection
    finally:
        connection.close()


def sorted_json(entries: list[dict[str, Any]]) -> list[str]:
    """Serialize ``entries`` as sorted canonical JSON strings for order-insensitive comparison.

    :param entries: Row or entity dictionaries.
    :type entries: list[dict[str, Any]]
    :return: Sorted JSON string representations.
    :rtype: list[str]
    """
    return sorted(json.dumps(entry, sort_keys=True, default=str) for entry in entries)


def read_data_rows(dataset_dir: Path, file_format: str | None = None) -> dict[str, list[dict[str, Any]]]:
    """Read a pipeline output dataset, dropping dlt metadata columns and metadata tables.

    :param dataset_dir: Dataset directory path.
    :type dataset_dir: Path
    :param file_format: Optional format override.
    :type file_format: str | None
    :return: Mapping from data table name to stripped row dicts.
    :rtype: dict[str, list[dict[str, Any]]]
    """
    tables = read_pipeline_tables(dataset_dir, file_format)
    return {
        name: [strip_metadata_columns(row) for row in rows]
        for name, rows in tables.items()
        if not is_metadata_table(name)
    }


def check_pipeline_report(path: Path, expected: dict[str, Any]) -> dict[str, Any]:
    """Verify a ``report.txt`` YAML file exists and matches all ``expected`` entries.

    :param path: Path to ``report.txt``.
    :type path: Path
    :param expected: Expected key-value subset.
    :type expected: dict[str, Any]
    :return: Parsed YAML report dictionary.
    :rtype: dict[str, Any]
    """
    assert path.is_file()
    with path.open("r", encoding="utf-8") as fh:
        report_data: dict[str, Any] = yaml.safe_load(fh)

    for key, val in expected.items():
        if key == "ingested_files" and isinstance(val, list):
            assert set(report_data.get(key, [])) == set(val)
        else:
            assert report_data[key] == val
    return report_data


def assert_pipeline_report_written(
    output_dir: Path,
    load_info: LoadInfo,
    *,
    metadata_moved: bool = False,
    ingested_files: list[str] | set[str] | None = None,
) -> None:
    """Assert that ``save_pipeline_metadata`` wrote the expected report file in ``output_dir``.

    :param output_dir: Pipeline CTS ``output_dir``.
    :type output_dir: Path
    :param load_info: Executed ``LoadInfo``.
    :type load_info: LoadInfo
    :param metadata_moved: Whether ``pipeline_metadata/`` subdirectory was used.
    :type metadata_moved: bool
    :param ingested_files: Expected ingested file paths or ``None``.
    :type ingested_files: list[str] | set[str] | None
    """
    if metadata_moved:
        assert (output_dir / PIPELINE_DIR_NAME).is_dir()
        report_file = output_dir / PIPELINE_DIR_NAME / "report.txt"
    else:
        report_file = output_dir / "report.txt"

    expected: dict[str, Any] = {
        "pipeline": load_info.pipeline.pipeline_name,
        "dataset": load_info.dataset_name,
        "status": "completed",
        "load_ids": list(load_info.loads_ids),
        "pipeline_load_info_errs": None,
    }
    if isinstance(ingested_files, list) and ingested_files:
        expected["ingested_files"] = ingested_files

    report_data = check_pipeline_report(report_file, expected)
    started_at = report_data["started"]
    finished_at = report_data["finished"]
    assert started_at <= load_info.started_at <= finished_at
    if load_info.finished_at is not None:
        assert load_info.started_at <= load_info.finished_at <= finished_at

    if isinstance(ingested_files, set):
        if ingested_files:
            assert set(report_data["ingested_files"]) == ingested_files
        else:
            assert "ingested_files" not in report_data
    elif ingested_files == []:
        assert "ingested_files" not in report_data
    elif ingested_files is None and "ingested_files" in report_data:
        assert report_data["ingested_files"]
    assert set(report_data) <= {
        "pipeline",
        "dataset",
        "started",
        "finished",
        "status",
        "load_ids",
        "ingested_files",
        "pipeline_load_info_errs",
    }


def run_and_read(
    run_pipeline: Callable[..., tuple[LoadInfo | None, Path]],
    chunk_dir: str,
    **overrides: object,
) -> tuple[LoadInfo, Path]:
    """Run ``run_pipeline`` on ``chunk_dir``, verify report output, and return ``(load_info, output_dir)``.

    :param run_pipeline: Isolated pipeline runner callable.
    :type run_pipeline: Callable[..., tuple[LoadInfo | None, Path]]
    :param chunk_dir: Input chunk directory name.
    :type chunk_dir: str
    :param overrides: Keyword overrides passed to ``run_pipeline``.
    :type overrides: object
    :return: Pair of ``(load_info, output_dir)``.
    :rtype: tuple[LoadInfo, Path]
    """
    load_info, output_dir = run_pipeline(chunk_dir, **overrides)
    assert load_info is not None
    assert not load_info.has_failed_jobs
    assert_pipeline_report_written(output_dir, load_info, metadata_moved=(output_dir / PIPELINE_DIR_NAME).is_dir())
    return load_info, output_dir


def denormalize_identifier(name: str) -> str:
    """Restore an ``xmltodict`` key from a dlt-normalized fragment.

    :param name: Normalized column or table name fragment.
    :type name: str
    :return: Restored XML key fragment.
    :rtype: str
    """
    return DLT_TO_XML_NAMES.get(name, name)


def denormalize_path(path: str) -> list[str]:
    """Split a dlt ``__``-delimited name into unmangled XML key fragments.

    :param path: ``__``-separated column or table name.
    :type path: str
    :return: List of restored XML key fragments.
    :rtype: list[str]
    """
    return [denormalize_identifier(fragment) for fragment in path.split("__")]


def rows_as_multiset(tables: dict[str, list[dict[str, Any]]], table_name: str) -> list[str]:
    """Serialize rows of ``table_name`` as sorted canonical JSON strings without metadata columns.

    :param tables: Mapping of table names to row lists.
    :type tables: dict[str, list[dict[str, Any]]]
    :param table_name: Target table name.
    :type table_name: str
    :return: Sorted JSON row strings.
    :rtype: list[str]
    """
    return sorted(
        json.dumps(strip_metadata_columns(row), sort_keys=True, default=str) for row in tables.get(table_name, [])
    )


def parse_json_string(value: object) -> object:
    """Parse a string holding a JSON object or array; pass any other value through unchanged.

    :param value: Candidate value.
    :type value: object
    :return: Parsed JSON container or original ``value``.
    :rtype: object
    """
    if isinstance(value, str) and value[:1] in ("{", "["):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return value
    return value


def _child_table_path(table_name: str, top_table: str) -> list[str]:
    """Derive the XML key path for a child table from its dlt name.

    :param table_name: Normalized child table name.
    :type table_name: str
    :param top_table: Root table name.
    :type top_table: str
    :return: Relative key path fragments from the root entry.
    :rtype: list[str]
    """
    fragments = denormalize_path(table_name)
    while len(fragments) > 1 and fragments[0] == top_table:
        fragments = fragments[1:]
    return fragments


def _attach_nested_rows(
    target: dict[str, Any],
    parent_id: str,
    parent_index: dict[str, list[tuple[str, dict[str, Any]]]],
    parent_root_path: list[str],
    *,
    depth_limit: int,
    top_table: str,
) -> None:
    """Attach child-table rows recursively to ``target``.

    :param target: Parent dictionary to populate.
    :type target: dict[str, Any]
    :param parent_id: Parent ``_dlt_id`` value.
    :type parent_id: str
    :param parent_index: Index of ``{_dlt_parent_id: [(table_name, row)]}``.
    :type parent_index: dict[str, list[tuple[str, dict[str, Any]]]]
    :param parent_root_path: Key path of ``target`` from the root entry.
    :type parent_root_path: list[str]
    :param depth_limit: Remaining recursion depth budget.
    :type depth_limit: int
    :param top_table: Root table name.
    :type top_table: str
    """
    if depth_limit <= 0:
        return
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for table_name, row in parent_index.get(parent_id, []):
        grouped[table_name].append(row)

    for table_name, rows in grouped.items():
        rows.sort(key=itemgetter("_dlt_list_idx"))
        full_path = _child_table_path(table_name, top_table)
        relative_path = full_path[len(parent_root_path) :] if parent_root_path else full_path
        children = []
        for row in rows:
            child: dict[str, Any] = {}
            for column, raw in strip_metadata_columns(row).items():
                if raw is None:
                    continue
                set_nested(child, denormalize_path(column), parse_json_string(raw))
            _attach_nested_rows(
                child,
                row["_dlt_id"],
                parent_index,
                full_path,
                depth_limit=depth_limit - 1,
                top_table=top_table,
            )
            children.append(child)
        set_nested(target, relative_path, children)


def reconstruct_entries(tables: dict[str, list[dict[str, Any]]]) -> list[dict[str, Any]]:
    """Rebuild nested dicts from flattened dlt output tables.

    :param tables: Mapping of table names to loaded row lists.
    :type tables: dict[str, list[dict[str, Any]]]
    :return: Reconstructed nested entry dictionaries.
    :rtype: list[dict[str, Any]]
    :raises ValueError: If a unique top-level table cannot be identified.
    """
    top_tables = [t for t in tables if "__" not in t]
    if len(top_tables) != 1:
        err_msg = "Could not find an appropriate top-level table for dataset"
        raise ValueError(err_msg)
    top_table = top_tables[0]

    main_rows = tables.get(top_table, [])
    if not main_rows:
        return []

    parent_index: dict[str, list[tuple[str, dict[str, Any]]]] = defaultdict(list)
    for table_name, rows in tables.items():
        if table_name == top_table or is_metadata_table(table_name):
            continue
        for row in rows:
            parent_index[row["_dlt_parent_id"]].append((table_name, row))

    entries: list[dict[str, Any]] = []
    for row in main_rows:
        entry: dict[str, Any] = {}
        for column, value in strip_metadata_columns(row).items():
            if column in XMLNS_COLUMNS or value is None:
                continue
            set_nested(entry, denormalize_path(column.removeprefix(f"{top_table}__")), parse_json_string(value))
        if "_dlt_id" in row:
            _attach_nested_rows(entry, row["_dlt_id"], parent_index, [], depth_limit=100, top_table=top_table)
        entries.append(entry)
    return entries


def _normalize_reference_entry(entry: dict[str, Any]) -> dict[str, Any]:
    """Normalize a DuckDB STRUCT entry dict to match reconstructed pipeline entries.

    :param entry: Raw DuckDB STRUCT row dictionary.
    :type entry: dict[str, Any]
    :return: Normalized dictionary with ``None`` values stripped and JSON strings parsed.
    :rtype: dict[str, Any]
    """
    normalized: dict[str, Any] = {}
    for key, raw_value in entry.items():
        if raw_value is None:
            continue
        value = json.loads(raw_value) if isinstance(raw_value, str) and raw_value.startswith(("{", "[")) else raw_value
        normalized[key] = strip_none_values(value)
    return normalized


def reference_entries_from_duckdb(connection: duckdb.DuckDBPyConnection) -> list[dict[str, Any]]:
    """Extract reference entries from DuckDB as canonical nested Python dicts.

    :param connection: Open DuckDB connection containing ``reference``.
    :type connection: duckdb.DuckDBPyConnection
    :return: Canonical nested entry dictionaries.
    :rtype: list[dict[str, Any]]
    """
    rows = connection.execute("SELECT entry FROM reference ORDER BY entry['_id']").fetchall()
    return [_normalize_reference_entry(entry) for (entry,) in rows]


def assert_datasets_equal(
    load_info_and_dir: dict[str, tuple[LoadInfo, Path]],
    output_format: str | None = None,
) -> None:
    """Assert that two or more pipeline runs wrote identical table sets, columns, and row multisets.

    :param load_info_and_dir: Mapping of run key to ``(load_info, output_dir)``.
    :type load_info_and_dir: dict[str, tuple[LoadInfo, Path]]
    :param output_format: Optional format override.
    :type output_format: str | None
    :raises ValueError: If fewer than ``MIN_DATASETS_TO_COMPARE`` datasets are given.
    """
    if len(load_info_and_dir) < MIN_DATASETS_TO_COMPARE:
        err_msg = "assert_datasets_equal requires at least two datasets to compare"
        raise ValueError(err_msg)

    all_keys = list(load_info_and_dir.keys())
    dataset_zero = all_keys[0]
    load_info_zero, output_dir_zero = load_info_and_dir[dataset_zero]
    tables_zero = read_data_rows(output_dir_zero / load_info_zero.dataset_name, output_format)
    table_names = set(tables_zero)
    rows_per_table = {t: len(rows) for t, rows in tables_zero.items()}
    col_names = {t: set().union(*(row.keys() for row in rows)) for t, rows in tables_zero.items()}
    sorted_row_data: dict[str, dict[str, list[str]]] = {
        dataset_zero: {t: rows_as_multiset(tables_zero, t) for t in table_names},
    }

    for key in all_keys[1:]:
        load_info, output_dir = load_info_and_dir[key]
        tables = read_data_rows(output_dir / load_info.dataset_name, output_format)
        assert set(tables) == table_names
        assert {t: len(rows) for t, rows in tables.items()} == rows_per_table
        sorted_row_data[key] = {}
        for t, rows in tables.items():
            assert set().union(*(row.keys() for row in rows)) == col_names[t]
            sorted_row_data[key][t] = rows_as_multiset(tables, t)
            assert sorted_row_data[key][t] == sorted_row_data[dataset_zero][t]


def assert_dataset_matches_reference(
    load_info_and_dir: dict[str, tuple[LoadInfo, Path]],
    sorted_reference_json: list[str],
    output_format: str | None = None,
    reconstruct: Callable[[dict[str, list[dict[str, Any]]]], list[dict[str, Any]]] = reconstruct_entries,
    canonicalize: Callable[[dict[str, Any]], dict[str, Any]] | None = None,
) -> None:
    """Assert that every run in ``load_info_and_dir`` reconstructs to ``sorted_reference_json``.

    :param load_info_and_dir: Mapping of run key to ``(load_info, output_dir)``.
    :type load_info_and_dir: dict[str, tuple[LoadInfo, Path]]
    :param sorted_reference_json: Expected sorted JSON entry strings.
    :type sorted_reference_json: list[str]
    :param output_format: Optional format override.
    :type output_format: str | None
    :param reconstruct: Function reconstructing entries from output tables.
    :type reconstruct: Callable[[dict[str, list[dict[str, Any]]]], list[dict[str, Any]]]
    :param canonicalize: Optional per-entry canonicalizer.
    :type canonicalize: Callable[[dict[str, Any]], dict[str, Any]] | None
    """
    for load_info, output_dir in load_info_and_dir.values():
        tables = read_pipeline_tables(output_dir / load_info.dataset_name, output_format)
        entries = reconstruct(tables)
        if canonicalize:
            entries = [canonicalize(entry) for entry in entries]
        assert sorted_json(entries) == sorted_reference_json


def run_and_compare_variations(
    run_pipeline: Callable[..., Any],
    variations: dict[str, tuple[str, dict[str, Any]]],
    *,
    sorted_reference_json: list[str] | None = None,
    output_format: str | None = None,
    reconstruct: Callable[[dict[str, list[dict[str, Any]]]], list[dict[str, Any]]] = reconstruct_entries,
    canonicalize: Callable[[dict[str, Any]], dict[str, Any]] | None = None,
) -> dict[str, tuple[LoadInfo, Path]]:
    """Run ``run_pipeline`` across named variations, assert equal datasets, and optionally match reference.

    :param run_pipeline: Isolated pipeline runner callable.
    :type run_pipeline: Callable[..., Any]
    :param variations: Mapping of variation label to ``(chunk_dir, overrides)``.
    :type variations: dict[str, tuple[str, dict[str, Any]]]
    :param sorted_reference_json: Optional expected sorted JSON entry strings.
    :type sorted_reference_json: list[str] | None
    :param output_format: Optional format override.
    :type output_format: str | None
    :param reconstruct: Function reconstructing entries from output tables.
    :type reconstruct: Callable[[dict[str, list[dict[str, Any]]]], list[dict[str, Any]]]
    :param canonicalize: Optional per-entry canonicalizer.
    :type canonicalize: Callable[[dict[str, Any]], dict[str, Any]] | None
    :return: Mapping of variation label to ``(load_info, output_dir)``.
    :rtype: dict[str, tuple[LoadInfo, Path]]
    """
    load_info_and_dir = {
        key: run_and_read(run_pipeline, chunk_dir, **overrides) for key, (chunk_dir, overrides) in variations.items()
    }
    if len(load_info_and_dir) >= MIN_DATASETS_TO_COMPARE:
        assert_datasets_equal(load_info_and_dir, output_format)
    if sorted_reference_json is not None:
        assert_dataset_matches_reference(
            load_info_and_dir,
            sorted_reference_json,
            output_format,
            reconstruct=reconstruct,
            canonicalize=canonicalize,
        )
    return load_info_and_dir


def _entry_sort_name(item: Mapping[str, Any]) -> str:
    """Extract ``name`` from an fsspec detail entry as a string for deterministic sorting.

    :param item: fsspec ``ls(..., detail=True)`` entry dictionary.
    :type item: Mapping[str, Any]
    :return: Entry ``name`` string.
    :rtype: str
    """
    return str(item.get("name", ""))


def _read_fsspec_data_file(fs: AbstractFileSystem, file_path: str) -> list[dict[str, Any]]:
    """Read a Parquet or JSONL (``.jsonl`` / ``.jsonl.gz``) file from an fsspec filesystem.

    :param fs: Source fsspec filesystem.
    :type fs: AbstractFileSystem
    :param file_path: Path on ``fs``.
    :type file_path: str
    :return: Parsed row dictionaries.
    :rtype: list[dict[str, Any]]
    """
    base_name = posixpath.basename(file_path)
    with fs.open(file_path, "rb") as fh:
        raw_bytes = fh.read()
    if base_name.endswith(".parquet"):
        return pq.read_table(io.BytesIO(raw_bytes)).to_pylist()
    if ".jsonl" in base_name:
        if raw_bytes[:2] == b"\x1f\x8b" or base_name.endswith(".gz"):
            raw_bytes = gzip.decompress(raw_bytes)
        text = raw_bytes.decode("utf-8")
        return [json.loads(line) for line in text.splitlines() if line.strip()]
    return []


def _read_fsspec_table_rows(fs: AbstractFileSystem, entry_path: str) -> list[dict[str, Any]]:
    """Read all data files from a table directory (or ``data/`` subdirectory) on an fsspec filesystem.

    :param fs: Source fsspec filesystem.
    :type fs: AbstractFileSystem
    :param entry_path: Table directory path on ``fs``.
    :type entry_path: str
    :return: Concatenated row dictionaries.
    :rtype: list[dict[str, Any]]
    """
    data_subdir = f"{entry_path}/data"
    search_dirs = [data_subdir] if fs.exists(data_subdir) and fs.isdir(data_subdir) else [entry_path]
    rows: list[dict[str, Any]] = []
    for sdir in search_dirs:
        for file_entry in sorted(fs.ls(sdir, detail=True), key=_entry_sort_name):
            file_path = str(file_entry["name"])
            if file_entry.get("type") == "directory" or fs.isdir(file_path):
                continue
            rows.extend(_read_fsspec_data_file(fs, file_path))
    return rows


def _read_open_iceberg_table_rows(client: object, table_name: str) -> list[dict[str, Any]] | None:
    """Read rows from an Iceberg open table on ``client`` when ``table_name`` exists.

    :param client: Destination client supporting ``get_open_table_catalog``.
    :type client: object
    :param table_name: Target table name.
    :type table_name: str
    :return: Table row dictionaries, or ``None`` if the table has not been created.
    :rtype: list[dict[str, Any]] | None
    """
    catalog = client.get_open_table_catalog("iceberg")  # pyright: ignore[reportAttributeAccessIssue]
    try:
        try:
            iceberg_table = catalog.load_table(f"{client.dataset_name}.{table_name}")  # pyright: ignore[reportAttributeAccessIssue]
        except NoSuchTableError:
            return None
        return iceberg_table.scan().to_arrow().to_pylist()
    finally:
        if hasattr(catalog, "close"):
            catalog.close()


def read_destination_tables_from_load_info(
    load_info: LoadInfo,
    *,
    include_load_info: bool = False,
) -> dict[str, list[dict[str, Any]]]:
    """Read data tables (and optionally ``_load_info``) from ``load_info`` across local, S3, or SQL destinations.

    :param load_info: Executed ``LoadInfo``.
    :type load_info: LoadInfo
    :param include_load_info: Whether to include the ``_load_info`` table.
    :type include_load_info: bool
    :return: Mapping from table name to loaded row dictionaries.
    :rtype: dict[str, list[dict[str, Any]]]
    """
    with load_info.pipeline.destination_client() as client:
        if hasattr(client, "fs_client") and hasattr(client, "dataset_path"):
            fs = client.fs_client  # pyright: ignore[reportAttributeAccessIssue]
            dataset_path = str(client.dataset_path).rstrip("/")  # pyright: ignore[reportAttributeAccessIssue]
            if not fs.exists(dataset_path):
                return {}
            tables: dict[str, list[dict[str, Any]]] = defaultdict(list)
            for entry in sorted(fs.ls(dataset_path, detail=True), key=_entry_sort_name):
                entry_path = str(entry["name"]).rstrip("/")
                table_name = posixpath.basename(entry_path)
                if table_name.startswith("_dlt") or (table_name == LOAD_INFO_TABLE_NAME and not include_load_info):
                    continue
                if entry.get("type") != "directory" and not fs.isdir(entry_path):
                    continue
                if hasattr(client, "is_open_table") and client.is_open_table("iceberg", table_name):  # pyright: ignore[reportAttributeAccessIssue]
                    maybe_rows = _read_open_iceberg_table_rows(client, table_name)
                    if maybe_rows is None:
                        continue
                    rows = maybe_rows
                else:
                    rows = _read_fsspec_table_rows(fs, entry_path)
                tables[table_name].extend(
                    rows if table_name == LOAD_INFO_TABLE_NAME else (strip_metadata_columns(r) for r in rows)
                )
            return dict(tables)

    dataset = load_info.pipeline.dataset()
    sql_tables: dict[str, list[dict[str, Any]]] = {}
    for table_name in sorted(dataset.tables):
        if table_name.startswith("_dlt"):
            continue
        if table_name == LOAD_INFO_TABLE_NAME and not include_load_info:
            continue
        raw_rows = dataset.table(table_name).arrow().to_pylist()
        sql_tables[table_name] = (
            raw_rows if table_name == LOAD_INFO_TABLE_NAME else [strip_metadata_columns(r) for r in raw_rows]
        )
    return sql_tables


def _strip_none_and_empty_lists(value: object) -> object:
    """Recursively remove keys with ``None`` or empty-list values from nested dicts and lists.

    :param value: Input Python value.
    :type value: object
    :return: Cleaned Python value.
    :rtype: object
    """
    if isinstance(value, dict):
        cleaned: dict[str, object] = {}
        for k, v in value.items():
            if v is None:
                continue
            stripped_v = _strip_none_and_empty_lists(v)
            if stripped_v is None or stripped_v == []:
                continue
            cleaned[k] = stripped_v
        return cleaned
    if isinstance(value, list):
        return [_strip_none_and_empty_lists(item) for item in value]
    return value


def canonicalize_table_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Strip ``None``/empty-list values, normalize top-level column names, parse JSON strings, and sort rows.

    :param rows: Loaded row dictionaries.
    :type rows: list[dict[str, Any]]
    :return: Canonicalized sorted row dictionaries.
    :rtype: list[dict[str, Any]]
    """
    naming = SnakeCaseNamingConvention()
    cleaned: list[dict[str, Any]] = []
    for row in rows:
        parsed_row = {
            naming.normalize_identifier(k): (parse_json_string(v) if k not in {"raw_record", "error_detail"} else v)
            for k, v in strip_metadata_columns(row).items()
        }
        stripped = _strip_none_and_empty_lists(parsed_row)
        normalized = json.loads(json.dumps(stripped, default=json_default))
        cleaned.append(normalized)
    return sorted(cleaned, key=partial(json.dumps, sort_keys=True))


def normalize_expected_table_rows(
    table: pa.Table,
    load_info: LoadInfo | None = None,
) -> list[dict[str, Any]]:
    """Convert an expected ``pa.Table`` to canonical row dicts with dlt-normalized top-level column names.

    :param table: Expected PyArrow table.
    :type table: pa.Table
    :param load_info: Optional ``LoadInfo`` supplying the active naming convention.
    :type load_info: LoadInfo | None
    :return: Canonicalized row dictionaries.
    :rtype: list[dict[str, Any]]
    """
    naming = None
    if load_info is not None and getattr(load_info.pipeline, "default_schema_name", None):
        naming = getattr(load_info.pipeline.default_schema, "naming", None)
    if naming is None:
        naming = SnakeCaseNamingConvention()
    renamed_cols = [naming.normalize_identifier(name) for name in table.column_names]
    renamed_table = table.rename_columns(renamed_cols)
    return canonicalize_table_rows(renamed_table.to_pylist())


def assert_smoke_run_output(
    load_info: LoadInfo | None,
    settings: CtsSettings,
    run: PipelineRun,
) -> None:
    """Validate ``LoadInfo``, ``report.txt``, ``_load_info``, row counts, and expected table contents.

    :param load_info: Executed ``LoadInfo``.
    :type load_info: LoadInfo | None
    :param settings: Pipeline ``CtsSettings``.
    :type settings: CtsSettings
    :param run: Expected ``PipelineRun`` contract.
    :type run: PipelineRun
    """
    assert load_info is not None
    if run.expected_report.status == "completed":
        assert not load_info.has_failed_jobs

    report_path = settings.report_path
    assert report_path.is_file(), f"Missing pipeline report at {report_path}"
    report_data = yaml.safe_load(report_path.read_text(encoding="utf-8"))
    assert isinstance(report_data, dict)
    assert report_data["pipeline"] == load_info.pipeline.pipeline_name
    assert report_data["dataset"] == load_info.dataset_name
    assert report_data["status"] == run.expected_report.status
    assert report_data["pipeline_load_info_errs"] == run.expected_report.pipeline_load_info_errs
    assert report_data["load_ids"] == list(load_info.loads_ids)
    assert report_data["started"] <= report_data["finished"]

    if run.expected_report.ingested_files:
        assert sorted(report_data.get("ingested_files", [])) == sorted(run.expected_report.ingested_files)
    else:
        assert "ingested_files" not in report_data

    if run.expected_report.load_errors:
        actual_errors = report_data.get("load_errors", [])
        assert len(actual_errors) == len(run.expected_report.load_errors)
        for expected_substr, actual_err in zip(run.expected_report.load_errors, actual_errors, strict=True):
            assert expected_substr in actual_err
    else:
        assert "load_errors" not in report_data

    all_tables = read_destination_tables_from_load_info(load_info, include_load_info=True)
    assert LOAD_INFO_TABLE_NAME in all_tables, f"Missing {LOAD_INFO_TABLE_NAME!r} table in destination"
    load_info_rows = all_tables.pop(LOAD_INFO_TABLE_NAME)
    expected_load_ids = list(load_info.loads_ids)
    matching_load_info_rows = [row for row in load_info_rows if row.get("loads_ids") == expected_load_ids]
    assert len(matching_load_info_rows) == 1, (
        f"Expected 1 {LOAD_INFO_TABLE_NAME!r} row for loads_ids={expected_load_ids!r}, "
        f"found {len(matching_load_info_rows)} out of {len(load_info_rows)}"
    )

    actual_counts = {name: len(rows) for name, rows in all_tables.items()}
    assert actual_counts == run.expected_counts, f"Expected {run.expected_counts}; got {actual_counts}"

    if run.expected_tables is not None:
        expected_pa_tables = load_expected_tables(run)
        assert set(all_tables) == set(expected_pa_tables)
        for table_name, exp_table in expected_pa_tables.items():
            actual_rows = canonicalize_table_rows(all_tables[table_name])
            expected_rows = normalize_expected_table_rows(exp_table, load_info)
            assert actual_rows == expected_rows, (
                f"Mismatch in table {table_name!r}:\nactual={actual_rows}\nexpected={expected_rows}"
            )

    if run.expected_json is not None:
        expected_json_data = load_expected_json_tables(run)
        actual_canonical = {name: canonicalize_table_rows(rows) for name, rows in sorted(all_tables.items())}
        expected_canonical = {name: canonicalize_table_rows(rows) for name, rows in sorted(expected_json_data.items())}
        assert actual_canonical == expected_canonical
