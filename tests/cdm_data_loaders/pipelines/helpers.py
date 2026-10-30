"""Misc helpers for integration tests."""

import gzip
import json
from collections import defaultdict
from collections.abc import Callable, Generator, Mapping
from contextlib import contextmanager
from logging import Logger, getLogger
from pathlib import Path
from typing import Any, Final
from unittest.mock import patch
from uuid import uuid4

import dlt
import duckdb
import pendulum
import pyarrow as pa
import pyarrow.parquet as pq
import yaml
from dlt.common.pipeline import LoadInfo
from dlt.common.storages.fsspec_filesystem import FileItemDict
from dlt.destinations import filesystem
from dlt.extract import DltResource
from pandas import DataFrame

from cdm_data_loaders.converters.core.paths import set_nested
from cdm_data_loaders.core.fields import PARQUET, PIPELINE_DIR_NAME, LoaderFileFormatEnum
from cdm_data_loaders.core.settings import FileFormatIngestSettings
from cdm_data_loaders.pipelines.core import LOAD_INFO_TABLE_NAME
from cdm_data_loaders.utils.gz import open_maybe_gzip
from tests.helpers import strip_none_values

logger: Logger = getLogger(__name__)


WORKER_CONFIGS = ((1, 1, 1), (3, 2, 2), (5, 3, 4))
BUFFER_SIZES = (1, 5, 100, 500)
LOADER_FILE_FORMATS = list(LoaderFileFormatEnum)

DLT_METADATA_PREFIX: Final[str] = "_dlt"

XMLNS_COLUMNS: Final[frozenset[str]] = frozenset({"entry__axmlns", "entry__axmlns_xsi"})

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
    """Return whether a table contains dlt pipeline metadata rather than loaded records."""
    return table_name.startswith("_dlt") or table_name == LOAD_INFO_TABLE_NAME


def read_jsonl_file(file_path: Path) -> list[dict[str, Any]]:
    """Read a plain or gzipped JSONLines file."""
    rows: list[dict[str, Any]] = []
    with open_maybe_gzip(file_path) as f:
        for raw_line in f:
            stripped = raw_line.decode("utf-8").strip()
            if not stripped:
                continue
            try:
                rows.append(json.loads(stripped))
            except json.JSONDecodeError:
                logger.warning("Skipping unparseable line in %s: %r", file_path, stripped[:200])
    return rows


def write_jsonl_gz_file(path: Path, rows: list[dict[str, Any]]) -> Path:
    """Write rows as gzip-compressed JSONLines to `path`, creating parent directories."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(path, "wt", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row) + "\n")
    return path


def write_parquet_file(path: Path, rows: list[dict[str, Any]]) -> Path:
    """Write rows as a Parquet file to `path`, creating parent directories."""
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows), path)
    return path


def read_parquet_tables(output_dir: Path) -> dict[str, list[dict[str, Any]]]:
    """Read all parquet files under a pipeline output dataset dir, grouped by table name."""
    tables: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for parquet_file in sorted(output_dir.rglob("*.parquet")):
        tables[parquet_file.parent.name].extend(pq.read_table(parquet_file).to_pylist())
    return dict(tables)


def read_pipeline_tables(output_dir: Path, output_format: str | None = None) -> dict[str, list[dict[str, Any]]]:
    """Read all json files under a pipeline output dataset dir, grouped by table name.

    Defaults to reading jsonlines files; supply "parquet" as file format to read parquet files.
    """
    if output_format and output_format == PARQUET:
        return read_parquet_tables(output_dir)

    tables: dict[str, list[dict[str, Any]]] = defaultdict(list)

    for json_file in sorted(output_dir.rglob("*.json*")):
        if not json_file.is_file():
            continue
        # ignore metadata dirs
        if is_metadata_table(json_file.parent.name):
            continue
        tables[json_file.parent.name].extend(read_jsonl_file(json_file))

    return dict(tables)


def read_destination_rows(
    base_dir: Path,
    table_name: str,
    *,
    strip_metadata: bool = True,
) -> list[dict[str, Any]]:
    """Read the rows the filesystem destination wrote for table_name under base_dir.

    Handles parquet, jsonl and gzipped jsonl files. Returns an empty list if the table has no files.
    """
    rows: list[dict[str, Any]] = []
    for data_file in sorted(base_dir.glob(f"**/{table_name}/*")):
        if data_file.name.endswith(".parquet"):
            rows.extend(pq.read_table(data_file).to_pylist())
        elif ".jsonl" in data_file.name:
            rows.extend(read_jsonl_file(data_file))
    if strip_metadata:
        return [strip_metadata_columns(row) for row in rows]
    return rows


def duckdb_pipeline(tmp_path: Path, pipeline_name: str, dataset_name: str, db_name: str | None = None) -> dlt.Pipeline:
    """Return a dlt pipeline with a duckdb destination in tmp_path.

    The database file defaults to a name derived from the pipeline name so that the DuckDB
    catalog differs from the dataset schema; equal names trigger a DuckDB binder error.
    """
    db_file = f"{db_name or pipeline_name}_db.duckdb"
    return dlt.pipeline(
        pipeline_name=pipeline_name,
        dataset_name=dataset_name,
        destination=dlt.destinations.duckdb(str(tmp_path / db_file)),
        pipelines_dir=str(tmp_path / "pipelines"),
    )


def assert_core_run_pipeline_args(
    pipeline_module: Any,  # noqa: ANN401
    run_fn: Callable[[Any], Any],
    settings: FileFormatIngestSettings,
    pipeline_name: str,
) -> None:
    """Assert that a pipeline runner delegates to core.run_pipeline with standard ingest kwargs."""
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
    assert kwargs["pipeline_run_kwargs"] == {"loader_file_format": settings.loader_file_format}
    assert isinstance(kwargs["resource"], DltResource)


def assert_binds_filesystem_reader(
    pipeline_module: Any,  # noqa: ANN401
    run_fn: Callable[[Any], Any],
    settings: Any,  # noqa: ANN401
    expected_reader_name: str | None = None,
) -> DltResource:
    """Assert that a pipeline runner passes a DltResource piped from `filesystem` to run_pipeline."""
    with patch.object(pipeline_module, "run_pipeline") as mock_run_pipeline:
        run_fn(settings)

    mock_run_pipeline.assert_called_once()
    resource = mock_run_pipeline.call_args.kwargs["resource"]
    assert isinstance(resource, dlt.sources.DltResource)
    if expected_reader_name is not None:
        assert resource.name == expected_reader_name
        assert resource._pipe.parent.name == "filesystem"  # noqa: SLF001
    return resource


def run_with_duckdb_pipeline(
    tmp_path: Path,
    pipeline_module: Any,  # noqa: ANN401
    invoke_fn: Callable[[], Any],
    *,
    expected_pipeline_name: str,
    expected_dataset_name: str,
    pipeline_name_prefix: str = "test_cli_pipeline",
) -> LoadInfo:
    """Patch `pipeline_module.run_pipeline` to run against a real DuckDB destination and return LoadInfo."""
    db_pipeline_name = f"{pipeline_name_prefix}_{uuid4().hex}"
    captured: dict[str, LoadInfo] = {}

    def fake_run_pipeline(
        *,
        resource: DltResource,
        pipeline_kwargs: dict[str, Any],
        **_: Any,  # noqa: ANN401
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
    """Check the output of parsing SIMPLE_LIBRARY_XML into a single book table."""
    load_info = assert_run_ok(load_info)
    dataset = load_info.pipeline.dataset()
    table_name = table_name or "book"
    assert set(dataset.tables) == {
        table_name,
        "_dlt_version",
        "_dlt_loads",
        "_dlt_pipeline_state",
        LOAD_INFO_TABLE_NAME,
    }
    book_df = dataset.table(table_name).df()
    required_cols = expected_columns or {"book___id", "book__title", "_dlt_id", "_dlt_load_id"}
    assert set(book_df.columns.tolist()) >= required_cols
    return book_df


def make_file_item(file_path: Path, mime_type: str = "application/octet-stream") -> FileItemDict:
    """Build a dlt FileItemDict for a local file."""
    return FileItemDict(
        {
            "file_url": file_path.as_uri(),
            "file_name": file_path.name,
            "relative_path": file_path.name,
            "mime_type": mime_type,
            "modification_date": pendulum.now(),
            "size_in_bytes": file_path.stat().st_size,
        }
    )


def fetch_all_duckdb(pipeline: dlt.Pipeline, query: str) -> list[tuple[Any, ...]]:
    """Run a query against a pipeline's DuckDB destination and return all rows."""
    with (
        pipeline.sql_client() as client,
        client.execute_query(query) as cur,
    ):
        return list(cur.fetchall())


def assert_duckdb_table_counts(pipeline: dlt.Pipeline, expected_counts: Mapping[str, int]) -> None:
    """Assert that each table in `expected_counts` has the expected row count in DuckDB."""
    for table_name, expected_count in expected_counts.items():
        ((count,),) = fetch_all_duckdb(pipeline, f"SELECT COUNT(*) FROM {table_name}")  # noqa: S608
        assert count == expected_count, f"expected {expected_count} rows in '{table_name}', got {count}"


def assert_destination_table_counts(dataset_dir: Path, expected_counts: Mapping[str, int]) -> None:
    """Assert that each table in `expected_counts` has the expected row count on the filesystem destination."""
    for table_name, expected_count in expected_counts.items():
        rows = read_destination_rows(dataset_dir, table_name)
        assert len(rows) == expected_count, f"expected {expected_count} rows in '{table_name}', got {len(rows)}"


@contextmanager
def duckdb_reference_from_jsonl(jsonl_path: Path) -> Generator[duckdb.DuckDBPyConnection]:
    """Load a reference JSONL file into an in-memory DuckDB connection with a `reference` table."""
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
    """Entries as sorted JSON strings for order-insensitive comparison."""
    return sorted(json.dumps(entry, sort_keys=True, default=str) for entry in entries)


def assert_run_ok(load_info: LoadInfo | None) -> LoadInfo:
    """Assert a pipeline run succeeded, and return the load info for chained use."""
    assert load_info is not None
    assert not load_info.has_failed_jobs
    return load_info


def read_data_rows(dataset_dir: Path, file_format: str | None = None) -> dict[str, list[dict[str, Any]]]:
    """Read a pipeline output dataset, dropping dlt metadata columns and metadata tables."""
    tables = read_pipeline_tables(dataset_dir, file_format)
    return {
        name: [strip_metadata_columns(row) for row in rows]
        for name, rows in tables.items()
        if not is_metadata_table(name)
    }


def check_pipeline_report(path: Path, expected: dict[str, Any]) -> dict[str, Any]:
    """Ensure a pipeline report contains what it purports to contain.

    :param path: path to the report file
    :type path: Path
    :param expected: key, value pairs that should appear in the report
    :type expected: dict[str, Any]
    :return: parsed YAML report dictionary
    :rtype: dict[str, Any]
    """
    assert path.is_file()
    with path.open("r", encoding="utf-8") as fh:
        report_data: dict[str, Any] = yaml.safe_load(fh)

    for e, v in expected.items():
        assert report_data[e] == v
    return report_data


def assert_pipeline_report_written(
    output_dir: Path,
    load_info: LoadInfo,
    *,
    metadata_moved: bool = False,
    ingested_files: list[str] | set[str] | None = None,
) -> None:
    """Assert that save_pipeline_metadata wrote the expected report file in the configured directory."""
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
    run_pipeline: Any,
    chunk_dir: str,
    **overrides: Any,
) -> tuple[LoadInfo, Path]:
    """Run the pipeline on a chunk dir and return the run result with its tables."""
    (load_info, output_dir) = run_pipeline(chunk_dir, **overrides)
    assert load_info is not None
    assert not load_info.has_failed_jobs
    assert_pipeline_report_written(output_dir, load_info, metadata_moved=(output_dir / PIPELINE_DIR_NAME).is_dir())
    return (load_info, output_dir)


def extract_table_data(dataset: dlt.Dataset, table_name: str) -> list[str]:
    """Convert table data into a JSON string, removing the _dlt fields first."""
    return sorted_json(
        [
            {k: v for k, v in datum.items() if not is_metadata_table(k)}
            for datum in dataset.table(table_name).df().to_dict("records")
        ]  # pyright: ignore[reportArgumentType]
    )


def get_pipeline(load_info: LoadInfo, output_dir: Path) -> dlt.Pipeline:
    """Return a Pipeline object for the given dataset and destination."""
    return dlt.pipeline(destination=filesystem(bucket_url=str(output_dir)), dataset_name=load_info.dataset_name)


def strip_metadata_columns(row: dict[str, Any]) -> dict[str, Any]:
    """Drop dlt metadata columns from a row dict."""
    return {key: value for key, value in row.items() if not key.startswith(DLT_METADATA_PREFIX)}


def denormalize_identifier(name: str) -> str:
    """Restore an xmltodict key from a dlt-normalized fragment."""
    return DLT_TO_XML_NAMES.get(name, name)


def denormalize_path(path: str) -> list[str]:
    """Split a dlt name into xml key fragments, unmangling each one."""
    return [denormalize_identifier(fragment) for fragment in path.split("__")]


def rows_as_multiset(tables: dict[str, list[dict[str, Any]]], table_name: str) -> list[str]:
    """Rows of a table as sorted JSON strings for order-insensitive comparison."""
    return sorted(
        json.dumps(strip_metadata_columns(row), sort_keys=True, default=str) for row in tables.get(table_name, [])
    )


def parse_json_string(value: Any) -> Any:
    """Parse a string holding a JSON object or array; pass any other value through.

    Parquet output with max_table_nesting=0 stores nested content as JSON strings,
    while jsonl output keeps it as parsed structures.
    """
    if isinstance(value, str) and value[:1] in ("{", "["):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return value
    return value


def reconstruct_entries(tables: dict[str, list[dict[str, Any]]]) -> list[dict[str, Any]]:
    """Rebuild nested dicts from the pipeline's flattened output tables.

    The main table holds scalar columns; child tables hold list contents,
    linked by _dlt_parent_id and ordered by _dlt_list_idx. A child table's parent
    may be a main entry row or another nested row, so the walk follows parentage
    rather than table-name prefixes.
    """
    top_tables = [t for t in tables if "__" not in t]
    if not top_tables or len(top_tables) > 1:
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


def _attach_nested_rows(
    target: dict[str, Any],
    parent_id: str,
    parent_index: dict[str, list[tuple[str, dict[str, Any]]]],
    parent_root_path: list[str],
    depth_limit: int,
    top_table: str,
) -> None:
    """Attach child-table rows (and their own children, recursively) to a target dict.

    parent_root_path is the target's own path from the root entry; child paths are
    made relative to it, since a child table name encodes the path from the root.
    """
    if depth_limit <= 0:
        return
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for table_name, row in parent_index.get(parent_id, []):
        grouped[table_name].append(row)

    for table_name, rows in grouped.items():
        rows.sort(key=lambda r: r["_dlt_list_idx"])
        full_path = _child_table_path(table_name, top_table)
        relative_path = full_path[len(parent_root_path) :] if parent_root_path else full_path
        children = []
        for row in rows:
            child: dict[str, Any] = {}
            for column, raw in strip_metadata_columns(row).items():
                if raw is None:
                    continue
                set_nested(child, denormalize_path(column), parse_json_string(raw))
            _attach_nested_rows(child, row["_dlt_id"], parent_index, full_path, depth_limit - 1, top_table)
            children.append(child)
        set_nested(target, relative_path, children)


def _child_table_path(table_name: str, top_table: str) -> list[str]:
    """Derive the xml key path for a child table from its dlt name.

    Table names are prefixed with the parent chain (entry__entry__property); the
    leading fragments repeat the root table name and are dropped.
    """
    fragments = denormalize_path(table_name)
    while len(fragments) > 1 and fragments[0] == top_table:
        fragments = fragments[1:]
    return fragments


def reference_entries_from_duckdb(connection: duckdb.DuckDBPyConnection) -> list[dict[str, Any]]:
    """Extract reference entries from DuckDB as canonical nested Python dicts."""
    rows = connection.execute("SELECT entry FROM reference ORDER BY entry['_id']").fetchall()
    return [_normalize_reference_entry(entry) for (entry,) in rows]


def _normalize_reference_entry(entry: dict[str, Any]) -> dict[str, Any]:
    """Normalize a DuckDB STRUCT entry dict to the shape of reconstructed pipeline entries."""
    normalized: dict[str, Any] = {}
    for key, raw_value in entry.items():
        if raw_value is None:
            continue
        value = raw_value
        if isinstance(value, str) and value.startswith(("{", "[")):
            value = json.loads(value)
        normalized[key] = strip_none_values(value)
    return normalized


def assert_datasets_equal(
    load_info_and_dir: dict[str, tuple[LoadInfo, Path]],
) -> None:
    """Compare datasets to check whether they are identical."""
    if len(load_info_and_dir) < 2:
        err_msg = "assert_datasets_equal requires at least two datasets to compare"
        raise ValueError(err_msg)

    datasets = {key: get_pipeline(*v) for key, v in load_info_and_dir.items()}

    all_keys = list(load_info_and_dir.keys())

    sorted_row_data = {k: {} for k in all_keys}
    dataset_zero = all_keys[0]
    table_names = {
        table_name for table_name in datasets[dataset_zero].dataset().tables if not is_metadata_table(table_name)
    }
    rows_per_table = {
        table_name: datasets[dataset_zero].dataset().table(table_name).df().shape[0] for table_name in table_names
    }
    col_names = {t: set(datasets[dataset_zero].dataset().table(t).df().columns) for t in table_names}
    sorted_row_data[dataset_zero] = {t: extract_table_data(datasets[dataset_zero].dataset(), t) for t in table_names}

    for key in all_keys[1:]:
        # check the table names are identical
        assert {
            table_name for table_name in datasets[key].dataset().tables if not is_metadata_table(table_name)
        } == table_names
        # check row counts
        assert {
            table_name: datasets[key].dataset().table(table_name).df().shape[0] for table_name in table_names
        } == rows_per_table
        # column names
        for t in table_names:
            assert set(datasets[key].dataset().table(t).df().columns) == col_names[t]
            sorted_row_data[key][t] = extract_table_data(datasets[key].dataset(), t)
            assert sorted_row_data[key][t] == sorted_row_data[dataset_zero][t]


def assert_dataset_matches_reference(
    load_info_and_dir: dict[str, tuple[LoadInfo, Path]],
    sorted_reference_json: list[str],
    output_format: str | None = None,
    reconstruct: Callable[[dict[str, list[dict[str, Any]]]], list[dict[str, Any]]] = reconstruct_entries,
    canonicalize: Callable[[dict[str, Any]], dict[str, Any]] | None = None,
) -> None:
    """Ensure that a dataset matches the reference dataset."""
    # parsed JSON output files for each table, indexed by key
    tables: dict[str, dict[str, Any]] = {}
    # original data structure, reconstructed from the tables, indexed by key
    reconstructed: dict[str, list[Any]] = {}

    for key, (load_info, output_dir) in load_info_and_dir.items():
        tables[key] = read_pipeline_tables(output_dir / load_info.dataset_name, output_format)
        entries = reconstruct(tables[key])
        if canonicalize:
            entries = [canonicalize(entry) for entry in entries]
        reconstructed[key] = sorted_json(entries)

    for value in reconstructed.values():
        assert value == sorted_reference_json


def run_and_compare_variations(
    run_pipeline: Callable[..., Any],
    variations: dict[str, tuple[str, dict[str, Any]]],
    *,
    sorted_reference_json: list[str] | None = None,
    output_format: str | None = None,
    reconstruct: Callable[[dict[str, list[dict[str, Any]]]], list[dict[str, Any]]] = reconstruct_entries,
    canonicalize: Callable[[dict[str, Any]], dict[str, Any]] | None = None,
) -> dict[str, tuple[LoadInfo, Path]]:
    """Run a pipeline across named (chunk_dir, overrides) variations, assert equal datasets, and optionally match reference."""
    load_info_and_dir = {
        key: run_and_read(run_pipeline, chunk_dir, **overrides) for key, (chunk_dir, overrides) in variations.items()
    }
    if len(load_info_and_dir) >= 2:  # noqa: PLR2004
        assert_datasets_equal(load_info_and_dir)
    if sorted_reference_json is not None:
        assert_dataset_matches_reference(
            load_info_and_dir,
            sorted_reference_json,
            output_format,
            reconstruct=reconstruct,
            canonicalize=canonicalize,
        )
    return load_info_and_dir
