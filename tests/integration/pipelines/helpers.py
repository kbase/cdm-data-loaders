"""Misc helpers for integration tests."""

import gzip
import json
from collections import defaultdict
from collections.abc import Callable
from logging import Logger, getLogger
from pathlib import Path
from typing import Any

import dlt
import pyarrow.parquet as pq
from dlt.common.pipeline import LoadInfo
from dlt.destinations import filesystem

from cdm_data_loaders.core.fields import PARQUET, LoaderFileFormatEnum
from tests.integration.pipelines.pipeline_helpers import (
    reconstruct_entries,
    strip_metadata_columns,
)

logger: Logger = getLogger(__name__)


WORKER_CONFIGS = ((1, 1, 1), (3, 2, 2), (5, 3, 4))
BUFFER_SIZES = (1, 5, 100, 500)
LOADER_FILE_FORMATS = list(LoaderFileFormatEnum)


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
        if json_file.parent.name.startswith("_dlt"):
            continue
        # Detect gzip by magic bytes rather than relying on the extension,
        # in case a .json file is actually gzipped or vice versa.
        with json_file.open("rb") as f:
            is_gzipped = f.read(2) == b"\x1f\x8b"

        opener = gzip.open if is_gzipped else open
        with opener(json_file, "rt", encoding="utf-8") as f:
            for raw_line in f:
                try:
                    tables[json_file.parent.name].append(json.loads(raw_line.strip()))
                except json.JSONDecodeError:
                    logger.warning("Skipping unparseable line in %s: %r", json_file, raw_line[:200])

    return dict(tables)


def sorted_json(entries: list[dict[str, Any]]) -> list[str]:
    """Entries as sorted JSON strings for order-insensitive comparison."""
    return sorted(json.dumps(entry, sort_keys=True, default=str) for entry in entries)


def settings_to_argv(settings: Any) -> list[str]:
    """Derive CLI arguments from a settings instance.

    Every serialized field becomes a ``--kebab-case`` flag, with bools rendered
    as their ``true``/``false`` CLI spelling and None-valued fields skipped.

    :param settings: a CtsSettings (or derivative) instance
    :type settings: Any
    :return: command-line argument list reproducing the settings
    :rtype: list[str]
    """
    argv: list[str] = []
    for name, value in settings.model_dump().items():
        flag = f"--{name.replace('_', '-')}"
        if isinstance(value, bool):
            argv.extend([flag, "true" if value else "false"])
        elif value is None:
            continue
        else:
            argv.extend([flag, str(value)])
    return argv


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
        if not name.startswith("_dlt")
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
    return (load_info, output_dir)


def extract_table_data(dataset: dlt.Dataset, table_name: str) -> list[str]:
    """Convert table data into a JSON string, removing the _dlt fields first."""
    return sorted_json(
        [
            {k: v for k, v in datum.items() if not k.startswith("_dlt")}
            for datum in dataset.table(table_name).df().to_dict("records")
        ]
    )


def is_metadata_table(table_name: str) -> bool:
    """Return whether a table contains dlt pipeline metadata rather than loaded records."""
    return table_name.startswith("_dlt")


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


def get_pipeline(load_info: LoadInfo, output_dir: Path) -> dlt.Pipeline:
    """Return a Pipeline object for the given dataset and destination."""
    return dlt.pipeline(destination=filesystem(bucket_url=str(output_dir)), dataset_name=load_info.dataset_name)
