"""Misc helpers for integration tests."""

import json
from pathlib import Path
from typing import Any

import dlt
from dlt.common.pipeline import LoadInfo
from dlt.destinations import filesystem

from tests.integration.pipelines.xml.xmltodict_reference_helpers import (
    read_pipeline_tables,
    reconstruct_entries,
)

WORKER_CONFIGS = ((1, 1, 1), (3, 2, 2), (5, 3, 4))
BUFFER_SIZES = (1, 5, 100, 500)


def _sorted_json(entries: list[dict[str, Any]]) -> list[str]:
    """Entries as sorted JSON strings for order-insensitive comparison."""
    return sorted(json.dumps(entry, sort_keys=True, default=str) for entry in entries)


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
    return _sorted_json(
        [
            {k: v for k, v in datum.items() if not k.startswith("_dlt")}
            for datum in dataset.table(table_name).df().to_dict("records")
        ]
    )


def assert_datasets_equal(
    load_info_and_dir: dict[str, tuple[LoadInfo, Path]],
) -> None:
    """Compare datasets to check whether they are identical."""
    datasets = {key: get_pipeline(*v) for key, v in load_info_and_dir.items()}

    all_keys = list(load_info_and_dir.keys())
    if len(all_keys) == 1:
        return

    sorted_row_data = {k: {} for k in all_keys}
    dataset_zero = all_keys[0]
    table_names = set(datasets[dataset_zero].dataset().tables)
    rows_per_table = dict(datasets[dataset_zero].dataset().row_counts().fetchall())
    col_names = {t: set(datasets[dataset_zero].dataset().table(t).df().columns) for t in table_names}
    sorted_row_data[dataset_zero] = {
        t: extract_table_data(datasets[dataset_zero].dataset(), t) for t in table_names if not t.startswith("_dlt")
    }

    for key in all_keys[1:]:
        # check the table names are identical
        assert set(datasets[key].dataset().tables) == table_names
        # check row counts
        assert dict(datasets[key].dataset().row_counts().fetchall()) == rows_per_table
        # column names
        for t in datasets[key].dataset().tables:
            assert set(datasets[key].dataset().table(t).df().columns) == col_names[t]
            if t.startswith("_dlt"):
                continue
            sorted_row_data[key][t] = extract_table_data(datasets[key].dataset(), t)
            assert sorted_row_data[key][t] == sorted_row_data[dataset_zero][t]


def assert_dataset_matches_reference(
    load_info_and_dir: dict[str, tuple[LoadInfo, Path]],
    sorted_reference_json: list[str],
    output_format: str | None = None,
) -> None:
    """Ensure that a dataset matches the reference dataset."""
    # parsed JSON output files for each table, indexed by key
    tables: dict[str, dict[str, Any]] = {}
    # original data structure, reconstructed from the tables, indexed by key
    reconstructed: dict[str, list[Any]] = {}

    for key, (load_info, output_dir) in load_info_and_dir.items():
        tables[key] = read_pipeline_tables(output_dir / load_info.dataset_name, output_format)
        reconstructed[key] = _sorted_json(reconstruct_entries(tables[key]))

    for value in reconstructed.values():
        assert value == sorted_reference_json


def get_pipeline(load_info: LoadInfo, output_dir: Path) -> dlt.Pipeline:
    """Return a Pipeline object for the given dataset and destination."""
    return dlt.pipeline(destination=filesystem(bucket_url=str(output_dir)), dataset_name=load_info.dataset_name)
