"""Integration tests for the xml_to_dict pipeline against the uniref reference dataset."""

import json
from pathlib import Path
from typing import Any

import dlt
import duckdb
import pytest
from dlt.common.pipeline import LoadInfo
from dlt.destinations import filesystem

from cdm_data_loaders.core.fields import LoaderFileFormatEnum
from tests.integration.pipelines.xml.xmltodict_reference_helpers import (
    read_pipeline_tables,
    reconstruct_entries,
    reference_entries_from_duckdb,
)

CHUNK_DIRS = ("chunk_5_el", "chunk_20_el", "chunk_100_el")
EXPECTED_ENTRY_COUNT = 100

WORKER_CONFIGS = ((1, 1, 1), (3, 2, 2), (5, 3, 4))
BUFFER_SIZES = (1, 5, 100)


def _sorted_json(entries: list[dict[str, Any]]) -> list[str]:
    """Entries as sorted JSON strings for order-insensitive comparison."""
    return sorted(json.dumps(entry, sort_keys=True, default=str) for entry in entries)


def _run_and_read(
    run_xmltodict_pipeline: Any,
    chunk_dir: str,
    **overrides: Any,
) -> tuple[LoadInfo, Path]:
    """Run the pipeline on a chunk dir and return the run result with its tables."""
    (load_info, output_dir) = run_xmltodict_pipeline(chunk_dir, **overrides)
    assert load_info is not None
    assert not load_info.has_failed_jobs
    return (load_info, output_dir)


# tests for the "reference" data structure
def test_xmltodict_reference_jsonl_pass_matches_source(
    reference_entries: list[dict[str, Any]],
    reference_jsonl: Path,
) -> None:
    """The generated reference JSONL has 100 entries with the expected shape."""
    assert len(reference_entries) == EXPECTED_ENTRY_COUNT
    with reference_jsonl.open(encoding="utf-8") as fh:
        line_count = sum(1 for line in fh if line.strip())
    assert line_count == EXPECTED_ENTRY_COUNT
    for key in ("@id", "name", "property", "representativeMember"):
        assert key in reference_entries[0]


def test_xml_ingest_pass_reference_dataset_has_nested_structures(
    reference_dataset: duckdb.DuckDBPyConnection,
) -> None:
    """The DuckDB reference stores nested JSON as STRUCT/LIST types, not JSON text columns."""
    property_type, member_type = reference_dataset.execute(
        "SELECT typeof(entry['property']), typeof(entry['representativeMember']) FROM reference LIMIT 1"
    ).fetchone()
    assert property_type.endswith("[]")
    assert member_type.startswith("STRUCT")

    count = reference_dataset.execute("SELECT COUNT(*) FROM reference").fetchone()[0]
    assert count == EXPECTED_ENTRY_COUNT


def assert_datasets_equal(
    load_info_and_dir: dict[str, tuple[LoadInfo, Path]],
    sorted_reference_json: list[str],
    output_format: str | None = None,
) -> None:
    """Compare a dataset against the reference."""
    datasets = {key: get_pipeline(*v) for key, v in load_info_and_dir.items()}

    # parsed JSON output files for each table, indexed by key
    tables: dict[str, dict[str, Any]] = {}
    # original data structure, reconstructed from the tables, indexed by key
    reconstructed: dict[str, list[Any]] = {}

    for key, (load_info, output_dir) in load_info_and_dir.items():
        tables[key] = read_pipeline_tables(output_dir / load_info.dataset_name, output_format)
        reconstructed[key] = reconstruct_entries(tables[key])
        # compare to reference
        assert _sorted_json(reconstructed[key]) == sorted_reference_json

    all_keys = list(load_info_and_dir.keys())
    if len(all_keys) > 1:
        dataset_zero = all_keys[0]
        table_names = set(datasets[dataset_zero].dataset().tables)
        rows_per_table = dict(datasets[dataset_zero].dataset().row_counts().fetchall())
        col_names = {t: set(datasets[dataset_zero].dataset().table(t).df().columns) for t in table_names}
        for key in all_keys[1:]:
            # check the table names are identical
            assert set(datasets[key].dataset().tables) == table_names
            # column names
            for t in datasets[key].dataset().tables:
                assert set(datasets[key].dataset().table(t).df().columns) == col_names[t]
            # check row counts
            assert dict(datasets[key].dataset().row_counts().fetchall()) == rows_per_table


def get_pipeline(load_info: LoadInfo, output_dir: Path) -> dlt.Pipeline:
    """Return a Pipeline object for the given dataset and destination."""
    return dlt.pipeline(destination=filesystem(bucket_url=str(output_dir)), dataset_name=load_info.dataset_name)


@pytest.mark.parametrize("loader_file_format", LoaderFileFormatEnum.__members__.values())
def test_xml_ingest_pass_output_identical_across_chunk_dirs(
    run_xmltodict_pipeline: Any, sorted_reference_entries: list[str], loader_file_format: str
) -> None:
    """Pipeline output is identical across all three chunk dirs and matches the reference."""
    load_info_and_dir = {
        key: _run_and_read(run_xmltodict_pipeline, key, loader_file_format=loader_file_format) for key in CHUNK_DIRS
    }

    assert_datasets_equal(load_info_and_dir, sorted_reference_entries, loader_file_format)


@pytest.mark.parametrize("loader_file_format", LoaderFileFormatEnum.__members__.values())
def test_xml_ingest_pass_output_identical_across_buffer_sizes(
    run_xmltodict_pipeline: Any, sorted_reference_entries: list[str], loader_file_format: str
) -> None:
    """List buffer size does not affect pipeline output for any chunk dir."""
    for chunk_dir in CHUNK_DIRS:
        load_info_and_dir = {
            str(buffer_size): _run_and_read(
                run_xmltodict_pipeline, chunk_dir, buffer_size=buffer_size, loader_file_format=loader_file_format
            )
            for buffer_size in BUFFER_SIZES
        }
        assert_datasets_equal(load_info_and_dir, sorted_reference_entries, loader_file_format)


@pytest.mark.parametrize(
    ("extract_workers", "normalize_workers", "load_workers"),
    WORKER_CONFIGS,
    ids=[f"extract_{e}-normalize_{n}-load_{l}" for e, n, l in WORKER_CONFIGS],  # noqa: E741
)
def test_xml_ingest_pass_no_data_lost_across_worker_configs(
    run_xmltodict_pipeline: Any,
    sorted_reference_entries: list[str],
    extract_workers: int,
    normalize_workers: int,
    load_workers: int,
) -> None:
    """No data is lost regardless of how the job is split across extract/normalize/load workers."""
    load_info_and_dir = {}
    with dlt.config.values(
        {
            "extract.workers": extract_workers,
            "normalize.workers": normalize_workers,
            "load.workers": load_workers,
        }
    ):
        load_info_and_dir["chunk_5_el"] = _run_and_read(run_xmltodict_pipeline, "chunk_5_el")

    assert_datasets_equal(load_info_and_dir, sorted_reference_entries)


@pytest.mark.parametrize("loader_file_format", LoaderFileFormatEnum.__members__.values())
def test_xml_ingest_pass_reconstructed_matches_duckdb_reference(
    run_xmltodict_pipeline: Any, reference_dataset: duckdb.DuckDBPyConnection, loader_file_format: str
) -> None:
    """Reconstructed pipeline entries equal the entries loaded from the DuckDB reference."""
    (load_info, output_dir) = _run_and_read(
        run_xmltodict_pipeline, "chunk_100_el", loader_file_format=loader_file_format
    )
    tables = read_pipeline_tables(output_dir / load_info.dataset_name, loader_file_format)
    reconstructed = reconstruct_entries(tables)
    from_duckdb = reference_entries_from_duckdb(reference_dataset)
    assert len(from_duckdb) == EXPECTED_ENTRY_COUNT
    assert _sorted_json(reconstructed) == _sorted_json(from_duckdb)
