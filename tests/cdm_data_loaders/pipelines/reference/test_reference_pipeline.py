"""Reference tests for pipeline configurations and processing elements using the generic reference pipeline."""

import itertools
import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import dlt
import duckdb
import pytest

from tests.cdm_data_loaders.pipelines.helpers import (
    BUFFER_SIZES,
    LOADER_FILE_FORMATS,
    WORKER_CONFIGS,
    assert_dataset_matches_reference,
    read_pipeline_tables,
    run_and_compare_variations,
    run_and_read,
)
from tests.cdm_data_loaders.pipelines.jsonlines.integration.jsonlines_reference_helpers import (
    EXPECTED_ENTRY_COUNT,
    canonicalize_entry,
    reconstructed_entries,
    reference_entries_from_duckdb,
)

CHUNK_DIRS = ("chunk_4", "chunk_10", "chunk_52")
PARALLEL_CONFIGS = list(itertools.product([True, False], repeat=3))


def test_reference_jsonl_pass_matches_source(
    jsonl_reference_entities: list[dict[str, Any]],
    reference_jsonl: Path,
) -> None:
    """The reference JSONL has all expected entries in the expected structure."""
    assert len(jsonl_reference_entities) == EXPECTED_ENTRY_COUNT
    with reference_jsonl.open(encoding="utf-8") as fh:
        line_count = sum(1 for line in fh if line.strip())
    assert line_count == EXPECTED_ENTRY_COUNT
    for tag in ("accession", "current_accession", "paired_accession", "source_database", "organism"):
        assert tag in jsonl_reference_entities[0]


def test_reference_pass_dataset_has_nested_structures(
    reference_dataset: duckdb.DuckDBPyConnection,
) -> None:
    """The DuckDB reference stores nested JSON as STRUCT/LIST types, not JSON text columns."""
    column_types = dict(
        reference_dataset.execute(
            "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'reference'"
        ).fetchall()
    )

    organism_type = column_types["organism"]
    assert organism_type.startswith("STRUCT")
    assembly_stats_type = column_types["assembly_stats"]
    assert assembly_stats_type.startswith("STRUCT")
    additional_submitters_type = column_types["additional_submitters"]
    assert additional_submitters_type.startswith("STRUCT(")
    assert additional_submitters_type.endswith("[]")

    count = reference_dataset.execute("SELECT COUNT(*) FROM reference").fetchone()
    assert count == (EXPECTED_ENTRY_COUNT,)


@pytest.mark.parametrize("loader_file_format", LOADER_FILE_FORMATS, ids=str)
def test_reference_pass_output_identical_across_chunk_dirs(
    run_reference_pipeline_fxt: Callable[..., Any],
    sorted_reference_entities: list[str],
    loader_file_format: str,
) -> None:
    """Pipeline output is identical across all three chunk dirs and matches the reference."""
    run_and_compare_variations(
        run_reference_pipeline_fxt,
        {f"{key}_{loader_file_format}": (key, {"loader_file_format": loader_file_format}) for key in CHUNK_DIRS},
        sorted_reference_json=sorted_reference_entities,
        output_format=loader_file_format,
        reconstruct=reconstructed_entries,
        canonicalize=canonicalize_entry,
    )


def test_reference_pass_diff_format_same_results(
    run_reference_pipeline_fxt: Callable[..., Any],
    sorted_reference_entities: list[str],
) -> None:
    """Ensure that the save format does not affect the output."""
    load_info_and_dir = {
        str(output_format): run_and_read(
            run_reference_pipeline_fxt,
            "chunk_10",
            loader_file_format=output_format,
        )
        for output_format in LOADER_FILE_FORMATS
    }
    for output_format, result in load_info_and_dir.items():
        assert_dataset_matches_reference(
            {output_format: result},
            sorted_reference_entities,
            output_format,
            reconstruct=reconstructed_entries,
            canonicalize=canonicalize_entry,
        )


@pytest.mark.parametrize("loader_file_format", LOADER_FILE_FORMATS, ids=str)
def test_reference_pass_output_identical_across_buffer_sizes(
    run_reference_pipeline_fxt: Callable[..., Any],
    sorted_reference_entities: list[str],
    loader_file_format: str,
) -> None:
    """Buffer size does not affect pipeline output for any chunk dir."""
    for chunk_dir in CHUNK_DIRS:
        run_and_compare_variations(
            run_reference_pipeline_fxt,
            {
                f"{buffer_size}_{loader_file_format}": (
                    chunk_dir,
                    {"buffer_size": buffer_size, "loader_file_format": loader_file_format},
                )
                for buffer_size in BUFFER_SIZES
            },
            sorted_reference_json=sorted_reference_entities,
            output_format=loader_file_format,
            reconstruct=reconstructed_entries,
            canonicalize=canonicalize_entry,
        )


@pytest.mark.parametrize(
    ("extract_workers", "normalize_workers", "load_workers"),
    WORKER_CONFIGS,
    ids=[f"extract_{extract}-normalize_{normalize}-load_{load}" for extract, normalize, load in WORKER_CONFIGS],
)
@pytest.mark.parametrize("loader_file_format", LOADER_FILE_FORMATS, ids=str)
def test_reference_pass_no_data_lost_across_worker_configs(
    *,
    run_reference_pipeline_fxt: Callable[..., Any],
    sorted_reference_entities: list[str],
    extract_workers: int,
    normalize_workers: int,
    load_workers: int,
    loader_file_format: str,
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
        load_info_and_dir["chunk_4"] = run_and_read(
            run_reference_pipeline_fxt,
            "chunk_4",
            loader_file_format=loader_file_format,
        )

    assert_dataset_matches_reference(
        load_info_and_dir,
        sorted_reference_entities,
        output_format=loader_file_format,
        reconstruct=reconstructed_entries,
        canonicalize=canonicalize_entry,
    )


@pytest.mark.parametrize("loader_file_format", LOADER_FILE_FORMATS, ids=str)
def test_reference_pass_reconstructed_matches_duckdb_reference(
    run_reference_pipeline_fxt: Callable[..., Any],
    reference_dataset: duckdb.DuckDBPyConnection,
    loader_file_format: str,
) -> None:
    """Reconstructed pipeline entries equal the entries loaded from the DuckDB reference."""
    (load_info, output_dir) = run_and_read(
        run_reference_pipeline_fxt,
        "chunk_52",
        loader_file_format=loader_file_format,
    )
    tables = read_pipeline_tables(output_dir / load_info.dataset_name, loader_file_format)
    reconstructed = reconstructed_entries(tables)
    from_duckdb = reference_entries_from_duckdb(reference_dataset)
    assert len(from_duckdb) == EXPECTED_ENTRY_COUNT
    from_duckdb_canonical = sorted(
        json.dumps(canonicalize_entry(entry), sort_keys=True, default=str) for entry in from_duckdb
    )
    from_pipeline = sorted(
        json.dumps(canonicalize_entry(entry), sort_keys=True, default=str) for entry in reconstructed
    )
    assert from_pipeline == from_duckdb_canonical


@pytest.mark.parametrize("loader_file_format", LOADER_FILE_FORMATS, ids=str)
def test_reference_pass_gzip_input_matches_plain_text(
    *,
    run_reference_pipeline_fxt: Callable[..., Any],
    tmp_path: Path,
    write_gzip_jsonl_file: Callable[[Path, str, list[str]], Path],
    jsonl_reference_entities: list[dict[str, Any]],
    sorted_reference_entities: list[str],
    loader_file_format: str,
) -> None:
    """A gzip-compressed input file produces the same rows as the plain-text reference."""
    input_dir = tmp_path / "input_gzip"
    table_dir = input_dir / "dataset"
    write_gzip_jsonl_file(table_dir, "compressed.jsonl.gz", [json.dumps(entity) for entity in jsonl_reference_entities])

    result = run_and_read(
        run_reference_pipeline_fxt,
        "chunk_52",
        input_dir=str(input_dir),
        loader_file_format=loader_file_format,
    )
    assert_dataset_matches_reference(
        {"gzip": result},
        sorted_reference_entities,
        loader_file_format,
        reconstruct=reconstructed_entries,
        canonicalize=canonicalize_entry,
    )


@pytest.mark.parametrize("preserve_table_nesting", [False, True], ids=["flattened", "nested"])
@pytest.mark.parametrize("loader_file_format", LOADER_FILE_FORMATS, ids=str)
def test_reference_pass_nesting_matches_pipeline_contract(
    run_reference_pipeline_fxt: Callable[..., Any],
    preserve_table_nesting: bool,
    loader_file_format: str,
    sorted_reference_entities: list[str],
) -> None:
    """Default extraction flattens nested fields; preserve_table_nesting keeps them nested without changing content."""
    result = run_and_read(
        run_reference_pipeline_fxt,
        "chunk_10",
        loader_file_format=loader_file_format,
        preserve_table_nesting=preserve_table_nesting,
    )
    load_info, output_dir = result
    tables = read_pipeline_tables(output_dir / load_info.dataset_name, loader_file_format)
    data_tables = {name for name in tables if not name.startswith("_dlt")}
    columns = set().union(*(row.keys() for row in tables["dataset"]))
    if preserve_table_nesting:
        assert data_tables == {"dataset"}
        assert {"organism", "assembly_info", "additional_submitters"} <= columns
    else:
        assert {"dataset", "dataset__additional_submitters"} <= data_tables
        assert "organism__tax_id" in columns
    assert_dataset_matches_reference(
        {f"nesting_{preserve_table_nesting}": result},
        sorted_reference_entities,
        loader_file_format,
        reconstruct=reconstructed_entries,
        canonicalize=canonicalize_entry,
    )


@pytest.mark.parametrize(
    ("parallel_step_1", "parallel_step_2", "parallel_step_3"),
    PARALLEL_CONFIGS,
    ids=[f"s1_{s1}-s2_{s2}-s3_{s3}" for s1, s2, s3 in PARALLEL_CONFIGS],
)
def test_reference_pass_parallel_step_combinations(
    *,
    run_reference_pipeline_fxt: Callable[..., Any],
    sorted_reference_entities: list[str],
    parallel_step_1: bool,
    parallel_step_2: bool,
    parallel_step_3: bool,
) -> None:
    """Every combination of parallelized=[True|False] across the three transform steps preserves all records."""
    result = run_and_read(
        run_reference_pipeline_fxt,
        "chunk_10",
        parallel_step_1=parallel_step_1,
        parallel_step_2=parallel_step_2,
        parallel_step_3=parallel_step_3,
    )
    assert_dataset_matches_reference(
        {f"{parallel_step_1}_{parallel_step_2}_{parallel_step_3}": result},
        sorted_reference_entities,
        "parquet",
        reconstruct=reconstructed_entries,
        canonicalize=canonicalize_entry,
    )
