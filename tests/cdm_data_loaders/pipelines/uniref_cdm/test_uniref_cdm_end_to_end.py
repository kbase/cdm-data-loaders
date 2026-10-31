"""Tests for the UniRef DLT pipeline.

The fixture file contains three clusters; exact per-table counts below are
derived from that fixture. Every test runs once per valid UniRef variant.
"""

from pathlib import Path
from typing import Any

import pytest
from frozendict import frozendict

from cdm_data_loaders.pipelines.uniref_cdm import (
    PIPELINE_NAME,
    UNIREF_VARIANTS,
    UnirefSettings,
    parse_uniref,
)
from tests.cdm_data_loaders.pipelines.framework import PipelineTestRunner
from tests.cdm_data_loaders.pipelines.helpers import (
    assert_destination_table_counts,
    assert_duckdb_table_counts,
    assert_pipeline_report_written,
    assert_run_ok,
    duckdb_pipeline,
    fetch_all_duckdb,
)
from tests.conftest import TEST_DATA_DIR
from tests.helpers import make_cts_settings

TEST_DEFAULT_UNIREF_VARIANT = "50"

UNIREF_FIXTURE_DIR = TEST_DATA_DIR / "uniprot" / "uniref" / "integration"
UNIREF_FIXTURE_FILE = "uniref_chunk_00001.xml"
UNIREF_CLUSTER_COUNT = 3

EXPECTED_ROW_COUNTS = frozendict(
    {
        "entity": UNIREF_CLUSTER_COUNT,
        "cluster": UNIREF_CLUSTER_COUNT,
        "clustermember": 4,
        "entity_x_source_file": UNIREF_CLUSTER_COUNT,
    }
)


# Integration test for the UniRef pipeline
# Data output goes to a local DuckDB destination.
@pytest.fixture(params=UNIREF_VARIANTS)
def duckdb_uniref_settings(tmp_path: Path, request: pytest.FixtureRequest) -> UnirefSettings:
    """Provide UnirefSettings pointing at the real UniRef XML fixtures, one per variant."""
    return make_cts_settings(
        UnirefSettings,
        tmp_path,
        variant=request.param,
        input_dir=UNIREF_FIXTURE_DIR,
        use_output_dir_for_pipeline_metadata=True,
    )


def _run_uniref_duckdb_pipeline(settings: UnirefSettings, tmp_path: Path, name: str) -> tuple[Any, Any]:
    """Run ``parse_uniref`` through a real DuckDB pipeline and return (pipeline, load_info).

    dlt writes the duckdb destination file into the working directory and appends
    on re-runs, so each test gets its own database file and pipeline name under
    tmp_path to stay isolated from other tests and parametrized cases.
    """
    pipeline = duckdb_pipeline(tmp_path, f"test_uniref_pipeline_{name}", "test_uniref")
    load_info = pipeline.run(parse_uniref(settings))
    return pipeline, load_info


def test_integration_uniref_pipeline_loads_into_duckdb(
    duckdb_uniref_settings: UnirefSettings,
    tmp_path: Path,
) -> None:
    """Full integration test: parse the real fixture files into a DuckDB destination.

    Confirms that the pipeline runs end-to-end without failed jobs and that the
    expected CDM tables are populated with the exact expected row counts.
    """
    pipeline, load_info = _run_uniref_duckdb_pipeline(duckdb_uniref_settings, tmp_path, "loads")

    assert_run_ok(load_info)
    assert_duckdb_table_counts(pipeline, EXPECTED_ROW_COUNTS)


def test_integration_uniref_pipeline_entity_and_cluster_content(
    duckdb_uniref_settings: UnirefSettings,
    tmp_path: Path,
) -> None:
    """Entity rows are UniRef clusters and every cluster carries the injected protocol label."""
    pipeline, load_info = _run_uniref_duckdb_pipeline(duckdb_uniref_settings, tmp_path, "content")

    assert_run_ok(load_info)

    # every entity must have a "uniref:"-prefixed entity_id and be a Cluster.
    rows = fetch_all_duckdb(pipeline, "SELECT entity_id, entity_type FROM entity")
    assert len(rows) == UNIREF_CLUSTER_COUNT
    assert all(entity_id.startswith("uniref:") for entity_id, _ in rows)
    assert all(entity_type == "Cluster" for _, entity_type in rows)

    # every cluster must carry the injected "UniRef <variant>" protocol label
    protocols = {row[0] for row in fetch_all_duckdb(pipeline, "SELECT DISTINCT protocol FROM cluster")}
    assert protocols == {f"UniRef {duckdb_uniref_settings.variant}"}

    # entity_x_source_file must reference exactly the fixture file we loaded from
    source_files = {
        row[0] for row in fetch_all_duckdb(pipeline, "SELECT DISTINCT source_file FROM entity_x_source_file")
    }
    assert source_files == {str(UNIREF_FIXTURE_DIR.resolve() / UNIREF_FIXTURE_FILE)}


def test_integration_cli_uniref_pipeline_output_validated(
    duckdb_uniref_settings: UnirefSettings,
    pipeline_runner: PipelineTestRunner,
) -> None:
    """Exercise the UniRef pipeline end-to-end across core, cli, and docker execution modes."""
    load_info = assert_run_ok(pipeline_runner.run(PIPELINE_NAME, settings=duckdb_uniref_settings))
    assert load_info.pipeline.pipeline_name == PIPELINE_NAME
    assert load_info.dataset_name == f"uniref_{duckdb_uniref_settings.variant}"
    assert_pipeline_report_written(
        Path(duckdb_uniref_settings.output_dir),
        load_info,
        metadata_moved=True,
        ingested_files=[UNIREF_FIXTURE_FILE],
    )

    dataset_dir = Path(duckdb_uniref_settings.output_dir) / load_info.dataset_name
    assert_destination_table_counts(dataset_dir, EXPECTED_ROW_COUNTS)
