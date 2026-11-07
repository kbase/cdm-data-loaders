"""End-to-end tests for the UniRef CDM DLT pipelines.

The fixture file contains three clusters; exact per-table counts below are
derived from that fixture. Every test runs once per UniRef CDM variant.
"""

from pathlib import Path
from typing import Any

import pytest
from frozendict import frozendict

from cdm_data_loaders.pipelines.uniref_cdm import UniRefCdmSettings
from cdm_data_loaders.readers.xml import process_xml_file_to_pyarrow
from tests.cdm_data_loaders.pipelines.framework import PipelineTestRunner
from tests.cdm_data_loaders.pipelines.helpers import (
    assert_destination_table_counts,
    assert_duckdb_table_counts,
    assert_pipeline_report_written,
    assert_run_ok,
    duckdb_pipeline,
    fetch_all_duckdb,
)
from tests.cdm_data_loaders.pipelines.uniref_cdm.conftest import UNIREF_CDM_VARIANTS
from tests.conftest import TEST_DATA_DIR
from tests.helpers import make_cts_settings

UNIREF_FIXTURE_DIR = TEST_DATA_DIR / "uniprot" / "uniref" / "integration"
UNIREF_FIXTURE_FILE = "uniref_chunk_00001.xml"
UNIREF_CLUSTER_COUNT = 3

EXPECTED_ROW_COUNTS = frozendict(
    {
        "entity": UNIREF_CLUSTER_COUNT,
        "cluster": UNIREF_CLUSTER_COUNT,
        "clustermember": 4,
    }
)


@pytest.fixture(params=UNIREF_CDM_VARIANTS, ids=[name for _, _, name, _ in UNIREF_CDM_VARIANTS])
def duckdb_uniref_variant(
    tmp_path: Path,
    request: pytest.FixtureRequest,
) -> tuple[UniRefCdmSettings, str, str]:
    """Provide (UniRefCdmSettings, cli_name, expected_protocol) pointing at the real UniRef XML fixtures."""
    settings_cls, _, cli_name, protocol = request.param
    settings = make_cts_settings(
        settings_cls,
        tmp_path,
        input_dir=UNIREF_FIXTURE_DIR,
        use_output_dir_for_pipeline_metadata=True,
    )
    return settings, cli_name, protocol


def _run_uniref_duckdb_pipeline(settings: UniRefCdmSettings, tmp_path: Path, name: str) -> tuple[Any, Any]:
    """Run ``process_xml_file_to_pyarrow`` through a real DuckDB pipeline and return (pipeline, load_info)."""
    pipeline = duckdb_pipeline(tmp_path, f"test_uniref_pipeline_{name}", "test_uniref")
    fixture_path = UNIREF_FIXTURE_DIR / UNIREF_FIXTURE_FILE
    load_info = pipeline.run(process_xml_file_to_pyarrow(fixture_path, settings))
    return pipeline, load_info


def test_integration_uniref_pipeline_loads_into_duckdb(
    duckdb_uniref_variant: tuple[UniRefCdmSettings, str, str],
    tmp_path: Path,
) -> None:
    """Full integration test: parse the real fixture files into a DuckDB destination.

    Confirms that the pipeline runs end-to-end without failed jobs and that the
    expected CDM tables are populated with the exact expected row counts.
    """
    settings, cli_name, _ = duckdb_uniref_variant
    pipeline, load_info = _run_uniref_duckdb_pipeline(settings, tmp_path, f"loads_{cli_name}")

    assert_run_ok(load_info)
    assert_duckdb_table_counts(pipeline, EXPECTED_ROW_COUNTS)


def test_integration_uniref_pipeline_entity_and_cluster_content(
    duckdb_uniref_variant: tuple[UniRefCdmSettings, str, str],
    tmp_path: Path,
) -> None:
    """Entity rows are UniRef clusters and every cluster carries the injected protocol label."""
    settings, cli_name, expected_protocol = duckdb_uniref_variant
    pipeline, load_info = _run_uniref_duckdb_pipeline(settings, tmp_path, f"content_{cli_name}")

    assert_run_ok(load_info)

    # every entity must have a "uniref:"-prefixed entity_id and be a Cluster.
    rows = fetch_all_duckdb(pipeline, "SELECT entity_id, entity_type FROM entity")
    assert len(rows) == UNIREF_CLUSTER_COUNT
    assert all(entity_id.startswith("uniref:") for entity_id, _ in rows)
    assert all(entity_type == "Cluster" for _, entity_type in rows)

    # every cluster must carry the injected "UniRef <variant>" protocol label
    protocols = {row[0] for row in fetch_all_duckdb(pipeline, "SELECT DISTINCT protocol FROM cluster")}
    assert protocols == {expected_protocol}


def test_integration_cli_uniref_pipeline_output_validated(
    duckdb_uniref_variant: tuple[UniRefCdmSettings, str, str],
    pipeline_runner: PipelineTestRunner,
) -> None:
    """Exercise each UniRef CDM pipeline end-to-end across core, cli, and docker execution modes."""
    settings, cli_name, _ = duckdb_uniref_variant
    load_info = assert_run_ok(pipeline_runner.run(cli_name, settings=settings))
    assert load_info.pipeline.pipeline_name == cli_name
    assert load_info.dataset_name == cli_name
    assert_pipeline_report_written(
        Path(settings.output_dir),
        load_info,
        metadata_moved=True,
        ingested_files=[UNIREF_FIXTURE_FILE],
    )

    dataset_dir = Path(settings.output_dir) / load_info.dataset_name
    assert_destination_table_counts(dataset_dir, EXPECTED_ROW_COUNTS)
