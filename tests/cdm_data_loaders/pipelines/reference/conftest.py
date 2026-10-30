"""Shared fixtures for the generic reference pipeline tests."""

import json
from collections.abc import Callable, Generator
from pathlib import Path
from typing import Any

import duckdb
import pytest
from dlt.common.pipeline import LoadInfo

from tests.cdm_data_loaders.pipelines.framework import PipelineTestRunner
from tests.cdm_data_loaders.pipelines.helpers import duckdb_reference_from_jsonl, read_jsonl_file
from tests.cdm_data_loaders.pipelines.jsonlines.integration.jsonlines_reference_helpers import (
    canonicalize_entry,
)
from tests.cdm_data_loaders.pipelines.reference.pipeline import (
    ReferencePipelineSettings,
    run_reference_pipeline,
)
from tests.conftest import TEST_DATA_DIR

type PipelineRunner = Callable[..., tuple[LoadInfo | None, Path]]


@pytest.fixture
def reference_data_dir() -> Path:
    """Return the directory containing all reference chunk configurations."""
    return TEST_DATA_DIR / "ncbi_rest_api" / "dataset_report"


@pytest.fixture(scope="session")
def reference_jsonl() -> Path:
    """Path to the reference JSONL file."""
    return TEST_DATA_DIR / "ncbi_rest_api" / "dataset_report" / "chunk_52" / "dataset" / "dataset_reports_52.jsonl"


@pytest.fixture(scope="session")
def jsonl_reference_entities(reference_jsonl: Path) -> list[dict[str, Any]]:
    """Parsed JSONL file."""
    return read_jsonl_file(reference_jsonl)


@pytest.fixture(scope="session")
def reference_dataset(reference_jsonl: Path) -> Generator[duckdb.DuckDBPyConnection]:
    """Load the reference JSONL into an in-memory DuckDB connection with nested structures."""
    with duckdb_reference_from_jsonl(reference_jsonl) as conn:
        yield conn


@pytest.fixture(scope="session")
def sorted_reference_entities(jsonl_reference_entities: list[dict[str, Any]]) -> list[str]:
    """Canonicalized entries as sorted JSON strings for order-insensitive comparison."""
    return sorted(json.dumps(canonicalize_entry(obj), sort_keys=True, default=str) for obj in jsonl_reference_entities)


@pytest.fixture
def run_reference_pipeline_fxt(
    reference_data_dir: Path,
    dlt_destination_config: str,
    isolated_run_factory: Callable[..., Callable[..., tuple[LoadInfo | None, Path]]],
    pipeline_runner: PipelineTestRunner,
) -> PipelineRunner:
    """Run the reference pipeline on a chunk dir with per-run isolated output directories."""
    base_kwargs: dict[str, Any] = {
        "buffer_size": 10,
        "dlt_dev_mode": False,
        "file_glob": "**/*.jsonl*",
        "settings_cls": ReferencePipelineSettings,
        "table_name": "dataset",
        "use_destination": dlt_destination_config,
        "use_output_dir_for_pipeline_metadata": True,
    }
    runner = isolated_run_factory(
        dataset_prefix="reference_pipeline_run",
        run_fn=run_reference_pipeline,
        base_kwargs=base_kwargs,
        pipeline_runner=pipeline_runner,
    )

    def _factory(chunk_dir: str, **overrides: Any) -> tuple[LoadInfo | None, Path]:  # noqa: ANN401
        kwargs = {"input_dir": str(reference_data_dir / chunk_dir), **overrides}
        return runner(**kwargs)

    return _factory
