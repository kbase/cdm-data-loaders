"""Shared fixtures for the generic reference pipeline tests."""

import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest
from dlt.common.pipeline import LoadInfo

from tests.cdm_data_loaders.pipelines.framework import PipelineTestRunner
from tests.cdm_data_loaders.pipelines.jsonlines.integration.jsonlines_reference_helpers import (
    canonicalize_entry,
)
from tests.cdm_data_loaders.pipelines.reference.pipeline import (
    ReferencePipelineSettings,
    run_reference_pipeline,
)

type PipelineRunner = Callable[..., tuple[LoadInfo | None, Path]]


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
