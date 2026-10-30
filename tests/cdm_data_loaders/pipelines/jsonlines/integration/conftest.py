"""Shared fixtures for jsonlines pipeline tests."""

import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest
from dlt.common.pipeline import LoadInfo
from pydantic import BaseModel

from cdm_data_loaders.pipelines.jsonlines.extract_jsonschema_validate_pipeline import (
    run_jsonlines_pipeline as run_jsonschema_pipeline,
)
from cdm_data_loaders.pipelines.jsonlines.extract_pipeline import run_jsonlines_pipeline
from cdm_data_loaders.pipelines.jsonlines.extract_pydantic_validate_pipeline import (
    run_jsonlines_with_validation_pipeline,
)
from cdm_data_loaders.pipelines.jsonlines.settings import (
    EXTRACT_PIPELINE_NAME,
    JSONSCHEMA_PIPELINE_NAME,
    PYDANTIC_PIPELINE_NAME,
    JsonlExtractSettings,
    JsonlJsonschemaIngestSettings,
    JsonlPydanticIngestSettings,
)
from tests.cdm_data_loaders.pipelines.jsonlines.integration.dataset_report_models import ENTITY_MODELS
from tests.cdm_data_loaders.pipelines.jsonlines.integration.dataset_report_schemas import ENTITY_SCHEMAS
from tests.cdm_data_loaders.pipelines.jsonlines.integration.jsonlines_reference_helpers import (
    canonicalize_entry,
)

type PipelineRunner = Callable[..., tuple[LoadInfo | None, Path]]
type PrepareReference = Callable[[dict[str, Any]], dict[str, Any]]


@pytest.fixture(params=[EXTRACT_PIPELINE_NAME, PYDANTIC_PIPELINE_NAME, JSONSCHEMA_PIPELINE_NAME], ids=str)
def pipeline_kind(request: pytest.FixtureRequest) -> str:
    """Run the same reference contract against all three JSONL pipelines."""
    return request.param


@pytest.fixture
def run_reference_pipeline(
    pipeline_kind: str,
    reference_data_dir: Path,
    dlt_destination_config: str,
    isolated_run_factory: Callable[..., Callable[..., tuple[LoadInfo | None, Path]]],
) -> PipelineRunner:
    """Run the selected JSONL pipeline on a chunk dir with per-run isolated output directories."""
    settings_cls, registry_kwarg, dataset_prefix, file_glob = {
        EXTRACT_PIPELINE_NAME: (JsonlExtractSettings, {"table_name": "dataset"}, "jsonl_extract_run", "**/*.jsonl*"),
        PYDANTIC_PIPELINE_NAME: (
            JsonlPydanticIngestSettings,
            {"entity_models_module": "tests.cdm_data_loaders.pipelines.jsonlines.integration.dataset_report_models"},
            "jsonl_extract_validate_run",
            "*.jsonl*",
        ),
        JSONSCHEMA_PIPELINE_NAME: (
            JsonlJsonschemaIngestSettings,
            {"schema_files_module": "tests.cdm_data_loaders.pipelines.jsonlines.integration.dataset_report_schemas"},
            "jsonschema_run",
            "*.jsonl*",
        ),
    }[pipeline_kind]
    run_fn = {
        EXTRACT_PIPELINE_NAME: run_jsonlines_pipeline,
        PYDANTIC_PIPELINE_NAME: run_jsonlines_with_validation_pipeline,
        JSONSCHEMA_PIPELINE_NAME: run_jsonschema_pipeline,
    }[pipeline_kind]
    base_kwargs: dict[str, Any] = {
        "buffer_size": 10,
        "dlt_dev_mode": False,
        "file_glob": file_glob,
        "settings_cls": settings_cls,
        "use_destination": dlt_destination_config,
        "use_output_dir_for_pipeline_metadata": True,
        **registry_kwarg,
    }
    runner = isolated_run_factory(dataset_prefix=dataset_prefix, run_fn=run_fn, base_kwargs=base_kwargs)

    def _factory(chunk_dir: str, **overrides: Any) -> tuple[LoadInfo | None, Path]:  # noqa: ANN401
        kwargs = {"input_dir": str(reference_data_dir / chunk_dir), **overrides}
        return runner(**kwargs)

    return _factory


@pytest.fixture
def prepare_reference(pipeline_kind: str) -> PrepareReference:
    """Apply Pydantic defaults only for model validation; other pipelines preserve input."""

    def _prepare(entry: dict[str, Any]) -> dict[str, Any]:
        if pipeline_kind == PYDANTIC_PIPELINE_NAME:
            return ENTITY_MODELS["dataset"].model_validate(entry).model_dump(mode="json")
        return entry

    return _prepare


@pytest.fixture
def reference_registry_factory(
    pipeline_kind: str,
    entity_models_module_factory: Callable[[dict[str, type[BaseModel]]], str],
    schema_module_factory: Callable[[object], str],
) -> Callable[[list[str]], dict[str, str]]:
    """Register the requested tables using the selected pipeline's assembly contract."""

    def _factory(table_names: list[str]) -> dict[str, str]:
        if pipeline_kind == PYDANTIC_PIPELINE_NAME:
            return {
                "entity_models_module": entity_models_module_factory(
                    dict.fromkeys(table_names, ENTITY_MODELS["dataset"])
                )
            }
        if pipeline_kind == JSONSCHEMA_PIPELINE_NAME:
            return {"schema_files_module": schema_module_factory(dict.fromkeys(table_names, ENTITY_SCHEMAS["dataset"]))}
        return {}

    return _factory


@pytest.fixture
def sorted_reference_entities(
    jsonl_reference_entities: list[dict[str, Any]], prepare_reference: PrepareReference
) -> list[str]:
    """Canonicalized entries as sorted JSON strings for order-insensitive comparison."""
    lines_as_json = [
        json.dumps(canonicalize_entry(prepare_reference(obj)), sort_keys=True, default=str)
        for obj in jsonl_reference_entities
    ]
    return sorted(lines_as_json)
