"""Shared fixtures for ``tests/e2e/pipelines/jsonlines``."""

import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest
from dlt.common.pipeline import LoadInfo
from pydantic import BaseModel

from cdm_data_loaders.pipelines.jsonlines.extract import run_jsonlines_pipeline
from cdm_data_loaders.pipelines.jsonlines.jsonschema import (
    run_jsonlines_pipeline as run_jsonschema_pipeline,
)
from cdm_data_loaders.pipelines.jsonlines.pydantic import (
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
from tests.data.ncbi_datasets.dataset_report_models import ENTITY_MODELS
from tests.data.ncbi_datasets.dataset_report_schemas import ENTITY_SCHEMAS
from tests.e2e.pipelines.jsonlines.jsonlines_reference_helpers import canonicalize_entry
from tests.helpers.cli_assertions import make_cts_settings
from tests.helpers.fixtures_jsonlines import (
    Widget,
    make_entity_models_factory,
    make_scenario_input_dir,
)
from tests.helpers.fixtures_jsonschema import (
    build_widget_schema,
    make_entity_schemas_factory,
    make_schema_settings_factory,
)

type PipelineRunner = Callable[..., tuple[LoadInfo | None, Path]]
type PrepareReference = Callable[[dict[str, Any]], dict[str, Any]]


@pytest.fixture
def widget_model() -> type[BaseModel]:
    """Return the sample ``Widget`` Pydantic entity model.

    :return: ``Widget`` model class.
    :rtype: type[BaseModel]
    """
    return Widget


@pytest.fixture
def entity_models_factory(
    monkeypatch: pytest.MonkeyPatch,
) -> Callable[[dict[str, type[BaseModel]]], str]:
    """Return a factory registering synthetic ``ENTITY_MODELS`` modules in ``sys.modules``.

    :param monkeypatch: Active pytest monkeypatch fixture.
    :type monkeypatch: pytest.MonkeyPatch
    :return: Module-registration callable.
    :rtype: Callable[[dict[str, type[BaseModel]]], str]
    """
    return make_entity_models_factory(monkeypatch)


@pytest.fixture
def entity_models(
    entity_models_factory: Callable[[dict[str, type[BaseModel]]], str],
    widget_model: type[BaseModel],
) -> str:
    """Register a single-entity ``ENTITY_MODELS = {'widget': Widget}`` module.

    :param entity_models_factory: Module-registration factory fixture.
    :type entity_models_factory: Callable[[dict[str, type[BaseModel]]], str]
    :param widget_model: Sample ``Widget`` model class fixture.
    :type widget_model: type[BaseModel]
    :return: Registered module name.
    :rtype: str
    """
    return entity_models_factory({"widget": widget_model})


@pytest.fixture
def scenario_input_dir(test_data_dir: Path) -> Callable[[str], str]:
    """Return a resolver mapping JSONL scenario names to directories under ``tests/data/jsonlines``.

    :param test_data_dir: Root ``tests/data`` directory fixture.
    :type test_data_dir: Path
    :return: Scenario path resolver callable.
    :rtype: Callable[[str], str]
    """
    return make_scenario_input_dir(test_data_dir)


@pytest.fixture
def widget_schema() -> dict[str, Any]:
    """Return a fresh ``widget`` JSON Schema dictionary.

    :return: JSON Schema mapping.
    :rtype: dict[str, Any]
    """
    return build_widget_schema()


@pytest.fixture
def entity_schemas_factory(monkeypatch: pytest.MonkeyPatch) -> Callable[[object], str]:
    """Return a factory registering synthetic ``ENTITY_SCHEMAS`` modules in ``sys.modules``.

    :param monkeypatch: Active pytest monkeypatch fixture.
    :type monkeypatch: pytest.MonkeyPatch
    :return: Module-registration callable.
    :rtype: Callable[[object], str]
    """
    return make_entity_schemas_factory(monkeypatch)


@pytest.fixture
def entity_schemas(
    entity_schemas_factory: Callable[[object], str],
    widget_schema: dict[str, Any],
) -> str:
    """Register an ``ENTITY_SCHEMAS`` module with ``widget``, ``gadget``, and ``missing`` tables.

    :param entity_schemas_factory: Schema module-registration factory fixture.
    :type entity_schemas_factory: Callable[[object], str]
    :param widget_schema: Sample ``widget`` JSON Schema fixture.
    :type widget_schema: dict[str, Any]
    :return: Registered module name.
    :rtype: str
    """
    return entity_schemas_factory({"widget": widget_schema, "gadget": widget_schema, "missing": widget_schema})


@pytest.fixture
def schema_settings_factory(
    tmp_path: Path,
    dlt_destination_config: str,
    entity_schemas: str,
) -> Callable[..., JsonlJsonschemaIngestSettings]:
    """Return a factory building ``JsonlJsonschemaIngestSettings`` with fresh output directories.

    :param tmp_path: Per-test temporary directory.
    :type tmp_path: Path
    :param dlt_destination_config: Active destination name fixture.
    :type dlt_destination_config: str
    :param entity_schemas: Default ``ENTITY_SCHEMAS`` module path fixture.
    :type entity_schemas: str
    :return: Settings factory callable.
    :rtype: Callable[..., JsonlJsonschemaIngestSettings]
    """
    return make_schema_settings_factory(tmp_path, dlt_destination_config, entity_schemas)


@pytest.fixture
def settings_factory(
    tmp_path: Path,
    dlt_destination_config: str,
    entity_models: str,
) -> Callable[..., JsonlPydanticIngestSettings]:
    """Return a factory building ``JsonlPydanticIngestSettings`` with overridable fields.

    :param tmp_path: Per-test temporary directory.
    :type tmp_path: Path
    :param dlt_destination_config: Active destination name fixture.
    :type dlt_destination_config: str
    :param entity_models: Default ``ENTITY_MODELS`` module path fixture.
    :type entity_models: str
    :return: Settings factory callable.
    :rtype: Callable[..., JsonlPydanticIngestSettings]
    """

    def _factory(**overrides: object) -> JsonlPydanticIngestSettings:
        defaults: dict[str, Any] = {
            "dataset_name": "test_dataset",
            "dlt_dev_mode": False,
            "entity_models": entity_models,
            "use_destination": dlt_destination_config,
        }
        defaults.update(overrides)
        return make_cts_settings(JsonlPydanticIngestSettings, tmp_path, **defaults)

    return _factory


@pytest.fixture(params=[EXTRACT_PIPELINE_NAME, PYDANTIC_PIPELINE_NAME, JSONSCHEMA_PIPELINE_NAME], ids=str)
def pipeline_kind(request: pytest.FixtureRequest) -> str:
    """Return the active JSONL pipeline variant under test.

    :param request: Active pytest fixture request.
    :type request: pytest.FixtureRequest
    :return: Pipeline name string.
    :rtype: str
    """
    return request.param


@pytest.fixture
def run_reference_pipeline(
    pipeline_kind: str,
    reference_data_dir: Path,
    dlt_destination_config: str,
    isolated_run_factory: Callable[..., Callable[..., tuple[LoadInfo | None, Path]]],
) -> PipelineRunner:
    """Run the selected JSONL pipeline on a chunk dir with per-run isolated output directories.

    :param pipeline_kind: Selected JSONL pipeline name.
    :type pipeline_kind: str
    :param reference_data_dir: Root reference dataset directory.
    :type reference_data_dir: Path
    :param dlt_destination_config: Active destination name fixture.
    :type dlt_destination_config: str
    :param isolated_run_factory: Builder for per-run isolated pipeline factories.
    :type isolated_run_factory: Callable[..., Callable[..., tuple[LoadInfo | None, Path]]]
    :return: Chunk-running callable ``(chunk_dir, **overrides) -> (load_info, output_dir)``.
    :rtype: PipelineRunner
    """
    settings_cls, registry_kwarg, dataset_prefix, file_glob = {
        EXTRACT_PIPELINE_NAME: (JsonlExtractSettings, {"table_name": "dataset"}, "jsonl_extract_run", "**/*.jsonl*"),
        PYDANTIC_PIPELINE_NAME: (
            JsonlPydanticIngestSettings,
            {"entity_models": "tests.data.ncbi_datasets.dataset_report_models"},
            "jsonl_extract_validate_run",
            "*.jsonl*",
        ),
        JSONSCHEMA_PIPELINE_NAME: (
            JsonlJsonschemaIngestSettings,
            {"entity_schemas": "tests.data.ncbi_datasets.dataset_report_schemas"},
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

    def _factory(chunk_dir: str, **overrides: object) -> tuple[LoadInfo | None, Path]:
        kwargs = {"input_dir": str(reference_data_dir / chunk_dir), **overrides}
        return runner(**kwargs)

    return _factory


@pytest.fixture
def prepare_reference(pipeline_kind: str) -> PrepareReference:
    """Apply Pydantic defaults for model validation while passing other pipeline inputs through unchanged.

    :param pipeline_kind: Selected JSONL pipeline name.
    :type pipeline_kind: str
    :return: Entry-normalizing callable.
    :rtype: PrepareReference
    """

    def _prepare(entry: dict[str, Any]) -> dict[str, Any]:
        if pipeline_kind == PYDANTIC_PIPELINE_NAME:
            return ENTITY_MODELS["dataset"].model_validate(entry).model_dump(mode="json")
        return entry

    return _prepare


@pytest.fixture
def reference_registry_factory(
    pipeline_kind: str,
    entity_models_factory: Callable[[dict[str, type[BaseModel]]], str],
    entity_schemas_factory: Callable[[object], str],
) -> Callable[[list[str]], dict[str, str]]:
    """Register requested tables using the selected pipeline's assembly contract.

    :param pipeline_kind: Selected JSONL pipeline name.
    :type pipeline_kind: str
    :param entity_models_factory: Pydantic entity model registry factory.
    :type entity_models_factory: Callable[[dict[str, type[BaseModel]]], str]
    :param entity_schemas_factory: JSON Schema registry factory.
    :type entity_schemas_factory: Callable[[object], str]
    :return: Registry keyword-override builder accepting table names.
    :rtype: Callable[[list[str]], dict[str, str]]
    """

    def _factory(table_names: list[str]) -> dict[str, str]:
        if pipeline_kind == PYDANTIC_PIPELINE_NAME:
            return {"entity_models": entity_models_factory(dict.fromkeys(table_names, ENTITY_MODELS["dataset"]))}
        if pipeline_kind == JSONSCHEMA_PIPELINE_NAME:
            return {"entity_schemas": entity_schemas_factory(dict.fromkeys(table_names, ENTITY_SCHEMAS["dataset"]))}
        return {}

    return _factory


@pytest.fixture
def sorted_reference_entities(
    jsonl_reference_entities: list[dict[str, Any]],
    prepare_reference: PrepareReference,
) -> list[str]:
    """Return canonicalized reference entries serialized as sorted JSON strings.

    :param jsonl_reference_entities: Raw parsed JSONL reference entities.
    :type jsonl_reference_entities: list[dict[str, Any]]
    :param prepare_reference: Pipeline-specific reference preparation callable.
    :type prepare_reference: PrepareReference
    :return: Sorted canonical JSON strings.
    :rtype: list[str]
    """
    lines_as_json = [
        json.dumps(canonicalize_entry(prepare_reference(obj)), sort_keys=True, default=str)
        for obj in jsonl_reference_entities
    ]
    return sorted(lines_as_json)
