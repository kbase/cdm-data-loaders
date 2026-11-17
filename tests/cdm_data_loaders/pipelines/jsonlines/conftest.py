"""Shared fixtures for ``tests/cdm_data_loaders/pipelines/jsonlines``."""

from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest
from pydantic import BaseModel

from cdm_data_loaders.pipelines.jsonlines.settings import (
    JsonlExtractSettings,
    JsonlJsonschemaIngestSettings,
    JsonlPydanticIngestSettings,
)
from tests._support.cli_assertions import make_cts_settings
from tests._support.fixtures_jsonlines import make_scenario_input_dir
from tests._support.fixtures_jsonschema import make_schema_settings_factory


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


@pytest.fixture
def extract_settings_factory(
    tmp_path: Path,
    dlt_destination_config: str,
) -> Callable[..., JsonlExtractSettings]:
    """Return a factory building ``JsonlExtractSettings`` with overridable fields.

    :param tmp_path: Per-test temporary directory.
    :type tmp_path: Path
    :param dlt_destination_config: Active destination name fixture.
    :type dlt_destination_config: str
    :return: Settings factory callable.
    :rtype: Callable[..., JsonlExtractSettings]
    """

    def _factory(**overrides: object) -> JsonlExtractSettings:
        defaults: dict[str, Any] = {
            "dataset_name": "test_dataset",
            "dlt_dev_mode": True,
            "table_name": "widget",
            "use_destination": dlt_destination_config,
        }
        defaults.update(overrides)
        return make_cts_settings(JsonlExtractSettings, tmp_path, **defaults)

    return _factory
