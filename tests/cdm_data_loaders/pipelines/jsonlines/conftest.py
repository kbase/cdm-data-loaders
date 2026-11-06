"""Shared fixtures for jsonlines pipeline tests."""

from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from cdm_data_loaders.pipelines.jsonlines.settings import JsonlExtractSettings, JsonlPydanticIngestSettings
from tests.helpers import make_cts_settings
from tests.jsonlines_fixtures import make_item

__all__ = ["extract_settings_factory", "make_item", "settings_factory"]


@pytest.fixture
def settings_factory(
    tmp_path: Path, dlt_destination_config: str, entity_models: str
) -> Callable[..., JsonlPydanticIngestSettings]:
    """Return a factory that builds a valid JsonlPydanticIngestSettings, with fields open to override."""

    def _factory(**overrides: Any) -> JsonlPydanticIngestSettings:  # noqa: ANN401
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
def extract_settings_factory(tmp_path: Path, dlt_destination_config: str) -> Callable[..., JsonlExtractSettings]:
    """Return a factory that builds a valid JsonlExtractSettings, with fields open to override."""

    def _factory(**overrides: Any) -> JsonlExtractSettings:  # noqa: ANN401
        defaults: dict[str, Any] = {
            "dataset_name": "test_dataset",
            "dlt_dev_mode": True,
            "table_name": "widget",
            "use_destination": dlt_destination_config,
        }
        defaults.update(overrides)
        return make_cts_settings(JsonlExtractSettings, tmp_path, **defaults)

    return _factory
