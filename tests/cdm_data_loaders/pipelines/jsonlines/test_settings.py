"""Tests for JsonlPydanticIngestSettings field validation and defaults."""

from collections.abc import Callable

import pytest
from pydantic import ValidationError

from cdm_data_loaders.pipelines.jsonlines.settings import JsonlPydanticIngestSettings


def test_jsonlines_settings_pass_defaults(settings_factory: Callable[..., JsonlPydanticIngestSettings]) -> None:
    """file_glob defaults to '*.jsonl*'."""
    settings = settings_factory()
    assert settings.file_glob == "*.jsonl*"


def test_jsonlines_settings_fail_missing_entity_models_module(
    settings_factory: Callable[..., JsonlPydanticIngestSettings],
) -> None:
    """Omitting entity_models_module raises ValidationError."""
    with pytest.raises(ValidationError):
        settings_factory(entity_models_module=None)


def test_jsonlines_settings_pass_custom_file_glob(
    settings_factory: Callable[..., JsonlPydanticIngestSettings],
) -> None:
    """file_glob accepts a custom glob pattern."""
    assert settings_factory(file_glob="*.txt").file_glob == "*.txt"
