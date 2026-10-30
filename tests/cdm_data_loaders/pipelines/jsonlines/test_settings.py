"""Tests for JsonlPydanticIngestSettings field validation and defaults."""

from collections.abc import Callable

import pytest
from pydantic import ValidationError
from pydantic_settings import CliApp

from cdm_data_loaders.pipelines.jsonlines.settings import (
    JsonlJsonschemaIngestSettings,
    JsonlPydanticIngestSettings,
)
from tests.helpers import settings_to_argv


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        pytest.param(None, None, id="none_passthrough"),
        pytest.param(["widget", "sample"], ["widget", "sample"], id="list_passthrough_unchanged"),
        pytest.param("widget,sample", ["widget", "sample"], id="comma_separated_string_split"),
        pytest.param(" widget ,  sample ", ["widget", "sample"], id="comma_separated_string_with_whitespace"),
        pytest.param("widget,,sample", ["widget", "sample"], id="empty_segments_dropped"),
        pytest.param("", None, id="empty_string_becomes_none"),
        pytest.param(",, ,", None, id="all_empty_segments_becomes_none"),
    ],
)
def test_split_table_names_pass_various_inputs(raw: str | list[str] | None, expected: list[str] | None) -> None:
    """split_table_names turns a comma-separated string into a list. Lists and None pass through unchanged."""
    assert JsonlPydanticIngestSettings.split_table_names(raw) == expected


def test_jsonlines_settings_pass_defaults(settings_factory: Callable[..., JsonlPydanticIngestSettings]) -> None:
    """write_disposition defaults to 'append'. table_names defaults to None. file_glob defaults to '*.jsonl*'."""
    settings = settings_factory()
    assert settings.table_names is None
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


def test_jsonschema_settings_pass_table_names_cli_shortcut(
    schema_settings_factory: Callable[..., JsonlJsonschemaIngestSettings],
) -> None:
    """JsonlJsonschemaIngestSettings exposes the -t shortcut for table_names."""
    base_settings = schema_settings_factory()
    settings = CliApp.run(
        JsonlJsonschemaIngestSettings,
        cli_args=[*settings_to_argv(base_settings, exclude=["table_names"]), "-t", "widget,sample"],
    )
    assert settings.table_names == ["widget", "sample"]
