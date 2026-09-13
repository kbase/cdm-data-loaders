"""Tests for JsonlIngestSettings field validation and defaults."""

from collections.abc import Callable

import pytest
from pydantic import ValidationError

from cdm_data_loaders.pipelines.jsonlines_ingest import JsonlIngestSettings


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
    assert JsonlIngestSettings.split_table_names(raw) == expected


def test_jsonlines_ingest_settings_pass_defaults(settings_factory: Callable[..., JsonlIngestSettings]) -> None:
    """write_disposition defaults to 'append'. table_names defaults to None. file_glob defaults to '*.jsonl*'."""
    settings = settings_factory()
    assert settings.table_names is None
    assert settings.file_glob == "*.jsonl*"


def test_jsonlines_ingest_settings_fail_missing_entity_models_module(
    settings_factory: Callable[..., JsonlIngestSettings],
) -> None:
    """Omitting entity_models_module raises ValidationError."""
    with pytest.raises(ValidationError):
        settings_factory(entity_models_module=None)


def test_jsonlines_ingest_settings_pass_custom_file_glob(settings_factory: Callable[..., JsonlIngestSettings]) -> None:
    """file_glob accepts a custom glob pattern."""
    assert settings_factory(file_glob="*.txt").file_glob == "*.txt"


def test_jsonlines_ingest_settings_fail_local_destination_with_s3_output_dir(
    settings_factory: Callable[..., JsonlIngestSettings],
) -> None:
    """use_destination='local_fs' with an s3:// output_dir raises ValidationError.

    This check comes from CtsSettings. It is tested here because a
    regression would silently break where this pipeline writes its output.
    """
    with pytest.raises(ValidationError, match="Mismatch between output location and use_destination"):
        settings_factory(output_dir="s3://some-bucket/path")


@pytest.mark.parametrize("dataset_name", [None, ""])
def test_jsonlines_ingest_settings_fail_missing_dataset_name(
    settings_factory: Callable[..., JsonlIngestSettings], dataset_name: str | None
) -> None:
    """Omitting dataset_name raises ValidationError."""
    with pytest.raises(ValidationError):
        settings_factory(dataset_name=dataset_name)
