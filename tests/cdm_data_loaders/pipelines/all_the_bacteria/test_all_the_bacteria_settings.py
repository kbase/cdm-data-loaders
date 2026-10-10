"""Tests for the all_the_bacteria pipeline."""

from pathlib import Path
from typing import Any

import pytest
from frozendict import frozendict

from cdm_data_loaders.core.fields import S3
from cdm_data_loaders.pipelines.all_the_bacteria import (
    AtbSettings,
)
from tests.cdm_data_loaders.pipelines.all_the_bacteria.conftest import (
    TEST_SETTINGS,
    TEST_SETTINGS_RECONCILED,
    TEST_SETTINGS_RECONCILED_V1,
    TEST_SETTINGS_V1,
)
from tests.helpers import assert_cli_field_roundtrips, assert_no_cli_clashes


@pytest.fixture
def test_settings(tmp_path: Path) -> AtbSettings:
    """Generate fake settings for testing."""
    return AtbSettings(output_dir=str(tmp_path))  # pyright: ignore[reportCallIssue]


@pytest.fixture
def test_s3_settings() -> AtbSettings:
    """Generate fake settings that use s3."""
    return AtbSettings(use_destination=S3)  # pyright: ignore[reportCallIssue]


# model_fields
def test_no_cli_param_clashes() -> None:
    """Ensure that the settings object does not have any internal alias collisions."""
    assert_no_cli_clashes(AtbSettings)


@pytest.mark.parametrize(
    ("settings", "reconciled"),
    [
        (TEST_SETTINGS, TEST_SETTINGS_RECONCILED),
        (TEST_SETTINGS_V1, TEST_SETTINGS_RECONCILED_V1),
    ],
)
@pytest.mark.parametrize("field_name", list(TEST_SETTINGS))
def test_cli_fields_parse_correctly(
    field_name: str, settings: frozendict[str, Any], reconciled: frozendict[str, Any]
) -> None:
    """Ensure that all fields and variants are parsed correctly."""
    assert_cli_field_roundtrips(
        AtbSettings,
        field_name,
        settings[field_name],
        reconciled[field_name],
    )
