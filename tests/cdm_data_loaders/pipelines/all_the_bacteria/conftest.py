from pathlib import Path

import pytest
from frozendict import frozendict

from cdm_data_loaders.core.fields import S3
from cdm_data_loaders.pipelines.all_the_bacteria import (
    AtbSettings,
)
from tests.cdm_data_loaders.core.conftest import (
    TEST_CTS_SETTINGS,
    TEST_CTS_SETTINGS_RECONCILED,
)

TEST_SETTINGS = frozendict(**TEST_CTS_SETTINGS, version="1.2.3")

TEST_SETTINGS_RECONCILED = frozendict(**TEST_CTS_SETTINGS_RECONCILED, version="1.2.3", pattern_file=None)

TEST_SETTINGS_V1 = frozendict(**TEST_CTS_SETTINGS, version=1, pattern_file="some/path/or/other.txt")

TEST_SETTINGS_RECONCILED_V1 = frozendict(
    **TEST_CTS_SETTINGS_RECONCILED, version="1", pattern_file="some/path/or/other.txt"
)


@pytest.fixture
def test_settings(tmp_path: Path) -> AtbSettings:
    """Generate fake settings for testing."""
    return AtbSettings(output_dir=str(tmp_path))  # pyright: ignore[reportCallIssue]


@pytest.fixture
def test_s3_settings() -> AtbSettings:
    """Generate fake settings that use s3."""
    return AtbSettings(use_destination=S3)  # pyright: ignore[reportCallIssue]
