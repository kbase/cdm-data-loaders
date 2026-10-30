"""Fixtures for the all_the_bacteria pipeline tests."""

from pathlib import Path
from typing import Any

import pytest
from frozendict import frozendict

from cdm_data_loaders.core.fields import OUTPUT_DIR, USE_DESTINATION
from cdm_data_loaders.pipelines.all_the_bacteria import PROJECT_PART_REGEX, AtbSettings, load_patterns
from tests.conftest import S3


@pytest.fixture(params=["default", "v1"])
def atb_settings_pair(
    request: pytest.FixtureRequest,
    input_dir: Path,
    test_dlt_run_settings: frozendict[str, Any],
    test_dlt_run_settings_reconciled: frozendict[str, Any],
) -> tuple[frozendict[str, Any], frozendict[str, Any]]:
    """Return (settings, reconciled) dicts for AtbSettings variants."""
    output_dir = test_dlt_run_settings_reconciled[OUTPUT_DIR]
    if request.param == "default":
        settings = frozendict(**test_dlt_run_settings, version="1.2.3", pattern_file=None)
        reconciled = frozendict(
            **test_dlt_run_settings_reconciled,
            version="1.2.3",
            pattern_file=None,
            raw_data_dir=output_dir / "raw_data" / "1.2.3",
            pattern_matches=PROJECT_PART_REGEX,
        )
        return settings, reconciled

    pattern_path = input_dir / "pattern_file.txt"
    pattern_path.parent.mkdir(parents=True, exist_ok=True)
    pattern_path.write_text("hello world\n")
    settings = frozendict(**test_dlt_run_settings, version=1, pattern_file=pattern_path)
    reconciled = frozendict(
        **test_dlt_run_settings_reconciled,
        version="1",
        pattern_file=pattern_path,
        raw_data_dir=output_dir / "raw_data" / "1",
        pattern_matches=load_patterns(pattern_path),
    )
    return settings, reconciled


@pytest.fixture
def test_settings(test_dlt_run_defaults: dict[str, Any]) -> AtbSettings:
    """Generate fake settings for testing."""
    return AtbSettings(**test_dlt_run_defaults)


@pytest.fixture
def test_s3_settings(test_dlt_run_defaults: dict[str, Any]) -> AtbSettings:
    """Generate fake settings that use s3."""
    return AtbSettings(**{**test_dlt_run_defaults, USE_DESTINATION: S3})
