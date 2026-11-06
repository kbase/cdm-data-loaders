"""Shared fixtures for the UniProt CDM pipeline tests."""

from typing import Any

import pytest
from frozendict import frozendict

from cdm_data_loaders.core.fields import (
    FILE_GLOB,
    LOADER_FILE_FORMAT,
    LoaderFileFormatEnum,
)
from cdm_data_loaders.pipelines.uniprot_kb_cdm import UNIPROT_LOG_INTERVAL, UniProtSettings


@pytest.fixture
def test_settings(test_defaults: dict[str, Any]) -> UniProtSettings:
    """Provide a minimal valid UniProtSettings object, with output_dir resolved against the test config."""
    return UniProtSettings(**test_defaults)


@pytest.fixture
def test_uniprot_settings(test_cts_settings: frozendict[str, Any]) -> frozendict[str, Any]:
    """Return uniprot settings dict extending test_cts_settings."""
    return frozendict(
        {
            **test_cts_settings,
            FILE_GLOB: "*.xml*",
            LOADER_FILE_FORMAT: "parquet",
            "log_interval": UNIPROT_LOG_INTERVAL,
        }
    )


@pytest.fixture
def test_uniprot_settings_reconciled(test_cts_settings_reconciled: frozendict[str, Any]) -> frozendict[str, Any]:
    """Return reconciled uniprot settings dict extending test_cts_settings_reconciled."""
    return frozendict(
        {
            **test_cts_settings_reconciled,
            FILE_GLOB: "*.xml*",
            LOADER_FILE_FORMAT: LoaderFileFormatEnum.PARQUET,
            "log_interval": UNIPROT_LOG_INTERVAL,
        }
    )
