from typing import Any

import pytest
from frozendict import frozendict

from cdm_data_loaders.pipelines.core import resolve_cts_settings
from cdm_data_loaders.pipelines.uniprot_kb import UNIPROT_LOG_INTERVAL, UniProtSettings
from tests.cdm_data_loaders.core.conftest import (
    TEST_CTS_SETTINGS,
    TEST_CTS_SETTINGS_RECONCILED,
)


@pytest.fixture
def test_settings(dlt_config: dict[str, Any]) -> UniProtSettings:
    """Provide a minimal valid UniProtSettings object, with output_dir resolved against the test config."""
    return resolve_cts_settings(UniProtSettings(), dlt_config)


TEST_SETTINGS = frozendict(
    {**TEST_CTS_SETTINGS, "log_interval": UNIPROT_LOG_INTERVAL},
)

TEST_SETTINGS_RECONCILED = frozendict({**TEST_CTS_SETTINGS_RECONCILED, "log_interval": UNIPROT_LOG_INTERVAL})
