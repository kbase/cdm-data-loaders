from typing import Any

import pytest
from frozendict import frozendict

from cdm_data_loaders.pipelines.core import resolve_cts_settings
from cdm_data_loaders.pipelines.uniref import (
    UNIREF_VARIANTS,
    VARIANT,
    UnirefSettings,
)
from tests.cdm_data_loaders.core.conftest import (
    TEST_CTS_SETTINGS,
    TEST_CTS_SETTINGS_RECONCILED,
)

TEST_DEFAULT_UNIREF_VARIANT = "50"


TEST_SETTINGS = frozendict(
    {**TEST_CTS_SETTINGS, VARIANT: TEST_DEFAULT_UNIREF_VARIANT},
)

TEST_SETTINGS_RECONCILED = frozendict(
    {**TEST_CTS_SETTINGS_RECONCILED, VARIANT: TEST_DEFAULT_UNIREF_VARIANT},
)

UNIREF_VARIANT_ALIASES = ["v", "variant"]


@pytest.fixture(params=UNIREF_VARIANTS)
def uniref_variant_value(request: pytest.FixtureRequest) -> str:
    """Parametrized fixture over all valid uniref variants."""
    return request.param


@pytest.fixture
def test_settings(uniref_variant_value: str, dlt_config: dict[str, Any]) -> UnirefSettings:
    """A valid UnirefSettings object for each uniref variant, with output_dir resolved against the test config."""
    settings = UnirefSettings(variant=uniref_variant_value, input_dir="/fake/input")  # type: ignore[reportReturnType]
    return resolve_cts_settings(settings, dlt_config)
