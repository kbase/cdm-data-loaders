"""Tests for the UniProt DLT pipeline."""

import pytest

from cdm_data_loaders.pipelines.uniprot_kb import (
    UniProtSettings,
)
from tests.cdm_data_loaders.core.conftest import (
    check_settings,
)
from tests.cdm_data_loaders.pipelines.uniprot.conftest import TEST_SETTINGS, TEST_SETTINGS_RECONCILED
from tests.helpers import assert_cli_field_roundtrips, assert_no_cli_clashes


def test_uniprot_settings_all_params_set() -> None:
    """Ensure that settings are set correctly when all args are specified.

    Note that TEST_SETTINGS includes a value for pipeline_dir.
    """
    s = UniProtSettings(**TEST_SETTINGS)  # type: ignore[reportReturnType]
    check_settings(s, TEST_SETTINGS_RECONCILED)


# model_fields
def test_no_cli_param_clashes() -> None:
    """Ensure that the settings object does not have any internal alias collisions."""
    assert_no_cli_clashes(UniProtSettings)


@pytest.mark.parametrize("field_name", list(TEST_SETTINGS))
def test_cli_fields_parse_correctly(field_name: str) -> None:
    """Ensure that all fields and variants are parsed correctly."""
    assert_cli_field_roundtrips(
        UniProtSettings, field_name, TEST_SETTINGS[field_name], TEST_SETTINGS_RECONCILED[field_name]
    )
