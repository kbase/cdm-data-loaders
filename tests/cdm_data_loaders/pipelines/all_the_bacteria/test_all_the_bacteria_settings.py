"""Tests for the all_the_bacteria pipeline."""

from typing import Any

import pytest
from frozendict import frozendict
from pydantic import ValidationError
from pydantic_settings import CliApp, SettingsError

from cdm_data_loaders.pipelines.all_the_bacteria import (
    AtbSettings,
)
from tests.helpers import assert_cli_field_roundtrips, assert_no_cli_clashes, settings_to_argv


# model_fields
def test_no_cli_param_clashes() -> None:
    """Ensure that the settings object does not have any internal alias collisions."""
    assert_no_cli_clashes(AtbSettings)


@pytest.mark.parametrize("field_name", [f for f in AtbSettings.model_fields if f != "pattern_file"])
def test_cli_fields_parse_correctly(
    field_name: str,
    atb_settings_pair: tuple[frozendict[str, Any], frozendict[str, Any]],
    test_dlt_run_defaults: dict[str, Any],
) -> None:
    """Ensure that all fields and variants are parsed correctly."""
    settings, reconciled = atb_settings_pair
    assert_cli_field_roundtrips(
        AtbSettings,
        field_name,
        raw_value=settings[field_name],
        expected_value=reconciled[field_name],
        required=test_dlt_run_defaults,
    )


def test_cli_pattern_file_parses_correctly(
    atb_settings_pair: tuple[frozendict[str, Any], frozendict[str, Any]],
    test_dlt_run_defaults: dict[str, Any],
) -> None:
    """Ensure that pattern_file is parsed correctly when set."""
    settings, reconciled = atb_settings_pair
    if settings["pattern_file"] is None:
        return
    assert_cli_field_roundtrips(
        AtbSettings,
        "pattern_file",
        raw_value=settings["pattern_file"],
        expected_value=reconciled["pattern_file"],
        required=test_dlt_run_defaults,
    )


@pytest.mark.parametrize(
    "extra_args",
    [
        pytest.param(["-i", "."], id="input_dir_shortcut"),
        pytest.param(["--input-dir", "."], id="input_dir"),
        pytest.param(["--use-resource", "cts"], id="use_resource"),
        pytest.param(["-r", "cts"], id="use_resource_shortcut"),
        pytest.param(["--buffer-size", "5"], id="buffer_size"),
        pytest.param(["--log-interval", "5"], id="log_interval"),
    ],
)
def test_settings_fail_unused_cli_fields(test_settings: AtbSettings, extra_args: list[str]) -> None:
    """Fields that the pipeline does not use are rejected on the command line."""
    with pytest.raises(SettingsError, match="unrecognized arguments"):
        CliApp.run(AtbSettings, cli_args=[*settings_to_argv(test_settings), *extra_args])


@pytest.mark.parametrize(
    "extra_kwargs",
    [
        pytest.param({"input_dir": "."}, id="input_dir"),
        pytest.param({"use_resource": "cts"}, id="use_resource"),
        pytest.param({"buffer_size": 5}, id="buffer_size"),
        pytest.param({"log_interval": 5}, id="log_interval"),
    ],
)
def test_settings_fail_unused_init_fields(test_dlt_run_defaults: dict[str, Any], extra_kwargs: dict[str, Any]) -> None:
    """Fields that the pipeline does not use are rejected as init kwargs."""
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        AtbSettings(**test_dlt_run_defaults, **extra_kwargs)
