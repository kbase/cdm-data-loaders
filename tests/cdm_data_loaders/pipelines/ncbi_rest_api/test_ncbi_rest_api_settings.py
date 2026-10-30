"""Tests for the NCBI datasets API pipeline functions."""

import re
from pathlib import Path
from typing import Any

import pytest
from frozendict import frozendict
from pydantic import ValidationError
from pydantic_settings import CliApp, SettingsError

from cdm_data_loaders.core.fields import INPUT_DIR, OUTPUT_DIR
from cdm_data_loaders.pipelines import ncbi_rest_api as ncbi_module
from cdm_data_loaders.pipelines.ncbi_rest_api import (
    ANNOTATION,
    DATASET,
    MAX_IDS_PER_QUERY,
    NcbiRestApiSettings,
    get_settings,
    set_settings,
)
from tests.cdm_data_loaders.conftest import check_settings
from tests.helpers import assert_cli_field_roundtrips, assert_no_cli_clashes, settings_to_argv

BATCH_SIZE = 200
BATCH_SIZE_STRING = str(BATCH_SIZE)


@pytest.fixture(params=["default", "v1"])
def ncbi_settings_pair(
    request: pytest.FixtureRequest,
    input_dir: Path,
    test_dlt_run_settings: frozendict[str, Any],
    test_dlt_run_settings_reconciled: frozendict[str, Any],
) -> tuple[frozendict[str, Any], frozendict[str, Any]]:
    """Return (settings, reconciled) dicts for NcbiRestApiSettings variants."""
    test_cts_settings = frozendict({**test_dlt_run_settings, INPUT_DIR: input_dir})
    test_cts_settings_reconciled = frozendict({**test_dlt_run_settings_reconciled, INPUT_DIR: input_dir})
    raw_data_dir = test_cts_settings_reconciled[OUTPUT_DIR] / "raw_data" / "api_responses"
    if request.param == "default":
        settings = frozendict(**test_cts_settings, batch_size=BATCH_SIZE_STRING, query_type="")
        reconciled = frozendict(
            **test_cts_settings_reconciled,
            batch_size=BATCH_SIZE,
            query_type=None,
            save_raw_responses=False,
            raw_data_dir=raw_data_dir,
        )
        return settings, reconciled
    settings = frozendict(**test_cts_settings, batch_size=BATCH_SIZE_STRING, query_type=" ANNOTATION ")
    reconciled = frozendict(
        **test_cts_settings_reconciled,
        batch_size=BATCH_SIZE,
        query_type=ANNOTATION,
        save_raw_responses=False,
        raw_data_dir=raw_data_dir,
    )
    return settings, reconciled


def test_get_set_settings(test_settings: NcbiRestApiSettings) -> None:
    """Test setting and retrieval of settings."""
    assert ncbi_module.PIPELINE_SETTINGS is None
    with pytest.raises(RuntimeError, match="Pipeline settings have not been initialised"):
        get_settings()

    set_settings(test_settings)
    assert test_settings == ncbi_module.PIPELINE_SETTINGS
    assert get_settings() == test_settings

    set_settings(None)  # type: ignore[reportArgumentType]
    assert ncbi_module.PIPELINE_SETTINGS is None
    with pytest.raises(RuntimeError, match="Pipeline settings have not been initialised"):
        get_settings()


@pytest.mark.parametrize(
    ("batch_size", "parsed_batch_size"),
    [
        ("10", 10),
        (10, 10),
        (None, MAX_IDS_PER_QUERY),
    ],
)
def test_settings_valid_batch_size(
    batch_size: int | str | None,
    parsed_batch_size: int,
    test_settings: NcbiRestApiSettings,
    test_defaults: dict[str, Any],
) -> None:
    """Ensure that a valid batch size is correctly parsed."""
    if batch_size is None:
        settings = test_settings
    else:
        settings: NcbiRestApiSettings = NcbiRestApiSettings(batch_size=batch_size, **test_defaults)  # type: ignore[reportReturnType]
    assert settings.batch_size == parsed_batch_size


@pytest.mark.parametrize(
    ("bad_batch_size", "message"),
    [
        ("0", "Input should be greater than or equal to 1"),
        ("-1", "Input should be greater than or equal to 1"),
        ("1001", f"Input should be less than or equal to {MAX_IDS_PER_QUERY}"),
        ("notanint", "Input should be a valid integer"),
        ("", "Input should be a valid integer"),
        ("1.2345", "Input should be a valid integer"),
    ],
)
@pytest.mark.parametrize("use_cliapp", [True, False])
def test_cli_invalid_batch_size_via_cli_raises(
    bad_batch_size: str,
    message: str,
    use_cliapp: bool,
    test_settings: NcbiRestApiSettings,
    test_defaults: dict[str, Any],
) -> None:
    """Ensure that an invalid batch size passed via CLI raises an error."""
    if use_cliapp:
        with pytest.raises(ValidationError, match=message):
            CliApp.run(
                NcbiRestApiSettings,
                cli_args=[*settings_to_argv(test_settings, exclude=["batch_size"]), "--batch-size", bad_batch_size],
            )
    else:
        with pytest.raises(ValidationError, match=message):
            NcbiRestApiSettings(batch_size=bad_batch_size, **test_defaults)  # pyright: ignore[reportCallIssue]


@pytest.mark.parametrize(
    ("query_type", "parsed_query_type"),
    [
        (ANNOTATION, ANNOTATION),
        (DATASET, DATASET),
        ("  ANNOTATION  ", ANNOTATION),
        ("\n\nDataset\t\n", DATASET),
        ("   ", None),
        ("", None),
        (None, None),
    ],
)
def test_cli_valid_query_type(
    query_type: str | None,
    parsed_query_type: str | None,
    test_settings: NcbiRestApiSettings,
    test_defaults: dict[str, Any],
) -> None:
    """Ensure that a valid batch size is correctly parsed."""
    settings: NcbiRestApiSettings = NcbiRestApiSettings(query_type=query_type, **test_defaults)
    assert settings.query_type == parsed_query_type

    if query_type is None:
        alt_settings: NcbiRestApiSettings = test_settings
        assert alt_settings.query_type == parsed_query_type


STRING_MATCH_MESSAGE = re.escape("String should match pattern '^(dataset|annotation)$'")


@pytest.mark.parametrize(
    "query_type",
    [
        "annot",
        "anotation",
        "data set",
    ],
)
@pytest.mark.parametrize("use_cliapp", [True, False])
def test_cli_invalid_query_type(
    query_type: str,
    use_cliapp: bool,
    test_settings: NcbiRestApiSettings,
    test_defaults: dict[str, Any],
) -> None:
    """Ensure that an invalid batch size passed via CLI raises an error."""
    if use_cliapp:
        with pytest.raises(ValidationError, match=STRING_MATCH_MESSAGE):
            CliApp.run(
                NcbiRestApiSettings,
                cli_args=[*settings_to_argv(test_settings, exclude=["query_type"]), "--query-type", query_type],
            )
    else:
        with pytest.raises(ValidationError, match=STRING_MATCH_MESSAGE):
            NcbiRestApiSettings(query_type=query_type, **test_defaults)  # pyright: ignore[reportCallIssue]


def test_settings_all_params_set(
    ncbi_settings_pair: tuple[frozendict[str, Any], frozendict[str, Any]],
) -> None:
    """Ensure that settings are set correctly when all args are specified."""
    settings, reconciled = ncbi_settings_pair
    s = NcbiRestApiSettings(**settings)
    check_settings(s, reconciled)


# model_fields
def test_no_cli_param_clashes() -> None:
    """Ensure that the settings object does not have any internal alias collisions."""
    assert_no_cli_clashes(NcbiRestApiSettings)


@pytest.mark.parametrize("field_name", [f for f in NcbiRestApiSettings.model_fields if f != "save_raw_responses"])
def test_cli_fields_parse_correctly(
    field_name: str,
    ncbi_settings_pair: tuple[frozendict[str, Any], frozendict[str, Any]],
    test_defaults: dict[str, Any],
) -> None:
    """Ensure that all fields and variants are parsed correctly."""
    settings, reconciled = ncbi_settings_pair
    assert_cli_field_roundtrips(
        NcbiRestApiSettings,
        field_name,
        raw_value=settings[field_name],
        expected_value=reconciled[field_name],
        required=test_defaults,
    )


@pytest.mark.parametrize(
    "extra_args",
    [
        pytest.param(["--use-resource", "cts"], id="use_resource"),
        pytest.param(["-r", "cts"], id="use_resource_shortcut"),
        pytest.param(["--buffer-size", "5"], id="buffer_size"),
        pytest.param(["--log-interval", "5"], id="log_interval"),
    ],
)
def test_settings_fail_unused_cli_fields(test_settings: NcbiRestApiSettings, extra_args: list[str]) -> None:
    """Fields that the pipeline does not use are rejected on the command line."""
    with pytest.raises(SettingsError, match="unrecognized arguments"):
        CliApp.run(NcbiRestApiSettings, cli_args=[*settings_to_argv(test_settings), *extra_args])


@pytest.mark.parametrize(
    "extra_kwargs",
    [
        pytest.param({"use_resource": "cts"}, id="use_resource"),
        pytest.param({"buffer_size": 5}, id="buffer_size"),
        pytest.param({"log_interval": 5}, id="log_interval"),
    ],
)
def test_settings_fail_unused_init_fields(test_defaults: dict[str, Any], extra_kwargs: dict[str, Any]) -> None:
    """Fields that the pipeline does not use are rejected as init kwargs."""
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        NcbiRestApiSettings(**test_defaults, **extra_kwargs)


def test_settings_fail_missing_input_dir(test_dlt_run_defaults: dict[str, Any]) -> None:
    """input_dir is required: the pipeline can only read assembly IDs from a local directory."""
    with pytest.raises(ValidationError) as exc_info:
        NcbiRestApiSettings(**test_dlt_run_defaults)
    assert [(err["loc"], err["type"]) for err in exc_info.value.errors()] == [(("input_dir",), "missing")]
