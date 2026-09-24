"""Tests for the UniRef DLT pipeline."""

from collections.abc import Callable
from unittest.mock import MagicMock, patch

import pytest
from frozendict import frozendict
from pydantic import ValidationError
from pydantic_settings import CliApp

from cdm_data_loaders.parsers.uniprot.uniref import ENTRY_XML_TAG
from cdm_data_loaders.pipelines import uniref as uniref_module
from cdm_data_loaders.pipelines.uniref import (
    UNIREF_VARIANTS,
    VARIANT,
    UnirefSettings,
    cli,
    parse_uniref,
    run_uniref_pipeline,
)
from tests.cdm_data_loaders.core.conftest import (
    TEST_BATCH_FILE_SETTINGS,
    TEST_BATCH_FILE_SETTINGS_RECONCILED,
    check_settings,
    make_settings_autofill_config,
)
from tests.cdm_data_loaders.pipelines.conftest import TEST_LOG_CONFIG_FILE
from tests.helpers import assert_cli_field_roundtrips, assert_no_cli_clashes, make_cli_arg

START_AT_VALUE = 25
START_AT_STRING = "25"

TEST_DEFAULT_UNIREF_VARIANT = "50"


TEST_SETTINGS = frozendict(
    {**TEST_BATCH_FILE_SETTINGS, VARIANT: TEST_DEFAULT_UNIREF_VARIANT},
)

TEST_SETTINGS_RECONCILED = frozendict(
    {**TEST_BATCH_FILE_SETTINGS_RECONCILED, VARIANT: TEST_DEFAULT_UNIREF_VARIANT},
)

UNIREF_VARIANT_ALIASES = ["v", "variant"]


@pytest.fixture(params=UNIREF_VARIANTS)
def uniref_variant_value(request: pytest.FixtureRequest) -> str:
    """Parametrized fixture over all valid uniref variants."""
    return request.param


@pytest.fixture
def test_settings(uniref_variant_value: str) -> UnirefSettings:
    """A valid UnirefSettings object for each uniref variant."""
    return make_settings_autofill_config(UnirefSettings, {VARIANT: uniref_variant_value, "input_dir": "/fake/input"})  # type: ignore[reportReturnType]


def test_uniref_settings_all_params_set() -> None:
    """Ensure that settings are set correctly when all args are specified.

    Note that TEST_SETTINGS includes a value for pipeline_dir.
    """
    s = make_settings_autofill_config(UnirefSettings, TEST_SETTINGS)  # type: ignore[reportReturnType]
    check_settings(s, TEST_SETTINGS_RECONCILED)


# model_fields
def test_no_cli_param_clashes() -> None:
    """Ensure that the settings object does not have any internal alias collisions."""
    assert_no_cli_clashes(UnirefSettings)


@pytest.mark.parametrize("field_name", list(TEST_SETTINGS))
def test_cli_fields_parse_correctly(field_name: str) -> None:
    """Ensure that all fields and variants are parsed correctly."""
    assert_cli_field_roundtrips(
        UnirefSettings,
        field_name,
        TEST_SETTINGS[field_name],
        TEST_SETTINGS_RECONCILED[field_name],
        {VARIANT: TEST_SETTINGS[VARIANT]},
    )


@pytest.mark.parametrize("uniref_variant_value", UNIREF_VARIANTS)
def test_settings_valid_variants_accepted(uniref_variant_value: str) -> None:
    """Ensure that each valid variant value is accepted without error."""
    s = make_settings_autofill_config(UnirefSettings, {VARIANT: uniref_variant_value})
    assert isinstance(s, UnirefSettings)
    assert s.variant == uniref_variant_value


@pytest.mark.parametrize("uniref_variant_value", UNIREF_VARIANTS)
@pytest.mark.parametrize(VARIANT, UNIREF_VARIANT_ALIASES)
def test_cli_valid_variants_accepted(uniref_variant_value: str, variant: str) -> None:
    """Ensure that each valid uniref variant value is accepted without error when passed via CLI."""
    s = CliApp.run(
        UnirefSettings,
        cli_args=[make_cli_arg(variant), uniref_variant_value],
    )
    assert isinstance(s, UnirefSettings)
    assert s.variant == uniref_variant_value


@pytest.mark.parametrize("value", ["25", "75", "uniref50", "", "ALL"])
def test_invalid_variant_raises(value: str) -> None:
    """Ensure that an unrecognised uniref variant raises a ValidationError."""
    with pytest.raises(ValidationError, match="1 validation error for UnirefSettings") as exc_info:
        make_settings_autofill_config(UnirefSettings, {VARIANT: value})

    exc_message = str(exc_info.value)
    assert "Input should be '50', '90' or '100'" in exc_message


@pytest.mark.parametrize("value", ["25", "75", "uniref50", "", "ALL"])
@pytest.mark.parametrize(VARIANT, UNIREF_VARIANT_ALIASES)
def test_cli_invalid_variant_via_cli_raises(
    value: str,
    variant: str,
) -> None:
    """Ensure that an invalid uniref variant passed via CLI raises an error."""
    with pytest.raises(ValidationError, match="1 validation error for UnirefSettings") as exc_info:
        CliApp.run(UnirefSettings, cli_args=[make_cli_arg(variant), value])

    exc_message = str(exc_info.value)
    assert "Input should be '50', '90' or '100'" in exc_message


def test_missing_required_uniref_variant_raises() -> None:
    """Ensure that omitting the required uniref variant argument raises a ValidationError."""
    with pytest.raises(ValidationError, match="1 validation error for UnirefSettings") as exc_info:
        make_settings_autofill_config(UnirefSettings)

    exc_message = str(exc_info.value)
    assert "Field required" in exc_message


def test_cli_missing_required_uniref_variant_raises() -> None:
    """Ensure that omitting the required uniref variant argument raises an error."""
    with pytest.raises(ValidationError, match="1 validation error for UnirefSettings") as exc_info:
        CliApp.run(UnirefSettings)

    exc_message = str(exc_info.value)
    assert "Field required" in exc_message


@pytest.mark.parametrize("value", ["25", "75", "uniref50", "", "ALL"])
@pytest.mark.parametrize(VARIANT, UNIREF_VARIANT_ALIASES)
def test_cli_invalid_variant_and_destination_via_cli_raises(value: str, variant: str) -> None:
    """Ensure that invalid uniref variant and use_destination passed via CLI raises an error with both errors.

    N.b. Pydantic only reports the first error!!!
    """
    with pytest.raises(ValidationError, match="1 validation error for UnirefSettings") as exc_info:
        CliApp.run(
            UnirefSettings,
            cli_args=[
                make_cli_arg(variant),
                value,
                "--use_destination",
                "some invalid destination",
            ],
        )

    # Check that both errors are present in the exception message
    exc_message = str(exc_info.value)
    assert "Input should be '50', '90' or '100'" in exc_message


def test_cli_passes_settings_class_to_run_cli() -> None:
    """Ensure that cli() calls run_cli with UnirefSettings and the UniRef log_interval default."""
    with patch.object(uniref_module, "run_cli") as mock_run_cli:
        cli()

    mock_run_cli.assert_called_once()
    assert mock_run_cli.call_args[0] == (UnirefSettings, run_uniref_pipeline)
    assert mock_run_cli.call_args.kwargs == {}


def test_cli_calls_run_uniref_pipeline(monkeypatch: pytest.MonkeyPatch) -> None:
    """Ensure that cli() calls run_uniref_pipeline with the settings."""
    mock_settings_instance = MagicMock()
    mock_settings_instance.log_config_file = str(TEST_LOG_CONFIG_FILE)
    mock_settings_cls = MagicMock(return_value=mock_settings_instance)
    mock_run_uniref_pipeline = MagicMock()

    monkeypatch.setattr(uniref_module, "UnirefSettings", mock_settings_cls)
    monkeypatch.setattr(uniref_module, "run_uniref_pipeline", mock_run_uniref_pipeline)

    cli()
    mock_settings_cls.assert_called_once_with()
    mock_run_uniref_pipeline.assert_called_once_with(mock_settings_instance)


# Tests for running the pipeline itself
def test_run_uniref_pipeline_args_set_correctly(test_settings: UnirefSettings) -> None:
    """Ensure that the pipeline arguments are set correctly, and each pipeline has a different name."""
    with patch.object(uniref_module, "run_pipeline") as mock_run_pipeline:
        run_uniref_pipeline(test_settings)

    assert mock_run_pipeline.call_count == 1
    _, kwargs = mock_run_pipeline.call_args
    assert kwargs.keys() == {"settings", "resource", "pipeline_kwargs"}
    assert kwargs["pipeline_kwargs"] == {
        "pipeline_name": f"uniref_{test_settings.variant}",
        "dataset_name": "uniprot_kb",
    }
    assert kwargs["settings"] == test_settings
    assert isinstance(kwargs["resource"], Callable)


def test_run_uniref_pipeline_sets_core_run_pipeline_args_correctly(
    test_settings: UnirefSettings, mock_dlt: MagicMock, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Ensure that run_uniref_pipeline calls core.run_pipeline with the correct args."""
    mock_parse_uniref = MagicMock()
    monkeypatch.setattr(uniref_module, "parse_uniref", mock_parse_uniref)

    run_uniref_pipeline(test_settings)

    # parse_uniref was called once with the settings to produce the resource
    mock_parse_uniref.assert_called_once_with(test_settings)

    # the return value of parse_uniref(settings) is what gets passed to pipeline.run
    expected_resource = mock_parse_uniref.return_value

    mock_dlt.destination.assert_called_once_with(test_settings.use_destination)
    mock_dlt.pipeline.assert_called_once_with(
        destination=mock_dlt.destination.return_value,
        pipeline_name=f"uniref_{test_settings.variant}",
        dataset_name="uniprot_kb",
    )
    first_run, load_info_save_run = mock_dlt.pipeline.return_value.run.call_args_list
    assert first_run.args == (expected_resource,)
    assert first_run.kwargs == {}
    assert load_info_save_run.args[0] is not None
    assert load_info_save_run.kwargs == {"loader_file_format": "jsonl"}


def test_parse_uniref_resource(test_settings: UnirefSettings) -> None:
    """Ensure that parse_uniref calls process_xml_file_batches with the namespaced UniRef XML tag."""
    with patch.object(uniref_module, "process_xml_file_batches") as mock_stream:
        mock_stream.return_value = iter([])
        list(parse_uniref(test_settings))

    assert mock_stream.call_count == 1
    kwargs = mock_stream.call_args.kwargs
    assert kwargs.keys() == {"settings", "xml_tag", "parse_fn"}
    assert kwargs["xml_tag"] == ENTRY_XML_TAG
    assert kwargs["settings"] == test_settings
    assert isinstance(kwargs["parse_fn"], Callable)
