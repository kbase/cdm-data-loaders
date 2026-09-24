"""Tests for the UniProt DLT pipeline."""

from collections.abc import Callable
from unittest.mock import MagicMock, patch

import pytest
from frozendict import frozendict

from cdm_data_loaders.parsers.uniprot.uniprot_kb import ENTRY_XML_TAG
from cdm_data_loaders.pipelines import uniprot_kb as uniprot_module
from cdm_data_loaders.pipelines.uniprot_kb import (
    UNIPROT_LOG_INTERVAL,
    UniProtSettings,
    cli,
    parse_uniprot,
    run_uniprot_pipeline,
)
from tests.cdm_data_loaders.core.conftest import (
    TEST_BATCH_FILE_SETTINGS,
    TEST_BATCH_FILE_SETTINGS_RECONCILED,
    check_settings,
    make_settings_autofill_config,
)
from tests.cdm_data_loaders.pipelines.conftest import TEST_LOG_CONFIG_FILE
from tests.helpers import assert_cli_field_roundtrips, assert_no_cli_clashes


@pytest.fixture
def test_settings() -> UniProtSettings:
    """Provide a minimal valid UniProtSettings object."""
    return make_settings_autofill_config(UniProtSettings)  # type: ignore[reportReturnType]


TEST_SETTINGS = frozendict(
    {**TEST_BATCH_FILE_SETTINGS, "log_interval": UNIPROT_LOG_INTERVAL},
)

TEST_SETTINGS_RECONCILED = frozendict({**TEST_BATCH_FILE_SETTINGS_RECONCILED, "log_interval": UNIPROT_LOG_INTERVAL})


def test_uniprot_settings_all_params_set() -> None:
    """Ensure that settings are set correctly when all args are specified.

    Note that TEST_SETTINGS includes a value for pipeline_dir.
    """
    s = make_settings_autofill_config(UniProtSettings, TEST_SETTINGS)  # type: ignore[reportReturnType]
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


def test_cli_passes_settings_class_to_run_cli() -> None:
    """Ensure that cli() calls run_cli with UniProtSettings and the UniProt log_interval default."""
    with patch.object(uniprot_module, "run_cli") as mock_run_cli:
        cli()

    mock_run_cli.assert_called_once()
    assert mock_run_cli.call_args[0] == (UniProtSettings, run_uniprot_pipeline)
    assert mock_run_cli.call_args.kwargs == {}


def test_cli_calls_run_uniprot_pipeline(monkeypatch: pytest.MonkeyPatch) -> None:
    """Ensure that cli() calls run_uniprot_pipeline with the test_settings."""
    mock_settings_instance = MagicMock()
    mock_settings_instance.log_config_file = str(TEST_LOG_CONFIG_FILE)
    mock_settings_cls = MagicMock(return_value=mock_settings_instance)
    mock_run_uniprot_pipeline = MagicMock()

    monkeypatch.setattr(uniprot_module, "UniProtSettings", mock_settings_cls)
    monkeypatch.setattr(uniprot_module, "run_uniprot_pipeline", mock_run_uniprot_pipeline)

    cli()

    mock_settings_cls.assert_called_once_with()
    mock_run_uniprot_pipeline.assert_called_once_with(mock_settings_instance)


# Tests for running the pipeline itself
def test_run_uniprot_pipeline_args_set_correctly(test_settings: UniProtSettings) -> None:
    """Ensure that the pipeline arguments are set correctly, and each pipeline has a different name."""
    with patch.object(uniprot_module, "run_pipeline") as mock_run_pipeline:
        run_uniprot_pipeline(test_settings)

    assert mock_run_pipeline.call_count == 1
    _, kwargs = mock_run_pipeline.call_args
    assert kwargs.keys() == {"settings", "resource", "pipeline_kwargs"}
    assert kwargs["pipeline_kwargs"] == {
        "pipeline_name": "uniprot_kb",
        "dataset_name": "uniprot_kb",
    }
    assert kwargs["settings"] == test_settings
    assert isinstance(kwargs["resource"], Callable)


def test_run_uniprot_pipeline_sets_core_run_pipeline_args_correctly(
    test_settings: UniProtSettings, mock_dlt: MagicMock, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Ensure that run_uniprot_pipeline calls core.run_pipeline with the correct args."""
    mock_parse_uniprot = MagicMock()
    monkeypatch.setattr(uniprot_module, "parse_uniprot", mock_parse_uniprot)

    run_uniprot_pipeline(test_settings)

    # parse_uniprot was called once with the test_settings to produce the resource
    mock_parse_uniprot.assert_called_once_with(test_settings)

    # the return value of parse_uniprot(test_settings) is what gets passed to pipeline.run
    expected_resource = mock_parse_uniprot.return_value

    mock_dlt.destination.assert_called_once_with(test_settings.use_destination)
    mock_dlt.pipeline.assert_called_once_with(
        destination=mock_dlt.destination.return_value,
        pipeline_name="uniprot_kb",
        dataset_name="uniprot_kb",
    )
    first_run, load_info_save_run = mock_dlt.pipeline.return_value.run.call_args_list
    assert first_run.args == (expected_resource,)
    assert first_run.kwargs == {}
    assert load_info_save_run.args[0] is not None
    assert load_info_save_run.kwargs == {"loader_file_format": "jsonl"}


def test_parse_uniprot_resource(test_settings: UniProtSettings) -> None:
    """Ensure that parse_uniprot calls process_xml_file_batches with the namespaced UniProt XML tag."""
    with patch.object(uniprot_module, "process_xml_file_batches") as mock_stream:
        mock_stream.return_value = iter([])
        list(parse_uniprot(test_settings))

    assert mock_stream.call_count == 1
    kwargs = mock_stream.call_args.kwargs
    assert kwargs.keys() == {"settings", "xml_tag", "parse_fn"}
    assert kwargs["xml_tag"] == ENTRY_XML_TAG
    assert kwargs["settings"] == test_settings
    assert isinstance(kwargs["parse_fn"], Callable)
