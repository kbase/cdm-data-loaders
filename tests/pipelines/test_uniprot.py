"""Tests for the UniProtDLT pipeline."""

from collections.abc import Callable
from unittest.mock import MagicMock, patch

import pytest
from pydantic import ValidationError
from pydantic_settings import CliApp

from cdm_data_loaders.parsers.uniprot.uniprot_kb import ENTRY_XML_TAG
from cdm_data_loaders.pipelines import uniprot_kb as uniprot_module
from cdm_data_loaders.pipelines.cts_defaults import INPUT_MOUNT, VALID_DESTINATIONS
from cdm_data_loaders.pipelines.uniprot_kb import (
    UNIPROT_LOG_INTERVAL,
    Settings,
    cli,
    parse_uniprot,
    run_uniprot_pipeline,
)

# TODO: add a test to ensure that parse_uniprot_entry is called with the appropriate args. Requires mocking the file batcher and stream_xml_file_resource functions.


@pytest.fixture
def config() -> Settings:
    """Provide a minimal valid Settings object."""
    return make_settings(input_dir="/fake/input")


START_AT_VALUE = 50
START_AT_STRING = "50"


def make_settings(**kwargs: str | int) -> Settings:
    """Generate a validated Settings object."""
    return Settings.model_validate(kwargs)


def test_settings_defaults() -> None:
    """Ensure the settings defaults are set up correctly."""
    s = make_settings()
    assert s.destination == "local_fs"
    assert s.start_at == 0
    assert s.output is None
    assert s.input_dir == INPUT_MOUNT


def test_settings_all_params_set() -> None:
    """Ensure that settings are set correctly when all args are specified."""
    s = make_settings(
        input_dir="/dir/path",
        destination=VALID_DESTINATIONS[0],
        start_at=START_AT_STRING,
        output="/some/dir",
    )
    assert s.input_dir == "/dir/path"
    assert s.destination == VALID_DESTINATIONS[0]
    assert s.start_at == START_AT_VALUE
    assert s.output == "/some/dir"


@pytest.mark.parametrize("destination", VALID_DESTINATIONS)
def test_settings_valid_variants_accepted(destination: str) -> None:
    """Ensure that each valid uniprot_kb_variant value is accepted without error."""
    s = make_settings(destination=destination)
    assert s.destination == destination


@pytest.mark.parametrize("bad", ["s3", "gcs", "filesystem", "", "LocalFs"])
def test_invalid_destination_raises(bad: str) -> None:
    """Ensure that an unrecognised destination raises a ValidationError."""
    with pytest.raises(ValidationError, match="destination must be one of"):
        make_settings(destination=bad)


def _cliapp_run(cli_args: list[str]) -> Settings:
    """Tests that Settings correctly parses command-line arguments via CliApp.

    Uses CliApp.run with explicit cli_args to avoid mutating sys.argv globally.
    """
    return CliApp.run(Settings, cli_args=cli_args)


@pytest.mark.parametrize("input_dir", ["-i", "--input-dir", "--input_dir"])
@pytest.mark.parametrize("destination", ["-d", "--destination"])
@pytest.mark.parametrize("start_at", ["-s", "--start-at", "--start_at"])
@pytest.mark.parametrize("output", ["-o", "--output"])
def test_cli_all_variants(input_dir: str, destination: str, start_at: str, output: str) -> None:
    """Test all the variants of the Settings fields."""
    s = _cliapp_run(
        [
            input_dir,
            "/dir/path",
            destination,
            VALID_DESTINATIONS[0],
            start_at,
            START_AT_STRING,
            output,
            "/some/dir",
        ]
    )
    assert s.input_dir == "/dir/path"
    assert s.destination == VALID_DESTINATIONS[0]
    assert s.start_at == START_AT_VALUE
    assert s.output == "/some/dir"


def test_cli_invalid_destination_via_cli_raises() -> None:
    """Ensure that an invalid destination passed via CLI causes a SystemExit."""
    with pytest.raises(ValidationError, match="Value error, destination must be one of"):
        _cliapp_run(["--destination", "s3"])


def test_cli_passes_settings_class_to_run_cli() -> None:
    """Ensure that cli() calls run_cli with Settings as the settings class."""
    with patch.object(uniprot_module, "run_cli") as mock_run_cli:
        cli()

    mock_run_cli.assert_called_once()
    assert mock_run_cli.call_args[0] == (Settings, run_uniprot_pipeline)


def test_cli_calls_run_uniprot_pipeline(monkeypatch: pytest.MonkeyPatch) -> None:
    """Ensure that cli() calls run_uniprot_pipeline with the config."""
    mock_settings_instance = MagicMock(spec=Settings)
    mock_settings_cls = MagicMock(return_value=mock_settings_instance)
    mock_run_uniprot_pipeline = MagicMock()

    monkeypatch.setattr(uniprot_module, "Settings", mock_settings_cls)
    monkeypatch.setattr(uniprot_module, "run_uniprot_pipeline", mock_run_uniprot_pipeline)

    cli()

    mock_settings_cls.assert_called_once_with()
    mock_run_uniprot_pipeline.assert_called_once_with(mock_settings_instance)


# Tests for running the pipeline itself
def test_run_uniprot_pipeline_args_set_correctly(config: Settings) -> None:
    """Ensure that the pipeline arguments are set correctly, and each pipeline has a different name."""
    with patch.object(uniprot_module, "run_pipeline") as mock_run_pipeline:
        run_uniprot_pipeline(config)

    assert mock_run_pipeline.call_count == 1
    _, kwargs = mock_run_pipeline.call_args
    assert kwargs.keys() == {"config", "resource", "pipeline_kwargs", "pipeline_run_kwargs"}
    assert kwargs["pipeline_kwargs"] == {"pipeline_name": "uniprot_kb", "dataset_name": "uniprot_kb"}
    assert kwargs["pipeline_run_kwargs"] == {"table_format": "delta"}
    assert kwargs["config"] == config
    assert isinstance(kwargs["resource"], Callable)


def test_run_uniprot_pipeline_sets_core_run_pipeline_args_correctly(
    config: Settings, mock_dlt: MagicMock, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Ensure that run_uniprot_pipeline calls core.run_pipeline with the correct args."""
    mock_parse_uniprot = MagicMock()
    monkeypatch.setattr(uniprot_module, "parse_uniprot", mock_parse_uniprot)

    run_uniprot_pipeline(config)

    # parse_uniprot was called once with the config to produce the resource
    mock_parse_uniprot.assert_called_once_with(config)

    # the return value of parse_uniprot(config) is what gets passed to pipeline.run
    expected_resource = mock_parse_uniprot.return_value

    mock_dlt.destination.assert_called_once_with(config.destination)
    mock_dlt.pipeline.assert_called_once_with(
        destination=mock_dlt.destination.return_value,
        pipeline_name="uniprot_kb",
        dataset_name="uniprot_kb",
    )
    mock_dlt.pipeline.return_value.run.assert_called_once_with(
        expected_resource,
        table_format="delta",
    )


def test_parse_uniprot_resource(config: Settings) -> None:
    """Ensure that parse_uniprot calls stream_xml_file_resource with the namespaced UniProt XML tag."""
    with patch.object(uniprot_module, "stream_xml_file_resource") as mock_stream:
        mock_stream.return_value = iter([])
        list(parse_uniprot(config))

    assert mock_stream.call_count == 1
    kwargs = mock_stream.call_args.kwargs
    assert kwargs.keys() == {"config", "xml_tag", "parse_fn", "log_interval"}
    assert kwargs["xml_tag"] == ENTRY_XML_TAG
    assert kwargs["log_interval"] == UNIPROT_LOG_INTERVAL
    assert kwargs["config"] == config
    assert isinstance(kwargs["parse_fn"], Callable)
