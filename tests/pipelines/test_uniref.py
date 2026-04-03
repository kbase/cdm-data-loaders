"""Tests for the uniref DLT pipeline."""

import datetime
from collections.abc import Callable
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from pydantic import ValidationError
from pydantic_settings import CliApp

from cdm_data_loaders.parsers.uniprot.uniref import ENTRY_XML_TAG, UNIREF_VARIANTS
from cdm_data_loaders.pipelines import uniref as uniref_module
from cdm_data_loaders.pipelines.cts_defaults import INPUT_MOUNT, VALID_DESTINATIONS
from cdm_data_loaders.pipelines.uniref import UNIREF_LOG_INTERVAL, Settings, cli, parse_uniref, run_uniref_pipeline
from tests.pipelines.conftest import make_batcher

# TODO: add a test to ensure that parse_uniref_entry is called with the appropriate args. Requires mocking the file batcher and stream_xml_file_resource functions.


START_AT_VALUE = 25
START_AT_STRING = "25"

TEST_DEFAULT_UNIREF_VARIANT = "50"


def make_settings(uniref_variant: str = TEST_DEFAULT_UNIREF_VARIANT, **kwargs: str | int) -> Settings:
    """Generate a validated Settings object."""
    data = {"uniref": uniref_variant, **kwargs}
    return Settings.model_validate(data)


@pytest.fixture(params=UNIREF_VARIANTS)
def variant(request: pytest.FixtureRequest) -> str:
    """Parametrized fixture over all valid uniref variants."""
    return request.param


@pytest.fixture
def config(variant: str) -> Settings:
    """A valid Settings object for each uniref variant."""
    return make_settings(uniref_variant=variant, input_dir="/fake/input")


def test_settings_defaults() -> None:
    """Ensure the settings defaults are set up correctly."""
    s = make_settings()
    assert s.destination == "local_fs"
    assert s.start_at == 0
    assert s.output is None
    assert s.input_dir == INPUT_MOUNT
    assert s.uniref_variant == TEST_DEFAULT_UNIREF_VARIANT


def test_settings_all_params_set() -> None:
    """Ensure that settings are set correctly when all args are specified."""
    s = make_settings(
        input_dir="/dir/path",
        destination=VALID_DESTINATIONS[0],
        uniref_variant="100",
        start_at=START_AT_STRING,
        output="/some/dir",
    )
    assert s.input_dir == "/dir/path"
    assert s.destination == VALID_DESTINATIONS[0]
    assert s.uniref_variant == "100"
    assert s.start_at == START_AT_VALUE
    assert s.output == "/some/dir"


@pytest.mark.parametrize("destination", VALID_DESTINATIONS)
@pytest.mark.parametrize("uniref_variant", UNIREF_VARIANTS)
def test_settings_valid_variants_accepted(uniref_variant: str, destination: str) -> None:
    """Ensure that each valid uniref_variant value is accepted without error."""
    s = make_settings(uniref_variant=uniref_variant, destination=destination)
    assert s.uniref_variant == uniref_variant
    assert s.destination == destination


@pytest.mark.parametrize("bad_variant", ["25", "75", "uniref50", "", "ALL"])
def test_invalid_variant_raises(bad_variant: str) -> None:
    """Ensure that an unrecognised uniref_variant raises a ValidationError."""
    with pytest.raises(ValidationError, match="uniref_variant must be one of"):
        make_settings(uniref_variant=bad_variant)


@pytest.mark.parametrize("bad_destination", ["s3", "gcs", "filesystem", "", "LocalFs"])
def test_invalid_destination_raises(bad_destination: str) -> None:
    """Ensure that an unrecognised destination raises a ValidationError."""
    with pytest.raises(ValidationError, match="destination must be one of"):
        make_settings(destination=bad_destination)


@pytest.mark.parametrize("input_dir", ["-i", "--input-dir", "--input_dir"])
@pytest.mark.parametrize("destination", ["-d", "--destination"])
@pytest.mark.parametrize("uniref_variant", ["-u", "--uniref", "--uniref-variant", "--uniref_variant"])
@pytest.mark.parametrize("start_at", ["-s", "--start-at", "--start_at"])
@pytest.mark.parametrize("output", ["-o", "--output"])
def test_cli_all_variants(input_dir: str, destination: str, uniref_variant: str, start_at: str, output: str) -> None:
    """Test all the variants of the Settings fields."""
    s = CliApp.run(
        Settings,
        [
            input_dir,
            "/dir/path",
            destination,
            VALID_DESTINATIONS[0],
            uniref_variant,
            TEST_DEFAULT_UNIREF_VARIANT,
            start_at,
            START_AT_STRING,
            output,
            "/some/dir",
        ],
    )
    assert s.input_dir == "/dir/path"
    assert s.destination == VALID_DESTINATIONS[0]
    assert s.uniref_variant == TEST_DEFAULT_UNIREF_VARIANT
    assert s.start_at == START_AT_VALUE
    assert s.output == "/some/dir"


@pytest.mark.parametrize("bad_variant", ["25", "75", "uniref50", "", "ALL"])
def test_cli_invalid_variant_via_cli_raises(bad_variant: str) -> None:
    """Ensure that an invalid uniref_variant passed via CLI raises an error."""
    with pytest.raises(ValidationError, match="Value error, uniref_variant must be one of"):
        CliApp.run(Settings, cli_args=["--uniref-variant", bad_variant])


@pytest.mark.parametrize("bad_destination", ["25", "75", "uniref50", "", "ALL"])
def test_cli_invalid_destination_via_cli_raises(bad_destination: str) -> None:
    """Ensure that an invalid destination passed via CLI raises an error."""
    with pytest.raises(ValidationError, match="Value error, destination must be one of"):
        CliApp.run(Settings, cli_args=["--uniref-variant", "50", "--destination", bad_destination])


def test_cli_missing_required_uniref_variant_raises() -> None:
    """Ensure that omitting the required uniref_variant argument raises an error."""
    with pytest.raises(ValidationError, match="Field required"):
        CliApp.run(Settings, cli_args=[])


@pytest.mark.parametrize("bad_variant", ["25", "75", "uniref50", "", "ALL"])
@pytest.mark.parametrize("bad_destination", ["s3", "gcs", "filesystem", "", "LocalFs"])
def test_cli_invalid_variant_and_destination_via_cli_raises(bad_variant: str, bad_destination: str) -> None:
    """Ensure that invalid uniref_variant and destination passed via CLI raises an error with both errors."""
    with pytest.raises(ValidationError, match="2 validation errors for Settings") as exc_info:
        CliApp.run(Settings, cli_args=["--uniref-variant", bad_variant, "--destination", bad_destination])

    # Check that both errors are present in the exception message
    exc_message = str(exc_info.value)
    assert "Value error, uniref_variant must be one of" in exc_message
    assert "Value error, destination must be one of" in exc_message


def test_cli_passes_settings_class_to_run_cli() -> None:
    """Ensure that cli() calls run_cli with Settings as the settings class."""
    with patch.object(uniref_module, "run_cli") as mock_run_cli:
        cli()

    mock_run_cli.assert_called_once()
    assert mock_run_cli.call_args[0] == (Settings, run_uniref_pipeline)


def test_cli_calls_run_uniref_pipeline(monkeypatch: pytest.MonkeyPatch) -> None:
    """Ensure that cli() calls run_uniref_pipeline with the config."""
    mock_settings_instance = MagicMock(spec=Settings)
    mock_settings_cls = MagicMock(return_value=mock_settings_instance)
    mock_run_uniref_pipeline = MagicMock()

    monkeypatch.setattr(uniref_module, "Settings", mock_settings_cls)
    monkeypatch.setattr(uniref_module, "run_uniref_pipeline", mock_run_uniref_pipeline)

    cli()
    # no args
    mock_settings_cls.assert_called_once_with()
    mock_run_uniref_pipeline.assert_called_once_with(mock_settings_instance)


# Tests for running the pipeline itself
def test_run_uniref_pipeline_args_set_correctly(config: Settings) -> None:
    """Ensure that the pipeline arguments are set correctly, and each pipeline has a different name."""
    with patch.object(uniref_module, "run_pipeline") as mock_run_pipeline:
        run_uniref_pipeline(config)

    assert mock_run_pipeline.call_count == 1
    _, kwargs = mock_run_pipeline.call_args
    assert kwargs.keys() == {"config", "resource", "pipeline_kwargs", "pipeline_run_kwargs"}
    assert kwargs["pipeline_kwargs"] == {
        "pipeline_name": f"uniref_{config.uniref_variant}",
        "dataset_name": "uniprot_kb",
    }
    assert kwargs["pipeline_run_kwargs"] == {"table_format": "delta"}
    assert kwargs["config"] == config
    assert isinstance(kwargs["resource"], Callable)


def test_run_uniref_pipeline_sets_core_run_pipeline_args_correctly(
    config: Settings, mock_dlt: MagicMock, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Ensure that run_uniref_pipeline calls core.run_pipeline with the correct args."""
    mock_parse_uniref = MagicMock()
    monkeypatch.setattr(uniref_module, "parse_uniref", mock_parse_uniref)

    run_uniref_pipeline(config)

    # parse_uniref was called once with the config to produce the resource
    mock_parse_uniref.assert_called_once_with(config)

    # the return value of parse_uniref(config) is what gets passed to pipeline.run
    expected_resource = mock_parse_uniref.return_value

    mock_dlt.destination.assert_called_once_with(config.destination)
    mock_dlt.pipeline.assert_called_once_with(
        destination=mock_dlt.destination.return_value,
        pipeline_name=f"uniref_{config.uniref_variant}",
        dataset_name="uniprot_kb",
    )
    mock_dlt.pipeline.return_value.run.assert_called_once_with(
        expected_resource,
        table_format="delta",
    )


def test_parse_uniref_resource(config: Settings) -> None:
    """Ensure that parse_uniref calls stream_xml_file_resource with the namespaced UniRef XML tag."""
    with patch.object(uniref_module, "stream_xml_file_resource") as mock_stream:
        mock_stream.return_value = iter([])
        list(parse_uniref(config))

    assert mock_stream.call_count == 1
    kwargs = mock_stream.call_args.kwargs
    assert kwargs.keys() == {"config", "xml_tag", "parse_fn", "log_interval"}
    assert kwargs["xml_tag"] == ENTRY_XML_TAG
    assert kwargs["log_interval"] == UNIREF_LOG_INTERVAL
    assert kwargs["config"] == config
    assert isinstance(kwargs["parse_fn"], Callable)


@pytest.mark.skip("FIXME: not working -- due to parallelization?")
def test_integration_cli_uniref_pipeline_output_validated(
    config: Settings,  # parametrized over UNIREF_VARIANTS via the existing fixture
    fake_files: list[Path],
    mock_dlt: MagicMock,
    patched_io: tuple[MagicMock, MagicMock],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Ensure that the full flow from cli() through launching the pipeline to yielding results is working correctly."""
    mock_batcher_cls, mock_stream = patched_io
    mock_batcher_cls.return_value = make_batcher(fake_files)

    def fake_stream_xml_file(file_path: Path, tag: str) -> list[dict[str, Any]]:
        """Fake streaming an XML file."""
        assert file_path in fake_files
        assert tag == ENTRY_XML_TAG
        return [{"stream_xml_file": file_path, "xml_tag": tag}]

    def fake_parse_uniref_entry(
        entry: dict[str, Any],
        timestamp: datetime.datetime,
        file_path: Path,
        uniref_variant: str,
    ) -> dict[str, list[Any]]:
        """Replacement for parse_uniref_entry; validates the injected label."""
        assert isinstance(timestamp, datetime.datetime)
        assert uniref_variant == f"UniRef {config.uniref_variant}"
        assert entry == {"stream_xml_file": file_path, "xml_tag": ENTRY_XML_TAG}
        return {"entry": [entry], "file_path": [file_path]}

    def fake_pipeline_run(resource, **kwargs) -> list:
        """Fake pipeline run that drains the resource generator to trigger all assertions."""
        assert isinstance(resource, Callable)  # after draining the generator, resource should be a list
        assert resource.name == "parse_uniref"  # the resource should be the output of parse_uniref
        assert kwargs == {"table_format": "delta"}
        output = list(resource)
        return output

    mock_stream.side_effect = fake_stream_xml_file
    monkeypatch.setattr(uniref_module, "parse_uniref_entry", fake_parse_uniref_entry)

    # make pipeline.run drain the generator so all assertions fire
    # mock_dlt.pipeline.return_value.run.side_effect = lambda resource, **_: list(resource)
    mock_dlt.pipeline.return_value.run.side_effect = fake_pipeline_run

    # exercise the real cli() wiring with Settings construction mocked out
    monkeypatch.setattr(uniref_module, "Settings", MagicMock(return_value=config))

    # RUN!
    cli()

    mock_dlt.pipeline.assert_called_once_with(
        pipeline_name=f"uniref_{config.uniref_variant}",
        destination=mock_dlt.destination.return_value,
        dataset_name="uniprot_kb",
    )
    mock_dlt.destination.assert_called_once_with(config.destination)
    mock_dlt.pipeline.return_value.run.assert_called_once()

    # verify with_table_name received the right rows and table names for every file, in order
    call_args_list = [list(c.args) for c in mock_dlt.mark.with_table_name.call_args_list]
    expected = []
    for f in fake_files:
        expected.extend(
            [
                [[{"stream_xml_file": f, "xml_tag": ENTRY_XML_TAG}], "entry"],
                [[f], "file_path"],
            ]
        )
    assert call_args_list == expected
