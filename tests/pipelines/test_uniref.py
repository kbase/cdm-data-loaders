"""Tests for the uniref DLT pipeline."""

from collections.abc import Callable
from unittest.mock import patch

import pytest
from pydantic import ValidationError
from pydantic_settings import CliApp

from cdm_data_loaders.parsers.uniprot.uniref import ENTRY_XML_TAG, UNIREF_VARIANTS
from cdm_data_loaders.pipelines import uniref as uniref_module
from cdm_data_loaders.pipelines.cts_defaults import INPUT_MOUNT, VALID_DESTINATIONS
from cdm_data_loaders.pipelines.uniref import UNIREF_LOG_INTERVAL, Settings, cli, parse_uniref, run_uniref_pipeline

# TODO: add a test to ensure that parse_uniref_entry is called with the appropriate args. Requires mocking the file batcher and stream_xml_file_resource functions.


START_AT_VALUE = 25
START_AT_STRING = "25"


def make_settings_with_variant(variant: str = "UniRef90", **kwargs: str | int) -> Settings:
    """Return a validated Settings object with sensible defaults."""
    return Settings.model_validate({"input_dir": "/fake/input", "uniref_variant": variant, **kwargs})


@pytest.fixture(params=UNIREF_VARIANTS)
def variant(request: pytest.FixtureRequest) -> str:
    """Parametrized fixture over all valid uniref variants."""
    return request.param


@pytest.fixture
def config(variant: str) -> Settings:
    """A valid Settings object for each uniref variant."""
    return make_settings_with_variant(variant=variant)


TEST_DEFAULT_UNIREF_VARIANT = "50"


def make_settings(uniref_variant: str = TEST_DEFAULT_UNIREF_VARIANT, **kwargs: str | int) -> Settings:
    """Generate a validated Settings object."""
    data = {"uniref": uniref_variant, **kwargs}
    return Settings.model_validate(data)


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


def _cliapp_run(cli_args: list[str]) -> Settings:
    """Tests that Settings correctly parses command-line arguments via CliApp.

    Uses CliApp.run with explicit cli_args to avoid mutating sys.argv globally.
    """
    return CliApp.run(Settings, cli_args=cli_args)


@pytest.mark.parametrize("input_dir", ["-i", "--input-dir", "--input_dir"])
@pytest.mark.parametrize("destination", ["-d", "--destination"])
@pytest.mark.parametrize("uniref_variant", ["-u", "--uniref", "--uniref-variant", "--uniref_variant"])
@pytest.mark.parametrize("start_at", ["-s", "--start-at", "--start_at"])
@pytest.mark.parametrize("output", ["-o", "--output"])
def test_cli_all_variants(input_dir: str, destination: str, uniref_variant: str, start_at: str, output: str) -> None:
    """Test all the variants of the Settings fields."""
    s = _cliapp_run(
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
        ]
    )
    assert s.input_dir == "/dir/path"
    assert s.destination == VALID_DESTINATIONS[0]
    assert s.uniref_variant == TEST_DEFAULT_UNIREF_VARIANT
    assert s.start_at == START_AT_VALUE
    assert s.output == "/some/dir"


@pytest.mark.parametrize("bad_variant", ["25", "75", "uniref50", "", "ALL"])
def test_cli_invalid_variant_via_cli_raises(bad_variant: str) -> None:
    """Ensure that an invalid uniref_variant passed via CLI causes a SystemExit."""
    with pytest.raises(ValidationError, match="Value error, uniref_variant must be one of"):
        _cliapp_run(["--uniref-variant", bad_variant])


@pytest.mark.parametrize("bad_destination", ["25", "75", "uniref50", "", "ALL"])
def test_cli_invalid_destination_via_cli_raises(bad_destination: str) -> None:
    """Ensure that an invalid destination passed via CLI causes a SystemExit."""
    with pytest.raises(ValidationError, match="Value error, destination must be one of"):
        _cliapp_run(["--uniref-variant", "50", "--destination", bad_destination])


def test_cli_missing_required_uniref_variant_raises() -> None:
    """Ensure that omitting the required uniref_variant argument causes a SystemExit."""
    with pytest.raises(ValidationError, match="Field required"):
        _cliapp_run([])


@pytest.mark.parametrize("bad_variant", ["25", "75", "uniref50", "", "ALL"])
@pytest.mark.parametrize("bad_destination", ["s3", "gcs", "filesystem", "", "LocalFs"])
def test_cli_invalid_variant_and_destination_via_cli_raises(bad_variant: str, bad_destination: str) -> None:
    """Ensure that invalid uniref_variant and destination passed via CLI causes a SystemExit with both errors."""
    with pytest.raises(ValidationError, match="2 validation errors for Settings") as exc_info:
        _cliapp_run(["--uniref-variant", bad_variant, "--destination", bad_destination])

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


def test_run_uniref_pipeline_args_set_correctly(config: Settings) -> None:
    """Ensure that the pipeline arguments are set correctly, and each pipeline has a different name."""
    with patch.object(uniref_module, "run_pipeline") as mock_run_pipeline:
        run_uniref_pipeline(config)

    assert mock_run_pipeline.call_count == 1
    _, kwargs = mock_run_pipeline.call_args
    assert kwargs.keys() == {"config", "resource", "pipeline_name", "dataset_name"}
    assert kwargs["pipeline_name"] == f"uniref_{config.uniref_variant}"
    assert kwargs["dataset_name"] == "uniprot_kb"
    assert kwargs["config"] == config
    assert isinstance(kwargs["resource"], Callable)


def test_parse_uniref_resource(config: Settings) -> None:
    """Ensure that parse_uniref calls stream_xml_file_resource with the namespaced UniRef XML tag."""
    with patch.object(uniref_module, "stream_xml_file_resource") as mock_resource:
        mock_resource.return_value = iter([])
        list(parse_uniref(config))

    assert mock_resource.call_count == 1
    kwargs = mock_resource.call_args.kwargs
    assert kwargs.keys() == {"config", "xml_tag", "parse_fn", "log_interval"}
    assert kwargs["xml_tag"] == ENTRY_XML_TAG
    assert kwargs["log_interval"] == UNIREF_LOG_INTERVAL
    assert kwargs["config"] == config
    assert isinstance(kwargs["parse_fn"], Callable)
