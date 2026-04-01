"""Tests for the uniprot_kb DLT pipeline."""

import pytest
from pydantic import ValidationError
from pydantic_settings import CliApp

from cdm_data_loaders.pipelines.cts_defaults import (
    INPUT_MOUNT,
    VALID_DESTINATIONS,
    BatchedFileInputSettings,
)

START_AT_VALUE = 50
START_AT_STRING = "50"


def make_settings(**kwargs: str | int) -> BatchedFileInputSettings:
    """Generate a validated Settings object."""
    return BatchedFileInputSettings.model_validate(kwargs)


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


def _cliapp_run(cli_args: list[str]) -> BatchedFileInputSettings:
    """Tests that Settings correctly parses command-line arguments via CliApp.

    Uses CliApp.run with explicit cli_args to avoid mutating sys.argv globally.
    """
    return CliApp.run(BatchedFileInputSettings, cli_args=cli_args)


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


@pytest.mark.parametrize("bad", ["s3", "gcs", "filesystem", "", "LocalFs"])
def test_cli_invalid_destination_via_cli_raises(bad: str) -> None:
    """Ensure that an invalid destination passed via CLI causes a SystemExit."""
    with pytest.raises(ValidationError, match="Value error, destination must be one of"):
        _cliapp_run(["--destination", bad])
