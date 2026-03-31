"""Tests for the UniRef DLT pipeline."""

import datetime
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from pydantic import ValidationError
from pydantic_settings import CliApp

from cdm_data_loaders.pipelines.cts_defaults import INPUT_MOUNT
from cdm_data_loaders.pipelines.uniref import (
    DEFAULT_BATCH_SIZE,
    Settings,
    parse_uniref,
    run_pipeline,
    UNIREF_URL,
    UNIREF_VARIANTS,
)

VALID_DESTINATIONS = ["local_fs", "minio"]
TEST_DEFAULT_UNIREF_VARIANT = "50"


def make_settings(
    extra_argv: list[str] | None = None, *, uniref_variant: str = TEST_DEFAULT_UNIREF_VARIANT, **kwargs
) -> Settings:
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
        start_at="50",
        output="/some/dir",
    )
    assert s.input_dir == "/dir/path"
    assert s.destination == VALID_DESTINATIONS[0]
    assert s.uniref_variant == "100"
    assert s.start_at == 50
    assert s.output == "/some/dir"


@pytest.mark.parametrize("destination", VALID_DESTINATIONS)
@pytest.mark.parametrize("uniref_variant", UNIREF_VARIANTS)
def test_settings_valid_variants_accepted(uniref_variant: str, destination: str) -> None:
    """Ensure that each valid uniref_variant value is accepted without error."""
    s = make_settings(uniref_variant=uniref_variant, destination=destination)
    assert s.uniref_variant == uniref_variant
    assert s.destination == destination


@pytest.mark.parametrize("bad", ["25", "75", "uniref50", "", "ALL"])
def test_invalid_variant_raises(bad: str) -> None:
    """Ensure that an unrecognised uniref_variant raises a ValidationError."""
    with pytest.raises(ValidationError, match="uniref_variant must be one of"):
        make_settings(uniref_variant=bad)


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
            "50",
            output,
            "/some/dir",
        ]
    )
    assert s.input_dir == "/dir/path"
    assert s.destination == VALID_DESTINATIONS[0]
    assert s.uniref_variant == TEST_DEFAULT_UNIREF_VARIANT
    assert s.start_at == 50
    assert s.output == "/some/dir"


def test_cli_invalid_variant_via_cli_raises() -> None:
    """Ensure that an invalid uniref_variant passed via CLI causes a SystemExit."""
    with pytest.raises(ValidationError, match="Value error, uniref_variant must be one of"):
        _cliapp_run(["--uniref-variant", "999"])


def test_cli_invalid_destination_via_cli_raises() -> None:
    """Ensure that an invalid destination passed via CLI causes a SystemExit."""
    with pytest.raises(ValidationError, match="Value error, destination must be one of"):
        _cliapp_run(["--uniref-variant", "50", "--destination", "s3"])


def test_cli_missing_required_uniref_variant_raises() -> None:
    """Ensure that omitting the required uniref_variant argument causes a SystemExit."""
    with pytest.raises(ValidationError, match="Field required"):
        _cliapp_run([])


def _collect(config: Settings):
    """Drain the generator returned by parse_uniref."""
    return list(parse_uniref(config))


def test_empty_batch_yields_nothing(config: Settings) -> None:
    """Ensure that no items are yielded when BatchCursor returns an empty batch."""
    with patch("cdm_data_loaders.pipelines.uniref.BatchCursor") as mock_batcher_cls:
        mock_batcher = MagicMock()
        mock_batcher.get_batch.return_value = []
        mock_batcher_cls.return_value = mock_batcher

        results = _collect(config)

    assert results == []


def test_start_at_zero_not_passed_to_batch_cursor(config: Settings) -> None:
    """Ensure that start_at=0 (falsy) is not forwarded as a kwarg to BatchCursor."""
    with patch("cdm_data_loaders.pipelines.uniref.BatchCursor") as mock_batcher_cls:
        mock_batcher = MagicMock()
        mock_batcher.get_batch.return_value = []
        mock_batcher_cls.return_value = mock_batcher

        _collect(config)

    _, kwargs = mock_batcher_cls.call_args
    assert "start_at" not in kwargs


def test_start_at_nonzero_passed_to_batch_cursor() -> None:
    """Ensure that a non-zero start_at value is forwarded as a kwarg to BatchCursor."""
    start_at = 2
    config_with_start: Settings = make_settings(
        uniref_variant="90",
        input_dir="/fake/input",
        start_at=start_at,
    )
    with patch("cdm_data_loaders.pipelines.uniref.BatchCursor") as mock_batcher_cls:
        mock_batcher = MagicMock()
        mock_batcher.get_batch.return_value = []
        mock_batcher_cls.return_value = mock_batcher

        _collect(config_with_start)

    _, kwargs = mock_batcher_cls.call_args
    assert kwargs.get("start_at") == start_at


def test_batch_cursor_receives_correct_input_dir_and_batch_size(config: Settings) -> None:
    """Ensure BatchCursor is constructed with the configured input_dir and DEFAULT_BATCH_SIZE."""
    with patch("cdm_data_loaders.pipelines.uniref.BatchCursor") as mock_batcher_cls:
        mock_batcher = MagicMock()
        mock_batcher.get_batch.return_value = []
        mock_batcher_cls.return_value = mock_batcher

        _collect(config)

    args, kwargs = mock_batcher_cls.call_args
    assert args[0] == "/fake/input"
    assert kwargs.get("batch_size") == DEFAULT_BATCH_SIZE


def test_yields_items_for_each_table_in_parsed_entry(config: Settings) -> None:
    """Ensure one item is yielded per table key returned by parse_uniref_entry."""
    fake_file = Path("/fake/input/uniref50_part1.xml")
    fake_entry = MagicMock()
    parsed_entry = {
        "uniref_member": [{"id": "A"}, {"id": "B"}],
        "uniref_cluster": [{"cluster_id": "UniRef50_A0A000"}],
    }

    with (
        patch("cdm_data_loaders.pipelines.uniref.BatchCursor") as mock_batcher_cls,
        patch("cdm_data_loaders.pipelines.uniref.stream_xml_file") as mock_stream,
        patch("cdm_data_loaders.pipelines.uniref.parse_uniref_entry") as mock_parse,
        patch("cdm_data_loaders.pipelines.uniref.dlt") as mock_dlt,
    ):
        mock_batcher = MagicMock()
        mock_batcher.get_batch.side_effect = [[fake_file], []]
        mock_batcher_cls.return_value = mock_batcher
        mock_stream.return_value = [fake_entry]
        mock_parse.return_value = parsed_entry
        mock_dlt.mark.with_table_name.return_value = object()

        results = _collect(config)

    assert len(results) == len(parsed_entry)
    assert mock_dlt.mark.with_table_name.call_count == 2


def test_parse_uniref_entry_called_with_correct_args(config: Settings) -> None:
    """Ensure parse_uniref_entry is called with the entry, timestamp, dataset label, and file path."""
    fake_file = Path("/fake/input/uniref50_part1.xml")
    mock_stream_return = ["one", "two", "three"]
    with (
        patch("cdm_data_loaders.pipelines.uniref.BatchCursor") as mock_batcher_cls,
        patch("cdm_data_loaders.pipelines.uniref.stream_xml_file") as mock_stream,
        patch("cdm_data_loaders.pipelines.uniref.parse_uniref_entry") as mock_parse,
        patch("cdm_data_loaders.pipelines.uniref.dlt"),
    ):
        mock_batcher = MagicMock()
        mock_batcher.get_batch.side_effect = [[fake_file], []]
        mock_batcher_cls.return_value = mock_batcher
        mock_stream.return_value = mock_stream_return
        mock_parse.return_value = {}

        _collect(config)

    assert mock_parse.call_count == len(mock_stream_return)
    call_args = [list(c[0]) for c in mock_parse.call_args_list]
    for idx, ca in enumerate(call_args):
        assert isinstance(ca[1], datetime.datetime)
        assert ca[0] == mock_stream_return[idx]
        assert ca[2] == "UniRef 50"
        assert ca[3] == fake_file


def test_multiple_files_in_batch_are_all_processed(config: Settings) -> None:
    """Ensure every file in a batch is passed to stream_xml_file."""
    files = [Path(f"/fake/input/part{i}.xml") for i in range(3)]

    with (
        patch("cdm_data_loaders.pipelines.uniref.BatchCursor") as mock_batcher_cls,
        patch("cdm_data_loaders.pipelines.uniref.stream_xml_file") as mock_stream,
        patch("cdm_data_loaders.pipelines.uniref.parse_uniref_entry") as mock_parse,
        patch("cdm_data_loaders.pipelines.uniref.dlt"),
    ):
        mock_batcher = MagicMock()
        mock_batcher.get_batch.side_effect = [files, []]
        mock_batcher_cls.return_value = mock_batcher
        mock_stream.return_value = []
        mock_parse.return_value = {}

        _collect(config)

    assert mock_stream.call_count == 3


def test_multiple_batches_are_consumed(config: Settings) -> None:
    """Ensure the generator continues processing until BatchCursor returns an empty batch."""
    fake_files = [Path(f"/fake/input/part_{n}.xml") for n in [1, 2, 3, 4, 5]]
    batch1 = fake_files[0:2]
    batch2 = fake_files[2:4]
    batch3 = fake_files[4:]

    with (
        patch("cdm_data_loaders.pipelines.uniref.BatchCursor") as mock_batcher_cls,
        patch("cdm_data_loaders.pipelines.uniref.stream_xml_file") as mock_stream,
        patch("cdm_data_loaders.pipelines.uniref.parse_uniref_entry") as mock_parse,
        patch("cdm_data_loaders.pipelines.uniref.dlt"),
    ):
        mock_batcher = MagicMock()
        mock_batcher.get_batch.side_effect = [batch1, batch2, batch3, []]
        mock_batcher_cls.return_value = mock_batcher
        mock_stream.return_value = []
        mock_parse.return_value = {}

        _collect(config)
    call_args = [list(c[0]) for c in mock_stream.call_args_list]
    assert call_args == [[f, f"{{{UNIREF_URL}}}entry"] for f in fake_files]


"""Smoke tests for run_pipeline, verifying pipeline construction and execution."""


@pytest.fixture
def config() -> Settings:
    """Provide a minimal valid Settings object."""
    return make_settings(uniref_variant="50", input_dir="/fake/input")


def test_pipeline_is_executed(config: Settings) -> None:
    """Ensure pipeline.run is called when run_pipeline is invoked."""
    with (
        patch("cdm_data_loaders.pipelines.uniref.dlt") as mock_dlt,
        patch("cdm_data_loaders.pipelines.uniref.parse_uniref"),
    ):
        mock_pipeline = MagicMock()
        mock_dlt.pipeline.return_value = mock_pipeline

        run_pipeline(config)

    mock_pipeline.run.assert_called_once()


def test_custom_output_sets_dlt_config() -> None:
    """Ensure a non-empty output sets the correct dlt.config bucket_url key."""
    config = make_settings(uniref_variant="50", output="/custom/output", destination="minio")

    with (
        patch("cdm_data_loaders.pipelines.uniref.dlt") as mock_dlt,
        patch("cdm_data_loaders.pipelines.uniref.parse_uniref"),
    ):
        mock_pipeline = MagicMock()
        mock_dlt.pipeline.return_value = mock_pipeline

        run_pipeline(config)

    mock_dlt.config.__setitem__.assert_called_once_with("destination.minio.bucket_url", "/custom/output")


def test_no_custom_output_does_not_set_dlt_config(config: Settings) -> None:
    """Ensure that an empty output does not mutate dlt.config."""
    with (
        patch("cdm_data_loaders.pipelines.uniref.dlt") as mock_dlt,
        patch("cdm_data_loaders.pipelines.uniref.parse_uniref"),
    ):
        mock_pipeline = MagicMock()
        mock_dlt.pipeline.return_value = mock_pipeline

        run_pipeline(config)

    mock_dlt.config.__setitem__.assert_not_called()


def test_pipeline_name_includes_uniref_variant(config: Settings) -> None:
    """Ensure the pipeline is created with a name derived from the uniref_variant and the correct dataset_name."""
    with (
        patch("cdm_data_loaders.pipelines.uniref.dlt") as mock_dlt,
        patch("cdm_data_loaders.pipelines.uniref.parse_uniref"),
    ):
        mock_dlt.pipeline.return_value = MagicMock()
        run_pipeline(config)

    _, kwargs = mock_dlt.pipeline.call_args
    assert kwargs["pipeline_name"] == "uniref_50"
    assert kwargs["dataset_name"] == "uniprot_kb"
