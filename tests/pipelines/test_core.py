"""Tests for the shared core DLT pipeline functions."""

import datetime
from collections.abc import Generator
from itertools import batched
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock, call, patch

import pytest
from pydantic import ValidationError
from pydantic_settings import SettingsError

from cdm_data_loaders.pipelines import core
from cdm_data_loaders.pipelines.core import run_cli, run_pipeline, stream_xml_file_resource
from cdm_data_loaders.pipelines.cts_defaults import (
    DEFAULT_BATCH_SIZE,
    BatchedFileInputSettings,
)


def make_settings(**kwargs: str | int) -> BatchedFileInputSettings:
    """Generate a validated BatchedFileInputSettings object."""
    return BatchedFileInputSettings.model_validate(kwargs)


@pytest.fixture(
    params=[
        pytest.param({"input_dir": "/fake/input"}, id="default"),
        pytest.param(
            {"input_dir": "/path/to/dir", "destination": "minio", "start_at": 15, "output": "/some/dir"},
            id="alt",
        ),
    ]
)
def config(request: pytest.FixtureRequest) -> BatchedFileInputSettings:
    """Parametrized fixture providing default and non-default settings."""
    return make_settings(**request.param)


@pytest.fixture
def default_config() -> BatchedFileInputSettings:
    """Minimal valid BatchedFileInputSettings (no start_at, no output)."""
    return make_settings(input_dir="/fake/input")


@pytest.fixture
def fake_files() -> list[Path]:
    """List of five files, used for testing."""
    return [Path(f"/fake/input/part_{n}.xml") for n in [1, 2, 3, 4, 5]]


@pytest.fixture
def mock_dlt(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    """Patch dlt in core, wiring pipeline.return_value to a fresh MagicMock."""
    mock = MagicMock()
    mock.pipeline.return_value = MagicMock()
    monkeypatch.setattr(core, "dlt", mock)
    return mock


@pytest.fixture
def patched_io() -> Generator[tuple[MagicMock | AsyncMock, MagicMock | AsyncMock], Any]:
    """Patch BatchCursor and stream_xml_file in core, yielding (mock_batcher_cls, mock_stream)."""
    with (
        patch("cdm_data_loaders.pipelines.core.BatchCursor") as mock_batcher_cls,
        patch("cdm_data_loaders.pipelines.core.stream_xml_file") as mock_stream,
    ):
        yield mock_batcher_cls, mock_stream


def make_batcher(files: list[Path], batch_size: int = 5) -> MagicMock:
    """Return a mock BatchCursor that yields ``files`` in batches then an empty list."""
    batches = [list(b) for b in batched(files, batch_size, strict=False)]
    mock_batcher = MagicMock()
    mock_batcher.get_batch.side_effect = [*batches, []]
    return mock_batcher


def assert_pipeline_run_correctly(
    mock_dlt: MagicMock,
    fake_resource: MagicMock,
    pipeline_name: str,
    dataset_name: str,
    destination: str,
) -> None:
    """Shared assertion block for run_pipeline tests."""
    assert mock_dlt.destination.call_args_list == [call(destination, max_table_nesting=0)]
    assert mock_dlt.pipeline.call_args_list == [
        call(
            pipeline_name=pipeline_name,
            destination=mock_dlt.destination.return_value,
            dataset_name=dataset_name,
        )
    ]
    mock_pipeline = mock_dlt.pipeline.return_value
    assert mock_pipeline.run.call_args_list == [call(fake_resource, table_format="delta")]


# run_cli tests
def test_run_cli_success() -> None:
    """Test that instantiating a settings object successfully allows the pipeline_fn to be run."""

    def pipeline_fn(config: Any) -> None:  # noqa: ANN401
        assert config == "fake config object"

    mock_settings_cls = MagicMock(return_value="fake config object")

    assert run_cli(mock_settings_cls, pipeline_fn) is None  # type: ignore[reportArgumentType]
    mock_settings_cls.assert_called_once_with()


@pytest.mark.parametrize(
    "error",
    [
        pytest.param(ValidationError("Oh no!", []), id="ValidationError"),
        pytest.param(SettingsError("something"), id="SettingsError"),
        pytest.param(TypeError("whatever"), id="TypeError"),
    ],
)
def test_run_cli_error(error: Exception) -> None:
    """Test that a settings instantiation failure re-raises without calling pipeline_fn."""
    mock_pipeline_fn = MagicMock()
    mock_settings_cls = MagicMock(side_effect=error)

    with pytest.raises(type(error), match=error.args[0]):
        run_cli(mock_settings_cls, mock_pipeline_fn)  # type: ignore[reportArgumentType]

    mock_pipeline_fn.assert_not_called()
    mock_settings_cls.assert_called_once_with()


# run_pipeline tests
def test_run_pipeline_no_output(default_config: BatchedFileInputSettings) -> None:
    """Ensure pipeline.run is called with correct args and dlt.config is not touched."""
    fake_resource = MagicMock()
    with patch("cdm_data_loaders.pipelines.core.dlt") as mock_dlt:
        mock_dlt.pipeline.return_value = MagicMock()
        run_pipeline(default_config, fake_resource, "pipeline_name", "dataset_name")

    mock_dlt.config.__setitem__.assert_not_called()
    assert_pipeline_run_correctly(mock_dlt, fake_resource, "pipeline_name", "dataset_name", default_config.destination)


def test_run_pipeline_custom_output_sets_dlt_config() -> None:
    """Ensure a non-empty output sets the correct dlt.config bucket_url key."""
    cfg = make_settings(output="/custom/output", destination="minio")
    fake_resource = MagicMock()

    with patch("cdm_data_loaders.pipelines.core.dlt") as mock_dlt:
        mock_dlt.pipeline.return_value = MagicMock()
        run_pipeline(cfg, fake_resource, "some pipeline", "some dataset")

    mock_dlt.config.__setitem__.assert_called_once_with("destination.minio.bucket_url", "/custom/output")
    assert_pipeline_run_correctly(mock_dlt, fake_resource, "some pipeline", "some dataset", "minio")


# stream_xml_file_resource tests
def test_stream_xml_file_resource_empty_batch_yields_nothing(
    config: BatchedFileInputSettings,
    patched_io: tuple[MagicMock, MagicMock],
    caplog: pytest.LogCaptureFixture,
) -> None:
    """No items yielded when BatchCursor returns an empty batch; BatchCursor receives correct args."""
    mock_batcher_cls, mock_stream = patched_io
    mock_batcher = MagicMock()
    mock_batcher.get_batch.return_value = []
    mock_batcher_cls.return_value = mock_batcher

    results = list(stream_xml_file_resource(config, "xml_tag", MagicMock()))

    assert results == []

    expected_batcher_kwargs = {"batch_size": DEFAULT_BATCH_SIZE}
    if config.start_at:
        expected_batcher_kwargs["start_at"] = config.start_at

    assert mock_batcher_cls.call_args_list == [call(config.input_dir, **expected_batcher_kwargs)]
    mock_batcher.get_batch.assert_called_once()
    mock_stream.assert_not_called()
    assert caplog.records == []


def test_stream_xml_file_resource_yields_items_for_each_table_in_parsed_entry(
    default_config: BatchedFileInputSettings,
    patched_io: tuple[MagicMock, MagicMock],
) -> None:
    """One item is yielded per table key returned by the parse function."""
    mock_batcher_cls, mock_stream = patched_io
    fake_file = Path("/fake/input/part1.xml")
    fake_entry = MagicMock()
    parsed_entry = {
        "table_1": [{"id": "A"}, {"id": "B"}],
        "table_2": [{"some_field": "some_value"}],
    }
    mock_batcher_cls.return_value = make_batcher([fake_file])
    mock_stream.return_value = [fake_entry]

    with patch("cdm_data_loaders.pipelines.core.dlt") as mock_dlt:
        mock_dlt.mark.with_table_name.return_value = object()
        results = list(stream_xml_file_resource(default_config, "xml_tag", MagicMock(return_value=parsed_entry)))

    assert len(results) == len(parsed_entry)
    actual_calls = [list(c.args) for c in mock_dlt.mark.with_table_name.call_args_list]
    assert len(actual_calls) == len(parsed_entry)
    for key, val in parsed_entry.items():
        assert [val, key] in actual_calls


def test_stream_xml_file_resource_parse_fn_correct_args(
    default_config: BatchedFileInputSettings,
    patched_io: tuple[MagicMock, MagicMock],
) -> None:
    """Ensure that parse_fn is called with (entry, timestamp, file_path) for every streamed XML entry."""
    mock_batcher_cls, mock_stream = patched_io
    fake_file = Path("/fake/input/part1.xml")
    xml_tag = "whatever"
    mock_stream_return = ["one", "two", "three"]
    mock_parse = MagicMock(return_value={})
    mock_batcher_cls.return_value = make_batcher([fake_file])
    mock_stream.return_value = mock_stream_return

    with patch("cdm_data_loaders.pipelines.core.dlt"):
        list(stream_xml_file_resource(default_config, xml_tag, mock_parse))

    assert mock_parse.call_count == len(mock_stream_return)
    for i, c in enumerate(mock_parse.call_args_list):
        assert c.kwargs["entry"] == mock_stream_return[i]
        assert isinstance(c.kwargs["timestamp"], datetime.datetime)
        assert c.kwargs["file_path"] == fake_file


@pytest.mark.parametrize("batch_size", [1, 2, 5])
def test_stream_xml_file_resource_processes_all_files_across_batches(
    fake_files: list[Path],
    batch_size: int,
    default_config: BatchedFileInputSettings,
    patched_io: tuple[MagicMock, MagicMock],
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Ensure that stream_xml_file is called for every file regardless of batch size; file reads are logged."""
    mock_batcher_cls, mock_stream = patched_io
    mock_batcher_cls.return_value = make_batcher(fake_files, batch_size)
    mock_stream.return_value = []

    with patch("cdm_data_loaders.pipelines.core.dlt"):
        list(stream_xml_file_resource(default_config, "some_tag", MagicMock()))

    assert mock_stream.call_args_list == [call(f, "some_tag") for f in fake_files]
    assert caplog.messages == [f"Reading from {f!s}" for f in fake_files]


def test_stream_xml_file_resource_multiple_batches_with_output(
    fake_files: list[Path],
    default_config: BatchedFileInputSettings,
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """End-to-end: generator processes all batches, parse output is passed to dlt.mark."""
    xml_tag = "some_tag"
    captured_output: dict[str, list[Any]] = {"entry": [], "file_path": []}

    def fake_stream_xml_file(file_path: Path, tag: str) -> list[dict[str, Any]]:
        assert file_path in fake_files
        assert tag == xml_tag
        return [{"file_path_from_stream_xml_file": file_path, "xml_tag": tag}]

    def fake_parse(entry: dict[str, Any], timestamp: datetime.datetime, file_path: Path) -> dict[str, list[Any]]:
        assert isinstance(timestamp, datetime.datetime)
        assert entry == {"file_path_from_stream_xml_file": file_path, "xml_tag": xml_tag}
        return {"entry": [entry], "file_path": [file_path]}

    def store_table_output(rows: list[dict[str, Any]], table: str) -> None:
        captured_output[table].extend(rows)

    monkeypatch.setattr(core, "stream_xml_file", fake_stream_xml_file)

    with (
        patch("cdm_data_loaders.pipelines.core.BatchCursor") as mock_batcher_cls,
        patch("cdm_data_loaders.pipelines.core.dlt") as mock_dlt,
    ):
        mock_batcher_cls.return_value = make_batcher(fake_files, batch_size=2)
        mock_dlt.mark.with_table_name.side_effect = store_table_output

        output = list(stream_xml_file_resource(default_config, xml_tag, fake_parse))

    assert len(output) == len(fake_files) * 2
    assert caplog.messages == [f"Reading from {f!s}" for f in fake_files]
    assert captured_output["file_path"] == fake_files
    assert captured_output["entry"] == [{"file_path_from_stream_xml_file": f, "xml_tag": xml_tag} for f in fake_files]


# test stream_xml_file_resource + run_pipeline
def test_integration_resource_and_pipeline_with_table_name_output_validated(
    default_config: BatchedFileInputSettings,
    fake_files: list[Path],
    mock_dlt: MagicMock,
    patched_io: tuple[MagicMock, MagicMock],
) -> None:
    """Run test_stream_xml_file_resource_multiple_batches_with_output within the run_pipeline method."""
    mock_batcher_cls, mock_stream = patched_io
    mock_batcher_cls.return_value = make_batcher(fake_files)
    xml_tag = "entry"

    def fake_stream_xml_file(file_path: Path, tag: str) -> list[dict[str, Any]]:
        assert file_path in fake_files
        assert tag == xml_tag
        return [{"file_path_from_stream_xml_file": file_path, "xml_tag": tag}]

    def fake_parse(entry: dict[str, Any], timestamp: datetime.datetime, file_path: Path) -> dict[str, list[Any]]:
        assert isinstance(timestamp, datetime.datetime)
        assert entry == {"file_path_from_stream_xml_file": file_path, "xml_tag": "entry"}
        return {"entry": [entry], "file_path": [file_path]}

    # use the fake_stream_xml_file function to mock the output of stream_xml_file
    mock_stream.side_effect = fake_stream_xml_file

    # make pipeline.run execute the stream_xml_file_resource generator
    mock_dlt.pipeline.return_value.run.side_effect = lambda resource, **_: list(resource)

    resource = stream_xml_file_resource(default_config, "entry", fake_parse)
    run_pipeline(default_config, resource, "test_pipeline", "test_dataset")

    mock_dlt.pipeline.return_value.run.assert_called_once()
    assert mock_dlt.pipeline.return_value.run.call_args_list == [call(resource, table_format="delta")]

    call_args_list = [list(c.args) for c in mock_dlt.mark.with_table_name.call_args_list]
    expected = []
    for f in fake_files:
        expected.extend(
            [
                [[{"file_path_from_stream_xml_file": f, "xml_tag": "entry"}], "entry"],
                [[f], "file_path"],
            ]
        )
    assert call_args_list == expected


# test run_cli + stream_xml_file_resource + run_pipeline
def test_integration_cli_resource_and_pipeline_with_table_name_output_validated(
    default_config: BatchedFileInputSettings,
    fake_files: list[Path],
    mock_dlt: MagicMock,
    patched_io: tuple[MagicMock, MagicMock],
) -> None:
    """Run test_stream_xml_file_resource_multiple_batches_with_output within the run_pipeline method."""
    mock_batcher_cls, mock_stream = patched_io
    mock_batcher_cls.return_value = make_batcher(fake_files)
    xml_tag = "entry"

    def fake_stream_xml_file(file_path: Path, tag: str) -> list[dict[str, Any]]:
        assert file_path in fake_files
        assert tag == xml_tag
        return [{"file_path_from_stream_xml_file": file_path, "xml_tag": tag}]

    def fake_parse(entry: dict[str, Any], timestamp: datetime.datetime, file_path: Path) -> dict[str, list[Any]]:
        assert isinstance(timestamp, datetime.datetime)
        assert entry == {"file_path_from_stream_xml_file": file_path, "xml_tag": xml_tag}
        return {"entry": [entry], "file_path": [file_path]}

    # use the fake_stream_xml_file function to mock the output of stream_xml_file
    mock_stream.side_effect = fake_stream_xml_file

    # make pipeline.run execute the stream_xml_file_resource generator
    mock_dlt.pipeline.return_value.run.side_effect = lambda resource, **_: list(resource)

    def pipeline_fn(cfg: BatchedFileInputSettings) -> None:
        """Fake pipeline function."""
        resource = stream_xml_file_resource(cfg, xml_tag, fake_parse)
        run_pipeline(cfg, resource, "test_pipeline_name", "test_dataset_name")

    run_cli(MagicMock(return_value=default_config), pipeline_fn)  # type: ignore[reportArgumentType]

    mock_dlt.pipeline.assert_called_once_with(
        pipeline_name="test_pipeline_name",
        destination=mock_dlt.destination.return_value,
        dataset_name="test_dataset_name",
    )
    mock_dlt.pipeline.return_value.run.assert_called_once()
    # check the output to with_table_name is correct for every file and both tables
    call_args_list = [list(c.args) for c in mock_dlt.mark.with_table_name.call_args_list]
    expected = []
    for f in fake_files:
        expected.extend(
            [
                [[{"file_path_from_stream_xml_file": f, "xml_tag": xml_tag}], "entry"],
                [[f], "file_path"],
            ]
        )
    assert call_args_list == expected


def test_integration_resource_and_pipeline_custom_output(
    mock_dlt: MagicMock,
    patched_io: tuple[MagicMock, MagicMock],
) -> None:
    """Ensure that when output is set, dlt.config bucket_url is written and pipeline.run still executes."""
    cfg = make_settings(output="/custom/output", destination="minio")
    mock_batcher_cls, _ = patched_io
    mock_batcher_cls.return_value.get_batch.return_value = []

    resource = stream_xml_file_resource(cfg, "entry", MagicMock(return_value={}))
    run_pipeline(cfg, resource, "p", "d")

    mock_dlt.config.__setitem__.assert_called_once_with("destination.minio.bucket_url", "/custom/output")
    mock_dlt.pipeline.return_value.run.assert_called_once()


def test_integration_empty_input_dir_pipeline_run_still_called(
    default_config: BatchedFileInputSettings,
    mock_dlt: MagicMock,
    patched_io: tuple[MagicMock, MagicMock],
) -> None:
    """Ensure that pipeline.run is called even when the resource generator yields nothing."""
    mock_batcher_cls, mock_stream = patched_io
    mock_batcher_cls.return_value.get_batch.return_value = []

    resource = stream_xml_file_resource(default_config, "entry", MagicMock(return_value={}))
    run_pipeline(default_config, resource, "p", "d")

    mock_stream.assert_not_called()
    mock_dlt.mark.with_table_name.assert_not_called()
    mock_dlt.pipeline.return_value.run.assert_called_once()


def test_integration_full_pipeline_config_propagated(
    config: BatchedFileInputSettings,
    patched_io: tuple[MagicMock, MagicMock],
    mock_dlt: MagicMock,
) -> None:
    """The exact config object from run_cli reaches stream_xml_file_resource unchanged."""
    mock_batcher_cls, _ = patched_io
    mock_batcher_cls.return_value.get_batch.return_value = []

    received: list[BatchedFileInputSettings] = []

    def pipeline_fn(inner_cfg: BatchedFileInputSettings) -> None:
        received.append(inner_cfg)
        resource = stream_xml_file_resource(inner_cfg, "entry", MagicMock(return_value={}))
        run_pipeline(inner_cfg, resource, "p", "d")

    run_cli(MagicMock(return_value=config), pipeline_fn)  # type: ignore[reportArgumentType]

    assert len(received) == 1
    assert received[0] == config
    mock_dlt.pipeline.return_value.run.assert_called_once()
