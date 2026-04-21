"""Tests for the shared core DLT pipeline functions."""

import datetime
import logging
import os
from collections.abc import Generator
from copy import deepcopy
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, call, patch

import pytest
from pydantic import ValidationError
from pydantic_settings import SettingsError

from cdm_data_loaders.pipelines import core
from cdm_data_loaders.pipelines.core import (
    construct_env_var,
    run_cli,
    run_pipeline,
    stream_xml_file_resource,
    sync_configs,
)
from cdm_data_loaders.pipelines.cts_defaults import (
    DEFAULT_BATCH_SIZE,
    VALID_DESTINATIONS,
    BatchedFileInputSettings,
    CtsSettings,
)
from tests.pipelines.conftest import TEST_CTS_SETTINGS, _generate_dlt_config, make_batcher
from tests.pipelines.test_cts_defaults import SETTINGS_CLASSES


@pytest.fixture(autouse=True)
def mock_send_slack_message(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    """Patch send_slack_message in core to prevent undue slack notifications."""
    slack_mock = MagicMock()
    monkeypatch.setattr(core, "send_slack_message", slack_mock)
    return slack_mock


@pytest.fixture(autouse=True)
def patch_dlt_config(dlt_config: dict[str, Any], monkeypatch: pytest.MonkeyPatch) -> None:
    """Monkeypatch the dlt config in all tests."""
    monkeypatch.setattr(core.dlt, "config", dlt_config)


def make_batched_settings(**kwargs: str | int) -> BatchedFileInputSettings:
    """Generate a validated BatchedFileInputSettings object with a valid dlt config."""
    return BatchedFileInputSettings.model_validate({"dlt_config": _generate_dlt_config(), **kwargs})


@pytest.fixture
def test_bfi_settings(tmp_path: Path) -> BatchedFileInputSettings:
    """Minimal valid BatchedFileInputSettings (no start_at, no output)."""
    return make_batched_settings(input_dir="/fake/input", output=str(tmp_path))


@pytest.fixture
def test_cts_settings() -> CtsSettings:
    """A fully validated CtsSettings instance using the test dlt config."""
    return CtsSettings.model_validate({**TEST_CTS_SETTINGS, "dlt_config": _generate_dlt_config()})


@pytest.fixture(
    params=[
        pytest.param({"input_dir": "/fake/input"}, id="default"),
        pytest.param(
            {"input_dir": "/path/to/dir", "use_destination": "s3", "start_at": 15, "output": "/some/dir"},
            id="alt",
        ),
    ]
)
def config(request: pytest.FixtureRequest) -> BatchedFileInputSettings:
    """Parametrized fixture providing default and non-default settings."""
    return make_batched_settings(**request.param)


def assert_pipeline_run_correctly(  # noqa: PLR0913
    mock_dlt: MagicMock,
    fake_resource: MagicMock,
    destination: str,
    destination_kwargs: dict[str, Any] | None,
    pipeline_kwargs: dict[str, Any] | None,
    pipeline_run_kwargs: dict[str, Any] | None,
) -> None:
    """Shared assertion block for run_pipeline tests."""
    assert mock_dlt.destination.call_args_list == [call(destination, **destination_kwargs or {})]
    assert mock_dlt.pipeline.call_args_list == [
        call(destination=mock_dlt.destination.return_value, **pipeline_kwargs or {})
    ]
    mock_pipeline = mock_dlt.pipeline.return_value
    assert mock_pipeline.run.call_args_list == [call(fake_resource, **pipeline_run_kwargs or {})]


# construct_env_var
@pytest.mark.parametrize(
    ("b_var", "t_var", "char_str", "expected"),
    [
        ("B123", "T456", "C789", "https://hooks.slack.com/services/B123/T456/C789/"),
        ("one", "two", "three", "https://hooks.slack.com/services/one/two/three/"),
    ],
)
def test_construct_env_var_sets_runtime_slack_incoming_hook(
    b_var: str, t_var: str, char_str: str, expected: str
) -> None:
    """Test successful setting of the RUNTIME__SLACK_INCOMING_HOOK env var when all three variables are present."""
    with patch.dict(
        os.environ,
        {
            "VARIABLE_B": b_var,
            "VARIABLE_T": t_var,
            "CHAR_STR": char_str,
        },
        clear=True,
    ):
        # function returns None
        assert construct_env_var() is None
        assert os.environ["RUNTIME__SLACK_INCOMING_HOOK"] == expected


@pytest.mark.parametrize(
    ("b_var", "t_var", "char_str"),
    [
        (None, "T456", "C789"),
        ("B123", None, "C789"),
        ("B123", "T456", None),
        (None, None, None),
        ("", "T456", "C789"),
    ],
)
def test_construct_env_var_does_not_set_when_any_variable_missing(
    b_var: str | None, t_var: str | None, char_str: str | None
) -> None:
    """Test unsuccessful interpolation of env vars if any of the three variables are missing or empty."""
    env = {}
    if b_var is not None:
        env["VARIABLE_B"] = b_var
    if t_var is not None:
        env["VARIABLE_T"] = t_var
    if char_str is not None:
        env["CHAR_STR"] = char_str

    with patch.dict(os.environ, env, clear=True):
        assert construct_env_var() is None
        assert "RUNTIME__SLACK_INCOMING_HOOK" not in os.environ


def test_construct_env_var_overwrites_existing_hook() -> None:
    """An existing RUNTIME__SLACK_INCOMING_HOOK is overwritten when all vars are present."""
    with patch.dict(
        os.environ,
        {
            "VARIABLE_B": "B",
            "VARIABLE_T": "T",
            "CHAR_STR": "C",
            "RUNTIME__SLACK_INCOMING_HOOK": "https://old.hook/",
        },
        clear=True,
    ):
        assert construct_env_var() is None
        assert os.environ["RUNTIME__SLACK_INCOMING_HOOK"] == "https://hooks.slack.com/services/B/T/C/"


# sync_configs
def test_sync_configs_mutates_dlt_config_in_place(dlt_config: dict[str, Any], test_cts_settings: CtsSettings) -> None:
    """sync_configs mutates the supplied dlt_config dict in-place."""
    original_id = id(dlt_config)
    sync_configs(test_cts_settings, dlt_config)
    assert id(dlt_config) == original_id


def test_sync_configs_with_mock_dlt_config_object(test_cts_settings: CtsSettings) -> None:
    """sync_configs works with any mapping that supports __setitem__."""
    mock_cfg = MagicMock()
    sync_configs(test_cts_settings, mock_cfg)
    mock_cfg.__setitem__.assert_any_call("normalize.data_writer.disable_compression", test_cts_settings.dev_mode)
    mock_cfg.__setitem__.assert_any_call(
        f"destination.{test_cts_settings.use_destination}.bucket_url",
        test_cts_settings.output,
    )


@pytest.mark.parametrize("use_destination", VALID_DESTINATIONS)
@pytest.mark.parametrize("dev_mode", [True, False])
@pytest.mark.parametrize("output", ["/some/path", "s3://bucket/whatever"])
def test_sync_configs_both_keys_set_in_single_call(
    dlt_config: dict[str, Any], test_cts_settings: CtsSettings, dev_mode: bool, use_destination: str, output: str
) -> None:
    """Test that sync_configs changes the disable_compression and bucket_url values."""
    original_dlt_config = deepcopy(dlt_config)
    test_cts_settings.dev_mode = dev_mode
    test_cts_settings.use_destination = use_destination
    test_cts_settings.output = output
    sync_configs(test_cts_settings, dlt_config)
    assert dlt_config == {
        **original_dlt_config,
        "normalize.data_writer.disable_compression": dev_mode,
        f"destination.{use_destination}.bucket_url": output,
    }


# tests for run_cli()
@pytest.mark.parametrize("settings_cls", SETTINGS_CLASSES)
def test_run_cli_calls_settings_cls_with_dlt_config(
    settings_cls: type[CtsSettings], dlt_config: dict[str, Any]
) -> None:
    """Ensure run_cli instantiates the supplied settings class with dlt.config."""
    captured: list[CtsSettings] = []
    original_cls = settings_cls

    class _CaptureCls(original_cls):  # type: ignore[valid-type]
        def __init__(self, **data: Any) -> None:  # noqa: ANN401
            super().__init__(**data)
            assert data == {"dlt_config": dlt_config}
            captured.append(self)

    run_cli(_CaptureCls, MagicMock())

    assert len(captured) == 1
    captured_config = captured[0]
    assert isinstance(captured_config, original_cls)
    # The object passed to sync_configs must be a fully initialised instance
    for attr in ["dev_mode", "input_dir", "output", "use_destination", "use_output_dir_for_pipeline_metadata"]:
        assert hasattr(captured_config, attr)


@pytest.mark.parametrize("settings_cls", SETTINGS_CLASSES)
def test_run_cli_function_calls_args(settings_cls: type[CtsSettings], dlt_config: dict[str, Any]) -> None:
    """Ensure run_cli calls sync_configs with the instantiated config and dlt.config."""
    instantiated_cls = settings_cls(dlt_config=dlt_config)
    pipeline_fn_mock = MagicMock()
    settings_cls_mock = MagicMock(return_value=instantiated_cls)

    with (
        patch("cdm_data_loaders.pipelines.core.construct_env_var") as mock_env_var,
        patch("cdm_data_loaders.pipelines.core.sync_configs") as mock_sync,
    ):
        run_cli(settings_cls_mock, pipeline_fn_mock)  # type: ignore[reportArgumentType]

    mock_env_var.assert_called_once_with()
    settings_cls_mock.assert_called_once_with(dlt_config=dlt_config)
    mock_sync.assert_called_once_with(instantiated_cls, dlt_config)
    pipeline_fn_mock.assert_called_once_with(instantiated_cls)


# error handling: SettingsError/ValidationError/ValueError
@pytest.mark.parametrize("settings_cls", SETTINGS_CLASSES)
def test_run_cli_reraises_settings_error(
    settings_cls: type[CtsSettings],
    caplog: pytest.LogCaptureFixture,
) -> None:
    """SettingsError is printed and re-raised."""
    err = SettingsError("bad CLI arg")

    with (
        patch.object(settings_cls, "__init__", side_effect=err),
        pytest.raises(SettingsError, match="bad CLI arg"),
    ):
        run_cli(settings_cls, MagicMock())

    log_records = caplog.records
    assert log_records[-1].levelno == logging.ERROR
    assert log_records[-1].message == "Error initialising config"


@pytest.mark.parametrize("settings_cls", SETTINGS_CLASSES)
@pytest.mark.parametrize(
    ("bad_dlt_config", "error", "err_msg"),
    [
        (None, ValidationError, "dlt_config must be defined"),
        ({}, ValueError, "No valid destinations found in dlt configuration"),
        ({"destination": {}}, ValueError, "No valid destinations found in dlt configuration"),
    ],
)
def test_run_cli_reraises_validation_errors(  # noqa: PLR0913
    settings_cls: type[CtsSettings],
    bad_dlt_config: None | dict[str, Any],
    error: type[Exception],
    err_msg: str,
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Ensure that errors in instantiating the configuration are re-raised.

    See also the cts_defaults test ``test_cli_app_run_dlt_config_errors``.
    """
    monkeypatch.setattr(core.dlt, "config", bad_dlt_config)
    with pytest.raises(error, match=err_msg):
        run_cli(settings_cls, MagicMock())

    log_records = caplog.records
    assert log_records[-1].levelno == logging.ERROR
    assert log_records[-1].message == "Error initialising config"


# error handling: unexpected Exception
@pytest.mark.parametrize("settings_cls", SETTINGS_CLASSES)
def test_run_cli_reraises_unexpected_exception(
    settings_cls: type[CtsSettings], caplog: pytest.LogCaptureFixture
) -> None:
    """Ensure that other exceptions are caught and re-raised."""
    boom = RuntimeError("disk on fire")
    mock_pipeline_fn = MagicMock()
    with (
        patch.object(settings_cls, "__init__", side_effect=boom),
        patch("cdm_data_loaders.pipelines.core.sync_configs") as mock_sync,
        pytest.raises(RuntimeError, match="disk on fire"),
    ):
        run_cli(settings_cls, mock_pipeline_fn)

    log_records = caplog.records
    assert log_records[-1].levelno == logging.ERROR
    assert log_records[-1].message == "Unexpected error setting up config"

    mock_sync.assert_not_called()
    mock_pipeline_fn.assert_not_called()


# sync_configs not called on error
@pytest.mark.parametrize(
    "exc",
    [
        SettingsError("bad"),
        ValueError("bad value"),
    ],
)
@pytest.mark.parametrize("settings_cls", SETTINGS_CLASSES)
def test_run_cli_sync_configs_not_called_on_settings_instantiation_error(
    exc: Exception,
    settings_cls: type[CtsSettings],
) -> None:
    """Ensure that further execution is stopped if settings instantiation fails."""
    mock_pipeline_fn = MagicMock()
    with (
        patch.object(settings_cls, "__init__", side_effect=exc),
        patch("cdm_data_loaders.pipelines.core.sync_configs") as mock_sync,
        pytest.raises(type(exc)),
    ):
        run_cli(settings_cls, mock_pipeline_fn)

    mock_sync.assert_not_called()
    mock_sync.assert_not_called()
    mock_pipeline_fn.assert_not_called()


def test_run_cli_uses_slack_env_var_if_set(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """RUNTIME__SLACK_INCOMING_HOOK built by construct_env_var is visible during run_cli."""
    monkeypatch.setenv("VARIABLE_B", "BBB")
    monkeypatch.setenv("VARIABLE_T", "TTT")
    monkeypatch.setenv("CHAR_STR", "CCC")

    run_cli(CtsSettings, MagicMock())

    expected = "https://hooks.slack.com/services/BBB/TTT/CCC/"
    assert os.environ.get("RUNTIME__SLACK_INCOMING_HOOK") == expected


def test_run_cli_no_slack_env_var_when_vars_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """RUNTIME__SLACK_INCOMING_HOOK is not set when source vars are absent."""
    for var in ("VARIABLE_B", "VARIABLE_T", "CHAR_STR", "RUNTIME__SLACK_INCOMING_HOOK"):
        monkeypatch.delenv(var, raising=False)

    run_cli(CtsSettings, MagicMock())

    assert "RUNTIME__SLACK_INCOMING_HOOK" not in os.environ


# dlt.config state after successful run
@pytest.mark.parametrize("settings_cls", SETTINGS_CLASSES)
@pytest.mark.parametrize("use_destination", VALID_DESTINATIONS)
@pytest.mark.parametrize("dev_mode", [True, False])
@pytest.mark.parametrize("output", ["/some/path", "s3://bucket/whatever"])
def test_run_cli_dlt_config_updated_after_success(
    dlt_config: dict[str, Any], settings_cls: type[CtsSettings], dev_mode: bool, use_destination: str, output: str
) -> None:
    """Test that sync_configs changes the disable_compression and bucket_url values."""
    original_dlt_config = deepcopy(dlt_config)
    settings = settings_cls(dlt_config=dlt_config, dev_mode=dev_mode, output=output, use_destination=use_destination)
    assert settings.dev_mode == dev_mode
    assert settings.output == output
    assert settings.use_destination == use_destination
    settings_cls_mock = MagicMock(return_value=settings)

    run_cli(settings_cls_mock, MagicMock())  # type: ignore[reportArgumentType]

    assert dlt_config == {
        **original_dlt_config,
        "normalize.data_writer.disable_compression": dev_mode,
        f"destination.{use_destination}.bucket_url": output,
    }


# run_pipeline tests
def test_run_pipeline_minimal(test_bfi_settings: BatchedFileInputSettings, mock_dlt: MagicMock) -> None:
    """Ensure pipeline.run is called with correct args in the simplest case."""
    fake_resource = MagicMock()
    run_pipeline(test_bfi_settings, fake_resource)
    assert_pipeline_run_correctly(mock_dlt, fake_resource, test_bfi_settings.use_destination, {}, {}, {})


@pytest.mark.parametrize("destination_kwargs", [None, {}, {"max_table_nesting": 0}])
@pytest.mark.parametrize("pipeline_kwargs", [None, {}, {"pipeline_name": "p", "dataset_name": "d"}])
@pytest.mark.parametrize("pipeline_run_kwargs", [None, {}, {"table_format": "delta"}])
def test_run_pipeline_destination_pipeline_pipeline_run_kwargs_set(
    mock_dlt: MagicMock,
    destination_kwargs: dict[str, Any] | None,
    pipeline_kwargs: dict[str, Any] | None,
    pipeline_run_kwargs: dict[str, Any] | None,
) -> None:
    """Ensure a non-empty output sets the correct dlt.config bucket_url key."""
    settings = make_batched_settings(input_dir="/i", output="/custom/output", use_destination="s3")
    fake_resource = MagicMock()
    run_pipeline(
        settings,
        fake_resource,
        destination_kwargs=destination_kwargs,
        pipeline_kwargs=pipeline_kwargs,
        pipeline_run_kwargs=pipeline_run_kwargs,
    )
    assert_pipeline_run_correctly(
        mock_dlt,
        fake_resource,
        "s3",
        destination_kwargs,
        pipeline_kwargs,
        pipeline_run_kwargs,
    )


def test_run_pipeline_graceful_fail(
    test_bfi_settings: BatchedFileInputSettings,
    mock_dlt: MagicMock,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Ensure that errors during pipeline runs are caught and do not cause the entire pipeline to go ka-boom."""
    error = RuntimeError("Oh crap!!")
    fake_resource = MagicMock()

    mock_dlt.pipeline.return_value.run.side_effect = error

    output = run_pipeline(test_bfi_settings, fake_resource)
    assert output is None

    assert caplog.records[-1].levelno == logging.ERROR
    assert caplog.records[-1].message.startswith("Pipeline failed: ")

    for m in caplog.records:
        assert not m.message.startswith("Work complete")


@pytest.mark.parametrize("slack_configured", [True, False])
@pytest.mark.parametrize("success", [True, False])
def test_run_pipeline_slack_configured(  # noqa: PLR0913
    test_bfi_settings: BatchedFileInputSettings,
    mock_dlt: MagicMock,
    mock_send_slack_message: MagicMock,
    slack_configured: bool,
    success: bool,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Test that a slack message is sent if the slack_incoming_hook runtime config value is available."""
    # set up slack config
    if slack_configured:
        slack_hook = "http://some.url.slack.com"
        mock_dlt.pipeline.return_value.runtime_config.slack_incoming_hook = slack_hook

    # set an error to be triggered if success is false
    if not success:
        error = RuntimeError("Oh crap!!")
        mock_dlt.pipeline.return_value.run.side_effect = error

    run_pipeline(test_bfi_settings, MagicMock())

    if slack_configured:
        if success:
            mock_send_slack_message.assert_called_once_with(
                "http://some.url.slack.com", "Pipeline completed successfully!"
            )
        else:
            mock_send_slack_message.assert_called_once_with("http://some.url.slack.com", f"Pipeline failed: {error!s}")
        assert "Slack webhook not configured; no Slack alerts will be sent" not in caplog.messages
    else:
        mock_send_slack_message.assert_not_called()
        assert "Slack webhook not configured; no Slack alerts will be sent." in caplog.messages

    if success:
        # log messages on success
        assert caplog.records[-1].levelno == logging.INFO
        assert caplog.records[-1].message.startswith("Work complete!")
    else:
        assert caplog.records[-1].levelno == logging.ERROR
        assert caplog.records[-1].message.startswith("Pipeline failed: ")


@pytest.mark.usefixtures("dlt_config")
def test_run_pipeline_sets_pipelines_dir_when_pipeline_dir_set(mock_dlt: MagicMock) -> None:
    """pipelines_dir is injected into pipeline_kwargs when config.pipeline_dir is set."""
    settings = make_batched_settings(input_dir="/i", output="/out", use_output_dir_for_pipeline_metadata=True)
    assert settings.pipeline_dir is not None

    run_pipeline(settings, MagicMock())

    pipeline_call_kwargs = mock_dlt.pipeline.call_args.kwargs
    assert pipeline_call_kwargs["pipelines_dir"] == settings.pipeline_dir


def test_run_pipeline_no_pipelines_dir_when_pipeline_dir_none(
    test_bfi_settings: BatchedFileInputSettings, mock_dlt: MagicMock
) -> None:
    """pipelines_dir is absent from pipeline_kwargs when config.pipeline_dir is None."""
    assert test_bfi_settings.pipeline_dir is None
    run_pipeline(test_bfi_settings, MagicMock())
    pipeline_call_kwargs = mock_dlt.pipeline.call_args.kwargs
    assert "pipelines_dir" not in pipeline_call_kwargs


@pytest.mark.usefixtures("dlt_config")
def test_run_pipeline_sets_dev_mode_in_pipeline_kwargs_when_true(mock_dlt: MagicMock) -> None:
    """dev_mode=True is forwarded to dlt.pipeline()."""
    settings = make_batched_settings(input_dir="/i", output="/out", dev_mode=True)
    run_pipeline(settings, MagicMock())
    pipeline_call_kwargs = mock_dlt.pipeline.call_args.kwargs
    assert pipeline_call_kwargs.get("dev_mode") is True


def test_run_pipeline_dev_mode_absent_from_pipeline_kwargs_when_false(
    test_bfi_settings: BatchedFileInputSettings, mock_dlt: MagicMock
) -> None:
    """dev_mode=False is NOT forwarded to dlt.pipeline() (the branch is if config.dev_mode:)."""
    assert test_bfi_settings.dev_mode is False
    run_pipeline(test_bfi_settings, MagicMock())
    pipeline_call_kwargs = mock_dlt.pipeline.call_args.kwargs
    assert "dev_mode" not in pipeline_call_kwargs


# stream_xml_file_resource tests
@pytest.mark.parametrize("start_at", [0, 5, 173])
def test_stream_xml_resource_nonzero_start_at_passed_to_batcher(
    patched_io: tuple[MagicMock, MagicMock], start_at: int
) -> None:
    """start_at > 0 is truthy and must be forwarded to BatchCursor."""
    mock_batcher_cls, _ = patched_io
    mock_batcher_cls.return_value = make_batcher([])
    settings = make_batched_settings(input_dir="/i", start_at=start_at)

    list(stream_xml_file_resource(settings, "tag", MagicMock()))
    if start_at == 0:
        assert "start_at" not in mock_batcher_cls.call_args.kwargs
    else:
        assert mock_batcher_cls.call_args.kwargs.get("start_at") == start_at


@pytest.mark.usefixtures("mock_dlt")
def test_stream_xml_resource_timestamp_is_utc(
    patched_io: tuple[MagicMock, MagicMock],
    test_bfi_settings: BatchedFileInputSettings,
) -> None:
    """The timestamp is set once and passed to parse_fn."""
    files = [Path(f"/f/{i}.xml") for i in range(3)]
    mock_batcher_cls, mock_stream = patched_io
    mock_batcher_cls.return_value = make_batcher(files)
    mock_stream.return_value = ["e1", "e2"]
    mock_parse = MagicMock(return_value={})

    list(stream_xml_file_resource(test_bfi_settings, "tag", mock_parse))

    timestamps = {c.kwargs["timestamp"] for c in mock_parse.call_args_list}
    assert len(timestamps) == 1
    ts: datetime.datetime = timestamps.pop()
    assert ts.tzinfo is datetime.UTC


@pytest.mark.usefixtures("mock_dlt")
@pytest.mark.parametrize("log_interval", [1, 3, 1000])
def test_stream_xml_resource_progress_logged_at_interval(
    patched_io: tuple[MagicMock, MagicMock],
    test_bfi_settings: BatchedFileInputSettings,
    log_interval: int,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A progress log is emitted every log_interval entries."""
    n_entries = 6
    fake_file = Path("/f/file.xml")
    mock_batcher_cls, mock_stream = patched_io
    mock_batcher_cls.return_value = make_batcher([fake_file])
    mock_stream.return_value = [MagicMock() for _ in range(n_entries)]

    list(stream_xml_file_resource(test_bfi_settings, "tag", MagicMock(return_value={}), log_interval=log_interval))

    progress_msgs = [m for m in caplog.messages if m.startswith("Processed")]
    expected_count = n_entries // log_interval
    assert len(progress_msgs) == expected_count


def test_stream_xml_file_resource_empty_batch_yields_nothing(
    config: BatchedFileInputSettings,
    patched_io: tuple[MagicMock, MagicMock],
    caplog: pytest.LogCaptureFixture,
) -> None:
    """No items yielded when BatchCursor returns an empty batch; BatchCursor receives correct args."""
    mock_batcher = MagicMock()
    mock_batcher.get_batch.return_value = []
    mock_batcher_cls, mock_stream = patched_io
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
    mock_dlt: MagicMock,
    patched_io: tuple[MagicMock, MagicMock],
    test_bfi_settings: BatchedFileInputSettings,
) -> None:
    """One item is yielded per table key returned by the parse function."""
    fake_file = Path("/fake/input/part1.xml")
    fake_entry = MagicMock()
    parsed_entry = {
        "table_1": [{"id": "A"}, {"id": "B"}],
        "table_2": [{"some_field": "some_value"}],
    }
    mock_batcher_cls, mock_stream = patched_io
    mock_batcher_cls.return_value = make_batcher([fake_file])
    mock_stream.return_value = [fake_entry]
    mock_dlt.mark.with_table_name.return_value = object()

    results = list(stream_xml_file_resource(test_bfi_settings, "xml_tag", MagicMock(return_value=parsed_entry)))

    assert len(results) == len(parsed_entry)
    actual_calls = [list(c.args) for c in mock_dlt.mark.with_table_name.call_args_list]
    assert len(actual_calls) == len(parsed_entry)
    for key, val in parsed_entry.items():
        assert [val, key] in actual_calls


@pytest.mark.usefixtures("mock_dlt")
def test_stream_xml_file_resource_parse_fn_correct_args(
    patched_io: tuple[MagicMock, MagicMock],
    test_bfi_settings: BatchedFileInputSettings,
) -> None:
    """Ensure that parse_fn is called with (entry, timestamp, file_path) for every streamed XML entry."""
    fake_file = Path("/fake/input/part1.xml")
    xml_tag = "whatever"
    mock_stream_return = ["one", "two", "three"]
    mock_batcher_cls, mock_stream = patched_io
    mock_batcher_cls.return_value = make_batcher([fake_file])
    mock_stream.return_value = mock_stream_return
    mock_parse = MagicMock(return_value={})

    list(stream_xml_file_resource(test_bfi_settings, xml_tag, mock_parse))

    assert mock_parse.call_count == len(mock_stream_return)
    for i, c in enumerate(mock_parse.call_args_list):
        assert c.kwargs["entry"] == mock_stream_return[i]
        assert isinstance(c.kwargs["timestamp"], datetime.datetime)
        assert c.kwargs["file_path"] == fake_file


@pytest.mark.usefixtures("mock_dlt")
@pytest.mark.parametrize("batch_size", [1, 2, 5])
def test_stream_xml_file_resource_processes_all_files_across_batches(
    patched_io: tuple[MagicMock, MagicMock],
    fake_files: list[Path],
    batch_size: int,
    test_bfi_settings: BatchedFileInputSettings,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Ensure that stream_xml_file is called for every file regardless of batch size; file reads are logged."""
    mock_batcher_cls, mock_stream = patched_io
    mock_batcher_cls.return_value = make_batcher(fake_files, batch_size)
    mock_stream.return_value = []

    list(stream_xml_file_resource(test_bfi_settings, "some_tag", MagicMock()))

    assert mock_stream.call_args_list == [call(f, "some_tag") for f in fake_files]
    assert caplog.messages == [f"Reading from {f!s}" for f in fake_files]


def test_stream_xml_file_resource_multiple_batches_with_output(  # noqa: PLR0913
    mock_dlt: MagicMock,
    patched_io: tuple[MagicMock, MagicMock],
    fake_files: list[Path],
    test_bfi_settings: BatchedFileInputSettings,
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """End-to-end: generator processes all batches, parse output is passed to dlt.mark."""
    mock_batcher_cls, _ = patched_io
    mock_batcher_cls.return_value = make_batcher(fake_files, batch_size=2)
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
    mock_dlt.mark.with_table_name.side_effect = store_table_output

    output = list(stream_xml_file_resource(test_bfi_settings, xml_tag, fake_parse))

    assert len(output) == len(fake_files) * 2
    assert caplog.messages == [f"Reading from {f!s}" for f in fake_files]
    assert captured_output["file_path"] == fake_files
    assert captured_output["entry"] == [{"file_path_from_stream_xml_file": f, "xml_tag": xml_tag} for f in fake_files]


# test stream_xml_file_resource + run_pipeline
def test_integration_resource_and_pipeline_with_table_name_output_validated(
    mock_dlt: MagicMock,
    patched_io: tuple[MagicMock, MagicMock],
    test_bfi_settings: BatchedFileInputSettings,
    fake_files: list[Path],
) -> None:
    """Run test_stream_xml_file_resource_multiple_batches_with_output within the run_pipeline method."""
    mock_batcher_cls, _ = patched_io
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

    # make pipeline.run execute the stream_xml_file_resource generator
    mock_dlt.pipeline.return_value.run.side_effect = lambda resource, **_: list(resource)

    resource = stream_xml_file_resource(test_bfi_settings, "entry", fake_parse)
    with patch.object(core, "stream_xml_file", fake_stream_xml_file):
        run_pipeline(
            test_bfi_settings,
            resource,
            destination_kwargs=None,
            pipeline_kwargs={"pipeline_name": "test_pipeline_name", "dataset_name": "test_dataset_name"},
            pipeline_run_kwargs={"table_format": "delta"},
        )

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


def test_integration_empty_input_pipeline_run_still_called(
    mock_dlt: MagicMock,
    patched_io_empty_batcher: tuple[MagicMock, MagicMock],
    test_bfi_settings: BatchedFileInputSettings,
) -> None:
    """pipeline.run is called even when the resource yields nothing."""
    _, mock_stream = patched_io_empty_batcher
    resource = stream_xml_file_resource(test_bfi_settings, "entry", MagicMock(return_value={}))
    run_pipeline(test_bfi_settings, resource, pipeline_kwargs={"pipeline_name": "p", "dataset_name": "d"})

    mock_stream.assert_not_called()
    mock_dlt.mark.with_table_name.assert_not_called()
    mock_dlt.pipeline.return_value.run.assert_called_once()


# test run_cli + stream_xml_file_resource + run_pipeline
@pytest.mark.usefixtures("patched_io_empty_batcher", "test_bfi_settings")
def test_integration_run_cli_calls_pipeline_fn_with_config(mock_dlt: MagicMock) -> None:
    """The exact config produced by run_cli reaches stream_xml_file_resource unchanged."""
    received: list[CtsSettings] = []

    def pipeline_fn(settings: BatchedFileInputSettings) -> None:
        received.append(settings)
        resource = stream_xml_file_resource(settings, "entry", MagicMock(return_value={}))
        run_pipeline(settings, resource, pipeline_kwargs={"pipeline_name": "p", "dataset_name": "d"})

    run_cli(BatchedFileInputSettings, pipeline_fn)

    assert len(received) == 1
    assert isinstance(received[0], BatchedFileInputSettings)
    mock_dlt.pipeline.return_value.run.assert_called_once()


@pytest.mark.usefixtures("patched_io_empty_batcher")
def test_integration_full_pipeline_config_propagated(
    mock_dlt: MagicMock,
    config: BatchedFileInputSettings,
) -> None:
    """The exact config object from run_cli reaches stream_xml_file_resource unchanged.

    TODO: this test and the previous test are almost identical. Remove one or the other.
    """
    received: list[BatchedFileInputSettings] = []

    def pipeline_fn(settings: BatchedFileInputSettings) -> None:
        received.append(settings)
        resource = stream_xml_file_resource(settings, "entry", MagicMock(return_value={}))
        run_pipeline(settings, resource, pipeline_kwargs={"pipeline_name": "p", "dataset_name": "d"})

    run_cli(MagicMock(return_value=config), pipeline_fn)  # type: ignore[reportArgumentType]

    assert len(received) == 1
    assert received[0] == config
    mock_dlt.pipeline.return_value.run.assert_called_once()


def test_integration_run_cli_full_xml_pipeline(
    mock_dlt: MagicMock,
    patched_io: tuple[MagicMock, MagicMock],
    fake_files: list[Path],
) -> None:
    """Full integration: run_cli => pipeline_fn => stream_xml_file_resource => run_pipeline."""
    xml_tag = "entry"
    mock_batcher_cls, _ = patched_io
    mock_batcher_cls.return_value = make_batcher(fake_files)

    def fake_stream(file_path: Path, tag: str) -> list[dict]:
        return [{"fp": file_path, "tag": tag}]

    def fake_parse(entry: dict, timestamp: datetime.datetime, file_path: Path) -> dict:  # noqa: ARG001
        return {"entry": [entry], "file_path": [file_path]}

    mock_dlt.pipeline.return_value.run.side_effect = lambda resource, **_: list(resource)

    def pipeline_fn(settings: BatchedFileInputSettings) -> None:
        resource = stream_xml_file_resource(settings, xml_tag, fake_parse)
        run_pipeline(settings, resource, pipeline_kwargs={"pipeline_name": "tp", "dataset_name": "td"})

    with (
        patch.object(core, "stream_xml_file", fake_stream),
    ):
        run_cli(BatchedFileInputSettings, pipeline_fn)

    # pipeline instantiation
    mock_dlt.pipeline.assert_called_once_with(
        destination=mock_dlt.destination.return_value,
        dataset_name="td",
        pipeline_name="tp",
    )

    # pipeline.run called once with resource (stream_xml_file_resource)
    mock_dlt.pipeline.return_value.run.assert_called_once()
    fn = next(iter(mock_dlt.pipeline.return_value.run.call_args_list[0].args))
    assert isinstance(fn, Generator)
    assert fn.__name__ == "stream_xml_file_resource"  # type: ignore[reportAttributeAccessIssue]

    # args to dlt.mark.with_table_name
    actual = [list(c.args) for c in mock_dlt.mark.with_table_name.call_args_list]
    expected = []
    for f in fake_files:
        expected.extend(
            [
                [[{"fp": f, "tag": xml_tag}], "entry"],
                [[f], "file_path"],
            ]
        )
    assert actual == expected
