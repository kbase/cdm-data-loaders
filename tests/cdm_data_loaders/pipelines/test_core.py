"""Tests for the shared core DLT pipeline functions."""

import gzip
import logging
import os
from collections.abc import Iterator
from copy import deepcopy
from json import loads
from pathlib import Path
from typing import Any, Final
from unittest.mock import MagicMock, patch

import dlt
import pytest
from dlt.extract.resource import DltResource
from pydantic import ValidationError
from pydantic_settings import SettingsError

from cdm_data_loaders.core.fields import (
    DEV_MODE,
    LOCAL_FS,
    OUTPUT_DIR,
    S3,
    USE_DESTINATION,
    VALID_DESTINATIONS,
)
from cdm_data_loaders.core.settings import (
    BatchedFileInputSettings,
    CtsSettings,
    InputOutputSettings,
    LoggerSettings,
)
from cdm_data_loaders.pipelines import core
from cdm_data_loaders.pipelines.core import (
    NO_MESSAGE,
    WEBHOOK_NOT_CONFIGURED,
    construct_env_var,
    run_cli,
    run_pipeline,
    send_slack_message_carefully,
    sync_configs,
)
from tests.cdm_data_loaders.core.test_settings import SETTINGS_CLASSES, TEST_CTS_SETTINGS
from tests.dlt_config_isolation import dlt_config_unset, isolated_dlt_config

TINY_RESOURCE_DATA: Final[list[dict[str, Any]]] = [
    {"entity_id": "tiny:one", "value": 1},
    {"entity_id": "tiny:two", "value": 2},
]
TINY_PIPELINE_NAME: Final[str] = "test_core_tiny_pipeline"
TINY_TABLE_NAME: Final[str] = "tiny"


def make_batched_settings(**kwargs: str | int) -> BatchedFileInputSettings:
    """Generate a validated BatchedFileInputSettings object with a valid dlt config."""
    return BatchedFileInputSettings.model_validate(kwargs)


@pytest.fixture
def test_bfi_settings(tmp_path: Path) -> BatchedFileInputSettings:
    """Minimal valid BatchedFileInputSettings (no start_at, no output_dir)."""
    return make_batched_settings(input_dir="/fake/input", output_dir=str(tmp_path))


@pytest.fixture
def dlt_test_settings(
    tmp_path: Path, dlt_destination_config: str, monkeypatch: pytest.MonkeyPatch
) -> BatchedFileInputSettings:
    """Settings pointing the real local_fs destination at a tmp_path bucket.

    Telemetry is disabled so the real pipeline runs do not fire analytics from tests.
    """
    monkeypatch.setenv("RUNTIME__DLTHUB_TELEMETRY", "false")
    return make_batched_settings(
        input_dir=str(tmp_path / "input"),
        output_dir=str(tmp_path / "output"),
        use_destination=dlt_destination_config,
        use_output_dir_for_pipeline_metadata=True,
    )


def tiny_resource() -> DltResource:
    """A tiny dlt resource emitting two records."""
    return dlt.resource(TINY_RESOURCE_DATA, name=TINY_TABLE_NAME)


def failing_resource() -> DltResource:
    """A tiny dlt resource that raises while extracting."""

    def _generate() -> Iterator[dict[str, Any]]:
        yield TINY_RESOURCE_DATA[0]
        err_msg = "Oh crap!!"
        raise RuntimeError(err_msg)

    return dlt.resource(_generate, name=TINY_TABLE_NAME)


def read_output_jsonl_records(settings: BatchedFileInputSettings) -> list[dict[str, Any]]:
    """Read every jsonl record written to the settings output directory, minus dlt's internal columns."""
    records: list[dict[str, Any]] = []
    for jsonl_file in sorted(Path(settings.output_dir).glob(f"**/{TINY_TABLE_NAME}/*.jsonl*")):
        if jsonl_file.name.endswith(".gz"):
            with gzip.open(jsonl_file, "rt") as f:
                content = f.read()
        else:
            content = jsonl_file.read_text()
        records.extend(loads(record) for record in content.splitlines() if record)
    assert records
    return [{key: value for key, value in record.items() if not key.startswith("_dlt")} for record in records]


@pytest.fixture
def test_cts_settings() -> CtsSettings:
    """A fully validated CtsSettings instance using the test dlt config."""
    return CtsSettings.model_validate(TEST_CTS_SETTINGS)


@pytest.fixture(
    params=[
        pytest.param({"input_dir": "/fake/input"}, id="default"),
        pytest.param(
            {
                "input_dir": "/path/to/dir",
                "use_destination": LOCAL_FS,
                "start_at": 15,
                "output_dir": "/some/dir",
            },
            id="alt",
        ),
    ]
)
def config(request: pytest.FixtureRequest) -> BatchedFileInputSettings:
    """Parametrized fixture providing default and non-default settings."""
    return make_batched_settings(**request.param)


@pytest.fixture(
    params=[
        pytest.param(lambda: LoggerSettings.model_validate({}), id="no-relevant-attrs"),
        pytest.param(
            lambda: InputOutputSettings.model_validate({"input_dir": "/fake/input", "output_dir": "/fake/output"}),
            id="output_dir-only-no-destination",
        ),
    ]
)
def settings_missing_sync_attrs(request: pytest.FixtureRequest) -> LoggerSettings | InputOutputSettings:
    """Settings instances missing one or more attribute that sync_configs checks for via hasattr."""
    return request.param()


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


# send slack message carefully
SLACK_HOOK: Final[str] = "https://slack.hook/what/ever"
TEST_MESSAGE: Final[str] = "3... 2... 1... testing?"


@pytest.mark.parametrize(
    ("slack_hook", "message", "err_msg"),
    [
        ("", TEST_MESSAGE, WEBHOOK_NOT_CONFIGURED),
        (SLACK_HOOK, "", NO_MESSAGE),
        ("", "", WEBHOOK_NOT_CONFIGURED),
    ],
)
def test_send_slack_message_carefully_params_fail(
    slack_hook: str,
    message: str,
    err_msg: str,
    caplog: pytest.LogCaptureFixture,
    mock_send_slack_message: MagicMock,
) -> None:
    """Test sending a slack message ever so carefully, but with incorrect parameters."""
    send_slack_message_carefully(slack_hook, message)
    mock_send_slack_message.assert_not_called()
    assert len(caplog.records) == 1
    assert caplog.records[-1].levelno == logging.WARNING
    assert caplog.records[-1].getMessage() == f"Cannot send slack message: {err_msg}"


@pytest.mark.parametrize("markdown", [True, False, None])
def test_send_slack_message_carefully_markdown_params(
    markdown: None | bool, mock_send_slack_message: MagicMock
) -> None:
    """Test that the markdown param is correctly passed on to send_slack_message.

    :param markdown: markdown parameter
    :type markdown: None | bool
    """
    if markdown is None:
        send_slack_message_carefully(SLACK_HOOK, TEST_MESSAGE)
    else:
        send_slack_message_carefully(SLACK_HOOK, TEST_MESSAGE, markdown)

    if markdown:
        mock_send_slack_message.assert_called_once_with(SLACK_HOOK, TEST_MESSAGE, True)  # noqa: FBT003
    else:
        mock_send_slack_message.assert_called_once_with(SLACK_HOOK, TEST_MESSAGE, False)  # noqa: FBT003


def test_send_slack_message_fail_error_oh_no(monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture) -> None:
    """Ensure that errors are caught and don't crash the whole goddamn ship of fools."""
    slack_mock = MagicMock(side_effect=ValueError("Oh no! An error!"))
    monkeypatch.setattr(core, "send_slack_message", slack_mock)
    send_slack_message_carefully(SLACK_HOOK, TEST_MESSAGE)
    assert len(caplog.records) == 1
    assert caplog.records[-1].levelno == logging.ERROR
    assert caplog.records[-1].getMessage() == "Failed to send slack message"


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
        test_cts_settings.output_dir,
    )


@pytest.mark.parametrize("use_destination", VALID_DESTINATIONS)
@pytest.mark.parametrize("dev_mode", [True, False])
@pytest.mark.parametrize("output_dir", ["/some/path", "s3://bucket/whatever"])
def test_sync_configs_both_keys_set_in_single_call(
    dlt_config: dict[str, Any],
    test_cts_settings: CtsSettings,
    dev_mode: bool,
    use_destination: str,
    output_dir: str,
) -> None:
    """Test that sync_configs changes the disable_compression and bucket_url values."""
    original_dlt_config = deepcopy(dlt_config)
    test_cts_settings.dev_mode = dev_mode
    test_cts_settings.use_destination = use_destination
    test_cts_settings.output_dir = output_dir
    sync_configs(test_cts_settings, dlt_config)
    assert dlt_config == {
        **original_dlt_config,
        "normalize.data_writer.disable_compression": dev_mode,
        f"destination.{use_destination}.bucket_url": output_dir,
    }


def test_sync_configs_attrs_missing_as_expected(
    settings_missing_sync_attrs: LoggerSettings | InputOutputSettings,
) -> None:
    """Sanity-check the fixture: confirm which attributes are genuinely absent."""
    assert hasattr(settings_missing_sync_attrs, OUTPUT_DIR) == isinstance(
        settings_missing_sync_attrs, InputOutputSettings
    )
    assert not hasattr(settings_missing_sync_attrs, DEV_MODE)
    assert not hasattr(settings_missing_sync_attrs, USE_DESTINATION)


def test_sync_configs_no_op_when_relevant_attrs_missing(
    settings_missing_sync_attrs: LoggerSettings | InputOutputSettings,
) -> None:
    """sync_configs must not write any keys when the settings object lacks the attrs it needs.

    This covers both the case where dev_mode/output/use_destination are all absent, and the case
    where output_dir is present but use_destination is not (so the bucket_url key must still be skipped).
    """
    empty_dlt_config = {}
    sync_configs(settings_missing_sync_attrs, empty_dlt_config)  # pyright: ignore[reportArgumentType]
    assert empty_dlt_config == {}


def test_sync_configs_no_op_with_mock_config_when_relevant_attrs_missing(
    settings_missing_sync_attrs: LoggerSettings | InputOutputSettings,
) -> None:
    """As above, but verified via a MagicMock config to confirm __setitem__ is never even attempted."""
    mock_cfg = MagicMock()
    sync_configs(settings_missing_sync_attrs, mock_cfg)  # pyright: ignore[reportArgumentType]
    mock_cfg.__setitem__.assert_not_called()


@pytest.mark.parametrize("settings_cls", SETTINGS_CLASSES)
def test_sync_configs_sets_both_keys_from_a_de_novo_dlt_config(settings_cls: type[CtsSettings]) -> None:
    """When dev_mode/output_dir/use_destination are all present, sync_configs sets exactly two keys."""
    settings = settings_cls(dev_mode=True, output_dir="/some/output", use_destination=LOCAL_FS)  # pyright: ignore[reportCallIssue]
    empty_dlt_config = {}
    sync_configs(settings, empty_dlt_config)

    assert empty_dlt_config == {
        "normalize.data_writer.disable_compression": True,
        "destination.local_fs.bucket_url": "/some/output",
    }


# tests for run_cli()
@pytest.mark.parametrize("settings_cls", SETTINGS_CLASSES)
def test_run_cli_calls_settings_cls_with_dlt_config(
    settings_cls: type[CtsSettings], dlt_config: dict[str, Any]
) -> None:
    """Ensure run_cli instantiates the supplied settings class with dlt.config."""
    captured: list[CtsSettings] = []

    class _CaptureCls(settings_cls):  # type: ignore[valid-type]
        def __init__(self, **data: Any) -> None:  # noqa: ANN401
            super().__init__(**data)
            assert data == {}
            captured.append(self)

    run_cli(_CaptureCls, MagicMock())

    assert len(captured) == 1
    captured_config = captured[0]
    assert isinstance(captured_config, settings_cls)
    # The object passed to sync_configs must be a fully initialised instance
    for attr in settings_cls.model_fields:
        assert hasattr(captured_config, attr)
    # no dlt_config override was supplied, so it falls back to the ambient dlt.config -- isolated
    # for this test, but still the real dlt accessor, not a plain dict
    assert captured_config.dlt_config is dlt.config
    # prevent annoying pylance warning on the next assertion
    assert captured_config.dlt_config is not None
    assert (
        captured_config.dlt_config["destination.local_fs.bucket_url"] == dlt_config["destination.local_fs.bucket_url"]
    )


@pytest.mark.parametrize("settings_cls", SETTINGS_CLASSES)
def test_run_cli_function_calls_args(settings_cls: type[CtsSettings]) -> None:
    """Ensure run_cli instantiates the settings class and dispatches to pipeline_fn with the result."""
    instantiated_cls = settings_cls()  # pyright: ignore[reportCallIssue]
    pipeline_fn_mock = MagicMock()
    settings_cls_mock = MagicMock(return_value=instantiated_cls)

    run_cli(settings_cls_mock, pipeline_fn_mock)  # type: ignore[reportArgumentType]

    settings_cls_mock.assert_called_once_with()
    pipeline_fn_mock.assert_called_once_with(instantiated_cls)


@pytest.mark.parametrize("settings_cls", SETTINGS_CLASSES)
@pytest.mark.parametrize("pipeline_fn_result", [None, {"loads_ids": ["load-1"]}], ids=["none", "load_info"])
def test_run_cli_returns_pipeline_fn_result_verbatim(
    settings_cls: type[CtsSettings],
    pipeline_fn_result: dict[str, Any] | None,
) -> None:
    """run_cli returns whatever pipeline_fn returns: load_info on success, None otherwise."""
    instantiated_cls = settings_cls()  # pyright: ignore[reportCallIssue]
    pipeline_fn = MagicMock(return_value=pipeline_fn_result)
    settings_cls_mock = MagicMock(return_value=instantiated_cls)

    returned = run_cli(settings_cls_mock, pipeline_fn)  # type: ignore[reportArgumentType]

    assert returned is pipeline_fn_result


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
    assert log_records[-1].getMessage() == "Error initialising config"


@pytest.mark.parametrize("settings_cls", SETTINGS_CLASSES)
@pytest.mark.parametrize(
    ("bad_dlt_config", "error", "err_msg"),
    [
        (None, ValidationError, "dlt_config must be defined"),
        ({}, ValueError, "No valid destinations found in dlt configuration"),
        ({"destination": {}}, ValueError, "No valid destinations found in dlt configuration"),
    ],
)
def test_run_cli_reraises_validation_errors(
    settings_cls: type[CtsSettings],
    bad_dlt_config: None | dict[str, Any],
    error: type[Exception],
    err_msg: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Ensure that errors in instantiating the configuration are re-raised.

    See also the cts_defaults test ``test_cli_app_run_dlt_config_errors``.
    """
    isolation = dlt_config_unset() if bad_dlt_config is None else isolated_dlt_config(bad_dlt_config)
    with isolation, pytest.raises(error, match=err_msg):
        run_cli(settings_cls, MagicMock())

    log_records = caplog.records
    assert log_records[-1].levelno == logging.ERROR
    assert log_records[-1].getMessage() == "Error initialising config"


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
        pytest.raises(RuntimeError, match="disk on fire"),
    ):
        run_cli(settings_cls, mock_pipeline_fn)

    log_records = caplog.records
    assert log_records[-1].levelno == logging.ERROR
    assert log_records[-1].getMessage() == "Unexpected error setting up config"

    mock_pipeline_fn.assert_not_called()


# pipeline_fn not called on error
@pytest.mark.parametrize(
    "exc",
    [
        SettingsError("bad"),
        ValueError("bad value"),
    ],
)
@pytest.mark.parametrize("settings_cls", SETTINGS_CLASSES)
def test_run_cli_pipeline_fn_not_called_on_settings_instantiation_error(
    exc: Exception,
    settings_cls: type[CtsSettings],
) -> None:
    """Ensure that further execution is stopped if settings instantiation fails."""
    mock_pipeline_fn = MagicMock()
    with (
        patch.object(settings_cls, "__init__", side_effect=exc),
        pytest.raises(type(exc)),
    ):
        run_cli(settings_cls, mock_pipeline_fn)

    mock_pipeline_fn.assert_not_called()


# run_pipeline bootstrap: construct_env_var and sync_configs
def test_run_pipeline_calls_construct_env_var_and_sync_configs(
    dlt_test_settings: BatchedFileInputSettings, mock_dlt: MagicMock
) -> None:
    """run_pipeline calls construct_env_var() and sync_configs(settings, dlt.config)."""
    with (
        patch("cdm_data_loaders.pipelines.core.construct_env_var") as mock_env_var,
        patch("cdm_data_loaders.pipelines.core.sync_configs") as mock_sync,
    ):
        run_pipeline(dlt_test_settings, MagicMock())

    mock_env_var.assert_called_once_with()
    mock_sync.assert_called_once_with(dlt_test_settings, mock_dlt.config)


@pytest.mark.parametrize("settings_cls", SETTINGS_CLASSES)
@pytest.mark.parametrize("dev_mode", [True, False])
@pytest.mark.parametrize(("use_destination", "output_dir"), [(LOCAL_FS, "/some/path"), (S3, "s3://bucket/whatever")])
def test_run_pipeline_dlt_config_updated_after_success(
    dlt_config: dict[str, Any],
    mock_dlt: MagicMock,
    settings_cls: type[CtsSettings],
    dev_mode: bool,
    use_destination: str,
    output_dir: str,
) -> None:
    """run_pipeline's sync_configs call changes the disable_compression and bucket_url values."""
    original_dlt_config = deepcopy(dlt_config)
    settings = settings_cls(dev_mode=dev_mode, output_dir=output_dir, use_destination=use_destination)  # pyright: ignore[reportCallIssue]

    run_pipeline(settings, MagicMock())

    mock_dlt.pipeline.return_value.run.assert_called_once()
    assert dlt_config == {
        **original_dlt_config,
        "normalize.data_writer.disable_compression": dev_mode,
        f"destination.{use_destination}.bucket_url": output_dir,
    }


def test_run_pipeline_uses_slack_env_var_if_set(
    dlt_test_settings: BatchedFileInputSettings,
    mock_send_slack_message: MagicMock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """RUNTIME__SLACK_INCOMING_HOOK built by construct_env_var reaches real dlt's runtime config."""
    monkeypatch.setenv("VARIABLE_B", "BBB")
    monkeypatch.setenv("VARIABLE_T", "TTT")
    monkeypatch.setenv("CHAR_STR", "CCC")

    load_info = run_pipeline(dlt_test_settings, tiny_resource())

    expected = "https://hooks.slack.com/services/BBB/TTT/CCC/"
    assert os.environ.get("RUNTIME__SLACK_INCOMING_HOOK") == expected
    assert load_info is not None
    assert not load_info.has_failed_jobs
    mock_send_slack_message.assert_called_once_with(expected, "Pipeline completed successfully!", False)  # noqa: FBT003


def test_run_pipeline_no_slack_env_var_when_vars_missing(
    dlt_test_settings: BatchedFileInputSettings,
    mock_send_slack_message: MagicMock,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """RUNTIME__SLACK_INCOMING_HOOK is not set when source vars are absent; no slack alerts are sent."""
    for var in ("VARIABLE_B", "VARIABLE_T", "CHAR_STR", "RUNTIME__SLACK_INCOMING_HOOK"):
        monkeypatch.delenv(var, raising=False)

    load_info = run_pipeline(dlt_test_settings, tiny_resource())

    assert load_info is not None
    assert not load_info.has_failed_jobs
    assert "RUNTIME__SLACK_INCOMING_HOOK" not in os.environ
    mock_send_slack_message.assert_not_called()
    assert f"No Slack alerts will be sent: {WEBHOOK_NOT_CONFIGURED}" in caplog.messages


# run_pipeline tests
def test_run_pipeline_minimal(dlt_test_settings: BatchedFileInputSettings) -> None:
    """Ensure a tiny pipeline loads its records through real dlt and returns load_info."""
    load_info = run_pipeline(dlt_test_settings, tiny_resource())

    assert load_info is not None
    assert not load_info.has_failed_jobs
    assert read_output_jsonl_records(dlt_test_settings) == TINY_RESOURCE_DATA


def test_run_pipeline_returns_none_on_pipeline_run_failure(
    dlt_test_settings: BatchedFileInputSettings, caplog: pytest.LogCaptureFixture
) -> None:
    """A resource that raises mid-extraction is caught: run_pipeline logs and returns None."""
    load_info = run_pipeline(dlt_test_settings, failing_resource())

    assert load_info is None
    assert caplog.records[-1].levelno == logging.ERROR
    assert caplog.records[-1].getMessage().startswith("Pipeline failed: ")
    for record in caplog.records:
        assert not record.getMessage().startswith("Work complete")


@pytest.mark.parametrize("slack_configured", [True, False])
def test_run_pipeline_slack_configured(
    dlt_test_settings: BatchedFileInputSettings,
    mock_send_slack_message: MagicMock,
    slack_configured: bool,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A slack message is sent if the slack_incoming_hook runtime config value is available."""
    if slack_configured:
        monkeypatch.setenv("VARIABLE_B", "BBB")
        monkeypatch.setenv("VARIABLE_T", "TTT")
        monkeypatch.setenv("CHAR_STR", "CCC")

    load_info = run_pipeline(dlt_test_settings, tiny_resource())

    assert load_info is not None
    assert not load_info.has_failed_jobs
    if slack_configured:
        expected = "https://hooks.slack.com/services/BBB/TTT/CCC/"
        mock_send_slack_message.assert_called_once_with(
            expected,
            "Pipeline completed successfully!",
            False,  # noqa: FBT003
        )
        assert f"No Slack alerts will be sent: {WEBHOOK_NOT_CONFIGURED}" not in caplog.messages
    else:
        mock_send_slack_message.assert_not_called()
        assert f"No Slack alerts will be sent: {WEBHOOK_NOT_CONFIGURED}" in caplog.messages
    assert caplog.records[-1].levelno == logging.INFO
    assert caplog.records[-1].getMessage().startswith("Work complete!")


@pytest.mark.parametrize("slack_configured", [True, False])
def test_run_pipeline_slack_configured_on_failure(
    dlt_test_settings: BatchedFileInputSettings,
    mock_send_slack_message: MagicMock,
    slack_configured: bool,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The failure message is sent to slack if a hook is configured; nothing is sent otherwise."""
    if slack_configured:
        monkeypatch.setenv("VARIABLE_B", "BBB")
        monkeypatch.setenv("VARIABLE_T", "TTT")
        monkeypatch.setenv("CHAR_STR", "CCC")

    load_info = run_pipeline(dlt_test_settings, failing_resource())

    assert load_info is None
    assert caplog.records[-1].levelno == logging.ERROR
    err_msg = caplog.records[-1].getMessage()
    assert err_msg.startswith("Pipeline failed: ")
    assert "Oh crap!!" in err_msg
    if slack_configured:
        expected = "https://hooks.slack.com/services/BBB/TTT/CCC/"
        assert mock_send_slack_message.call_args.args[0] == expected
        assert mock_send_slack_message.call_args.args[1].startswith("Pipeline failed: ")
        assert "Oh crap!!" in mock_send_slack_message.call_args.args[1]
        assert mock_send_slack_message.call_args.args[2] is False
    else:
        mock_send_slack_message.assert_not_called()


def test_run_pipeline_pipelines_dir_used_for_pipeline_metadata(dlt_test_settings: BatchedFileInputSettings) -> None:
    """The pipeline metadata directory is the settings pipeline_dir, not the default ~/.dlt."""
    assert dlt_test_settings.pipeline_dir is not None
    load_info = run_pipeline(dlt_test_settings, tiny_resource())

    assert load_info is not None
    assert not load_info.has_failed_jobs
    pipeline_state_files = list(Path(dlt_test_settings.pipeline_dir).glob("**/*.json"))
    assert pipeline_state_files


def test_run_pipeline_dev_mode_true_loads_into_fresh_dataset(dlt_test_settings: BatchedFileInputSettings) -> None:
    """dev_mode=True is forwarded to dlt.pipeline(); the pipeline loads into a suffixed dataset."""
    settings = make_batched_settings(
        input_dir=str(Path(dlt_test_settings.input_dir)),
        output_dir=str(Path(dlt_test_settings.output_dir)),
        use_destination=LOCAL_FS,
        use_output_dir_for_pipeline_metadata=True,
        dev_mode=True,
    )
    assert settings.dev_mode is True

    load_info = run_pipeline(settings, tiny_resource())

    assert load_info is not None
    assert not load_info.has_failed_jobs
    assert load_info.dataset_name != f"{load_info.pipeline.pipeline_name}_dataset"
    assert read_output_jsonl_records(settings) == TINY_RESOURCE_DATA


@pytest.mark.parametrize(
    ("pipeline_kwargs", "pipeline_run_kwargs"),
    [
        ({}, {}),
        ({"pipeline_name": TINY_PIPELINE_NAME, "dataset_name": "core_test_dataset"}, {}),
        ({}, {"loader_file_format": "jsonl"}),
    ],
    ids=["minimal", "named", "jsonl-format"],
)
def test_run_pipeline_passes_kwargs_to_real_pipeline(
    dlt_test_settings: BatchedFileInputSettings,
    pipeline_kwargs: dict[str, Any],
    pipeline_run_kwargs: dict[str, Any],
) -> None:
    """pipeline_kwargs and pipeline_run_kwargs are honored by the real dlt pipeline."""
    load_info = run_pipeline(
        dlt_test_settings,
        tiny_resource(),
        pipeline_kwargs=pipeline_kwargs,
        pipeline_run_kwargs=pipeline_run_kwargs,
    )

    assert load_info is not None
    assert not load_info.has_failed_jobs
    if pipeline_kwargs:
        assert load_info.pipeline.pipeline_name == TINY_PIPELINE_NAME
        assert load_info.dataset_name == "core_test_dataset"
    assert read_output_jsonl_records(dlt_test_settings) == TINY_RESOURCE_DATA
