"""Unit tests for ``cdm_data_loaders.core.cli``."""

import logging
import sys
from collections.abc import Callable
from dataclasses import FrozenInstanceError
from pathlib import Path
from typing import Any, ClassVar, Final, cast
from unittest.mock import MagicMock, patch

import pytest
from dlt.common.pipeline import LoadInfo
from pydantic import ValidationError
from pydantic_settings import SettingsError

from cdm_data_loaders.core.cli import CliRunner, SpecError, run_cli
from cdm_data_loaders.core.settings import (
    CdmDataLoadersBase,
    CtsSettings,
    InputLocalOutputSettings,
    LoggerSettings,
)
from tests._support.cli_assertions import make_cts_settings, settings_to_argv
from tests._support.dlt_isolation import dlt_config_unset

LOCAL_FS_DEST: Final[str] = "local_fs"
RUN_RESULT: Final[LoadInfo] = cast("LoadInfo", object())

SETTINGS_CLASSES: Final[list[type[CdmDataLoadersBase]]] = [
    LoggerSettings,
    InputLocalOutputSettings,
    CtsSettings,
]
SETTINGS_CLASS_IDS: Final[list[str]] = [
    "logger-settings",
    "local-io-settings",
    "cts-settings",
]


class _SampleSpecError(SpecError):
    """Concrete ``SpecError`` subclass for metadata assertions."""

    code: ClassVar[str] = "E1"
    title: ClassVar[str] = "Sample spec failure"


def _settings_kwargs_for(settings_cls: type[CdmDataLoadersBase], tmp_path: Path) -> dict[str, Any]:
    """Return minimal required kwargs for instantiating ``settings_cls`` in ``run_cli`` tests."""
    if issubclass(settings_cls, CtsSettings):
        return {"input_dir": tmp_path, "output_dir": tmp_path, "use_destination": LOCAL_FS_DEST}
    if issubclass(settings_cls, InputLocalOutputSettings):
        return {"input_dir": tmp_path, "output_dir": tmp_path}
    return {}


def _make_recorder() -> tuple[list[CtsSettings], Callable[[CtsSettings], LoadInfo | None]]:
    """Build a pipeline action callable that records received settings and returns ``RUN_RESULT``."""
    received: list[CtsSettings] = []

    def action_fn(settings: CtsSettings) -> LoadInfo | None:
        received.append(settings)
        return RUN_RESULT

    return received, action_fn


def test_spec_error_exposes_code_and_title() -> None:
    """Verify SpecError subclasses retain code, title, and message attributes."""
    err = _SampleSpecError("invalid input")
    assert err.code == "E1"
    assert err.title == "Sample spec failure"
    assert str(err) == "invalid input"
    assert SpecError.code == ""
    assert SpecError.title == ""


@pytest.mark.parametrize("settings_cls", SETTINGS_CLASSES, ids=SETTINGS_CLASS_IDS)
def test_run_cli_calls_settings_cls_with_cli_args(
    settings_cls: type[CdmDataLoadersBase],
    tmp_path: Path,
) -> None:
    """Instantiate the supplied settings class via CliApp.run."""
    captured: list[CdmDataLoadersBase] = []

    class _CaptureCls(settings_cls):  # type: ignore[valid-type]
        def __init__(self, **data: Any) -> None:  # noqa: ANN401
            super().__init__(**data)
            captured.append(self)

    run_cli(_CaptureCls, MagicMock(), _settings_kwargs_for(settings_cls, tmp_path), cli_args=[])

    assert len(captured) == 1
    captured_config = captured[0]
    assert isinstance(captured_config, settings_cls)
    for attr in settings_cls.model_fields:
        assert hasattr(captured_config, attr)


@pytest.mark.parametrize("dlt_config_set", [True, False], ids=["dlt-config-set", "dlt-config-unset"])
def test_run_cli_pass_does_not_read_dlt_config(tmp_path: Path, *, dlt_config_set: bool) -> None:
    """Pass settings through unchanged without filling output_dir from dlt config."""
    received: list[CtsSettings] = []

    def action_fn(settings: CtsSettings) -> LoadInfo | None:
        received.append(settings)
        return None

    kwargs = _settings_kwargs_for(CtsSettings, tmp_path)
    if dlt_config_set:
        run_cli(CtsSettings, action_fn, kwargs, cli_args=[])
    else:
        with dlt_config_unset():
            run_cli(CtsSettings, action_fn, kwargs, cli_args=[])

    assert len(received) == 1
    assert received[0].output_dir == tmp_path.resolve()
    assert received[0].use_destination == LOCAL_FS_DEST


@pytest.mark.parametrize("settings_cls", SETTINGS_CLASSES, ids=SETTINGS_CLASS_IDS)
def test_run_cli_returns_action_fn_result(
    settings_cls: type[CdmDataLoadersBase],
    tmp_path: Path,
) -> None:
    """Dispatch instantiated settings to action_fn and return its result."""
    sentinel = {"loads_ids": ["load-1"]}
    received: list[CdmDataLoadersBase] = []

    def action_fn(settings: CdmDataLoadersBase) -> dict[str, Any]:
        received.append(settings)
        return sentinel

    returned = run_cli(settings_cls, action_fn, _settings_kwargs_for(settings_cls, tmp_path), cli_args=[])

    assert returned is sentinel
    assert len(received) == 1
    assert isinstance(received[0], settings_cls)


@pytest.mark.parametrize("settings_cls", SETTINGS_CLASSES, ids=SETTINGS_CLASS_IDS)
def test_run_cli_reraises_settings_error(
    settings_cls: type[CdmDataLoadersBase],
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Log and re-raise SettingsError on invalid CLI configuration."""
    err = SettingsError("bad CLI arg")

    with (
        patch.object(settings_cls, "__init__", side_effect=err),
        pytest.raises(SettingsError, match="bad CLI arg"),
    ):
        run_cli(settings_cls, MagicMock())

    error_records = [record for record in caplog.records if record.levelno == logging.ERROR]
    assert error_records
    assert error_records[-1].getMessage() == "Error initialising config"


@pytest.mark.parametrize("settings_cls", SETTINGS_CLASSES, ids=SETTINGS_CLASS_IDS)
def test_run_cli_reraises_unexpected_exception(
    settings_cls: type[CdmDataLoadersBase],
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Log and re-raise unexpected exceptions during settings instantiation."""
    boom = RuntimeError("disk on fire")
    mock_action_fn = MagicMock()
    with (
        patch.object(settings_cls, "__init__", side_effect=boom),
        pytest.raises(RuntimeError, match="disk on fire"),
    ):
        run_cli(settings_cls, mock_action_fn)

    error_records = [record for record in caplog.records if record.levelno == logging.ERROR]
    assert error_records
    assert error_records[-1].getMessage() == "Unexpected error setting up config"
    mock_action_fn.assert_not_called()


@pytest.mark.parametrize(
    "exc",
    [
        SettingsError("bad"),
        ValueError("bad value"),
    ],
    ids=["settings-error", "value-error"],
)
@pytest.mark.parametrize("settings_cls", SETTINGS_CLASSES, ids=SETTINGS_CLASS_IDS)
def test_run_cli_action_fn_not_called_on_settings_instantiation_error(
    exc: Exception,
    settings_cls: type[CdmDataLoadersBase],
) -> None:
    """Stop before calling action_fn when settings instantiation fails."""
    mock_action_fn = MagicMock()
    with (
        patch.object(settings_cls, "__init__", side_effect=exc),
        pytest.raises(type(exc)),
    ):
        run_cli(settings_cls, mock_action_fn)

    mock_action_fn.assert_not_called()


def test_cli_runner_pass_run_and_pipeline_fn_alias(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Build settings from cli_args, env vars, and kwargs, then return action_fn result."""
    received, action_fn = _make_recorder()
    monkeypatch.setenv("CDL_DLT_DEV_MODE", "true")
    runner = CliRunner(CtsSettings, action_fn)
    assert runner.pipeline_fn is action_fn
    result = runner.run(
        cli_args=["-i", str(tmp_path), "-o", str(tmp_path / "out"), "-d", LOCAL_FS_DEST, "--buffer-size", "7"],
        log_interval=3,
    )
    assert result is RUN_RESULT
    assert [(s.input_dir, s.buffer_size, s.log_interval, s.dlt_dev_mode) for s in received] == [
        (str(tmp_path.resolve()), 7, 3, True)
    ]


def test_cli_runner_pass_call_reads_argv(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Parse sys.argv, execute action_fn, and return None so the process exits with code 0."""
    settings = make_cts_settings(CtsSettings, tmp_path, use_destination=LOCAL_FS_DEST)
    received, action_fn = _make_recorder()
    monkeypatch.setattr(sys, "argv", ["prog", *settings_to_argv(settings)])
    assert CliRunner(CtsSettings, action_fn)() is None
    assert [s.input_dir for s in received] == [settings.input_dir]


@pytest.mark.parametrize(
    "cli_args",
    [
        pytest.param(["--buffer-size", "0"], id="invalid_value"),
        pytest.param(["--no-such-flag", "x"], id="unknown_arg"),
        pytest.param(["-p", "true"], id="metadata_without_output_dir"),
    ],
)
def test_cli_runner_fail_invalid_settings(cli_args: list[str]) -> None:
    """Raise before calling action_fn when CLI settings are invalid."""
    received, action_fn = _make_recorder()
    with pytest.raises((SettingsError, ValidationError)):
        CliRunner(CtsSettings, action_fn).run(cli_args=cli_args)
    assert received == []


def test_cli_runner_fail_frozen() -> None:
    """Reject attribute mutation on frozen CliRunner instances."""
    _, action_fn = _make_recorder()
    runner = CliRunner(CtsSettings, action_fn)
    with pytest.raises(FrozenInstanceError):
        runner.settings_cls = LoggerSettings  # pyright: ignore[reportAttributeAccessIssue]
