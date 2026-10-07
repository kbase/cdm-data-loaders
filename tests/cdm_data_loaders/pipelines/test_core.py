"""Tests for cdm_data_loaders.pipelines.core."""

import copy
import gzip
import json
import logging
import os
import re
from collections.abc import Iterator
from pathlib import Path
from typing import Any, Final
from unittest.mock import MagicMock

import dlt
import pytest
from dlt.extract import DltResource
from pydantic import ValidationError
from pydantic_settings import SettingsError
from requests import RequestException

from cdm_data_loaders.core.destination import PIPELINE_METADATA_NOT_LOCAL
from cdm_data_loaders.core.settings import CtsSettings, InputOutputSettings, LoggerSettings
from cdm_data_loaders.pipelines import core
from cdm_data_loaders.pipelines.core import (
    DISABLE_COMPRESSION_ENV_VAR,
    NO_MESSAGE,
    UNRESOLVED_OUTPUT_DIR,
    WEBHOOK_NOT_CONFIGURED,
    build_destination,
    compression_disabled,
    filesystem_resource,
    resolve_cts_settings,
    run_cli,
    run_pipeline,
    send_slack_message_carefully,
)
from tests.cdm_data_loaders.pipelines.conftest import (
    DLT_CONFIG,
)

CORE_LOGGER: Final[str] = core.__name__
SLACK_HOOK: Final[str] = "https://slack.hook/what/ever"
MESSAGE: Final[str] = "3... 2... 1... testing?"
SUCCESS_MESSAGE: Final[str] = "Pipeline completed successfully!"
NO_ALERTS_MESSAGE: Final[str] = f"No Slack alerts will be sent: {WEBHOOK_NOT_CONFIGURED}"

TINY_TABLE: Final[str] = "tiny"
TINY_PIPELINE: Final[str] = "test_core_tiny"
TINY_DATA: Final[list[dict[str, Any]]] = [
    {"entity_id": "tiny:one", "value": 1},
    {"entity_id": "tiny:two", "value": 2},
]


class ExtendedSettings(CtsSettings):
    """CtsSettings subclass with an extra field."""

    table_name: str = "things"


def make_settings(**kwargs: Any) -> CtsSettings:  # noqa: ANN401
    """Validate CtsSettings from kwargs only; model_validate does not read env vars or the CLI."""
    return CtsSettings.model_validate(kwargs)


def core_records(caplog: pytest.LogCaptureFixture) -> list[tuple[int, str]]:
    """Level and message of each record logged by the core module."""
    return [(record.levelno, record.getMessage()) for record in caplog.records if record.name == CORE_LOGGER]


def tiny_resource() -> DltResource:
    """A dlt resource emitting TINY_DATA."""
    return dlt.resource(TINY_DATA, name=TINY_TABLE)


def failing_resource() -> DltResource:
    """A dlt resource that raises part way through extraction."""

    def _generate() -> Iterator[dict[str, Any]]:
        yield TINY_DATA[0]
        err_msg = "extract went wrong"
        raise RuntimeError(err_msg)

    return dlt.resource(_generate, name=TINY_TABLE)


def read_jsonl_records(root: Path, table: str) -> list[dict[str, Any]]:
    """Read all jsonl records for a table under root, without dlt's internal columns."""
    records: list[dict[str, Any]] = []
    for path in sorted(root.glob(f"**/{table}/*.jsonl*")):
        if path.suffix == ".gz":
            with gzip.open(path, "rt") as f:
                lines = f.read().splitlines()
        else:
            lines = path.read_text().splitlines()
        records.extend(json.loads(line) for line in lines if line.strip())
    return [{k: v for k, v in record.items() if not k.startswith("_dlt")} for record in records]


@pytest.mark.parametrize(
    ("slack_hook", "message", "reason"),
    [
        pytest.param("", MESSAGE, WEBHOOK_NOT_CONFIGURED, id="no-hook"),
        pytest.param(SLACK_HOOK, "", NO_MESSAGE, id="empty-message"),
        pytest.param(SLACK_HOOK, " \n\t", NO_MESSAGE, id="whitespace-message"),
        pytest.param("", "", WEBHOOK_NOT_CONFIGURED, id="hook-checked-first"),
    ],
)
def test_send_slack_message_carefully_invalid_args(
    slack_hook: str, message: str, reason: str, slack_mock: MagicMock, caplog: pytest.LogCaptureFixture
) -> None:
    """Nothing is sent and a single warning is logged."""
    caplog.set_level(logging.WARNING, logger=CORE_LOGGER)
    assert send_slack_message_carefully(slack_hook, message) is None
    slack_mock.assert_not_called()
    assert core_records(caplog) == [(logging.WARNING, f"Cannot send slack message: {reason}")]


@pytest.mark.parametrize(
    ("kwargs", "expected_markdown"),
    [
        pytest.param({}, False, id="markdown-default"),
        pytest.param({"is_markdown": True}, True, id="markdown-true"),
        pytest.param({"is_markdown": False}, False, id="markdown-false"),
    ],
)
@pytest.mark.parametrize(
    ("message", "sent"),
    [
        pytest.param(MESSAGE, MESSAGE, id="message-as-is"),
        pytest.param(f"  {MESSAGE}\n", MESSAGE, id="message-stripped"),
    ],
)
def test_send_slack_message_carefully_sends(
    message: str, sent: str, kwargs: dict[str, bool], *, expected_markdown: bool, slack_mock: MagicMock
) -> None:
    """The stripped message and markdown flag are passed to the Slack sender."""
    send_slack_message_carefully(SLACK_HOOK, message, **kwargs)
    slack_mock.assert_called_once_with(SLACK_HOOK, sent, expected_markdown)


def test_send_slack_message_carefully_logs_request_errors(
    slack_mock: MagicMock, caplog: pytest.LogCaptureFixture
) -> None:
    """A RequestException is logged with its traceback rather than raised."""
    error = RequestException("slack is down")
    slack_mock.side_effect = error
    send_slack_message_carefully(SLACK_HOOK, MESSAGE)
    assert core_records(caplog) == [(logging.ERROR, "Failed to send slack message")]
    (record,) = [r for r in caplog.records if r.name == CORE_LOGGER]
    assert record.exc_info is not None
    assert record.exc_info[1] is error


def test_send_slack_message_carefully_propagates_other_errors(slack_mock: MagicMock) -> None:
    """Only RequestException is caught."""
    slack_mock.side_effect = ValueError("not a request problem")
    with pytest.raises(ValueError, match=r"^not a request problem$"):
        send_slack_message_carefully(SLACK_HOOK, MESSAGE)


COMPUTED_AND_OUTPUT: Final[set[str]] = {"output_dir", "output_is_local", "raw_data_dir", "pipeline_dir"}


@pytest.mark.parametrize(
    ("settings_kwargs", "expected_output_dir"),
    [
        pytest.param({}, "/configured/out", id="from-destination-config"),
        pytest.param({"output_dir": "/explicit/out"}, "/explicit/out", id="explicit-output-dir-wins"),
        pytest.param({"use_destination": "s3"}, "s3://bucket/prefix", id="other-destination"),
        pytest.param({"use_output_dir_for_pipeline_metadata": True}, "/configured/out", id="metadata-local"),
    ],
)
def test_resolve_cts_settings(settings_kwargs: dict[str, Any], expected_output_dir: str) -> None:
    """A copy is returned with output_dir resolved; the settings and config are untouched."""
    settings = make_settings(**settings_kwargs)
    original_dump = settings.model_dump()
    config = copy.deepcopy(DLT_CONFIG)

    resolved = resolve_cts_settings(settings, config)

    assert resolved is not settings
    assert resolved.output_dir == expected_output_dir
    assert resolved.raw_data_dir == f"{expected_output_dir}/raw_data"
    assert resolved.model_dump(exclude=COMPUTED_AND_OUTPUT) == settings.model_dump(exclude=COMPUTED_AND_OUTPUT)
    assert settings.model_dump() == original_dump
    assert config == DLT_CONFIG


def test_resolve_cts_settings_keeps_subclass() -> None:
    """The returned copy keeps the subclass and its extra fields."""
    settings = ExtendedSettings.model_validate({"table_name": "widgets"})
    resolved = resolve_cts_settings(settings, copy.deepcopy(DLT_CONFIG))
    assert type(resolved) is ExtendedSettings
    assert resolved.table_name == "widgets"
    assert resolved.output_dir == "/configured/out"


def test_resolve_cts_settings_defaults_to_dlt_config(monkeypatch: pytest.MonkeyPatch) -> None:
    """Without a config argument, dlt.config is used."""
    monkeypatch.setattr(dlt, "config", copy.deepcopy(DLT_CONFIG))
    assert resolve_cts_settings(make_settings()).output_dir == "/configured/out"


@pytest.mark.parametrize(
    ("settings_kwargs", "config", "message"),
    [
        pytest.param(
            {"use_destination": "missing"},
            DLT_CONFIG,
            "use_destination must be one of ['local_fs', 's3'], got 'missing'",
            id="unknown-destination",
        ),
        pytest.param({}, {}, "No valid destinations found in dlt configuration.", id="no-destinations"),
        pytest.param(
            {"use_destination": "s3", "use_output_dir_for_pipeline_metadata": True},
            DLT_CONFIG,
            PIPELINE_METADATA_NOT_LOCAL.format(url="s3://bucket/prefix"),
            id="metadata-remote-configured-url",
        ),
        pytest.param(
            {"output_dir": "s3://x"},
            DLT_CONFIG,
            "output_dir 's3://x' uses protocol 's3', but destination 'local_fs' is configured for 'file' "
            "('/configured/out/'). Choose a destination configured for 's3', or omit output_dir.",
            id="protocol-mismatch",
        ),
    ],
)
def test_resolve_cts_settings_errors(settings_kwargs: dict[str, Any], config: dict[str, Any], message: str) -> None:
    """Resolution errors are raised unchanged."""
    with pytest.raises(ValueError, match=f"^{re.escape(message)}$"):
        resolve_cts_settings(make_settings(**settings_kwargs), copy.deepcopy(config))


def test_build_destination_rejects_bucket_url(monkeypatch: pytest.MonkeyPatch) -> None:
    """bucket_url must come from settings.output_dir, not destination_kwargs."""
    factory = MagicMock()
    monkeypatch.setattr(dlt, "destination", factory)
    message = "Set the output location with settings.output_dir, not destination_kwargs['bucket_url']"
    with pytest.raises(ValueError, match=f"^{re.escape(message)}$"):
        build_destination(make_settings(output_dir="/out"), {"bucket_url": "/elsewhere"})
    factory.assert_not_called()


@pytest.mark.parametrize(
    ("destination_kwargs", "expected_kwargs"),
    [
        pytest.param(None, {}, id="none"),
        pytest.param({}, {}, id="empty"),
        pytest.param(
            {"layout": "{table_name}/{load_id}.{ext}"}, {"layout": "{table_name}/{load_id}.{ext}"}, id="extra"
        ),
    ],
)
def test_build_destination(
    destination_kwargs: dict[str, Any] | None, expected_kwargs: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    """The named destination is built with output_dir as bucket_url; the caller's kwargs are not mutated."""
    factory = MagicMock()
    monkeypatch.setattr(dlt, "destination", factory)
    original_kwargs = copy.deepcopy(destination_kwargs)

    result = build_destination(make_settings(use_destination="s3", output_dir="s3://bucket/prefix"), destination_kwargs)

    assert result is factory.return_value
    factory.assert_called_once_with("s3", bucket_url="s3://bucket/prefix", **expected_kwargs)
    assert destination_kwargs == original_kwargs


PREVIOUS_VALUES = pytest.mark.parametrize("previous", [None, "false", "true"], ids=["unset", "false", "true"])


@PREVIOUS_VALUES
def test_compression_disabled_active(previous: str | None, monkeypatch: pytest.MonkeyPatch) -> None:
    """The env var is 'true' inside the block and restored afterwards."""
    if previous is not None:
        monkeypatch.setenv(DISABLE_COMPRESSION_ENV_VAR, previous)
    with compression_disabled(active=True):
        assert os.environ[DISABLE_COMPRESSION_ENV_VAR] == "true"
    assert os.environ.get(DISABLE_COMPRESSION_ENV_VAR) == previous


@PREVIOUS_VALUES
def test_compression_disabled_active_restores_on_error(previous: str | None, monkeypatch: pytest.MonkeyPatch) -> None:
    """The env var is restored and the error re-raised when the block fails."""
    if previous is not None:
        monkeypatch.setenv(DISABLE_COMPRESSION_ENV_VAR, previous)
    with pytest.raises(RuntimeError, match="^boom$"), compression_disabled(active=True):
        raise RuntimeError("boom")  # noqa: EM101
    assert os.environ.get(DISABLE_COMPRESSION_ENV_VAR) == previous


@PREVIOUS_VALUES
def test_compression_disabled_inactive(previous: str | None, monkeypatch: pytest.MonkeyPatch) -> None:
    """When inactive, the env var is never touched."""
    if previous is not None:
        monkeypatch.setenv(DISABLE_COMPRESSION_ENV_VAR, previous)
    with compression_disabled(active=False):
        assert os.environ.get(DISABLE_COMPRESSION_ENV_VAR) == previous
    assert os.environ.get(DISABLE_COMPRESSION_ENV_VAR) == previous


@pytest.mark.parametrize(
    ("file_glob", "expected"),
    [
        pytest.param("*.jsonl", ["a.jsonl", "b.jsonl"], id="several-matches"),
        pytest.param("*.txt", ["c.txt"], id="one-match"),
        pytest.param("*.xml", [], id="no-matches"),
    ],
)
def test_filesystem_resource(tmp_path: Path, file_glob: str, expected: list[str]) -> None:
    """Only files matching the glob are listed."""
    for name in ("a.jsonl", "b.jsonl", "c.txt"):
        (tmp_path / name).write_text("{}\n")
    resource = filesystem_resource(str(tmp_path), file_glob)
    assert sorted(item["file_name"] for item in resource) == expected


@pytest.mark.parametrize(
    ("settings_kwargs", "expected_output_dir"),
    [
        pytest.param({"cli_args": []}, "/configured/out", id="from-destination-config"),
        pytest.param({"cli_args": ["--output-dir", "/cli/out/"]}, "/cli/out", id="long-output-dir"),
        pytest.param({"cli_args": ["-o", "/cli/out"]}, "/cli/out", id="short-output-dir"),
        pytest.param({"cli_args": ["-d", "s3"]}, "s3://bucket/prefix", id="short-use-destination"),
        pytest.param({"cli_args": [], "output_dir": "/init/out"}, "/init/out", id="init-kwarg"),
    ],
)
def test_run_cli_resolves_cts_settings(
    settings_kwargs: dict[str, Any],
    expected_output_dir: str,
    mock_init_logger: MagicMock,
    caplog: pytest.LogCaptureFixture,  # noqa: ARG001
) -> None:
    """CtsSettings are parsed, resolved, and passed to the pipeline function."""
    pipeline_fn = MagicMock()

    result = run_cli(CtsSettings, pipeline_fn, settings_kwargs)

    assert result is pipeline_fn.return_value
    pipeline_fn.assert_called_once()
    (passed,) = pipeline_fn.call_args.args
    assert type(passed) is CtsSettings
    assert passed.output_dir == expected_output_dir
    mock_init_logger.assert_called_once()
    assert isinstance(mock_init_logger.call_args.args[0], CtsSettings)


@pytest.mark.parametrize("settings_cls", [LoggerSettings, InputOutputSettings])
def test_run_cli_does_not_resolve_other_settings(
    settings_cls: type[LoggerSettings], mock_init_logger: MagicMock, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Settings that are not CtsSettings are passed on without resolution."""
    resolve = MagicMock()
    monkeypatch.setattr(core, "resolve_cts_settings", resolve)
    pipeline_fn = MagicMock()

    result = run_cli(settings_cls, pipeline_fn, {"cli_args": []})

    assert result is pipeline_fn.return_value
    resolve.assert_not_called()
    (passed,) = pipeline_fn.call_args.args
    assert type(passed) is settings_cls
    mock_init_logger.assert_called_once_with(passed)


@pytest.mark.parametrize(
    ("cli_args", "exc_type"),
    [
        pytest.param(["--buffer-size", "0"], ValidationError, id="invalid-field-value"),
        pytest.param(["--buffer-size"], SettingsError, id="missing-cli-value"),
        pytest.param(
            ["--output-dir", "s3://bucket/x", "--use-output-dir-for-pipeline-metadata", "true"],
            ValidationError,
            id="remote-metadata-rejected-by-settings",
        ),
        pytest.param(["--use-destination", "missing"], ValueError, id="unknown-destination"),
    ],
)
def test_run_cli_config_errors(
    cli_args: list[str],
    exc_type: type[Exception],
    mock_init_logger: MagicMock,  # noqa: ARG001
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Config errors are logged and re-raised, and the pipeline does not run."""
    pipeline_fn = MagicMock()
    with pytest.raises(exc_type) as excinfo:
        run_cli(CtsSettings, pipeline_fn, {"cli_args": cli_args})
    assert excinfo.type is exc_type
    pipeline_fn.assert_not_called()
    assert core_records(caplog) == [(logging.ERROR, "Error initialising config")]


def test_run_cli_unexpected_error(mock_init_logger: MagicMock, caplog: pytest.LogCaptureFixture) -> None:
    """Other errors are logged separately and re-raised."""
    mock_init_logger.side_effect = RuntimeError("logger exploded")
    pipeline_fn = MagicMock()
    with pytest.raises(RuntimeError, match="^logger exploded$"):
        run_cli(CtsSettings, pipeline_fn, {"cli_args": []})
    pipeline_fn.assert_not_called()
    assert core_records(caplog) == [(logging.ERROR, "Unexpected error setting up config")]


def test_run_pipeline_requires_resolved_output_dir(mock_pipeline: MagicMock) -> None:
    """Unresolved settings are rejected before any pipeline is created."""
    with pytest.raises(ValueError, match=f"^{re.escape(UNRESOLVED_OUTPUT_DIR)}$"):
        run_pipeline(make_settings(), tiny_resource())
    mock_pipeline.assert_not_called()


@pytest.mark.parametrize(
    ("settings_kwargs", "expected_extra"),
    [
        pytest.param({}, {}, id="defaults"),
        pytest.param({"dlt_dev_mode": True}, {"dev_mode": True}, id="dev-mode"),
        pytest.param(
            {"use_output_dir_for_pipeline_metadata": True}, {"pipelines_dir": "/out/.dlt_conf"}, id="metadata-dir"
        ),
        pytest.param(
            {"dlt_dev_mode": True, "use_output_dir_for_pipeline_metadata": True},
            {"dev_mode": True, "pipelines_dir": "/out/.dlt_conf"},
            id="dev-mode-and-metadata-dir",
        ),
    ],
)
def test_run_pipeline_pipeline_kwargs(
    settings_kwargs: dict[str, Any],
    expected_extra: dict[str, Any],
    mock_pipeline: MagicMock,
    mock_build_destination: MagicMock,
) -> None:
    """Settings add pipelines_dir and dev_mode; the caller's dict is not mutated."""
    pipeline_kwargs = {"pipeline_name": "p", "dataset_name": "d"}
    run_pipeline(make_settings(output_dir="/out", **settings_kwargs), tiny_resource(), pipeline_kwargs=pipeline_kwargs)
    mock_pipeline.assert_called_once_with(
        destination=mock_build_destination.return_value, pipeline_name="p", dataset_name="d", **expected_extra
    )
    assert pipeline_kwargs == {"pipeline_name": "p", "dataset_name": "d"}


def test_run_pipeline_builds_destination_when_not_given(
    mock_pipeline: MagicMock, mock_build_destination: MagicMock
) -> None:
    """Without a destination, one is built from the settings and destination_kwargs."""
    settings = make_settings(output_dir="/out")
    destination_kwargs = {"layout": "{table_name}/{load_id}.{ext}"}
    run_pipeline(settings, tiny_resource(), destination_kwargs=destination_kwargs)
    mock_build_destination.assert_called_once_with(settings, destination_kwargs)
    mock_pipeline.assert_called_once_with(destination=mock_build_destination.return_value)


def test_run_pipeline_uses_given_destination(mock_pipeline: MagicMock, mock_build_destination: MagicMock) -> None:
    """A supplied destination is used as is and destination_kwargs are ignored."""
    destination = MagicMock()
    run_pipeline(
        make_settings(output_dir="/out"), tiny_resource(), destination=destination, destination_kwargs={"layout": "x"}
    )
    mock_build_destination.assert_not_called()
    mock_pipeline.assert_called_once_with(destination=destination)


@pytest.mark.parametrize(("dlt_dev_mode", "expected"), [(True, "true"), (False, None)], ids=["dev-mode", "normal"])
def test_run_pipeline_compression_only_disabled_in_dev_mode(
    *,
    dlt_dev_mode: bool,
    expected: str | None,
    mock_pipeline: MagicMock,
    mock_build_destination: MagicMock,  # noqa: ARG001
) -> None:
    """Compression is disabled while the pipeline is created and run in dev mode, then restored."""
    seen: list[str | None] = []
    pipeline = mock_pipeline.return_value

    def create(**_kwargs: Any) -> MagicMock:  # noqa: ANN401
        seen.append(os.environ.get(DISABLE_COMPRESSION_ENV_VAR))
        return pipeline

    def run(*_args: Any, **_kwargs: Any) -> None:  # noqa: ANN401
        seen.append(os.environ.get(DISABLE_COMPRESSION_ENV_VAR))

    mock_pipeline.side_effect = create
    pipeline.run.side_effect = run

    run_pipeline(make_settings(output_dir="/out", dlt_dev_mode=dlt_dev_mode), tiny_resource())

    assert seen == [expected, expected]
    assert DISABLE_COMPRESSION_ENV_VAR not in os.environ
