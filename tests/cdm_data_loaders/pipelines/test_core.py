"""Tests for the shared core DLT pipeline functions."""

import gzip
import logging
import sys
from collections.abc import Callable, Iterator
from dataclasses import FrozenInstanceError
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Final, cast
from unittest.mock import MagicMock, call, patch

import dlt
import pytest
from dlt.common.destination.exceptions import DestinationTerminalException
from dlt.common.pipeline import LoadInfo
from dlt.common.schema.typing import TTableSchema
from dlt.common.typing import TDataItems
from dlt.destinations.impl.filesystem.filesystem import FilesystemLoadJob
from dlt.extract.resource import DltResource
from pydantic import ValidationError
from pydantic_settings import SettingsError
from requests import RequestException

from cdm_data_loaders.core.fields import DEFAULT_REJECTED_SUFFIX, OUTPUT_DIR, PIPELINE_DIR_NAME, USE_DESTINATION
from cdm_data_loaders.core.settings import (
    CdmDataLoadersBase,
    CtsSettings,
    FileFormatIngestSettings,
    LocalInputOutputSettings,
    LoggerSettings,
)
from cdm_data_loaders.pipelines import core
from cdm_data_loaders.pipelines.core import (
    DEFAULT_SCHEMA_CONTRACT,
    LOAD_INFO_TABLE_NAME,
    NO_MESSAGE,
    WEBHOOK_NOT_CONFIGURED,
    PipelineCli,
    build_file_ingest_kwargs,
    build_filesystem_resource,
    run_cli,
    run_pipeline,
    send_slack_message_carefully,
)
from tests.cdm_data_loaders.pipelines.conftest import (
    HOOK_ENV_VAR,
    LOAD_INFO_SAVE_FAILED,
    LOAD_INFO_SAVE_FAILED_JOBS,
)
from tests.cdm_data_loaders.pipelines.helpers import (
    check_pipeline_report,
    read_destination_rows,
)
from tests.conftest import LOCAL_FS, S3
from tests.dlt_config_isolation import dlt_config_unset, isolated_dlt_config
from tests.helpers import make_cts_settings, settings_to_argv

TINY_RESOURCE_DATA: Final[list[dict[str, Any]]] = [
    {"entity_id": "tiny:one", "value": 1},
    {"entity_id": "tiny:two", "value": 2},
]
TINY_PIPELINE_NAME: Final[str] = "test_core_tiny_pipeline"
TINY_TABLE_NAME: Final[str] = "tiny"
FILESYSTEM_DESTINATION_TYPE: Final[str] = "dlt.destinations.filesystem"
ALL_JOBS_COMPLETED: Final[str] = "All pipeline jobs completed successfully"
FAILED_JOBS_WARNING: Final[str] = "Pipeline contains failed jobs"

SETTINGS_CLASSES: Final[list[type[CdmDataLoadersBase]]] = [LoggerSettings, LocalInputOutputSettings, CtsSettings]


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


def read_output_jsonl_records(base_dir: Path) -> list[dict[str, Any]]:
    """Read the tiny table records under base_dir, minus dlt's internal columns."""
    records = read_destination_rows(base_dir, TINY_TABLE_NAME)
    assert records
    return records


def make_mock_pipeline(save_run_result: object, slack_hook: str | None = None) -> MagicMock:
    """A mock dlt pipeline: the first run returns an entity load info; the second returns or raises save_run_result."""
    pipeline = MagicMock()
    pipeline.pipeline_name = TINY_PIPELINE_NAME
    pipeline.dataset_name = "test_dataset"
    pipeline.runtime_config.slack_incoming_hook = slack_hook
    pipeline.last_trace.started_at = datetime(2025, 1, 2, 3, 4, 0, tzinfo=UTC)
    pipeline.last_trace.finished_at = datetime(2025, 1, 2, 3, 4, 10, tzinfo=UTC)
    entity_load_info = MagicMock()
    entity_load_info.has_failed_jobs = False
    entity_load_info.loads_ids = ["1.0"]
    entity_load_info.started_at = datetime(2025, 1, 2, 3, 4, 5, tzinfo=UTC)
    entity_load_info.load_packages = []
    entity_load_info.asdict.return_value = {"loads_ids": ["1.0"]}
    pipeline.run.side_effect = [entity_load_info, save_run_result]
    return pipeline


@pytest.fixture(params=["no-relevant-attrs", "output_dir-only-no-destination"])
def settings_missing_sync_attrs(
    request: pytest.FixtureRequest, tmp_path: Path
) -> LoggerSettings | LocalInputOutputSettings:
    """Settings instances missing one or more attribute the sync tests check for via hasattr."""
    if request.param == "no-relevant-attrs":
        return LoggerSettings.model_validate({})
    input_dir = tmp_path / "input"
    input_dir.mkdir(exist_ok=True)
    return LocalInputOutputSettings.model_validate({"input_dir": input_dir, "output_dir": tmp_path / "output"})


# send slack message carefully
SLACK_HOOK: Final[str] = "https://slack.hook/what/ever"
TEST_MESSAGE: Final[str] = "3... 2... 1... testing?"


@pytest.mark.parametrize(
    ("slack_hook", "message", "err_msg"),
    [
        pytest.param(None, TEST_MESSAGE, WEBHOOK_NOT_CONFIGURED, id="hook_none"),
        pytest.param("", TEST_MESSAGE, WEBHOOK_NOT_CONFIGURED, id="hook_empty"),
        pytest.param(SLACK_HOOK, "", NO_MESSAGE, id="message_empty"),
        pytest.param(SLACK_HOOK, "   ", NO_MESSAGE, id="message_spaces"),
        pytest.param(SLACK_HOOK, "\n\t", NO_MESSAGE, id="message_newline_tab"),
        pytest.param("", "", WEBHOOK_NOT_CONFIGURED, id="both_empty"),
    ],
)
def test_send_slack_message_carefully_params_fail(
    slack_hook: str | None,
    message: str,
    err_msg: str,
    caplog: pytest.LogCaptureFixture,
    slack_mock: MagicMock,
) -> None:
    """A missing/empty hook or a blank message logs exactly one warning and sends nothing."""
    send_slack_message_carefully(slack_hook, message)  # pyright: ignore[reportArgumentType]
    slack_mock.assert_not_called()
    assert [(record.levelno, record.getMessage()) for record in caplog.records] == [
        (logging.WARNING, f"Cannot send slack message: {err_msg}")
    ]


@pytest.mark.parametrize("markdown", [True, False, None])
def test_send_slack_message_carefully_markdown_params(markdown: bool | None, slack_mock: MagicMock) -> None:
    """The message is stripped and sent once, with the is_markdown flag passed through."""
    if markdown is None:
        send_slack_message_carefully(SLACK_HOOK, f"  {TEST_MESSAGE} \n")
    else:
        send_slack_message_carefully(SLACK_HOOK, f"  {TEST_MESSAGE} \n", markdown)

    slack_mock.assert_called_once_with(SLACK_HOOK, TEST_MESSAGE, bool(markdown))


@pytest.mark.parametrize("exc", [RequestException("Oh no! An error!"), ValueError("bad payload")])
def test_send_slack_message_fail_error_oh_no(
    exc: Exception, slack_mock: MagicMock, caplog: pytest.LogCaptureFixture
) -> None:
    """Any error from the Slack sender is caught and logged as an error."""
    slack_mock.side_effect = exc
    send_slack_message_carefully(SLACK_HOOK, TEST_MESSAGE)
    assert [(record.levelno, record.getMessage()) for record in caplog.records] == [
        (logging.ERROR, "Failed to send slack message")
    ]


def test_settings_missing_sync_attrs_pass_fixture_shape(
    settings_missing_sync_attrs: LoggerSettings | LocalInputOutputSettings,
) -> None:
    """Sanity-check the fixture: confirm which attributes are genuinely absent."""
    assert hasattr(settings_missing_sync_attrs, OUTPUT_DIR) == isinstance(
        settings_missing_sync_attrs, LocalInputOutputSettings
    )
    assert not hasattr(settings_missing_sync_attrs, USE_DESTINATION)


@pytest.mark.parametrize(
    ("config", "use_destination"),
    [
        pytest.param({}, LOCAL_FS, id="empty-config"),
        pytest.param({"destination": {}}, LOCAL_FS, id="no-destinations"),
        pytest.param({"destination": {LOCAL_FS: {"bucket_url": "/out"}}}, LOCAL_FS, id="no-destination-type"),
        pytest.param({"destination": {LOCAL_FS: {"destination_type": ""}}}, LOCAL_FS, id="empty-destination-type"),
        pytest.param({"destination": {S3: {"destination_type": "filesystem"}}}, LOCAL_FS, id="other-name-only"),
    ],
)
def test_run_pipeline_fail_invalid_destination_config(
    config: dict[str, Any], use_destination: str, tmp_path: Path
) -> None:
    """Invalid destination configs cause the pipeline to fail with an 'Invalid destination config' message."""
    settings = CtsSettings(input_dir=tmp_path, output_dir=tmp_path, use_destination=use_destination)  # pyright: ignore[reportCallIssue]

    with (
        isolated_dlt_config(config),
        pytest.raises(ValueError, match=f"Invalid destination configuration for {use_destination}"),
    ):
        run_pipeline(settings, [])


@pytest.mark.parametrize(
    ("config", "use_destination"),
    [
        pytest.param({}, "filesystem", id="built-in-type-name-not-configured"),
    ],
)
def test_run_pipeline_invalid_destination_config_fails_at_pipeline_run(
    config: dict[str, Any], use_destination: str, tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """An under-specified destination config fails at pipeline.run."""
    settings = CtsSettings(input_dir=tmp_path, output_dir=tmp_path, use_destination=use_destination)  # pyright: ignore[reportCallIssue]
    with isolated_dlt_config(config):
        load_info = run_pipeline(settings, [])
    assert load_info is None
    error_records = [record for record in caplog.records if record.levelno == logging.ERROR]
    assert error_records
    err_msg = error_records[-1].getMessage()
    assert err_msg.startswith("Pipeline failed: ")
    assert "Missing 1 field(s) in configuration `FilesystemDestinationClientConfiguration`: `bucket_url`" in err_msg


# tests for run_cli()
def _settings_kwargs_for(settings_cls: type[CdmDataLoadersBase], tmp_path: Path) -> dict[str, Any]:
    """Return minimal required kwargs for instantiating `settings_cls` in `run_cli` tests."""
    if issubclass(settings_cls, CtsSettings):
        return {"input_dir": tmp_path, "output_dir": tmp_path, "use_destination": LOCAL_FS}
    if issubclass(settings_cls, LocalInputOutputSettings):
        return {"input_dir": tmp_path, "output_dir": tmp_path}
    return {}


@pytest.mark.parametrize("settings_cls", SETTINGS_CLASSES)
def test_run_cli_calls_settings_cls_with_cli_args(settings_cls: type[CtsSettings], tmp_path: Path) -> None:
    """Ensure run_cli instantiates the supplied settings class via CliApp.run."""
    captured: list[CtsSettings] = []

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
    """run_cli passes the settings through unchanged: output_dir is not filled in from the dlt config."""
    pipeline_fn_config: list[CtsSettings] = []

    def pipeline_fn(settings: CtsSettings) -> LoadInfo | None:
        pipeline_fn_config.append(settings)
        return None

    kwargs = _settings_kwargs_for(CtsSettings, tmp_path)
    if dlt_config_set:
        run_cli(CtsSettings, pipeline_fn, kwargs, cli_args=[])
    else:
        with dlt_config_unset():
            run_cli(CtsSettings, pipeline_fn, kwargs, cli_args=[])

    assert len(pipeline_fn_config) == 1
    assert pipeline_fn_config[0].output_dir == tmp_path.resolve()
    assert pipeline_fn_config[0].use_destination == LOCAL_FS


@pytest.mark.parametrize("settings_cls", SETTINGS_CLASSES)
def test_run_cli_returns_pipeline_fn_result_verbatim(settings_cls: type[CtsSettings], tmp_path: Path) -> None:
    """run_cli dispatches the instantiated settings to pipeline_fn and returns its result verbatim."""
    sentinel = {"loads_ids": ["load-1"]}
    received: list[CtsSettings] = []

    def pipeline_fn(settings: CtsSettings) -> dict[str, Any]:
        received.append(settings)
        return sentinel

    returned = run_cli(settings_cls, pipeline_fn, _settings_kwargs_for(settings_cls, tmp_path), cli_args=[])

    assert returned is sentinel
    assert len(received) == 1
    assert isinstance(received[0], settings_cls)


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

    error_records = [record for record in caplog.records if record.levelno == logging.ERROR]
    assert error_records
    assert error_records[-1].getMessage() == "Error initialising config"


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

    error_records = [record for record in caplog.records if record.levelno == logging.ERROR]
    assert error_records
    assert error_records[-1].getMessage() == "Unexpected error setting up config"

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


# run_pipeline bootstrap: destination and dev_mode derivation
def test_run_pipeline_builds_destination_from_settings(mock_dlt: MagicMock, tmp_path: Path) -> None:
    """The destination comes from settings.use_destination only; output_dir has no effect on it."""
    settings = make_cts_settings(
        CtsSettings,
        tmp_path,
        use_destination=S3,
        dlt_dev_mode=True,
    )

    run_pipeline(settings, MagicMock())

    mock_dlt.destination.assert_called_once_with(S3)
    mock_dlt.pipeline.assert_called_once_with(dev_mode=True)
    assert mock_dlt.pipeline.return_value.destination == mock_dlt.destination.return_value
    assert mock_dlt.pipeline.return_value.run.call_count == 2


@pytest.mark.parametrize(
    ("use_destination", "destination_kwargs"),
    [
        pytest.param(S3, {}, id="no-kwargs"),
        pytest.param(LOCAL_FS, {"max_table_nesting": 0}, id="extra-kwarg"),
        pytest.param(LOCAL_FS, {"bucket_url": "/elsewhere"}, id="bucket-url-override"),
    ],
)
def test_run_pipeline_forwards_destination_kwargs(
    mock_dlt: MagicMock, use_destination: str, destination_kwargs: dict[str, Any], tmp_path: Path
) -> None:
    """destination_kwargs are forwarded to the dlt.destination constructor."""
    settings = make_cts_settings(CtsSettings, tmp_path, use_destination=use_destination)

    run_pipeline(settings, MagicMock(), destination_kwargs=dict(destination_kwargs))

    mock_dlt.destination.assert_called_once_with(use_destination, **destination_kwargs)


def test_run_pipeline_fail_destination_not_configured(mock_dlt: MagicMock, tmp_path: Path) -> None:
    """An unconfigured use_destination raises ValueError before the pipeline runs."""
    mock_dlt.destination.side_effect = ValueError("not configured")
    settings = make_cts_settings(CtsSettings, tmp_path, use_destination="missing_dest")

    with pytest.raises(ValueError, match=r"^Invalid destination configuration for missing_dest$"):
        run_pipeline(settings, MagicMock())

    mock_dlt.destination.assert_called_once_with("missing_dest")
    mock_dlt.pipeline.return_value.run.assert_not_called()


def test_run_pipeline_no_slack_env_var_when_vars_missing(
    dlt_test_settings: CtsSettings,
    slack_mock: MagicMock,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """No slack hook is configured; no slack alerts are sent."""
    for var in ("VARIABLE_B", "VARIABLE_T", "CHAR_STR", "RUNTIME__SLACK_INCOMING_HOOK"):
        monkeypatch.delenv(var, raising=False)

    load_info = run_pipeline(dlt_test_settings, tiny_resource())

    assert load_info is not None
    assert not load_info.has_failed_jobs
    slack_mock.assert_not_called()
    assert f"No Slack alerts will be sent: {WEBHOOK_NOT_CONFIGURED}" in caplog.messages


# run_pipeline tests
def test_run_pipeline_minimal(dlt_test_settings: CtsSettings, destination_bucket: Path) -> None:
    """A tiny pipeline loads its records into the destination bucket, not into output_dir."""
    load_info = run_pipeline(dlt_test_settings, tiny_resource())

    assert load_info is not None
    assert not load_info.has_failed_jobs
    assert read_output_jsonl_records(destination_bucket) == TINY_RESOURCE_DATA
    assert read_destination_rows(Path(dlt_test_settings.output_dir), TINY_TABLE_NAME) == []


def test_run_pipeline_pass_destination_kwargs_bucket_url_override(
    dlt_test_settings: CtsSettings, tmp_path: Path
) -> None:
    """A bucket_url in destination_kwargs overrides the bucket_url in the dlt config."""
    override_dir = tmp_path / "override"
    override_dir.mkdir()

    load_info = run_pipeline(dlt_test_settings, tiny_resource(), destination_kwargs={"bucket_url": str(override_dir)})

    assert load_info is not None
    assert not load_info.has_failed_jobs
    assert read_output_jsonl_records(override_dir) == TINY_RESOURCE_DATA
    assert read_destination_rows(tmp_path / "bucket", TINY_TABLE_NAME) == []


def test_run_pipeline_returns_none_on_pipeline_run_failure(
    dlt_test_settings: CtsSettings, caplog: pytest.LogCaptureFixture
) -> None:
    """A resource that raises mid-extraction is caught: run_pipeline logs, writes an error report, and returns None."""
    load_info = run_pipeline(dlt_test_settings, failing_resource())

    assert load_info is None
    assert caplog.records[-1].levelno == logging.ERROR
    err_msg = caplog.records[-1].getMessage()
    assert err_msg.startswith("Pipeline failed: ")
    assert ALL_JOBS_COMPLETED not in caplog.messages
    assert FAILED_JOBS_WARNING not in caplog.messages
    # check for the pipeline dir
    pipeline_dir = Path(dlt_test_settings.output_dir) / PIPELINE_DIR_NAME
    assert pipeline_dir.is_dir()
    report_file_path = pipeline_dir / "report.txt"
    report_data = check_pipeline_report(
        report_file_path,
        {
            "status": "error",
            "pipeline_err_msg": err_msg,
        },
    )
    assert report_data["pipeline"]
    assert report_data["dataset"]
    assert report_data["started"] <= report_data["finished"]


@pytest.mark.parametrize("slack_configured", [True, False])
def test_run_pipeline_slack_configured(
    dlt_test_settings: CtsSettings,
    slack_mock: MagicMock,
    slack_configured: bool,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A slack message is sent if the slack_incoming_hook runtime config value is available."""
    if slack_configured:
        monkeypatch.setenv("RUNTIME__SLACK_INCOMING_HOOK", SLACK_HOOK)

    load_info = run_pipeline(dlt_test_settings, tiny_resource())

    assert load_info is not None
    assert not load_info.has_failed_jobs
    if slack_configured:
        slack_mock.assert_called_once_with(
            SLACK_HOOK,
            "Pipeline completed successfully!",
            False,  # noqa: FBT003
        )
        assert f"No Slack alerts will be sent: {WEBHOOK_NOT_CONFIGURED}" not in caplog.messages
    else:
        slack_mock.assert_not_called()
        assert f"No Slack alerts will be sent: {WEBHOOK_NOT_CONFIGURED}" in caplog.messages
    assert ALL_JOBS_COMPLETED in caplog.messages


@pytest.mark.parametrize("slack_configured", [True, False])
def test_run_pipeline_slack_configured_on_failure(
    dlt_test_settings: CtsSettings,
    slack_mock: MagicMock,
    slack_configured: bool,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The failure message is sent to slack if a hook is configured; nothing is sent otherwise."""
    if slack_configured:
        monkeypatch.setenv("RUNTIME__SLACK_INCOMING_HOOK", SLACK_HOOK)

    load_info = run_pipeline(dlt_test_settings, failing_resource())

    assert load_info is None
    assert caplog.records[-1].levelno == logging.ERROR
    err_msg = caplog.records[-1].getMessage()
    assert err_msg.startswith("Pipeline failed: ")
    assert "Oh crap!!" in err_msg
    if slack_configured:
        assert slack_mock.call_args.args[0] == SLACK_HOOK
        assert slack_mock.call_args.args[1].startswith("Pipeline failed: ")
        assert "Oh crap!!" in slack_mock.call_args.args[1]
        assert slack_mock.call_args.args[2] is False
    else:
        slack_mock.assert_not_called()


def test_run_pipeline_pipelines_dir_used_for_pipeline_metadata(dlt_test_settings: CtsSettings) -> None:
    """The pipeline metadata directory is created at settings.pipeline_dir and contains state and report files."""
    assert dlt_test_settings.pipeline_dir is not None
    load_info = run_pipeline(dlt_test_settings, tiny_resource())

    assert load_info is not None
    assert not load_info.has_failed_jobs
    assert dlt_test_settings.pipeline_dir.exists()
    pipeline_state_files = list(dlt_test_settings.pipeline_dir.glob("**/*.json"))
    assert any(p.name == "state.json" for p in pipeline_state_files)
    # report is also present
    assert dlt_test_settings.report_path.is_file()


def test_run_pipeline_dev_mode_true_loads_into_fresh_dataset(
    dlt_test_settings: CtsSettings, destination_bucket: Path
) -> None:
    """dev_mode=True is forwarded to dlt.pipeline(); the pipeline loads into a suffixed dataset."""
    settings = dlt_test_settings.model_copy(update={"dlt_dev_mode": True})

    load_info = run_pipeline(settings, tiny_resource())

    assert load_info is not None
    assert not load_info.has_failed_jobs
    # dataset_name is `dlt_pipeline_dataset_\d+`, where \d+ is YYYYMMDDHHMMSS
    assert load_info.dataset_name.startswith(f"{load_info.pipeline.pipeline_name}_dataset_")
    assert load_info.dataset_name != f"{load_info.pipeline.pipeline_name}_dataset"
    assert read_output_jsonl_records(destination_bucket) == TINY_RESOURCE_DATA


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
    dlt_test_settings: CtsSettings,
    destination_bucket: Path,
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
    assert read_output_jsonl_records(destination_bucket) == TINY_RESOURCE_DATA


# load_info saved as part of the dataset
def test_run_pipeline_saves_load_info_to_dataset(dlt_test_settings: CtsSettings, destination_bucket: Path) -> None:
    """The pipeline's load_info is saved as a table in the same dataset."""
    load_info = run_pipeline(dlt_test_settings, tiny_resource())

    assert load_info is not None
    assert not load_info.has_failed_jobs
    rows = read_destination_rows(destination_bucket, LOAD_INFO_TABLE_NAME)
    assert len(rows) == 1
    row = rows[0]
    assert row["dataset_name"] == load_info.dataset_name
    assert row["destination_name"] == dlt_test_settings.use_destination
    assert row["first_run"] is not None
    assert row["load_packages"]
    assert isinstance(row["load_packages"][0], dict)
    assert "load_id" in row["load_packages"][0]
    assert set(row["pipeline"]) == {"pipeline_name"}


def test_run_pipeline_load_info_saved_to_same_dataset(dlt_test_settings: CtsSettings, destination_bucket: Path) -> None:
    """The load info table lands in the same dataset directory as the resource tables."""
    load_info = run_pipeline(dlt_test_settings, tiny_resource())

    assert load_info is not None
    dataset_dir = destination_bucket / load_info.dataset_name
    assert (dataset_dir / TINY_TABLE_NAME).is_dir()
    assert (dataset_dir / LOAD_INFO_TABLE_NAME).is_dir()
    assert read_output_jsonl_records(destination_bucket) == TINY_RESOURCE_DATA
    assert read_destination_rows(destination_bucket, LOAD_INFO_TABLE_NAME)


def test_run_pipeline_load_info_flat_structure(dlt_test_settings: CtsSettings, destination_bucket: Path) -> None:
    """max_table_nesting=0 keeps nested structures as json columns; no child tables."""
    load_info = run_pipeline(dlt_test_settings, tiny_resource())

    assert load_info is not None
    dataset_dir = destination_bucket / load_info.dataset_name
    assert list(dataset_dir.glob("_dlt_load_info__*")) == []
    rows = read_destination_rows(destination_bucket, LOAD_INFO_TABLE_NAME)
    assert len(rows) == 1
    row = rows[0]
    for nested in ("load_packages", "outputs", "job_metrics"):
        assert isinstance(row[nested], list)
        assert row[nested], f"{nested} must not be normalized into a child table"
        assert all(isinstance(item, dict) for item in row[nested])


def test_run_pipeline_load_info_runs_accumulate(dlt_test_settings: CtsSettings, destination_bucket: Path) -> None:
    """Each pipeline run appends its own load_info row; earlier rows are preserved."""
    first = run_pipeline(dlt_test_settings, tiny_resource())
    assert first is not None

    second = run_pipeline(dlt_test_settings, tiny_resource())
    assert second is not None

    rows = read_destination_rows(destination_bucket, LOAD_INFO_TABLE_NAME)
    assert len(rows) == 2
    saved_load_ids = {tuple(row["loads_ids"]) for row in rows}
    assert len(saved_load_ids) == 2
    # each accumulated row references one of the entity data loads
    tiny_load_ids = {
        row["_dlt_load_id"]
        for row in read_destination_rows(destination_bucket, TINY_TABLE_NAME, strip_metadata=False)
    }
    assert saved_load_ids == {(load_id,) for load_id in tiny_load_ids}


def test_run_pipeline_load_info_in_dev_mode(dlt_test_settings: CtsSettings, destination_bucket: Path) -> None:
    """dev_mode runs save the load_info into the same suffixed dataset as the resource data."""
    settings = dlt_test_settings.model_copy(update={"dlt_dev_mode": True})
    load_info = run_pipeline(settings, tiny_resource())

    assert load_info is not None
    assert load_info.dataset_name != "dlt_pipeline_dataset"
    dataset_dir = destination_bucket / load_info.dataset_name
    assert (dataset_dir / TINY_TABLE_NAME).is_dir()
    rows = read_destination_rows(destination_bucket, LOAD_INFO_TABLE_NAME)
    assert len(rows) == 1
    assert rows[0]["dataset_name"] == load_info.dataset_name


def test_run_pipeline_load_info_named_pipeline_and_dataset(
    dlt_test_settings: CtsSettings, destination_bucket: Path
) -> None:
    """A named pipeline/dataset save lands in the named dataset with a load_info row."""
    load_info = run_pipeline(
        dlt_test_settings,
        tiny_resource(),
        pipeline_kwargs={"pipeline_name": TINY_PIPELINE_NAME, "dataset_name": "core_test_dataset"},
    )

    assert load_info is not None
    rows = read_destination_rows(destination_bucket, LOAD_INFO_TABLE_NAME)
    assert len(rows) == 1
    assert rows[0]["dataset_name"] == "core_test_dataset"
    assert rows[0]["pipeline"] == {"pipeline_name": TINY_PIPELINE_NAME}
    assert (destination_bucket / "core_test_dataset" / LOAD_INFO_TABLE_NAME).is_dir()


def test_run_pipeline_load_info_returns_original_import_result(
    dlt_test_settings: CtsSettings, destination_bucket: Path
) -> None:
    """run_pipeline returns the original resource load; the saved row references it, not the save load."""
    load_info = run_pipeline(dlt_test_settings, tiny_resource())

    assert load_info is not None
    assert not load_info.has_failed_jobs
    rows = read_destination_rows(destination_bucket, LOAD_INFO_TABLE_NAME, strip_metadata=False)
    assert len(rows) == 1
    # loads_ids is the original payload; _dlt_load_id is the save run's own load id
    assert rows[0]["loads_ids"] != [rows[0]["_dlt_load_id"]]
    assert load_info.loads_ids == rows[0]["loads_ids"]
    assert load_info.dataset_name == rows[0]["dataset_name"]


def test_run_pipeline_load_info_save_resource_error_still_returns_load_info(
    dlt_test_settings: CtsSettings,
    destination_bucket: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """An error building the load_info resource is logged; the entity load_info is still returned."""
    real_resource = core.dlt.resource

    def resource_boom(*args: Any, **kwargs: Any) -> Any:  # noqa: ANN401
        if kwargs.get("name") == LOAD_INFO_TABLE_NAME:
            err_msg = "load info save failed"
            raise RuntimeError(err_msg)
        return real_resource(*args, **kwargs)

    resource = tiny_resource()
    monkeypatch.setattr(core.dlt, "resource", resource_boom)
    load_info = run_pipeline(dlt_test_settings, resource)

    assert load_info is not None
    assert not load_info.has_failed_jobs
    assert read_output_jsonl_records(destination_bucket) == TINY_RESOURCE_DATA
    assert read_destination_rows(destination_bucket, LOAD_INFO_TABLE_NAME) == []
    error_messages = [record.getMessage() for record in caplog.records if record.levelno == logging.ERROR]
    assert error_messages == [f"{LOAD_INFO_SAVE_FAILED}load info save failed"]
    assert ALL_JOBS_COMPLETED in caplog.messages
    check_pipeline_report(
        dlt_test_settings.report_path,
        {"pipeline_load_info_errs": f"{LOAD_INFO_SAVE_FAILED}load info save failed"},
    )


@pytest.mark.parametrize("slack_configured", [True, False])
def test_run_pipeline_load_info_save_run_error_still_returns_load_info(
    dlt_test_settings: CtsSettings,
    careful_slack_mock: MagicMock,
    caplog: pytest.LogCaptureFixture,
    *,
    slack_configured: bool,
) -> None:
    """A failure in the save run is logged; the entity load_info is returned and only the success slack is sent."""
    assert dlt_test_settings.pipeline_dir is not None
    Path(dlt_test_settings.pipeline_dir).mkdir(parents=True, exist_ok=True)
    slack_hook = SLACK_HOOK if slack_configured else None
    mock_pipeline = make_mock_pipeline(RuntimeError("save pipeline boom"), slack_hook)
    with patch.object(core.dlt, "pipeline", return_value=mock_pipeline):
        load_info = run_pipeline(dlt_test_settings, tiny_resource())

    assert load_info is not None
    assert load_info.asdict.return_value == {"loads_ids": ["1.0"]}
    error_messages = [record.getMessage() for record in caplog.records if record.levelno == logging.ERROR]
    assert error_messages == [f"{LOAD_INFO_SAVE_FAILED}save pipeline boom"]
    assert ALL_JOBS_COMPLETED in caplog.messages
    check_pipeline_report(
        dlt_test_settings.report_path,
        {"pipeline_load_info_errs": f"{LOAD_INFO_SAVE_FAILED}save pipeline boom"},
    )
    if slack_configured:
        careful_slack_mock.assert_called_once_with(SLACK_HOOK, "Pipeline completed successfully!")
    else:
        careful_slack_mock.assert_not_called()


def test_run_pipeline_load_info_save_failed_jobs_logged(
    dlt_test_settings: CtsSettings,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Failed jobs in the save run are logged as an error; the entity load_info is still returned."""
    assert dlt_test_settings.pipeline_dir is not None
    Path(dlt_test_settings.pipeline_dir).mkdir(parents=True, exist_ok=True)
    save_info = MagicMock()
    save_info.has_failed_jobs = True
    save_info.__str__.return_value = "save info with failed jobs"
    mock_pipeline = make_mock_pipeline(save_info)
    with patch.object(core.dlt, "pipeline", return_value=mock_pipeline):
        load_info = run_pipeline(dlt_test_settings, tiny_resource())

    assert load_info is not None
    assert load_info is not save_info
    assert load_info.asdict.return_value == {"loads_ids": ["1.0"]}
    error_messages = [record.getMessage() for record in caplog.records if record.levelno == logging.ERROR]
    assert error_messages == [f"{LOAD_INFO_SAVE_FAILED_JOBS}save info with failed jobs"]
    assert ALL_JOBS_COMPLETED in caplog.messages
    check_pipeline_report(
        dlt_test_settings.report_path,
        {"pipeline_load_info_errs": f"{LOAD_INFO_SAVE_FAILED_JOBS}save info with failed jobs"},
    )


def test_run_pipeline_load_info_save_run_mock_shape(
    dlt_test_settings: CtsSettings,
    mock_dlt: MagicMock,
) -> None:
    """The save run reuses the same pipeline object and passes a named flat resource."""
    run_pipeline(dlt_test_settings, tiny_resource())

    assert mock_dlt.pipeline.call_count == 1
    assert mock_dlt.pipeline.return_value.run.call_count == 2
    first_call, save_call = mock_dlt.pipeline.return_value.run.call_args_list
    assert first_call.args[0] is not None
    assert save_call.args[0] is not None
    assert save_call.kwargs == {"loader_file_format": "jsonl"}


# PipelineCli

RUN_RESULT: Final[LoadInfo] = cast("LoadInfo", object())


def make_recorder() -> tuple[list[CtsSettings], Callable[[CtsSettings], LoadInfo | None]]:
    """A pipeline function that records the settings it gets and returns RUN_RESULT."""
    received: list[CtsSettings] = []

    def pipeline_fn(settings: CtsSettings) -> LoadInfo | None:
        received.append(settings)
        return RUN_RESULT

    return received, pipeline_fn


@pytest.mark.usefixtures("restore_logging")
def test_pipeline_cli_pass_run(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """run() builds settings from cli_args, env vars and kwargs, then returns the pipeline function's result."""
    received, pipeline_fn = make_recorder()
    monkeypatch.setenv("CDL_DLT_DEV_MODE", "true")
    result = PipelineCli(CtsSettings, pipeline_fn).run(
        cli_args=["-i", str(tmp_path), "-o", str(tmp_path / "out"), "-d", LOCAL_FS, "--buffer-size", "7"],
        log_interval=3,
    )
    assert result is RUN_RESULT
    assert [(s.input_dir, s.buffer_size, s.log_interval, s.dlt_dev_mode) for s in received] == [
        (tmp_path.resolve(), 7, 3, True)
    ]


@pytest.mark.usefixtures("restore_logging")
def test_pipeline_cli_pass_call_reads_argv(dlt_test_settings: CtsSettings, monkeypatch: pytest.MonkeyPatch) -> None:
    """Calling the CLI parses sys.argv, runs the pipeline and returns None so the process exits with 0."""
    received, pipeline_fn = make_recorder()
    monkeypatch.setattr(sys, "argv", ["prog", *settings_to_argv(dlt_test_settings)])
    assert PipelineCli(CtsSettings, pipeline_fn)() is None
    assert [s.input_dir for s in received] == [dlt_test_settings.input_dir]


@pytest.mark.parametrize(
    "cli_args",
    [
        pytest.param(["--buffer-size", "0"], id="invalid_value"),
        pytest.param(["--no-such-flag", "x"], id="unknown_arg"),
        pytest.param(["-p", "true"], id="metadata_without_output_dir"),
    ],
)
def test_pipeline_cli_fail_invalid_settings(cli_args: list[str]) -> None:
    """Invalid settings raise before the pipeline function is called."""
    received, pipeline_fn = make_recorder()
    with pytest.raises((SettingsError, ValidationError)):
        PipelineCli(CtsSettings, pipeline_fn).run(cli_args=cli_args)
    assert received == []


def test_pipeline_cli_fail_frozen() -> None:
    """PipelineCli instances cannot be changed after creation."""
    _, pipeline_fn = make_recorder()
    pipeline_cli = PipelineCli(CtsSettings, pipeline_fn)
    with pytest.raises(FrozenInstanceError):
        pipeline_cli.settings_cls = LoggerSettings  # pyright: ignore[reportAttributeAccessIssue]


# run_pipeline with failed load jobs

FAILING_SINK: Final[str] = "failing_sink"
SINK_ERROR: Final[str] = "sink refused the data"


def failing_sink(items: TDataItems, table: TTableSchema) -> None:  # noqa: ARG001
    """A dlt custom destination that rejects every batch with a terminal error."""
    raise DestinationTerminalException(SINK_ERROR)


def test_run_pipeline_pass_failed_jobs_reported(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    slack_mock: MagicMock,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """With raise_on_failed_jobs off, failed jobs are returned in the LoadInfo and Slack says 'with errors'."""
    monkeypatch.setenv("LOAD__RAISE_ON_FAILED_JOBS", "false")
    monkeypatch.setenv(HOOK_ENV_VAR, SLACK_HOOK)
    settings = make_cts_settings(
        CtsSettings,
        tmp_path,
        log_config_file=None,
        use_destination=FAILING_SINK,
        use_output_dir_for_pipeline_metadata=True,
    )
    with isolated_dlt_config({"destination": {FAILING_SINK: {"destination_type": "destination"}}}):
        load_info = run_pipeline(
            settings,
            tiny_resource(),
            pipeline_kwargs={"pipeline_name": TINY_PIPELINE_NAME},
            destination_kwargs={"destination_callable": failing_sink},
        )

    assert load_info is not None
    assert load_info.has_failed_jobs
    assert slack_mock.call_args_list == [call(SLACK_HOOK, "Pipeline completed with errors", False)]
    core_errors = [
        record.getMessage()
        for record in caplog.records
        if record.name == core.__name__ and record.levelno == logging.ERROR
    ]
    # the load info is saved to the same failing sink, so saving it fails too; the run is still returned
    assert len(core_errors) == 1
    assert core_errors[0].startswith((LOAD_INFO_SAVE_FAILED_JOBS, LOAD_INFO_SAVE_FAILED))


def test_run_pipeline_pass_partial_load_worker_failure_filesystem(
    dlt_test_settings: CtsSettings,
    destination_bucket: Path,
    monkeypatch: pytest.MonkeyPatch,
    slack_mock: MagicMock,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """When one load worker thread fails and others succeed with raise_on_failed_jobs=False, surviving tables and _load_info are written."""
    monkeypatch.setenv("LOAD__RAISE_ON_FAILED_JOBS", "false")
    monkeypatch.setenv("DATA_WRITER__FILE_MAX_ITEMS", "1")
    monkeypatch.setenv(HOOK_ENV_VAR, SLACK_HOOK)

    real_run = FilesystemLoadJob.run

    def fail_second_tiny_record(self: FilesystemLoadJob) -> None:
        if self.load_table_name == TINY_TABLE_NAME:
            with gzip.open(self._file_path, "rt") as f:
                if "tiny:two" in f.read():
                    raise DestinationTerminalException(SINK_ERROR)
        real_run(self)

    monkeypatch.setattr(FilesystemLoadJob, "run", fail_second_tiny_record)

    load_info = run_pipeline(
        dlt_test_settings,
        tiny_resource(),
        pipeline_kwargs={"pipeline_name": TINY_PIPELINE_NAME},
    )

    assert load_info is not None
    assert load_info.has_failed_jobs
    assert slack_mock.call_args_list == [
        call(SLACK_HOOK, "Pipeline completed with errors", False),  # noqa: FBT003
    ]

    # The record from the succeeding worker thread is in the tiny table; the failed worker's record is missing
    assert read_output_jsonl_records(destination_bucket) == [TINY_RESOURCE_DATA[0]]

    # Both the partially-failed data load and the _load_info metadata load are recorded in _dlt_loads as status 0
    dlt_loads = read_destination_rows(destination_bucket, "_dlt_loads")
    assert [row["status"] for row in dlt_loads] == [0, 0]
    assert len(read_destination_rows(destination_bucket, "_dlt_pipeline_state")) == 1

    # _load_info is saved to the filesystem destination and records both completed and failed worker jobs
    load_info_rows = read_destination_rows(destination_bucket, LOAD_INFO_TABLE_NAME)
    assert len(load_info_rows) == 1
    package = load_info_rows[0]["load_packages"][0]
    assert package["state"] == "loaded"
    failed_jobs = [job for job in package["jobs"] if job["state"] == "failed_jobs"]
    completed_jobs = [job for job in package["jobs"] if job["state"] == "completed_jobs"]
    assert len(failed_jobs) == 1
    assert SINK_ERROR in failed_jobs[0]["failed_message"]
    assert [job["failed_message"] for job in completed_jobs] == [None, None]

    assert FAILED_JOBS_WARNING in caplog.messages
    assert [r for r in caplog.records if r.name == core.__name__ and r.levelno == logging.ERROR] == []
    report_data = check_pipeline_report(
        dlt_test_settings.report_path,
        {
            "pipeline": TINY_PIPELINE_NAME,
            "dataset": load_info.dataset_name,
            "status": "completed with errors",
            "load_ids": list(load_info.loads_ids),
            "pipeline_load_info_errs": None,
        },
    )
    started_at = report_data["started"]
    finished_at = report_data["finished"]
    assert started_at <= load_info.started_at <= finished_at
    if load_info.finished_at is not None:
        assert load_info.started_at <= load_info.finished_at <= finished_at
    assert any(SINK_ERROR in msg for msg in report_data["load_errors"])


def test_run_pipeline_fail_partial_extract_worker_failure_filesystem(
    dlt_test_settings: CtsSettings,
    destination_bucket: Path,
    monkeypatch: pytest.MonkeyPatch,
    slack_mock: MagicMock,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """When one parallel extract worker thread fails and others succeed, extract aborts and writes no tables even with raise_on_failed_jobs=False."""
    monkeypatch.setenv("RAISE_ON_FAILED_JOBS", "false")
    monkeypatch.setenv("LOAD__RAISE_ON_FAILED_JOBS", "false")
    monkeypatch.setenv(HOOK_ENV_VAR, SLACK_HOOK)

    @dlt.resource
    def seeds() -> Iterator[dict[str, Any]]:
        yield from TINY_RESOURCE_DATA

    @dlt.transformer(data_from=seeds, name=TINY_TABLE_NAME, parallelized=True)
    def parallel_transform(item: dict[str, Any]) -> dict[str, Any]:
        if item["entity_id"] == "tiny:two":
            raise RuntimeError(SINK_ERROR)
        return item

    load_info = run_pipeline(
        dlt_test_settings,
        parallel_transform(),
        pipeline_kwargs={"pipeline_name": TINY_PIPELINE_NAME},
    )

    assert load_info is None
    assert list(destination_bucket.iterdir()) == []
    assert read_destination_rows(destination_bucket, TINY_TABLE_NAME) == []
    assert read_destination_rows(destination_bucket, LOAD_INFO_TABLE_NAME) == []
    assert caplog.records[-1].levelno == logging.ERROR
    assert caplog.records[-1].getMessage().startswith("Pipeline failed: ")
    assert SINK_ERROR in caplog.records[-1].getMessage()
    assert slack_mock.call_args.args[0] == SLACK_HOOK
    assert slack_mock.call_args.args[1].startswith("Pipeline failed: ")
    report_data = check_pipeline_report(
        dlt_test_settings.report_path,
        {
            "pipeline": TINY_PIPELINE_NAME,
            "status": "error",
        },
    )
    assert report_data["pipeline_err_msg"].startswith("Pipeline failed: ")
    assert SINK_ERROR in report_data["pipeline_err_msg"]


def test_build_filesystem_resource_pass_default_glob_and_callable_transformer(
    input_dir: Path, output_dir: Path
) -> None:
    """build_filesystem_resource uses settings.file_glob or DEFAULT_XML_FILE_GLOB and wraps a callable in dlt.transformer."""
    file_path = input_dir / "sample.xml"
    file_path.write_text("hello", encoding="utf-8")
    settings = LocalInputOutputSettings(input_dir=input_dir, output_dir=output_dir)

    def read_items(items: TDataItems, prefix: str) -> Iterator[dict[str, Any]]:
        for item in items:
            text = Path(item["file_url"].removeprefix("file://")).read_text(encoding="utf-8")
            yield {"text": f"{prefix}:{text}"}

    resource = build_filesystem_resource(settings, read_items, "tag", name="sample_reader")
    rows = list(resource)
    assert rows == [{"text": "tag:hello"}]


def test_build_filesystem_resource_pass_custom_glob_resource_transformer_and_contract(
    input_dir: Path, output_dir: Path
) -> None:
    """build_filesystem_resource accepts a DltResource transformer, explicit file_glob, and schema_contract."""
    matched = input_dir / "data.jsonl"
    matched.write_text("matched", encoding="utf-8")
    ignored = input_dir / "ignore.txt"
    ignored.write_text("ignored", encoding="utf-8")
    settings = LocalInputOutputSettings(input_dir=input_dir, output_dir=output_dir)

    @dlt.transformer(name="jsonl_test_reader")
    def read_jsonl_items(items: TDataItems) -> Iterator[dict[str, Any]]:
        for item in items:
            yield {"name": Path(item["file_url"].removeprefix("file://")).name}

    resource = build_filesystem_resource(
        settings,
        read_jsonl_items,
        name="jsonl_test_reader",
        file_glob="*.jsonl",
        schema_contract=DEFAULT_SCHEMA_CONTRACT,
    )
    rows = list(resource)
    assert rows == [{"name": "data.jsonl"}]


@pytest.mark.parametrize(
    ("dataset_name", "fallback_dataset_name", "expected_dataset_name"),
    [
        pytest.param("custom_ds", False, "custom_ds", id="explicit_no_fallback"),
        pytest.param("custom_ds", True, "custom_ds", id="explicit_with_fallback"),
        pytest.param(None, False, None, id="none_no_fallback"),
        pytest.param(None, True, TINY_PIPELINE_NAME, id="none_with_fallback"),
    ],
)
def test_build_file_ingest_kwargs_pass(
    tmp_path: Path,
    dataset_name: str | None,
    fallback_dataset_name: bool,
    expected_dataset_name: str | None,
) -> None:
    """build_file_ingest_kwargs constructs pipeline_kwargs and pipeline_run_kwargs from FileFormatIngestSettings."""
    base_settings: FileFormatIngestSettings = make_cts_settings(
        FileFormatIngestSettings,
        tmp_path,
        dataset_name="custom_ds",
        loader_file_format="jsonl",
    )
    settings = base_settings.model_copy(update={"dataset_name": dataset_name})

    result = build_file_ingest_kwargs(
        settings,
        TINY_PIPELINE_NAME,
        fallback_dataset_name=fallback_dataset_name,
    )

    assert DEFAULT_REJECTED_SUFFIX == "_rejected"
    assert result == {
        "pipeline_kwargs": {
            "pipeline_name": TINY_PIPELINE_NAME,
            "dataset_name": expected_dataset_name,
        },
        "pipeline_run_kwargs": {
            "loader_file_format": "jsonl",
        },
    }
