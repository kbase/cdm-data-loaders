"""Tests for the shared core DLT pipeline functions."""

import gzip
import logging
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Final
from unittest.mock import MagicMock, patch

import dlt
import pyarrow as pa
import pytest
import yaml
from dlt.common.destination.exceptions import DestinationTerminalException
from dlt.common.schema.typing import TTableSchema
from dlt.common.typing import TDataItems
from dlt.destinations.impl.filesystem.filesystem import FilesystemLoadJob
from dlt.extract.resource import DltResource
from dlt.sources.filesystem import filesystem

from cdm_data_loaders.core import reporting
from cdm_data_loaders.core.fields import (
    OUTPUT_DIR,
    PIPELINE_DIR_NAME,
    REJECTED_TABLE_NAME,
    USE_DESTINATION,
)
from cdm_data_loaders.core.filesystem import build_filesystem_resource
from cdm_data_loaders.core.preprocessors import FilePreprocessError
from cdm_data_loaders.core.reporting import LOAD_INFO_TABLE_NAME, WEBHOOK_NOT_CONFIGURED
from cdm_data_loaders.core.settings import (
    CtsSettings,
    FileFormatIngestSettings,
    InputLocalOutputSettings,
    LoggerSettings,
)
from cdm_data_loaders.core.transforms.hints import (
    OutputTableHints,
    propagate_run_hints_to_variants,
)
from cdm_data_loaders.pipelines import runner as core
from cdm_data_loaders.pipelines.jsonlines.settings import JsonlExtractSettings
from cdm_data_loaders.pipelines.runner import run_pipeline
from tests.helpers.cli_assertions import (
    HOOK_ENV_VAR,
    LOAD_INFO_SAVE_FAILED,
    LOAD_INFO_SAVE_FAILED_JOBS,
    SLACK_HOOK,
    make_cts_settings,
    register_temp_module,
)
from tests.helpers.dlt_isolation import isolated_dlt_config
from tests.helpers.paths_and_constants import LOCAL_FS, S3
from tests.helpers.table_readers import (
    check_pipeline_report,
    read_destination_rows,
)

TINY_RESOURCE_DATA: Final[list[dict[str, Any]]] = [
    {"entity_id": "tiny:one", "value": 1},
    {"entity_id": "tiny:two", "value": 2},
]
TINY_PIPELINE_NAME: Final[str] = "test_core_tiny_pipeline"
TINY_TABLE_NAME: Final[str] = "tiny"
EXPECTED_PIPELINE_RUNS: Final[int] = 2
ALL_JOBS_COMPLETED: Final[str] = "All pipeline jobs completed successfully"
FAILED_JOBS_WARNING: Final[str] = "Pipeline contains failed jobs"


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
) -> LoggerSettings | InputLocalOutputSettings:
    """Settings instances missing one or more attribute the sync tests check for via hasattr."""
    if request.param == "no-relevant-attrs":
        return LoggerSettings.model_validate({})
    input_dir = tmp_path / "input"
    input_dir.mkdir(exist_ok=True)
    return InputLocalOutputSettings.model_validate({"input_dir": input_dir, "output_dir": tmp_path / "output"})


def test_settings_missing_sync_attrs_pass_fixture_shape(
    settings_missing_sync_attrs: LoggerSettings | InputLocalOutputSettings,
) -> None:
    """Sanity-check the fixture: confirm which attributes are genuinely absent."""
    assert hasattr(settings_missing_sync_attrs, OUTPUT_DIR) == isinstance(
        settings_missing_sync_attrs, InputLocalOutputSettings
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
    assert mock_dlt.pipeline.return_value.run.call_count == EXPECTED_PIPELINE_RUNS


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


@pytest.mark.parametrize("slack_configured", [True, False], ids=["slack-configured", "slack-unconfigured"])
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


@pytest.mark.parametrize("slack_configured", [True, False], ids=["slack-configured", "slack-unconfigured"])
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
    """The pipeline's load_info is saved as a flat table in the same dataset and returns the original import result."""
    load_info = run_pipeline(dlt_test_settings, tiny_resource())

    assert load_info is not None
    assert not load_info.has_failed_jobs
    dataset_dir = destination_bucket / load_info.dataset_name
    assert (dataset_dir / TINY_TABLE_NAME).is_dir()
    assert (dataset_dir / LOAD_INFO_TABLE_NAME).is_dir()
    assert list(dataset_dir.glob("_dlt_load_info__*")) == []
    assert read_output_jsonl_records(destination_bucket) == TINY_RESOURCE_DATA

    rows = read_destination_rows(destination_bucket, LOAD_INFO_TABLE_NAME, strip_metadata=False)
    assert len(rows) == 1
    row = rows[0]
    assert row["dataset_name"] == load_info.dataset_name
    assert row["destination_name"] == dlt_test_settings.use_destination
    assert row["first_run"] is not None
    assert set(row["pipeline"]) == {"pipeline_name"}
    # loads_ids is the original payload; _dlt_load_id is the save run's own load id
    assert row["loads_ids"] != [row["_dlt_load_id"]]
    assert load_info.loads_ids == row["loads_ids"]
    for nested in ("load_packages", "outputs", "job_metrics"):
        assert isinstance(row[nested], list)
        assert row[nested], f"{nested} must not be normalized into a child table"
        assert all(isinstance(item, dict) for item in row[nested])
    assert "load_id" in row["load_packages"][0]


def test_run_pipeline_load_info_runs_accumulate(dlt_test_settings: CtsSettings, destination_bucket: Path) -> None:
    """Each pipeline run appends its own load_info row; earlier rows are preserved."""
    first = run_pipeline(dlt_test_settings, tiny_resource())
    assert first is not None

    second = run_pipeline(dlt_test_settings, tiny_resource())
    assert second is not None

    expected_runs = 2
    rows = read_destination_rows(destination_bucket, LOAD_INFO_TABLE_NAME)
    assert len(rows) == expected_runs
    saved_load_ids = {tuple(row["loads_ids"]) for row in rows}
    assert len(saved_load_ids) == expected_runs
    # each accumulated row references one of the entity data loads
    tiny_load_ids = {
        row["_dlt_load_id"] for row in read_destination_rows(destination_bucket, TINY_TABLE_NAME, strip_metadata=False)
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


def test_run_pipeline_load_info_save_resource_error_still_returns_load_info(
    dlt_test_settings: CtsSettings,
    destination_bucket: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """An error building the load_info resource is logged; the entity load_info is still returned."""
    real_resource = dlt.resource

    def resource_boom(*args: Any, **kwargs: Any) -> Any:  # noqa: ANN401
        if kwargs.get("name") == LOAD_INFO_TABLE_NAME:
            err_msg = "load info save failed"
            raise RuntimeError(err_msg)
        return real_resource(*args, **kwargs)

    resource = tiny_resource()
    monkeypatch.setattr(dlt, "resource", resource_boom)
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


@pytest.mark.parametrize("slack_configured", [True, False], ids=["slack-configured", "slack-unconfigured"])
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
    assert mock_dlt.pipeline.return_value.run.call_count == EXPECTED_PIPELINE_RUNS
    first_call, save_call = mock_dlt.pipeline.return_value.run.call_args_list
    assert first_call.args[0] is not None
    assert save_call.args[0] is not None
    assert save_call.kwargs == {"loader_file_format": "jsonl"}


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
    assert [c.args for c in slack_mock.call_args_list] == [(SLACK_HOOK, "Pipeline completed with errors", False)]
    reporting_errors = [
        record.getMessage()
        for record in caplog.records
        if record.name == reporting.__name__ and record.levelno == logging.ERROR
    ]
    # the load info is saved to the same failing sink, so saving it fails too; the run is still returned
    assert len(reporting_errors) == 1
    assert reporting_errors[0].startswith((LOAD_INFO_SAVE_FAILED_JOBS, LOAD_INFO_SAVE_FAILED))


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
    assert [c.args for c in slack_mock.call_args_list] == [(SLACK_HOOK, "Pipeline completed with errors", False)]

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
    assert [
        r for r in caplog.records if r.name in {core.__name__, reporting.__name__} and r.levelno == logging.ERROR
    ] == []
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


@pytest.mark.parametrize(
    ("has_hint_method", "hints_empty", "pipeline_run_kwargs"),
    [
        pytest.param(True, False, None, id="run_kwargs_none"),
        pytest.param(True, False, {}, id="run_kwargs_empty"),
        pytest.param(False, False, {"loader_file_format": "parquet"}, id="no_get_output_table_hints_method"),
        pytest.param(True, True, {"loader_file_format": "parquet"}, id="empty_output_table_hints"),
        pytest.param(True, False, {"schema_contract": "evolve"}, id="irrelevant_run_kwargs_only"),
    ],
)
def test_propagate_run_hints_to_variants_pass_noop_cases(
    tmp_path: Path,
    *,
    has_hint_method: bool,
    hints_empty: bool,
    pipeline_run_kwargs: dict[str, Any] | None,
) -> None:
    """propagate_run_hints_to_variants leaves resources untouched when hints or relevant run kwargs are absent."""
    schema = pa.schema([pa.field("id", pa.string())])

    class _HintedSettings(CtsSettings):
        def get_output_table_hints(self) -> tuple[OutputTableHints, ...]:
            return () if hints_empty else (OutputTableHints(table_name="variant_tbl", pyarrow_schema=schema),)

    settings_cls = _HintedSettings if has_hint_method else CtsSettings
    settings = make_cts_settings(settings_cls, tmp_path)
    res = tiny_resource()

    propagate_run_hints_to_variants(settings, res, pipeline_run_kwargs)

    marked = dlt.mark.with_table_name({"id": "1"}, "variant_tbl")
    variant_schema = res.compute_table_schema(marked.data, marked.meta)
    assert "table_format" not in variant_schema
    assert "file_format" not in variant_schema
    assert variant_schema.get("write_disposition") != "replace"


@pytest.mark.parametrize("as_list", [False, True], ids=["single-resource", "resource-list"])
def test_propagate_run_hints_to_variants_pass_single_and_list_resources(
    tmp_path: Path,
    as_list: bool,
) -> None:
    """propagate_run_hints_to_variants applies table_format, file_format, and write_disposition onto variants."""
    schema = pa.schema([pa.field("id", pa.string())])

    class _MultiHintSettings(CtsSettings):
        def get_output_table_hints(self) -> tuple[OutputTableHints, ...]:
            return (
                OutputTableHints(table_name="tbl_one", pyarrow_schema=schema),
                OutputTableHints(table_name="tbl_two", pyarrow_schema=schema),
            )

    settings = make_cts_settings(_MultiHintSettings, tmp_path)
    resources: list[DltResource] = [tiny_resource(), tiny_resource()] if as_list else [tiny_resource()]
    target: DltResource | list[DltResource] = resources if as_list else resources[0]
    run_kwargs = {
        "table_format": "iceberg",
        "loader_file_format": "parquet",
        "write_disposition": "replace",
    }

    propagate_run_hints_to_variants(settings, target, run_kwargs)

    for res in resources:
        for tbl_name in ("tbl_one", "tbl_two"):
            marked = dlt.mark.with_table_name({"id": "1"}, tbl_name)
            variant_schema = res.compute_table_schema(marked.data, marked.meta)
            assert variant_schema["table_format"] == "iceberg"
            assert variant_schema["file_format"] == "parquet"
            assert variant_schema["write_disposition"] == "replace"


def test_run_pipeline_pass_filesystem_resource_records_ingested_files_in_report(
    dlt_test_settings: CtsSettings,
) -> None:
    """run_pipeline records relative paths from filesystem resources and writes them to report.txt."""
    input_dir = Path(dlt_test_settings.input_dir)
    (input_dir / "first.xml").write_text("one", encoding="utf-8")
    (input_dir / "sub").mkdir()
    (input_dir / "sub" / "second.xml").write_text("two", encoding="utf-8")

    def read_items(items: TDataItems, _settings: CtsSettings) -> Iterator[dict[str, Any]]:
        for item in items:
            text = Path(item["file_url"].removeprefix("file://")).read_text(encoding="utf-8")
            yield {"file": item["relative_path"], "text": text}

    resource = build_filesystem_resource(
        dlt_test_settings,
        read_items,
        name="xml_items",
        file_glob="**/*.xml",
    )

    load_info = run_pipeline(
        dlt_test_settings,
        resource,
        pipeline_kwargs={"pipeline_name": TINY_PIPELINE_NAME, "dataset_name": "fs_report_ds"},
    )

    assert load_info is not None
    report_data = check_pipeline_report(
        dlt_test_settings.report_path,
        {
            "pipeline": TINY_PIPELINE_NAME,
            "dataset": "fs_report_ds",
            "status": "completed",
            "pipeline_load_info_errs": None,
        },
    )
    assert set(report_data["ingested_files"]) == {"first.xml", "sub/second.xml"}


def test_run_pipeline_pass_shared_and_multiple_filesystem_roots_deduplicated(
    dlt_test_settings: CtsSettings,
) -> None:
    """run_pipeline deduplicates shared filesystem roots and collects across multiple filesystem roots."""
    input_dir = Path(dlt_test_settings.input_dir)
    dir_a = input_dir / "dir_a"
    dir_b = input_dir / "dir_b"
    dir_a.mkdir()
    dir_b.mkdir()
    (dir_a / "a.txt").write_text("alpha", encoding="utf-8")
    (dir_b / "b.txt").write_text("beta", encoding="utf-8")

    shared_fs = filesystem(bucket_url=str(dir_a), file_glob="*.txt")
    second_fs = filesystem(bucket_url=str(dir_b), file_glob="*.txt")

    @dlt.transformer(name="trans_one")
    def trans_one(items: TDataItems) -> Iterator[dict[str, Any]]:
        for item in items:
            yield {"path": item["relative_path"], "branch": 1}

    @dlt.transformer(name="trans_two")
    def trans_two(items: TDataItems) -> Iterator[dict[str, Any]]:
        for item in items:
            yield {"path": item["relative_path"], "branch": 2}

    @dlt.transformer(name="trans_three")
    def trans_three(items: TDataItems) -> Iterator[dict[str, Any]]:
        for item in items:
            yield {"path": item["relative_path"], "branch": 3}

    load_info = run_pipeline(
        dlt_test_settings,
        [shared_fs | trans_one, shared_fs | trans_two, second_fs | trans_three],
        pipeline_kwargs={"pipeline_name": TINY_PIPELINE_NAME, "dataset_name": "multi_fs_ds"},
    )

    assert load_info is not None
    check_pipeline_report(
        dlt_test_settings.report_path,
        {
            "pipeline": TINY_PIPELINE_NAME,
            "dataset": "multi_fs_ds",
            "status": "completed",
            "ingested_files": ["a.txt", "b.txt"],
            "pipeline_load_info_errs": None,
        },
    )


def test_run_pipeline_pass_file_preprocessors_reject_skip_and_record_report_errors(
    dlt_test_settings: CtsSettings,
    destination_bucket: Path,
    monkeypatch: pytest.MonkeyPatch,
    slack_mock: MagicMock,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """File pre-processing failures emit to _rejected, log an error, notify Slack, and record load_errors."""
    monkeypatch.setenv(HOOK_ENV_VAR, SLACK_HOOK)
    input_dir = Path(dlt_test_settings.input_dir)
    (input_dir / "good.jsonl").write_text('{"id": "1"}\n', encoding="utf-8")
    (input_dir / "skip.jsonl").write_text('{"id": "2"}\n', encoding="utf-8")
    (input_dir / "bad.jsonl").write_text('{"id": "3"}\n', encoding="utf-8")

    def preprocess_file(file_item: Any) -> Any:  # noqa: ANN401
        rel = file_item["relative_path"]
        if rel == "skip.jsonl":
            return None
        if rel == "bad.jsonl":
            msg = "checksum mismatch"
            raise FilePreprocessError(msg)
        return file_item

    pre_mod = register_temp_module(
        monkeypatch,
        "_test_pipeline_report_preprocessors",
        FILE_PREPROCESSORS=[preprocess_file],
    )
    settings = JsonlExtractSettings(
        **dlt_test_settings.model_dump(exclude={"pipeline_dir", "report_path", "file_listing_path"}),
        dataset_name="preprocess_report_ds",
        table_name="items",
        loader_file_format="jsonl",
        preprocessors_module=pre_mod,
    )

    read_files: list[str] = []

    def read_items(items: TDataItems, cfg: JsonlExtractSettings) -> Iterator[Any]:
        for item in items:
            read_files.append(item["relative_path"])
            yield dlt.mark.with_table_name({"file": item["relative_path"]}, cfg.table_name)

    resource = build_filesystem_resource(
        settings,
        read_items,
        name="jsonl_items",
        parallelized=False,
    )

    load_info = run_pipeline(
        settings,
        resource,
        pipeline_kwargs={"pipeline_name": TINY_PIPELINE_NAME, "dataset_name": "preprocess_report_ds"},
        pipeline_run_kwargs={"loader_file_format": "jsonl"},
    )

    assert load_info is not None
    assert read_files == ["good.jsonl"]
    assert [c.args for c in slack_mock.call_args_list] == [(SLACK_HOOK, "Pipeline completed with errors", False)]

    error_logs = [r.getMessage() for r in caplog.records if r.levelno == logging.ERROR]
    assert error_logs == ["File pre-processing failed for bad.jsonl: checksum mismatch"]

    valid_rows = read_destination_rows(destination_bucket, "items")
    assert valid_rows == [{"file": "good.jsonl"}]

    rejected_rows = read_destination_rows(
        destination_bucket,
        REJECTED_TABLE_NAME,
    )
    assert rejected_rows == [
        {
            "table_name": "items",
            "source_file": "bad.jsonl",
            "error_detail": "checksum mismatch",
        }
    ]

    check_pipeline_report(
        settings.report_path,
        {
            "pipeline": TINY_PIPELINE_NAME,
            "dataset": "preprocess_report_ds",
            "status": "completed with errors",
            "ingested_files": ["good.jsonl"],
            "load_errors": ["File pre-processing failed for bad.jsonl: checksum mismatch"],
            "pipeline_load_info_errs": None,
        },
    )


@pytest.mark.parametrize("use_metadata_dir", [False, True], ids=["output-root", "pipeline-metadata-dir"])
def test_run_pipeline_pass_file_listing_writes_yaml_and_skips_pipeline(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    slack_mock: MagicMock,
    *,
    use_metadata_dir: bool,
) -> None:
    """When loader_file_format is 'file_listing', run_pipeline writes file_listing.txt and skips dlt and preprocessors."""
    monkeypatch.setenv(HOOK_ENV_VAR, SLACK_HOOK)

    def fail_preprocess(_file_item: Any) -> Any:  # noqa: ANN401
        msg = "preprocessor must not run during file_listing"
        raise AssertionError(msg)

    pre_mod = register_temp_module(
        monkeypatch,
        f"_test_core_listing_preprocessors_{int(use_metadata_dir)}",
        FILE_PREPROCESSORS=[fail_preprocess],
    )
    settings = make_cts_settings(
        JsonlExtractSettings,
        tmp_path,
        use_destination="unconfigured_dest",
        use_output_dir_for_pipeline_metadata=use_metadata_dir,
        dataset_name="listing_ds",
        table_name="listing_tbl",
        loader_file_format="file_listing",
        file_glob="**/*.xml*",
        preprocessors_module=pre_mod,
    )
    input_dir = Path(settings.input_dir)
    (input_dir / "sub").mkdir()
    (input_dir / "b.xml").write_text("<broken", encoding="utf-8")
    (input_dir / "sub" / "a.xml.gz").write_bytes(b"not-valid-gzip")
    (input_dir / "sub" / "readme.txt").write_text("ignored", encoding="utf-8")

    def fail_if_called(_items: TDataItems, _cfg: JsonlExtractSettings) -> Iterator[dict[str, Any]]:
        msg = "reader must not run during file_listing"
        raise AssertionError(msg)

    resource = build_filesystem_resource(settings, fail_if_called, name="xml_listing")
    with isolated_dlt_config({}):
        load_info = run_pipeline(settings, resource)

    expected_path = (
        settings.output_dir / PIPELINE_DIR_NAME / "file_listing.txt"
        if use_metadata_dir
        else settings.output_dir / "file_listing.txt"
    )
    assert load_info is None
    assert settings.file_listing_path == expected_path
    assert expected_path.is_file()
    assert not settings.report_path.exists()
    slack_mock.assert_not_called()
    assert yaml.safe_load(expected_path.read_text(encoding="utf-8")) == {
        "input_dir_files": ["b.xml", "sub/a.xml.gz", "sub/readme.txt"],
        "matched_files": ["b.xml", "sub/a.xml.gz"],
    }


def test_run_pipeline_fail_file_listing_without_filesystem_resource(
    dlt_test_settings: CtsSettings,
) -> None:
    """When loader_file_format is 'file_listing' and resource has no filesystem root, run_pipeline raises ValueError."""
    settings = FileFormatIngestSettings(
        **dlt_test_settings.model_dump(exclude={"pipeline_dir", "report_path", "file_listing_path"}),
        dataset_name="listing_ds",
        loader_file_format="file_listing",
    )
    with pytest.raises(
        ValueError,
        match="loader_file_format='file_listing' requires at least one dlt filesystem resource",
    ):
        run_pipeline(settings, tiny_resource())
