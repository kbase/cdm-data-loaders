"""Tests for pipeline report and metadata saving using real dlt LoadInfo objects."""

import logging
from collections.abc import Iterator
from dataclasses import FrozenInstanceError
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Final
from unittest.mock import MagicMock

import dlt
import pytest
from dlt.common.destination.exceptions import DestinationHasFailedJobs
from dlt.common.metrics import LoadJobMetrics, LoadMetrics
from dlt.common.pipeline import LoadInfo, SupportsPipeline
from dlt.common.schema import Schema
from dlt.common.storages.load_package import (
    LoadJobInfo,
    LoadPackageInfo,
    ParsedLoadJobFileName,
    TPackageJobState,
)
from dlt.common.typing import TDataItems
from dlt.sources.filesystem import filesystem

from cdm_data_loaders.core.fields import PIPELINE_DIR_NAME, REPORT_FILE_NAME
from cdm_data_loaders.core.settings import CtsSettings
from cdm_data_loaders.pipelines.core import (
    PipelineReport,
    build_filesystem_resource,
    run_pipeline,
    save_pipeline_metadata,
    write_pipeline_report,
)
from tests.cdm_data_loaders.pipelines.helpers import check_pipeline_report
from tests.helpers import make_cts_settings

TEST_PIPELINE_NAME: Final[str] = "test_pipeline"
TEST_DATASET_NAME: Final[str] = "test_dataset"
MOCK_LOADS_ROOT: Final[str] = "/var/dlt/loads"
MOCK_BUCKET_URL: Final[str] = "file:///var/dlt/output"
MOCK_START_TIME: Final[datetime] = datetime(2026, 3, 30, 12, 0, 0, tzinfo=UTC)
MOCK_LOAD_START_TIME: Final[datetime] = datetime(2026, 3, 30, 12, 0, 5, tzinfo=UTC)
MOCK_FINISH_TIME: Final[datetime] = datetime(2026, 3, 30, 12, 0, 10, tzinfo=UTC)
MOCK_SAVE_START_TIME: Final[datetime] = datetime(2026, 3, 30, 12, 1, 0, tzinfo=UTC)
MOCK_SAVE_FINISH_TIME: Final[datetime] = datetime(2026, 3, 30, 12, 1, 5, tzinfo=UTC)
LOAD_INFO_SAVE_FAILED: Final[str] = "Loading pipeline load info failed: "
LOAD_INFO_SAVE_FAILED_JOBS: Final[str] = "Loading pipeline load info had failed jobs: "


@pytest.fixture
def dlt_test_settings(tmp_path: Path, dlt_destination_config: str) -> CtsSettings:
    """Settings that load into the local_fs destination and keep pipeline metadata in a separate output_dir."""
    return make_cts_settings(
        CtsSettings,
        tmp_path,
        log_config_file=None,
        use_destination=dlt_destination_config,
        use_output_dir_for_pipeline_metadata=True,
    )


def make_job_info(
    *,
    table_name: str,
    file_id: str,
    state: TPackageJobState,
    created_at: datetime,
    failed_message: str | None = None,
) -> LoadJobInfo:
    """Construct a real dlt LoadJobInfo instance."""
    job_file_info = ParsedLoadJobFileName(
        table_name=table_name,
        file_id=file_id,
        retry_count=0,
        file_format="jsonl",
        is_compressed=False,
    )
    file_name = job_file_info.file_name()
    return LoadJobInfo(
        state=state,
        file_path=f"{MOCK_LOADS_ROOT}/{state}/{file_name}",
        file_size=1024,
        created_at=created_at,
        elapsed=1.0,
        job_file_info=job_file_info,
        failed_message=failed_message,
    )


def make_package_info(
    *,
    load_id: str,
    completed_jobs: list[LoadJobInfo],
    failed_jobs: list[LoadJobInfo],
    completed_at: datetime,
) -> tuple[LoadPackageInfo, LoadMetrics]:
    """Construct a real dlt LoadPackageInfo and its matching LoadMetrics."""
    started_at = completed_at.replace(second=0)
    all_jobs = [*completed_jobs, *failed_jobs]
    job_metrics = {
        job.job_file_info.job_id(): LoadJobMetrics(
            job_id=job.job_file_info.job_id(),
            file_path=job.file_path,
            table_name=job.job_file_info.table_name,
            started_at=started_at,
            finished_at=completed_at,
            state="failed" if job.state == "failed_jobs" else "completed",
            remote_url=None,
            retry_count=job.job_file_info.retry_count,
        )
        for job in all_jobs
    }
    package_info = LoadPackageInfo(
        load_id=load_id,
        package_path=f"{MOCK_LOADS_ROOT}/{load_id}",
        state="loaded",
        schema=Schema("default"),
        schema_update={},
        completed_at=completed_at,
        jobs={
            "new_jobs": [],
            "started_jobs": [],
            "completed_jobs": completed_jobs,
            "failed_jobs": failed_jobs,
        },
    )
    metrics: LoadMetrics = {
        "started_at": started_at,
        "finished_at": completed_at,
        "job_metrics": job_metrics,
        "dataset_name": TEST_DATASET_NAME,
        "outputs": [],
    }
    return package_info, metrics


def create_load_info_one_completed_two_failed() -> LoadInfo:
    """Create a LoadInfo with 1 completed load package and 2 load packages that failed with errors."""
    ts = MOCK_FINISH_TIME

    # Load 1: completed cleanly
    pkg_1, metrics_1 = make_package_info(
        load_id="1700000001.0",
        completed_jobs=[
            make_job_info(
                table_name="uniref_entry",
                file_id="a1b2c3d4e5",
                state="completed_jobs",
                created_at=ts,
            )
        ],
        failed_jobs=[],
        completed_at=ts,
    )

    # Load 2: failed with error
    pkg_2, metrics_2 = make_package_info(
        load_id="1700000002.0",
        completed_jobs=[],
        failed_jobs=[
            make_job_info(
                table_name="uniref_entry",
                file_id="f6g7h8i9j0",
                state="failed_jobs",
                created_at=ts,
                failed_message="DatabaseTerminalException: constraint violation on row 42",
            )
        ],
        completed_at=ts,
    )

    # Load 3: failed with error
    pkg_3, metrics_3 = make_package_info(
        load_id="1700000003.0",
        completed_jobs=[],
        failed_jobs=[
            make_job_info(
                table_name="uniref_member",
                file_id="k1l2m3n4o5",
                state="failed_jobs",
                created_at=ts,
                failed_message="OSError: disk quota exceeded while writing parquet file",
            )
        ],
        completed_at=ts,
    )

    pipeline_stub: SupportsPipeline = SimpleNamespace(
        pipeline_name=TEST_PIPELINE_NAME,
        first_run=False,
    )  # type: ignore[assignment]

    return LoadInfo(
        pipeline=pipeline_stub,
        metrics={
            pkg_1.load_id: [metrics_1],
            pkg_2.load_id: [metrics_2],
            pkg_3.load_id: [metrics_3],
        },
        destination_type="dlt.destinations.filesystem",
        destination_displayable_credentials=MOCK_BUCKET_URL,
        destination_name="filesystem",
        environment="local",
        staging_type=None,
        staging_name=None,
        staging_displayable_credentials=None,
        destination_fingerprint="",
        dataset_name=TEST_DATASET_NAME,
        loads_ids=[pkg_1.load_id, pkg_2.load_id, pkg_3.load_id],
        load_packages=[pkg_1, pkg_2, pkg_3],
        first_run=False,
    )


def create_single_package_one_completed_two_failed_jobs() -> LoadInfo:
    """Create a LoadInfo with a single load package containing 1 completed job and 2 failed jobs."""
    ts = MOCK_FINISH_TIME
    pkg, metrics = make_package_info(
        load_id="1700000001.0",
        completed_jobs=[
            make_job_info(
                table_name="uniref_entry",
                file_id="a1b2c3d4e5",
                state="completed_jobs",
                created_at=ts,
            ),
        ],
        failed_jobs=[
            make_job_info(
                table_name="uniref_entry",
                file_id="f6g7h8i9j0",
                state="failed_jobs",
                created_at=ts,
                failed_message="DatabaseTerminalException: invalid byte sequence for encoding UTF8",
            ),
            make_job_info(
                table_name="uniref_member",
                file_id="k1l2m3n4o5",
                state="failed_jobs",
                created_at=ts,
                failed_message="OSError: permission denied writing target file",
            ),
        ],
        completed_at=ts,
    )

    pipeline_stub: SupportsPipeline = SimpleNamespace(
        pipeline_name=TEST_PIPELINE_NAME,
        first_run=False,
    )  # type: ignore[assignment]

    return LoadInfo(
        pipeline=pipeline_stub,
        metrics={pkg.load_id: [metrics]},
        destination_type="dlt.destinations.filesystem",
        destination_displayable_credentials=MOCK_BUCKET_URL,
        destination_name="filesystem",
        environment="local",
        staging_type=None,
        staging_name=None,
        staging_displayable_credentials=None,
        destination_fingerprint="",
        dataset_name=TEST_DATASET_NAME,
        loads_ids=[pkg.load_id],
        load_packages=[pkg],
        first_run=False,
    )


def test_create_load_info_one_completed_two_failed_pass_invariants() -> None:
    """Verify dlt LoadInfo invariants on 1 completed load package and 2 failed load packages."""
    expected_package_count = 3
    load_info = create_load_info_one_completed_two_failed()

    assert load_info.has_failed_jobs is True
    assert load_info.loads_ids == ["1700000001.0", "1700000002.0", "1700000003.0"]
    assert len(load_info.load_packages) == expected_package_count

    as_dict = load_info.asdict()
    assert as_dict["pipeline"]["pipeline_name"] == TEST_PIPELINE_NAME
    assert as_dict["dataset_name"] == TEST_DATASET_NAME
    assert len(as_dict["load_packages"]) == expected_package_count

    with pytest.raises(
        DestinationHasFailedJobs,
        match=r"Destination `filesystem` has failed jobs in load package `1700000002\.0`",
    ) as exc_info:
        load_info.raise_on_failed_jobs()
    assert exc_info.value.failed_jobs[0].failed_message == "DatabaseTerminalException: constraint violation on row 42"


def test_create_single_package_one_completed_two_failed_jobs_pass_invariants() -> None:
    """Verify dlt LoadInfo invariants on a single load package with 1 completed job and 2 failed jobs."""
    expected_failed_jobs = 2
    expected_total_jobs = 3
    load_info = create_single_package_one_completed_two_failed_jobs()

    assert load_info.has_failed_jobs is True
    assert load_info.loads_ids == ["1700000001.0"]
    assert len(load_info.load_packages) == 1
    assert len(load_info.load_packages[0].jobs["completed_jobs"]) == 1
    assert len(load_info.load_packages[0].jobs["failed_jobs"]) == expected_failed_jobs

    as_dict = load_info.asdict()
    assert len(as_dict["load_packages"]) == 1
    assert len(as_dict["load_packages"][0]["jobs"]) == expected_total_jobs

    with pytest.raises(
        DestinationHasFailedJobs,
        match=r"Destination `filesystem` has failed jobs in load package `1700000001\.0`",
    ) as exc_info:
        load_info.raise_on_failed_jobs()
    assert len(exc_info.value.failed_jobs) == expected_failed_jobs


def test_save_pipeline_metadata_pass_real_load_info_one_completed_two_failed(
    dlt_test_settings: CtsSettings,
) -> None:
    """save_pipeline_metadata extracts failed messages and serializes a real multi-package LoadInfo."""
    assert dlt_test_settings.pipeline_dir is not None
    dlt_test_settings.pipeline_dir.mkdir(parents=True, exist_ok=True)

    load_info = create_load_info_one_completed_two_failed()

    pipeline = MagicMock()
    pipeline.pipeline_name = TEST_PIPELINE_NAME
    pipeline.dataset_name = TEST_DATASET_NAME
    pipeline.last_trace.started_at = MOCK_START_TIME
    pipeline.last_trace.finished_at = MOCK_FINISH_TIME
    save_info = MagicMock()
    save_info.has_failed_jobs = False
    pipeline.run.return_value = save_info

    save_pipeline_metadata(dlt_test_settings, pipeline, load_info, None)

    check_pipeline_report(
        dlt_test_settings.report_path,
        {
            "pipeline": TEST_PIPELINE_NAME,
            "dataset": TEST_DATASET_NAME,
            "started": MOCK_START_TIME,
            "finished": MOCK_FINISH_TIME,
            "status": "completed with errors",
            "load_ids": ["1700000001.0", "1700000002.0", "1700000003.0"],
            "load_errors": [
                "DatabaseTerminalException: constraint violation on row 42",
                "OSError: disk quota exceeded while writing parquet file",
            ],
            "pipeline_load_info_errs": None,
        },
    )


def test_write_pipeline_report_pass_one_completed_two_failed_loads(tmp_path: Path) -> None:
    """write_pipeline_report formats a PipelineReport built from a LoadInfo with 1 completed and 2 failed loads."""
    load_info = create_load_info_one_completed_two_failed()
    report_path = tmp_path / REPORT_FILE_NAME
    report = PipelineReport(
        pipeline_name=TEST_PIPELINE_NAME,
        dataset_name=TEST_DATASET_NAME,
        started_at=MOCK_START_TIME,
        finished_at=MOCK_FINISH_TIME,
        pipeline_load_info=load_info,
        pipeline_err_msg=None,
        info_load_err_msg=None,
    )

    write_pipeline_report(report_path, report)

    check_pipeline_report(
        report_path,
        {
            "pipeline": TEST_PIPELINE_NAME,
            "dataset": TEST_DATASET_NAME,
            "started": MOCK_START_TIME,
            "finished": MOCK_FINISH_TIME,
            "status": "completed with errors",
            "load_ids": ["1700000001.0", "1700000002.0", "1700000003.0"],
            "load_errors": [
                "DatabaseTerminalException: constraint violation on row 42",
                "OSError: disk quota exceeded while writing parquet file",
            ],
            "pipeline_load_info_errs": None,
        },
    )


def test_pipeline_report_frozen() -> None:
    """PipelineReport instances are frozen and cannot be mutated."""
    report = PipelineReport(
        pipeline_name=TEST_PIPELINE_NAME,
        dataset_name="test_dataset",
        started_at=MOCK_START_TIME,
        finished_at=MOCK_FINISH_TIME,
    )
    with pytest.raises(FrozenInstanceError):
        report.pipeline_name = "other"  # type: ignore[misc]


def test_write_pipeline_report_pass_completed_no_errors(tmp_path: Path) -> None:
    """write_pipeline_report writes completed status and null pipeline_load_info_errs when there are no errors."""
    report_path = tmp_path / "report_1.0.txt"
    report = PipelineReport(
        pipeline_name=TEST_PIPELINE_NAME,
        dataset_name="test_dataset",
        started_at=MOCK_START_TIME,
        finished_at=MOCK_FINISH_TIME,
        pipeline_load_info=MagicMock(loads_ids=["1700000000.123"]),
    )

    write_pipeline_report(report_path, report)
    report_data = check_pipeline_report(
        report_path,
        {
            "pipeline": TEST_PIPELINE_NAME,
            "dataset": "test_dataset",
            "started": MOCK_START_TIME,
            "finished": MOCK_FINISH_TIME,
            "status": "completed",
            "load_ids": ["1700000000.123"],
            "pipeline_load_info_errs": None,
        },
    )
    assert "load_errors" not in report_data
    assert "ingested_files" not in report_data


def test_write_pipeline_report_pass_completed_with_errors_and_unsaved_info(tmp_path: Path) -> None:
    """write_pipeline_report includes failed job messages and info_load_err_msg when present."""
    report_path = tmp_path / "report_1.0.txt"
    report = PipelineReport(
        pipeline_name=TEST_PIPELINE_NAME,
        dataset_name="test_dataset",
        started_at=MOCK_START_TIME,
        finished_at=MOCK_FINISH_TIME,
        pipeline_load_info=MagicMock(
            loads_ids=["1.0", "2.0"],
            load_packages=[
                MagicMock(
                    jobs={
                        "failed_jobs": [
                            MagicMock(failed_message="job 1 failed"),
                            MagicMock(failed_message=None),
                            MagicMock(failed_message="job 2 failed"),
                        ]
                    }
                )
            ],
        ),
        info_load_err_msg="Loading pipeline load info failed: boom",
    )

    write_pipeline_report(report_path, report)

    check_pipeline_report(
        report_path,
        {
            "pipeline": TEST_PIPELINE_NAME,
            "dataset": "test_dataset",
            "started": MOCK_START_TIME,
            "finished": MOCK_FINISH_TIME,
            "status": "completed with errors",
            "load_ids": ["1.0", "2.0"],
            "load_errors": ["job 1 failed", "job 2 failed"],
            "pipeline_load_info_errs": "Loading pipeline load info failed: boom",
        },
    )


@pytest.mark.parametrize(
    "err_msg",
    [
        pytest.param("Pipeline failed: step boom", id="with-err-msg"),
        pytest.param(None, id="no-err-msg"),
        pytest.param("", id="empty-err-msg"),
    ],
)
def test_write_pipeline_report_pass_load_info_none(
    tmp_path: Path,
    err_msg: str | None,
) -> None:
    """write_pipeline_report writes status: error when loads_ids is None."""
    report_path = tmp_path / "report_err.txt"
    report = PipelineReport(
        pipeline_name=TEST_PIPELINE_NAME,
        dataset_name="test_dataset",
        started_at=MOCK_START_TIME,
        finished_at=MOCK_FINISH_TIME,
        pipeline_err_msg=err_msg,
    )

    write_pipeline_report(report_path, report)

    expected: dict[str, Any] = {
        "pipeline": TEST_PIPELINE_NAME,
        "dataset": "test_dataset",
        "started": MOCK_START_TIME,
        "finished": MOCK_FINISH_TIME,
        "status": "error",
    }
    if err_msg:
        expected["pipeline_err_msg"] = err_msg
    report_data = check_pipeline_report(report_path, expected)
    if not err_msg:
        assert "pipeline_err_msg" not in report_data


def test_write_pipeline_report_fail_oserror_logged(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """write_pipeline_report logs an exception when writing the report file fails."""
    report_path = tmp_path / "report_err.txt"
    report = PipelineReport(
        pipeline_name=TEST_PIPELINE_NAME,
        dataset_name="test_dataset",
        started_at=MOCK_START_TIME,
        finished_at=MOCK_FINISH_TIME,
        pipeline_load_info=MagicMock(loads_ids=["1700000000.123"]),
    )
    monkeypatch.setattr(Path, "open", MagicMock(side_effect=OSError("disk full")))

    write_pipeline_report(report_path, report)
    error_messages = [r.getMessage() for r in caplog.records if r.levelno == logging.ERROR]
    assert error_messages == ["Could not write pipeline report file: disk full"]


@pytest.mark.parametrize(
    "ingested_files",
    [
        pytest.param(["a.xml", "sub/b.xml"], id="multiple-files"),
        pytest.param(["single.jsonl"], id="single-file"),
        pytest.param([], id="empty-list"),
        pytest.param(None, id="none"),
    ],
)
def test_write_pipeline_report_pass_ingested_files(
    tmp_path: Path,
    ingested_files: list[str] | None,
) -> None:
    """write_pipeline_report includes ingested_files when non-empty and omits it otherwise."""
    report_path = tmp_path / "report.txt"
    report = PipelineReport(
        pipeline_name=TEST_PIPELINE_NAME,
        dataset_name="test_dataset",
        started_at=MOCK_START_TIME,
        finished_at=MOCK_FINISH_TIME,
        pipeline_load_info=MagicMock(loads_ids=["1700000000.123"]),
        ingested_files=ingested_files,
    )

    write_pipeline_report(report_path, report)

    expected: dict[str, Any] = {
        "pipeline": TEST_PIPELINE_NAME,
        "dataset": "test_dataset",
        "started": MOCK_START_TIME,
        "finished": MOCK_FINISH_TIME,
        "status": "completed",
        "load_ids": ["1700000000.123"],
        "pipeline_load_info_errs": None,
    }
    if ingested_files:
        expected["ingested_files"] = ingested_files
    report_data = check_pipeline_report(report_path, expected)
    if not ingested_files:
        assert "ingested_files" not in report_data


def make_metadata_pipeline_and_load_info(
    *,
    has_failed_jobs: bool = False,
    loads_ids: list[str] | None = None,
    failed_messages: list[str | None] | None = None,
    save_run_result: object = None,
) -> tuple[MagicMock, MagicMock]:
    """Build a mock dlt.Pipeline and LoadInfo for testing save_pipeline_metadata."""
    pipeline = MagicMock()
    pipeline.pipeline_name = TEST_PIPELINE_NAME
    pipeline.dataset_name = TEST_DATASET_NAME
    pipeline.last_trace.started_at = MOCK_START_TIME
    pipeline.last_trace.finished_at = MOCK_FINISH_TIME

    if save_run_result is None:
        default_save_info = MagicMock()
        default_save_info.has_failed_jobs = False
        pipeline.run.return_value = default_save_info
    elif isinstance(save_run_result, Exception):
        pipeline.run.side_effect = save_run_result
    else:
        pipeline.run.return_value = save_run_result

    load_info = MagicMock()
    load_info.has_failed_jobs = has_failed_jobs
    load_info.loads_ids = ["1700000000.123"] if loads_ids is None else loads_ids
    load_info.started_at = MOCK_LOAD_START_TIME
    load_info.asdict.return_value = {"loads_ids": load_info.loads_ids}

    if failed_messages:
        failed_jobs = []
        for msg in failed_messages:
            job = MagicMock()
            job.failed_message = msg
            failed_jobs.append(job)
        package = MagicMock()
        package.jobs = {"failed_jobs": failed_jobs}
        load_info.load_packages = [package]
    else:
        load_info.load_packages = []

    return pipeline, load_info


def test_save_pipeline_metadata_pass_completed_no_errors(
    dlt_test_settings: CtsSettings,
) -> None:
    """When load_info has no failed jobs and saving it succeeds, the pipeline dir exists and report says completed."""
    pipeline_dir = dlt_test_settings.pipeline_dir
    assert pipeline_dir is not None
    pipeline_dir.mkdir(parents=True, exist_ok=True)
    (pipeline_dir / "state.json").write_text("{}", encoding="utf-8")

    pipeline, load_info = make_metadata_pipeline_and_load_info()

    save_pipeline_metadata(dlt_test_settings, pipeline, load_info, None)

    assert pipeline_dir.exists()
    assert (dlt_test_settings.output_dir / PIPELINE_DIR_NAME / "state.json").is_file()

    report_file = dlt_test_settings.output_dir / PIPELINE_DIR_NAME / "report.txt"
    check_pipeline_report(
        report_file,
        {
            "pipeline": TEST_PIPELINE_NAME,
            "dataset": TEST_DATASET_NAME,
            "started": MOCK_START_TIME,
            "finished": MOCK_FINISH_TIME,
            "status": "completed",
            "load_ids": ["1700000000.123"],
            "pipeline_load_info_errs": None,
        },
    )


def test_save_pipeline_metadata_pass_uses_initial_trace_timestamps_before_save_run(
    dlt_test_settings: CtsSettings,
) -> None:
    """When saving load_info replaces pipeline.last_trace, Started and Finished use the initial trace."""
    assert dlt_test_settings.pipeline_dir is not None
    Path(dlt_test_settings.pipeline_dir).mkdir(parents=True, exist_ok=True)

    pipeline, load_info = make_metadata_pipeline_and_load_info()
    pipeline.last_trace.started_at = MOCK_START_TIME
    pipeline.last_trace.finished_at = MOCK_FINISH_TIME

    def update_trace_on_save(*_args: object, **_kwargs: object) -> MagicMock:
        post_save_trace = MagicMock()
        post_save_trace.started_at = MOCK_SAVE_START_TIME
        post_save_trace.finished_at = MOCK_SAVE_FINISH_TIME
        pipeline.last_trace = post_save_trace
        save_info = MagicMock()
        save_info.has_failed_jobs = False
        return save_info

    pipeline.run.side_effect = update_trace_on_save

    save_pipeline_metadata(dlt_test_settings, pipeline, load_info, None)
    check_pipeline_report(
        dlt_test_settings.report_path,
        {
            "pipeline": TEST_PIPELINE_NAME,
            "dataset": TEST_DATASET_NAME,
            "started": MOCK_START_TIME,
            "finished": MOCK_FINISH_TIME,
            "status": "completed",
            "load_ids": ["1700000000.123"],
            "pipeline_load_info_errs": None,
        },
    )


def test_save_pipeline_metadata_pass_completed_with_errors(
    dlt_test_settings: CtsSettings,
) -> None:
    """When load_info has failed jobs, all non-empty failed_message values are included in the report."""
    assert dlt_test_settings.pipeline_dir is not None
    Path(dlt_test_settings.pipeline_dir).mkdir(parents=True, exist_ok=True)

    pipeline, load_info = make_metadata_pipeline_and_load_info(
        has_failed_jobs=True,
        loads_ids=["1700000000.1", "1700000000.2"],
        failed_messages=["first job failed", None, "", "second job failed"],
    )

    save_pipeline_metadata(dlt_test_settings, pipeline, load_info, None)

    check_pipeline_report(
        dlt_test_settings.report_path,
        {
            "pipeline": TEST_PIPELINE_NAME,
            "dataset": TEST_DATASET_NAME,
            "started": MOCK_START_TIME,
            "finished": MOCK_FINISH_TIME,
            "status": "completed with errors",
            "load_ids": ["1700000000.1", "1700000000.2"],
            "load_errors": ["first job failed", "second job failed"],
            "pipeline_load_info_errs": None,
        },
    )


@pytest.mark.parametrize(
    "err_msg",
    [
        pytest.param("Pipeline failed: step boom", id="with-err-msg"),
        pytest.param(None, id="no-err-msg"),
        pytest.param("", id="empty-err-msg"),
    ],
)
def test_save_pipeline_metadata_pass_load_info_none(
    dlt_test_settings: CtsSettings,
    err_msg: str | None,
) -> None:
    """When load_info is None, last_trace.started_at is used for the start time and status is error."""
    assert dlt_test_settings.pipeline_dir is not None
    Path(dlt_test_settings.pipeline_dir).mkdir(parents=True, exist_ok=True)

    pipeline, _ = make_metadata_pipeline_and_load_info()

    save_pipeline_metadata(dlt_test_settings, pipeline, None, err_msg)

    pipeline.run.assert_not_called()
    assert dlt_test_settings.pipeline_dir.is_dir()

    expected: dict[str, Any] = {
        "pipeline": TEST_PIPELINE_NAME,
        "dataset": TEST_DATASET_NAME,
        "started": MOCK_START_TIME,
        "finished": MOCK_FINISH_TIME,
        "status": "error",
    }
    if err_msg:
        expected["pipeline_err_msg"] = err_msg
    report_data = check_pipeline_report(dlt_test_settings.report_path, expected)
    if not err_msg:
        assert "pipeline_err_msg" not in report_data


def test_save_pipeline_metadata_pass_empty_loads_ids_uses_last_trace_timestamp(
    dlt_test_settings: CtsSettings,
) -> None:
    """When load_info.loads_ids is empty, the report is written with an empty load_ids list."""
    assert dlt_test_settings.pipeline_dir is not None
    Path(dlt_test_settings.pipeline_dir).mkdir(parents=True, exist_ok=True)

    pipeline, load_info = make_metadata_pipeline_and_load_info(loads_ids=[])
    save_pipeline_metadata(dlt_test_settings, pipeline, load_info, None)
    check_pipeline_report(
        dlt_test_settings.report_path,
        {
            "pipeline": TEST_PIPELINE_NAME,
            "dataset": TEST_DATASET_NAME,
            "started": MOCK_START_TIME,
            "finished": MOCK_FINISH_TIME,
            "status": "completed",
            "load_ids": [],
            "pipeline_load_info_errs": None,
        },
    )


def test_save_pipeline_metadata_pass_use_output_dir_false_writes_report_to_output_dir(
    dlt_test_settings: CtsSettings,
) -> None:
    """When use_output_dir_for_pipeline_metadata is False, report.txt is written to output_dir and overwritten on rerun."""
    settings = dlt_test_settings.model_copy(update={"use_output_dir_for_pipeline_metadata": False})
    pipeline, load_info = make_metadata_pipeline_and_load_info()
    finish_times = (MOCK_FINISH_TIME, MOCK_SAVE_FINISH_TIME)
    for finish_time in finish_times:
        pipeline.last_trace.finished_at = finish_time
        save_pipeline_metadata(settings, pipeline, load_info, None)

    # report is saved in the output dir, not the pipelines dir
    assert not dlt_test_settings.report_path.exists()
    assert (dlt_test_settings.output_dir / REPORT_FILE_NAME).exists()
    assert settings.report_path.is_file()

    report_files = list(Path(settings.output_dir).glob("**/report.txt"))
    assert report_files == [settings.report_path]
    check_pipeline_report(settings.report_path, {"finished": MOCK_SAVE_FINISH_TIME})


def test_save_pipeline_metadata_fail_load_info_save_raises_exception(
    dlt_test_settings: CtsSettings,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """When saving load_info raises an exception, it is logged and recorded in the report."""
    assert dlt_test_settings.pipeline_dir is not None
    Path(dlt_test_settings.pipeline_dir).mkdir(parents=True, exist_ok=True)

    pipeline, load_info = make_metadata_pipeline_and_load_info(save_run_result=RuntimeError("db unreachable"))

    save_pipeline_metadata(dlt_test_settings, pipeline, load_info, None)

    error_messages = [r.getMessage() for r in caplog.records if r.levelno == logging.ERROR]
    assert error_messages == [f"{LOAD_INFO_SAVE_FAILED}db unreachable"]
    check_pipeline_report(
        dlt_test_settings.report_path,
        {
            "pipeline": TEST_PIPELINE_NAME,
            "dataset": TEST_DATASET_NAME,
            "started": MOCK_START_TIME,
            "finished": MOCK_FINISH_TIME,
            "status": "completed",
            "load_ids": ["1700000000.123"],
            "pipeline_load_info_errs": f"{LOAD_INFO_SAVE_FAILED}db unreachable",
        },
    )


def test_save_pipeline_metadata_fail_load_info_save_has_failed_jobs(
    dlt_test_settings: CtsSettings,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """When saving load_info returns failed jobs, an error is logged and recorded in the report."""
    assert dlt_test_settings.pipeline_dir is not None
    Path(dlt_test_settings.pipeline_dir).mkdir(parents=True, exist_ok=True)

    save_info = MagicMock()
    save_info.has_failed_jobs = True
    save_info.__str__.return_value = "save job failed"
    pipeline, load_info = make_metadata_pipeline_and_load_info(save_run_result=save_info)

    save_pipeline_metadata(dlt_test_settings, pipeline, load_info, None)

    error_messages = [r.getMessage() for r in caplog.records if r.levelno == logging.ERROR]
    assert error_messages == [f"{LOAD_INFO_SAVE_FAILED_JOBS}save job failed"]

    check_pipeline_report(
        dlt_test_settings.report_path,
        {
            "pipeline": TEST_PIPELINE_NAME,
            "dataset": TEST_DATASET_NAME,
            "started": MOCK_START_TIME,
            "finished": MOCK_FINISH_TIME,
            "status": "completed",
            "load_ids": ["1700000000.123"],
            "pipeline_load_info_errs": f"{LOAD_INFO_SAVE_FAILED_JOBS}save job failed",
        },
    )


def test_save_pipeline_metadata_fail_write_report_error_logged(
    dlt_test_settings: CtsSettings,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """If writing the report file fails, an exception is logged with 'Could not write pipeline report file: <err>'."""
    assert dlt_test_settings.pipeline_dir is not None
    Path(dlt_test_settings.pipeline_dir).mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(Path, "open", MagicMock(side_effect=OSError("read-only filesystem")))
    pipeline, load_info = make_metadata_pipeline_and_load_info()

    save_pipeline_metadata(dlt_test_settings, pipeline, load_info, None)

    error_messages = [r.getMessage() for r in caplog.records if r.levelno == logging.ERROR]
    assert error_messages == ["Could not write pipeline report file: read-only filesystem"]


def test_save_pipeline_metadata_pass_forwards_ingested_files(
    dlt_test_settings: CtsSettings,
) -> None:
    """save_pipeline_metadata forwards ingested_files to PipelineReport and writes them to report.txt."""
    assert dlt_test_settings.pipeline_dir is not None
    Path(dlt_test_settings.pipeline_dir).mkdir(parents=True, exist_ok=True)
    pipeline, load_info = make_metadata_pipeline_and_load_info()

    save_pipeline_metadata(
        dlt_test_settings,
        pipeline,
        load_info,
        None,
        ingested_files=["alpha.jsonl", "nested/beta.jsonl"],
    )

    check_pipeline_report(
        dlt_test_settings.report_path,
        {
            "pipeline": TEST_PIPELINE_NAME,
            "dataset": TEST_DATASET_NAME,
            "started": MOCK_START_TIME,
            "finished": MOCK_FINISH_TIME,
            "status": "completed",
            "load_ids": ["1700000000.123"],
            "ingested_files": ["alpha.jsonl", "nested/beta.jsonl"],
            "pipeline_load_info_errs": None,
        },
    )


def test_run_pipeline_pass_filesystem_resource_records_ingested_files_in_report(
    dlt_test_settings: CtsSettings,
) -> None:
    """run_pipeline records relative paths from filesystem resources and writes them to report.txt."""
    input_dir = Path(dlt_test_settings.input_dir)
    (input_dir / "first.xml").write_text("one", encoding="utf-8")
    (input_dir / "sub").mkdir()
    (input_dir / "sub" / "second.xml").write_text("two", encoding="utf-8")

    def read_items(items: TDataItems) -> Iterator[dict[str, Any]]:
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
        pipeline_kwargs={"pipeline_name": TEST_PIPELINE_NAME, "dataset_name": "fs_report_ds"},
    )

    assert load_info is not None
    report_data = check_pipeline_report(
        dlt_test_settings.report_path,
        {
            "pipeline": TEST_PIPELINE_NAME,
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
        pipeline_kwargs={"pipeline_name": TEST_PIPELINE_NAME, "dataset_name": "multi_fs_ds"},
    )

    assert load_info is not None
    check_pipeline_report(
        dlt_test_settings.report_path,
        {
            "pipeline": TEST_PIPELINE_NAME,
            "dataset": "multi_fs_ds",
            "status": "completed",
            "ingested_files": ["a.txt", "b.txt"],
            "pipeline_load_info_errs": None,
        },
    )
