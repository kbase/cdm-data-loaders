"""Reporting and output for pipelines."""

from datetime import datetime
from logging import Logger, getLogger
from pathlib import Path
from typing import TYPE_CHECKING, Annotated, Final

import dlt
import yaml
from dlt.common.pipeline import LoadInfo
from dlt.common.runtime.slack import send_slack_message
from pydantic import BaseModel, ConfigDict, GetPydanticSchema
from pydantic_core import core_schema

from cdm_data_loaders.core.fields import JSONL
from cdm_data_loaders.core.settings import CtsSettings

if TYPE_CHECKING:
    from dlt.extract import DltResource

WEBHOOK_NOT_CONFIGURED: Final[str] = "Slack webhook not configured"
NO_MESSAGE: Final[str] = "No message supplied"
LOAD_INFO_TABLE_NAME: Final[str] = "_load_info"

logger: Logger = getLogger(__name__)

type _LoadInfoField = Annotated[
    LoadInfo,
    GetPydanticSchema(lambda _tp, _handler: core_schema.is_instance_schema(LoadInfo)),
]


class PipelineReport(BaseModel):
    """Summary information for a pipeline run report."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    pipeline_name: str
    dataset_name: str | None
    started_at: datetime
    finished_at: datetime | None = None
    pipeline_load_info: _LoadInfoField | None = None
    pipeline_err_msg: str | None = None
    info_load_err_msg: str | None = None
    ingested_files: list[str] | None = None
    preprocess_errors: list[str] | None = None


def send_slack_message_carefully(
    slack_hook: str,
    message: str,
    is_markdown: bool = False,  # noqa: FBT001, FBT002
) -> None:
    """Send a Slack webhook message while logging and suppressing delivery errors.

    :param slack_hook: slack webhook URL
    :type slack_hook: str
    :param message: message to be slacked
    :type message: str
    :param is_markdown: whether or not the message is markdown, defaults to False
    :type is_markdown: bool, optional
    :return: ``None``
    :rtype: None
    """
    if not slack_hook:
        logger.warning("Cannot send slack message: %s", WEBHOOK_NOT_CONFIGURED)
        return
    if not message or not message.strip():
        logger.warning("Cannot send slack message: %s", NO_MESSAGE)
        return

    try:
        send_slack_message(slack_hook, message.strip(), is_markdown)
    except Exception:
        logger.exception("Failed to send slack message")


def write_pipeline_report(
    report_path: Path,
    report: PipelineReport,
) -> None:
    """Write a summary report of a pipeline run to ``report_path``.

    :param report_path: path to the report file to write
    :type  report_path: Path
    :param report: pipeline report information to write
    :type  report: PipelineReport
    :return: ``None``
    :rtype: None
    """
    report_data = {
        "pipeline": report.pipeline_name,
        "dataset": report.dataset_name,
        "started": report.started_at,
        "finished": report.finished_at,
    }

    if report.pipeline_load_info is None or report.pipeline_load_info.loads_ids is None:
        report_data["status"] = "error"
        if report.pipeline_err_msg:
            report_data["pipeline_err_msg"] = report.pipeline_err_msg
    else:
        failed_messages = [
            failed_job.failed_message
            for load_package in report.pipeline_load_info.load_packages
            for failed_job in load_package.jobs["failed_jobs"]
            if failed_job.failed_message
        ]
        if report.preprocess_errors:
            failed_messages.extend(report.preprocess_errors)
        status = "completed with errors" if failed_messages else "completed"
        report_data["status"] = status
        report_data["load_ids"] = report.pipeline_load_info.loads_ids
        if report.ingested_files:
            report_data["ingested_files"] = report.ingested_files
        if failed_messages:
            report_data["load_errors"] = failed_messages
        if report.info_load_err_msg is None:
            report_data["pipeline_load_info_errs"] = None
        else:
            report_data["pipeline_load_info_errs"] = report.info_load_err_msg

    class ReportYamlDumper(yaml.SafeDumper):
        """SafeDumper subclass that serializes datetime subclasses such as pendulum.DateTime."""

    ReportYamlDumper.add_multi_representer(datetime, ReportYamlDumper.represent_datetime)

    try:
        with report_path.open("w", encoding="utf-8") as fh:
            yaml.dump(report_data, fh, Dumper=ReportYamlDumper, default_flow_style=False, sort_keys=False)
    except Exception as err:
        report_err_msg = f"Could not write pipeline report file: {err!s}"
        logger.exception(report_err_msg)


def write_file_listing(
    file_listing_path: Path,
    input_dir_files: list[str],
    matched_files: list[str],
) -> None:
    """Write ``input_dir_files`` and ``matched_files`` as YAML to ``file_listing_path``.

    :param file_listing_path: path to the output file listing file
    :type file_listing_path: Path
    :param input_dir_files: sorted relative paths of all files under ``input_dir``
    :type input_dir_files: list[str]
    :param matched_files: sorted relative paths of files matched by ``filesystem`` globs
    :type matched_files: list[str]
    """
    file_listing_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "input_dir_files": input_dir_files,
        "matched_files": matched_files,
    }
    with file_listing_path.open("w", encoding="utf-8") as fh:
        yaml.safe_dump(payload, fh, default_flow_style=False, sort_keys=False)


def save_pipeline_metadata(  # noqa: PLR0913, PLR0917
    settings: CtsSettings,
    pipeline: dlt.Pipeline,
    load_info: LoadInfo | None,
    err_msg: str | None,
    ingested_files: list[str] | None = None,
    preprocess_errors: list[str] | None = None,
) -> None:
    """Save the pipeline metadata to a unique directory in settings.output_dir.

    Writes a brief report of the pipeline state and saves it to report_<unique pipeline load id>.txt.

    :param settings: settings for this pipeline
    :type  settings: CtsSettings
    :param pipeline: dlt pipeline instance
    :type  pipeline: dlt.Pipeline
    :param load_info: pipeline load info, or None if the pipeline did not complete
    :type  load_info: LoadInfo | None
    :param err_msg: error message indicating why pipeline failed, or None if the pipeline succeeded
    :type  err_msg: str | None
    :param ingested_files: relative paths of files ingested from the filesystem resource, if any
    :type  ingested_files: list[str] | None, optional
    :param preprocess_errors: error messages from files that failed pre-processing, if any
    :type  preprocess_errors: list[str] | None, optional
    :return: ``None``
    :rtype: None
    """
    started_at = pipeline.last_trace.started_at
    finished_at = pipeline.last_trace.finished_at

    info_load_err_msg: str | None = None
    if load_info is not None:
        try:
            # save the load info as JSONL in the same destination as the ingested data
            load_info_resource: DltResource = dlt.resource(
                [load_info.asdict()],
                name=LOAD_INFO_TABLE_NAME,
                max_table_nesting=0,
                columns={
                    k: {"data_type": "text"}
                    for k in [
                        "environment",
                        "staging_type",
                        "staging_name",
                        "staging_displayable_credentials",
                        "dataset_name",
                    ]
                },
            )
            load_info_save_info: LoadInfo = pipeline.run(load_info_resource, loader_file_format=str(JSONL))
            if load_info_save_info.has_failed_jobs:
                info_load_err_msg = f"Loading pipeline load info had failed jobs: {load_info_save_info}"
                logger.error(info_load_err_msg)
        except Exception as e:
            info_load_err_msg = f"Loading pipeline load info failed: {e!s}"
            logger.exception(info_load_err_msg)

    # write a summary and save it to the output directory
    report = PipelineReport(
        pipeline_name=pipeline.pipeline_name,
        dataset_name=pipeline.dataset_name,
        started_at=started_at,
        finished_at=finished_at,
        pipeline_load_info=load_info,
        pipeline_err_msg=err_msg,
        info_load_err_msg=info_load_err_msg,
        ingested_files=ingested_files,
        preprocess_errors=preprocess_errors,
    )
    write_pipeline_report(settings.report_path, report)
