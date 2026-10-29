"""Common reusable pipeline elements."""

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from logging import Logger, getLogger
from pathlib import Path
from typing import Any, Final

import dlt
import yaml
from dlt.common.pipeline import LoadInfo
from dlt.common.runtime.slack import send_slack_message
from dlt.common.schema.typing import TSchemaContractDict
from dlt.extract import DltResource
from dlt.pipeline.exceptions import PipelineStepFailed
from dlt.sources.filesystem import filesystem
from pydantic import ValidationError
from pydantic_settings import CliApp, SettingsError

from cdm_data_loaders.core.fields import JSONL
from cdm_data_loaders.core.settings import (
    CtsSettings,
    FileFormatIngestSettings,
    LocalInputOutputSettings,
    LoggerSettings,
)
from cdm_data_loaders.utils.cdm_logger import init_logger

WEBHOOK_NOT_CONFIGURED: Final[str] = "Slack webhook not configured"
NO_MESSAGE: Final[str] = "No message supplied"
LOAD_INFO_TABLE_NAME: Final[str] = "_load_info"
DISABLE_COMPRESSION_ENV_VAR: Final[str] = "NORMALIZE__DATA_WRITER__DISABLE_COMPRESSION"
DESTINATION_NOT_CONFIGURED: Final[str] = (
    "Destination {name!r} is not configured: set destination.{name}.destination_type in the dlt config."
)
DEFAULT_SCHEMA_CONTRACT: Final[TSchemaContractDict] = {
    "tables": "evolve",
    "columns": "evolve",
    "data_type": "discard_row",
}
DEFAULT_REJECTED_SUFFIX: Final[str] = "_rejected"

yaml.SafeDumper.add_multi_representer(datetime, yaml.SafeDumper.represent_datetime)

logger: Logger = getLogger(__name__)


@dataclass(frozen=True, slots=True)
class PipelineCli[S: LoggerSettings]:
    """Console-script entry point for a pipeline; ``run`` returns the LoadInfo."""

    settings_cls: type[S]
    pipeline_fn: Callable[[S], LoadInfo | None]

    def __call__(self) -> None:
        """Parse sys.argv and run the pipeline. Returns None so the process exits with 0."""
        self.run()

    def run(self, cli_args: list[str] | None = None, **settings_kwargs: Any) -> LoadInfo | None:
        """Parse ``cli_args`` (default ``sys.argv[1:]``), run the pipeline and return its LoadInfo."""
        return run_cli(self.settings_cls, self.pipeline_fn, settings_kwargs, cli_args)


@dataclass(frozen=True, slots=True)
class PipelineReport:
    """Summary information for a pipeline run report."""

    pipeline_name: str
    dataset_name: str
    started_at: datetime
    finished_at: datetime
    pipeline_load_info: LoadInfo | None = None
    pipeline_err_msg: str | None = None
    info_load_err_msg: str | None = None
    ingested_files: list[str] | None = None


def send_slack_message_carefully(
    slack_hook: str,
    message: str,
    is_markdown: bool = False,  # noqa: FBT001, FBT002
) -> None:
    """Carefully send a slack message by wrapping it in a try/except.

    :param slack_hook: slack webhook URL
    :type slack_hook: str
    :param message: message to be slacked
    :type message: str
    :param is_markdown: whether or not the message is markdown, defaults to False
    :type is_markdown: bool, optional
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


def run_cli(
    settings_cls: type[LoggerSettings],
    pipeline_fn: Callable[[Any], LoadInfo | None],
    settings_kwargs: dict[str, Any] | None = None,
    cli_args: list[str] | None = None,
) -> LoadInfo | None:
    """Run a DLT pipeline from a generic CLI entry point.

    :param settings_cls: the Settings class to instantiate
    :type  settings_cls: type[LoggerSettings]
    :param pipeline_fn: the run_pipeline function to call with the config
    :type  pipeline_fn: Callable[[Any], LoadInfo | None]
    :param settings_kwargs: any extra non-cli/env var settings to be added
    :type  settings_kwargs: dict[str, Any] | None, optional
    :param cli_args: command line args
    :type  cli_args: list[str] | None, optional
    :return: pipeline load information
    :rtype: LoadInfo | None
    """
    try:
        settings = CliApp.run(settings_cls, cli_args=cli_args, **(settings_kwargs or {}))
        init_logger(settings)
    except (SettingsError, ValidationError, ValueError):
        logger.exception("Error initialising config")
        raise
    except Exception:
        logger.exception("Unexpected error setting up config")
        raise
    return pipeline_fn(settings)


def write_pipeline_report(
    report_path: Path,
    report: PipelineReport,
) -> None:
    """Write a summary report of a pipeline run to ``report_path``.

    :param report_path: path to the report file to write
    :type  report_path: Path
    :param report: pipeline report information to write
    :type  report: PipelineReport
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

    try:
        with report_path.open("w", encoding="utf-8") as fh:
            yaml.safe_dump(report_data, fh, default_flow_style=False, sort_keys=False)
    except Exception as err:
        report_err_msg = f"Could not write pipeline report file: {err!s}"
        logger.exception(report_err_msg)


def save_pipeline_metadata(
    settings: CtsSettings,
    pipeline: dlt.Pipeline,
    load_info: LoadInfo | None,
    err_msg: str | None,
    ingested_files: list[str] | None = None,
) -> None:
    """Save the pipeline metadata to a unique directory in settings.output_dir.

    Writes a brief report of the pipeline state and saves it to report_<unique pipeline load id>.txt.

    :param settings: settings for this pipeline
    :type  settings: CtsSettings
    :param pipeline: dlt pipeline instance
    :type  pipeline: dlt.Pipeline
    :param load_info: pipeline load info, or None if the pipeline did not complete successfully
    :type  load_info: LoadInfo | None
    :param err_msg: error message indicating why pipeline failed, or None if the pipeline was successful
    :type  err_msg: str | None
    :param ingested_files: relative paths of files ingested from the filesystem resource, if any
    :type  ingested_files: list[str] | None, optional
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
    )
    write_pipeline_report(settings.report_path, report)


def _attach_file_collector(resource: DltResource | list[DltResource]) -> list[str]:
    """Attach a map step to any root ``filesystem`` resource to record ingested file paths."""
    ingested_files: list[str] = []
    seen_pipes: set[str] = set()
    resources = resource if isinstance(resource, list) else [resource]

    def _record_file(item: Any) -> Any:  # noqa: ANN401
        if isinstance(item, dict) and "relative_path" in item:
            ingested_files.append(item["relative_path"])
        return item

    for res in resources:
        curr: DltResource | None = res
        while isinstance(curr, DltResource):
            if curr.name == "filesystem" and curr._pipe.instance_id not in seen_pipes:  # noqa: SLF001
                seen_pipes.add(curr._pipe.instance_id)  # noqa: SLF001
                curr.add_map(_record_file)
            curr = curr._parent  # noqa: SLF001
    return ingested_files


def run_pipeline(
    settings: CtsSettings,
    resource: DltResource | list[DltResource],
    destination_kwargs: dict[str, Any] | None = None,
    pipeline_kwargs: dict[str, Any] | None = None,
    pipeline_run_kwargs: dict[str, Any] | None = None,
) -> LoadInfo | None:
    """Execute a dlt pipeline.

    :param settings: pipeline settings
    :type settings: CtsSettings
    :param resource: dlt resource to run
    :type resource: DltResource | list[DltResource]
    :param destination_kwargs: keyword arguments for the dlt destination
    :type destination_kwargs: dict[str, Any] | None
    :param pipeline_kwargs: keyword arguments for the dlt pipeline
    :type pipeline_kwargs: dict[str, Any] | None
    :param pipeline_run_kwargs: keyword arguments for the dlt pipeline run
    :type pipeline_run_kwargs: dict[str, Any] | None
    :raises ValueError: if settings.use_destination is not configured in the dlt config
    :return: a load info object, if successful; otherwise None
    :rtype: LoadInfo | None
    """
    pipeline_kwargs = dict(pipeline_kwargs or {})

    # set the output directory for all the pipeline gubbins
    if settings.use_output_dir_for_pipeline_metadata:
        pipeline_kwargs["pipelines_dir"] = settings.pipeline_dir

    # update dev mode
    if settings.dlt_dev_mode:
        pipeline_kwargs["dev_mode"] = settings.dlt_dev_mode

    pipeline = dlt.pipeline(**pipeline_kwargs)

    try:
        pipeline.destination = dlt.destination(settings.use_destination, **(destination_kwargs or {}))
    except Exception as e:
        err_msg = f"Invalid destination configuration for {settings.use_destination}"
        logger.exception(err_msg)
        raise ValueError(err_msg) from e

    slack_hook: str | None = pipeline.runtime_config.slack_incoming_hook

    if not slack_hook:
        logger.info("No Slack alerts will be sent: %s", WEBHOOK_NOT_CONFIGURED)

    ingested_files = _attach_file_collector(resource)
    load_info: LoadInfo | None = None
    err_msg: str | None = None
    try:
        load_info = pipeline.run(resource, **(pipeline_run_kwargs or {}))
    except PipelineStepFailed as e:
        # errors raise PipelineStepFailed
        err_msg = f"Pipeline failed: {e!s}"
        logger.exception(err_msg)

    if load_info:
        logger.info(load_info)
        if load_info.has_failed_jobs:
            logger.warning("Pipeline contains failed jobs")
        else:
            logger.info("All pipeline jobs completed successfully")

    save_pipeline_metadata(settings, pipeline, load_info, err_msg, ingested_files=ingested_files or None)

    if slack_hook:
        msg = err_msg or f"Pipeline completed {'with errors' if load_info.has_failed_jobs else 'successfully!'}"
        send_slack_message_carefully(slack_hook, msg)

    return load_info


def build_filesystem_resource(
    settings: LocalInputOutputSettings,
    reader_fn: Callable[..., Any] | DltResource,
    *reader_args: Any,  # noqa: ANN401
    name: str,
    file_glob: str | None = None,
    schema_contract: TSchemaContractDict | None = None,
    **transformer_kwargs: Any,  # noqa: ANN401
) -> DltResource:
    """Build a dlt resource that pipes ``filesystem(settings.input_dir, file_glob)`` into a parallelized transformer.

    :param settings: pipeline settings with ``input_dir`` (and optionally ``file_glob``)
    :param reader_fn: transformer function or resource accepting ``items`` followed by ``*reader_args``
    :param reader_args: positional arguments bound to the transformer after ``items``
    :param name: name of the resulting dlt transformer resource
    :param file_glob: glob pattern for files; defaults to ``settings.file_glob`` if present, else ``DEFAULT_XML_FILE_GLOB``
    :param schema_contract: optional dlt schema contract hint applied to the resource
    :param transformer_kwargs: additional keyword arguments passed to ``dlt.transformer``
    :return: configured ``DltResource``
    """
    fs_args: dict[str, Any] = {"bucket_url": str(settings.input_dir)}
    if file_glob is not None:
        fs_args["file_glob"] = file_glob
    elif glob := getattr(settings, "file_glob", None):
        fs_args["file_glob"] = glob

    files = filesystem(**fs_args)
    reader = (
        reader_fn
        if isinstance(reader_fn, DltResource)
        else dlt.transformer(reader_fn, name=name, parallelized=True, **transformer_kwargs)
    )
    resource: DltResource = files | reader(*reader_args)
    if schema_contract is not None:
        resource.apply_hints(schema_contract=TSchemaContractDict(**schema_contract))  # pyright: ignore[reportCallIssue]
    return resource


def build_file_ingest_kwargs(
    settings: FileFormatIngestSettings,
    pipeline_name: str,
    *,
    fallback_dataset_name: bool = False,
) -> dict[str, dict[str, Any]]:
    """Build the standard ``pipeline_kwargs`` and ``pipeline_run_kwargs`` for a file-ingest pipeline.

    :param settings: file-format ingest settings with ``dataset_name`` and ``loader_file_format``
    :param pipeline_name: name of the dlt pipeline
    :param fallback_dataset_name: whether to fall back to ``pipeline_name`` when ``settings.dataset_name`` is None
    :return: mapping with ``pipeline_kwargs`` and ``pipeline_run_kwargs`` suitable for ``run_pipeline``
    """
    dataset_name = (settings.dataset_name or pipeline_name) if fallback_dataset_name else settings.dataset_name
    return {
        "pipeline_kwargs": {
            "pipeline_name": pipeline_name,
            "dataset_name": dataset_name,
        },
        "pipeline_run_kwargs": {
            "loader_file_format": str(settings.loader_file_format),
        },
    }
