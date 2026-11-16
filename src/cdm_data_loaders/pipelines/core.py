"""Common reusable pipeline elements."""

from collections.abc import Callable
from dataclasses import dataclass
from logging import Logger, getLogger
from typing import TYPE_CHECKING, Any, Final

import dlt
from dlt.common.pipeline import LoadInfo
from dlt.common.schema.typing import TSchemaContractDict
from dlt.extract import DltResource
from dlt.pipeline.exceptions import PipelineStepFailed
from pydantic import ValidationError
from pydantic_settings import CliApp, SettingsError

from cdm_data_loaders.core.fields import LoaderFileFormatEnum
from cdm_data_loaders.core.filesystem import attach_file_collector, collect_file_listing
from cdm_data_loaders.core.reporting import (
    WEBHOOK_NOT_CONFIGURED,
    save_pipeline_metadata,
    send_slack_message_carefully,
    write_file_listing,
)
from cdm_data_loaders.core.settings import (
    CtsSettings,
    FileFormatIngestSettings,
    LoggerSettings,
)
from cdm_data_loaders.utils.cdm_logger import init_logger
from cdm_data_loaders.utils.iceberg_cleanup import install_iceberg_catalog_cleanup

if TYPE_CHECKING:
    from cdm_data_loaders.core.transforms import OutputTableHints

DEFAULT_SCHEMA_CONTRACT: Final[TSchemaContractDict] = {
    "tables": "evolve",
    "columns": "evolve",
    "data_type": "discard_row",
}

logger: Logger = getLogger(__name__)


@dataclass(frozen=True, slots=True)
class PipelineCli[S: LoggerSettings]:
    """Console-script entry point for a pipeline; ``run`` returns the LoadInfo."""

    settings_cls: type[S]
    pipeline_fn: Callable[[S], LoadInfo | None]

    def __call__(self) -> None:
        """Parse sys.argv and run the pipeline. Returns None so the process exits with 0."""
        self.run()

    def run(self, cli_args: list[str] | None = None, **settings_kwargs: Any) -> LoadInfo | None:  # noqa: ANN401
        """Parse ``cli_args`` (default ``sys.argv[1:]``), run the pipeline and return its LoadInfo."""
        return run_cli(self.settings_cls, self.pipeline_fn, settings_kwargs, cli_args)


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


def propagate_run_hints_to_variants(
    settings: CtsSettings,
    resource: DltResource | list[DltResource],
    pipeline_run_kwargs: dict[str, Any] | None,
) -> None:
    """Propagate ``pipeline.run`` table/file format and write disposition kwargs onto registered table variants.

    :param settings: pipeline settings optionally exposing ``get_output_table_hints()``
    :type settings: CtsSettings
    :param resource: target dlt resource or list of resources
    :type resource: DltResource | list[DltResource]
    :param pipeline_run_kwargs: keyword arguments forwarded to ``pipeline.run``
    :type pipeline_run_kwargs: dict[str, Any] | None
    :return: ``None``
    :rtype: None
    """
    if not pipeline_run_kwargs:
        return
    output_table_hints: tuple[OutputTableHints, ...] = (
        settings.get_output_table_hints() if hasattr(settings, "get_output_table_hints") else ()
    )
    if not output_table_hints:
        return
    table_format = pipeline_run_kwargs.get("table_format")
    file_format = pipeline_run_kwargs.get("loader_file_format")
    write_disposition = pipeline_run_kwargs.get("write_disposition")
    if table_format is None and file_format is None and write_disposition is None:
        return
    resources = resource if isinstance(resource, list) else [resource]
    for res in resources:
        for table_hint in output_table_hints:
            res.apply_hints(
                table_name=table_hint.table_name,
                table_format=table_format,
                file_format=file_format,
                write_disposition=write_disposition,
                create_table_variant=True,
            )


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
    :raises ValueError: if settings.use_destination is not configured in the dlt config, or if
        loader_file_format is 'file_listing' without a dlt filesystem resource
    :return: a load info object, if successful; otherwise None
    :rtype: LoadInfo | None
    """
    if isinstance(settings, FileFormatIngestSettings) and settings.loader_file_format == LoaderFileFormatEnum.FILE_LISTING:
        input_dir_files, matched_files = collect_file_listing(settings, resource)
        write_file_listing(settings.file_listing_path, input_dir_files, matched_files)
        return None

    install_iceberg_catalog_cleanup()
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

    collector = attach_file_collector(resource)
    propagate_run_hints_to_variants(settings, resource, pipeline_run_kwargs)
    load_info: LoadInfo | None = None
    err_msg: str | None = None
    try:
        load_info = pipeline.run(resource, **(pipeline_run_kwargs or {}))
    except PipelineStepFailed as e:
        # errors raise PipelineStepFailed
        err_msg = f"Pipeline failed: {e!s}"
        logger.exception(err_msg)

    has_errors = bool(load_info and load_info.has_failed_jobs) or bool(collector.preprocess_errors)
    if load_info:
        logger.info(load_info)
        if has_errors:
            logger.warning("Pipeline contains failed jobs")
        else:
            logger.info("All pipeline jobs completed successfully")

    save_pipeline_metadata(
        settings,
        pipeline,
        load_info,
        err_msg,
        ingested_files=collector.ingested_files or None,
        preprocess_errors=collector.preprocess_errors or None,
    )

    if slack_hook:
        msg = err_msg or f"Pipeline completed {'with errors' if has_errors else 'successfully!'}"
        send_slack_message_carefully(slack_hook, msg)

    return load_info
