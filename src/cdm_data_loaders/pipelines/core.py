"""Common reusable dlt pipeline execution engine."""

from logging import Logger, getLogger
from typing import Any, Final

import dlt
from dlt.common.pipeline import LoadInfo
from dlt.common.schema.typing import TSchemaContractDict
from dlt.extract import DltResource
from dlt.pipeline.exceptions import PipelineStepFailed

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
)
from cdm_data_loaders.core.transforms.hints import propagate_run_hints_to_variants
from cdm_data_loaders.utils.iceberg.cleanup import install_iceberg_catalog_cleanup

DEFAULT_SCHEMA_CONTRACT: Final[TSchemaContractDict] = {
    "tables": "evolve",
    "columns": "evolve",
    "data_type": "discard_row",
}

logger: Logger = getLogger(__name__)


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
    :raises ValueError: if ``settings.use_destination`` is invalid in ``dlt`` config, or if
        ``loader_file_format`` is ``'file_listing'`` without a ``dlt`` ``filesystem`` resource
    :return: a load info object, if run; else ``None``
    :rtype: LoadInfo | None
    """
    if (
        isinstance(settings, FileFormatIngestSettings)
        and settings.loader_file_format == LoaderFileFormatEnum.FILE_LISTING
    ):
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
