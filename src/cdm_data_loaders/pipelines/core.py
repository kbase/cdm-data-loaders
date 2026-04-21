"""Common reusable pipeline elements."""

import datetime
import logging
import os
from collections.abc import Callable, Generator
from typing import Any

import dlt
from dlt.common.runtime.slack import send_slack_message
from dlt.extract.items import DataItemWithMeta
from pydantic import ValidationError
from pydantic_settings import SettingsError

from cdm_data_loaders.pipelines.cts_defaults import DEFAULT_BATCH_SIZE, BatchedFileInputSettings, CtsSettings
from cdm_data_loaders.utils.file_system import BatchCursor
from cdm_data_loaders.utils.xml_utils import stream_xml_file

logger = logging.getLogger("dlt")


def construct_env_var() -> None:
    """Use environment variables to construct a new environment variable."""
    b_var = os.environ.get("VARIABLE_B")
    t_var = os.environ.get("VARIABLE_T")
    char_str = os.environ.get("CHAR_STR")
    if all([b_var, t_var, char_str]):
        os.environ["RUNTIME__SLACK_INCOMING_HOOK"] = f"https://hooks.slack.com/services/{b_var}/{t_var}/{char_str}/"


def sync_configs(settings: CtsSettings, dlt_config: Any) -> None:
    """Sync the dlt config with the config derived from the CLI settings."""
    dlt_config["normalize.data_writer.disable_compression"] = settings.dev_mode
    # make sure that the destination bucket_url is set correctly
    dlt_config[f"destination.{settings.use_destination}.bucket_url"] = settings.output


def dump_settings(settings: CtsSettings) -> None:
    """Dump the pipeline settings to the logger."""
    settings_minus_dlt_config = settings.model_dump(exclude={"dlt_config"})
    logger.info("Pipeline settings:")
    logger.info(settings_minus_dlt_config)


def run_cli(settings_cls: type[CtsSettings], pipeline_fn: Callable[[Any], None]) -> None:
    """Generic CLI entry point for any pipeline.

    :param settings_cls: the Settings class to instantiate
    :param pipeline_fn: the run_pipeline function to call with the config
    """
    # piece together env vars
    construct_env_var()

    # instantiate the config
    try:
        settings = settings_cls(dlt_config=dlt.config)
        sync_configs(settings, dlt.config)
    except (SettingsError, ValidationError, ValueError):
        logger.exception("Error initialising config")
        raise
    except Exception:
        logger.exception("Unexpected error setting up config")
        raise

    dump_settings(settings)
    pipeline_fn(settings)


def run_pipeline(
    settings: CtsSettings,
    resource: Any,  # noqa: ANN401
    destination_kwargs: dict[str, Any] | None = None,
    pipeline_kwargs: dict[str, Any] | None = None,
    pipeline_run_kwargs: dict[str, Any] | None = None,
) -> None:
    """Execute a dlt pipeline.

    :param settings: pipeline config with output and destination
    :type settings: BatchedFileInputSettings
    :param resource: dlt resource to run
    :type resource: Any
    :param destination_kwargs: keyword arguments for the dlt destination
    :type destination_kwargs: dict[str, Any] | None
    :param pipeline_kwargs: keyword arguments for the dlt pipeline
    :type pipeline_kwargs: dict[str, Any] | None
    :param pipeline_run_kwargs: keyword arguments for the dlt pipeline run
    :type pipeline_run_kwargs: dict[str, Any] | None
    """
    if not pipeline_kwargs:
        pipeline_kwargs = {}

    # set the output directory for all the pipeline gubbins
    if settings.pipeline_dir:
        pipeline_kwargs["pipelines_dir"] = settings.pipeline_dir

    # update dev mode
    if settings.dev_mode:
        pipeline_kwargs["dev_mode"] = settings.dev_mode

    destination = dlt.destination(settings.use_destination, **(destination_kwargs or {}))

    pipeline = dlt.pipeline(destination=destination, **pipeline_kwargs)

    slack_hook: str | None = pipeline.runtime_config.slack_incoming_hook

    if not slack_hook:
        logger.info("Slack webhook not configured; no Slack alerts will be sent.")

    try:
        load_info = pipeline.run(resource, **(pipeline_run_kwargs or {}))
    except Exception as e:
        err_msg = f"Pipeline failed: {e!s}"
        logger.exception(err_msg)
        if slack_hook:
            send_slack_message(slack_hook, err_msg)
        return

    logger.info(load_info)
    logger.info("Work complete!")
    if slack_hook:
        send_slack_message(slack_hook, "Pipeline completed successfully!")


def stream_xml_file_resource(
    settings: BatchedFileInputSettings,
    xml_tag: str,
    parse_fn: Callable,
    log_interval: int = 1000,
) -> Generator[DataItemWithMeta, Any]:
    """Core generator shared by XML-based dlt pipeline resources.

    :param settings: pipeline config with input_dir and start_at
    :type settings: BatchedFileInputSettings
    :param xml_tag: XML element tag to stream
    :type xml_tag: str
    :param parse_fn: callable(entry, timestamp, file_path) -> dict[str, rows]
    :type parse_fn: Callable
    :param log_interval: log a progress message every N entries
    :type log_interval: int
    """
    timestamp = datetime.datetime.now(tz=datetime.UTC)
    batch_params: dict[str, Any] = {}
    if settings.start_at:
        batch_params["start_at"] = settings.start_at

    batcher = BatchCursor(settings.input_dir, batch_size=DEFAULT_BATCH_SIZE, **batch_params)
    while files := batcher.get_batch():
        for file_path in files:
            logger.info("Reading from %s", str(file_path))
            for n_entries, entry in enumerate(stream_xml_file(file_path, xml_tag)):
                parsed_entry = parse_fn(entry=entry, timestamp=timestamp, file_path=file_path)
                for table, rows in parsed_entry.items():
                    yield dlt.mark.with_table_name(rows, table)
                if (n_entries + 1) % log_interval == 0:
                    logger.info("Processed %d entries", n_entries + 1)
