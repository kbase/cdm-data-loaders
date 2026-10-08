"""Common reusable pipeline elements."""

import os
from collections.abc import Callable
from logging import Logger, getLogger
from typing import TYPE_CHECKING, Any, Final

import dlt
from dlt.common.destination.reference import AnyDestination
from dlt.common.pipeline import LoadInfo
from dlt.common.runtime.slack import send_slack_message
from dlt.extract import DltResource
from dlt.sources.filesystem import filesystem
from pydantic import ValidationError
from pydantic_settings import CliApp, SettingsError
from requests import RequestException

from cdm_data_loaders.core.fields import DEV_MODE, JSONL, OUTPUT_DIR, USE_DESTINATION
from cdm_data_loaders.core.settings import CtsSettings, LoggerSettings
from cdm_data_loaders.utils.cdm_logger import init_logger

if TYPE_CHECKING:
    from dlt.extract import DltResource

WEBHOOK_NOT_CONFIGURED: Final[str] = "Slack webhook not configured"
NO_MESSAGE: Final[str] = "No message supplied"
LOAD_INFO_TABLE_NAME: Final[str] = "_dlt_load_info"


logger: Logger = getLogger(__name__)


def send_slack_message_carefully(slack_hook: str, message: str, is_markdown: bool = False) -> None:  # noqa: FBT001, FBT002
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
    except RequestException:
        logger.exception("Failed to send slack message")


def construct_env_var() -> None:
    """Use environment variables to construct a new environment variable."""
    b_var = os.environ.get("VARIABLE_B")
    t_var = os.environ.get("VARIABLE_T")
    char_str = os.environ.get("CHAR_STR")
    if all([b_var, t_var, char_str]):
        os.environ["RUNTIME__SLACK_INCOMING_HOOK"] = f"https://hooks.slack.com/services/{b_var}/{t_var}/{char_str}/"


def sync_configs(settings: LoggerSettings, dlt_config: Any) -> None:  # noqa: ANN401
    """Sync the dlt config with the config derived from the CLI settings."""
    if hasattr(settings, DEV_MODE):
        dlt_config["normalize.data_writer.disable_compression"] = settings.dev_mode  # pyright: ignore[reportAttributeAccessIssue]
    if hasattr(settings, OUTPUT_DIR) and hasattr(settings, USE_DESTINATION):
        # make sure that the destination bucket_url is set correctly
        dlt_config[f"destination.{settings.use_destination}.bucket_url"] = settings.output_dir  # pyright: ignore[reportAttributeAccessIssue]


def dump_settings(settings: LoggerSettings) -> None:
    """Dump the pipeline settings to the logger."""
    logger.info("Pipeline settings:")
    logger.info(settings.model_dump())


def filesystem_resource(bucket_url: str, file_glob: str) -> DltResource:
    """Build a dlt filesystem source over an input directory.

    :param bucket_url: directory holding the input files
    :type  bucket_url: str
    :param file_glob: glob pattern selecting the files to read
    :type  file_glob: str
    :return: a dlt filesystem source over the matching files
    :rtype: DltResource
    """
    return filesystem(bucket_url=bucket_url, file_glob=file_glob)


def run_cli(
    settings_cls: type[LoggerSettings],
    pipeline_fn: Callable[[Any], LoadInfo | None],
    settings_kwargs: dict[str, Any] | None = None,
) -> LoadInfo | None:
    """Run any pipeline through the generic CLI entry point.

    :param settings_cls: the Settings class to instantiate
    :type  settings_cls: type[LoggerSettings]
    :param pipeline_fn: the run_pipeline function to call with the config
    :type  pipeline_fn: Callable[[Any], LoadInfo | None]
    :param settings_kwargs: any extra non-cli/env var settings to be added
    :type  settings_kwargs: dict[str, Any] | None, optional
    :return: pipeline load information
    :rtype: LoadInfo | None
    """
    # instantiate the config
    try:
        settings = settings_cls(**(settings_kwargs or {}))
        init_logger(settings)
    except (SettingsError, ValidationError, ValueError):
        logger.exception("Error initialising config")
        raise
    except Exception:
        logger.exception("Unexpected error setting up config")
        raise
    return pipeline_fn(settings)


def run_pipeline(
    settings: CtsSettings,
    resource: Any,  # noqa: ANN401
    destination_kwargs: dict[str, Any] | None = None,
    pipeline_kwargs: dict[str, Any] | None = None,
    pipeline_run_kwargs: dict[str, Any] | None = None,
) -> LoadInfo | None:
    """Execute a dlt pipeline.

    :param settings: pipeline config with output_dir and destination
    :type settings: BatchedFileInputSettings
    :param resource: dlt resource to run
    :type resource: Any
    :param destination_kwargs: keyword arguments for the dlt destination
    :type destination_kwargs: dict[str, Any] | None
    :param pipeline_kwargs: keyword arguments for the dlt pipeline
    :type pipeline_kwargs: dict[str, Any] | None
    :param pipeline_run_kwargs: keyword arguments for the dlt pipeline run
    :type pipeline_run_kwargs: dict[str, Any] | None
    :return: a load info object, if successful; otherwise None
    :rtype: LoadInfo | None
    """
    # piece together env vars
    construct_env_var()
    sync_configs(settings, dlt.config)

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
        logger.info("No Slack alerts will be sent: %s", WEBHOOK_NOT_CONFIGURED)

    try:
        load_info: LoadInfo | None = pipeline.run(resource, **(pipeline_run_kwargs or {}))
        if load_info:
            load_info_resource: DltResource = dlt.resource(
                [load_info.asdict()], name=LOAD_INFO_TABLE_NAME, max_table_nesting=0
            )
            pipeline.run(load_info_resource, loader_file_format=JSONL)
    except Exception as e:
        err_msg = f"Pipeline failed: {e!s}"
        logger.exception(err_msg)
        if slack_hook:
            send_slack_message_carefully(slack_hook, err_msg)
        return None

    logger.info(load_info)
    logger.info("Work complete!")
    if slack_hook:
        send_slack_message_carefully(slack_hook, "Pipeline completed successfully!")

    return load_info
