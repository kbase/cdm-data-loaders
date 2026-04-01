"""Common reusable pipeline elements."""

import datetime
from collections.abc import Callable, Generator
from typing import Any

import dlt
from dlt.extract.items import DataItemWithMeta
from pydantic import ValidationError
from pydantic_settings import BaseSettings, SettingsError

from cdm_data_loaders.pipelines.cts_defaults import DEFAULT_BATCH_SIZE, BatchedFileInputSettings, CtsDefaultSettings
from cdm_data_loaders.utils.cdm_logger import get_cdm_logger
from cdm_data_loaders.utils.file_system import BatchCursor
from cdm_data_loaders.utils.xml_utils import stream_xml_file

logger = get_cdm_logger()


def run_cli(settings_cls: type[BaseSettings], pipeline_fn: Callable[[Any], None]) -> None:
    """Generic CLI entry point for any pipeline.

    :param settings_cls: the Settings class to instantiate
    :param pipeline_fn: the run_pipeline function to call with the config
    """
    try:
        config = settings_cls()
    except (SettingsError, ValidationError) as e:
        print(f"Error initialising config: {e}")
        raise
    except Exception as e:
        print(f"Unexpected error setting up config: {e}")
        raise

    pipeline_fn(config)


def run_pipeline(
    config: CtsDefaultSettings,
    resource: Any,  # noqa: ANN401
    pipeline_name: str,
    dataset_name: str,
) -> None:
    """Execute a dlt pipeline.

    :param config: pipeline config with output and destination
    :type config: BatchedFileInputSettings
    :param resource: dlt resource to run
    :type resource: Any
    :param pipeline_name: name for the dlt pipeline
    :type pipeline_name: str
    :param dataset_name: dataset name for the destination
    :type dataset_name: str
    """
    if config.output:
        dlt.config[f"destination.{config.destination}.bucket_url"] = config.output

    pipeline = dlt.pipeline(
        destination=dlt.destination(config.destination, max_table_nesting=0),
        pipeline_name=pipeline_name,
        dataset_name=dataset_name,
    )
    load_info = pipeline.run(resource, table_format="delta")
    logger.info(load_info)
    logger.info("Work complete!")


def stream_xml_file_resource(
    config: BatchedFileInputSettings,
    xml_tag: str,
    parse_fn: Callable,
    log_interval: int = 1000,
) -> Generator[DataItemWithMeta, Any]:
    """Core generator shared by XML-based dlt pipeline resources.

    :param config: pipeline config with input_dir and start_at
    :param xml_tag: XML element tag to stream
    :param parse_fn: callable(entry, timestamp, file_path) -> dict[str, rows]
    :param log_interval: log a progress message every N entries
    """
    timestamp = datetime.datetime.now(tz=datetime.UTC)
    batch_params: dict[str, Any] = {}
    if config.start_at:
        batch_params["start_at"] = config.start_at

    batcher = BatchCursor(config.input_dir, batch_size=DEFAULT_BATCH_SIZE, **batch_params)
    while files := batcher.get_batch():
        for file_path in files:
            logger.info("Reading from %s", str(file_path))
            for n_entries, entry in enumerate(stream_xml_file(file_path, xml_tag)):
                parsed_entry = parse_fn(entry=entry, timestamp=timestamp, file_path=file_path)
                for table, rows in parsed_entry.items():
                    yield dlt.mark.with_table_name(rows, table)
                if (n_entries + 1) % log_interval == 0:
                    logger.info("Processed %d entries", n_entries + 1)
