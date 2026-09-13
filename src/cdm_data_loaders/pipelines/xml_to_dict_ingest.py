"""XML to dictionary pipeline for the KBase CTS."""

from collections.abc import Generator, Iterator
from logging import Logger, getLogger
from typing import Annotated, Any, Final

import dlt
import xmltodict
from dlt.common.storages.fsspec_filesystem import FileItemDict
from dlt.common.typing import TDataItems
from dlt.sources.filesystem import filesystem
from lxml.etree import tostring
from pydantic import Field
from pydantic_settings import SettingsConfigDict

from cdm_data_loaders.core.fields import FILE_GLOB, BufferSize, DatasetName, FileGlob, LogInterval, TableName
from cdm_data_loaders.core.settings import CLI_SHORTCUTS, DEFAULT_SETTINGS_CONFIG_DICT, CtsSettings
from cdm_data_loaders.pipelines.core import run_cli, run_pipeline
from cdm_data_loaders.readers.xml import stream_xml_file
from cdm_data_loaders.utils.buffer import ListBuffer

logger: Logger = getLogger(__name__)

PIPELINE_NAME: Final[str] = "xml_to_dict_ingest"

DEFAULT_FILE_GLOB: Final[str] = "*.xml*"
GZIP_SUFFIX: Final[str] = ".gz"

SCHEMA_CONTRACT: Final[dict[str, str]] = {
    "tables": "evolve",
    "columns": "evolve",
    "data_type": "discard_row",
}


class XmlToDictIngestSettings(CtsSettings):
    """Settings for the XML ingestion pipeline."""

    model_config = SettingsConfigDict(
        **DEFAULT_SETTINGS_CONFIG_DICT,
        cli_prog_name=PIPELINE_NAME,
        cli_shortcuts={
            **CLI_SHORTCUTS,
            FILE_GLOB.replace("_", "-"): "g",
        },
    )

    buffer_size: BufferSize
    dataset_name: DatasetName
    file_glob: Annotated[
        FileGlob,
        Field(
            default=DEFAULT_FILE_GLOB,
            description="Glob pattern for XML files inside each entity's input subdirectory.",
        ),
    ]
    log_interval: LogInterval
    table_name: TableName
    xml_tag: Annotated[
        str,
        Field(
            description="XML tag to capture the contents of",
        ),
    ]


@dlt.transformer(name="xml_to_dict_reader", parallelized=True)
def xml_to_dict_reader(
    items: Iterator[FileItemDict], settings: XmlToDictIngestSettings
) -> Generator[TDataItems, Any, Any]:
    """Read each file in items. Yield one dict per `xml_tag` element.

    :param items: file items to read
    :type  items: Iterator[FileItemDict]
    :yield: one dict per line
    :rtype: Generator[TDataItems, Any, Any]
    """
    for file_item in items:
        logger.info("Reading from %s", file_item["relative_path"])
        n_entries = -1
        buffer = ListBuffer(table_name=settings.table_name, max_items=settings.buffer_size)
        for n_entries, entry in enumerate(stream_xml_file(file_item.local_file_path, settings.xml_tag)):
            parsed = xmltodict.parse(tostring(entry))
            if parsed:
                yield from buffer.add_item(parsed)

            if (n_entries + 1) % settings.log_interval == 0:
                logger.debug("Processed %d entries", n_entries + 1)
        if n_entries >= 0 and (n_entries + 1) % settings.log_interval != 0:
            logger.debug("Processed %d entries from %s", n_entries + 1, file_item["relative_path"])

        yield from buffer.flush()


def run_xml_ingest_pipeline(settings: XmlToDictIngestSettings) -> None:
    """Run the XML to dict pipeline on matching files in settings.input_dir.

    :param settings: pipeline configuration
    :type  settings: XmlToDictIngestSettings
    """
    pipeline_kwargs = {
        "pipeline_name": PIPELINE_NAME,
        "dataset_name": settings.dataset_name,
    }

    xml_to_dict_reader.bind(settings)

    files = filesystem(bucket_url=settings.input_dir, file_glob=settings.file_glob)  # pyright: ignore[reportArgumentType]

    run_pipeline(
        settings=settings,
        resource=files | xml_to_dict_reader,
        destination_kwargs={"max_table_nesting": 0},
        pipeline_kwargs=pipeline_kwargs,
        pipeline_run_kwargs={
            "loader_file_format": "parquet",
        },
    )


def cli() -> None:
    """Command-line entry point for the XML to dictionary pipeline."""
    run_cli(XmlToDictIngestSettings, run_xml_ingest_pipeline)


if __name__ == "__main__":
    cli()
