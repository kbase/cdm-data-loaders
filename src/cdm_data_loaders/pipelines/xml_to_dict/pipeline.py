"""XML to dictionary pipeline for the KBase CTS."""

from collections.abc import Generator, Iterator
from logging import Logger, getLogger
from pathlib import Path
from typing import Any, Final

import dlt
from dlt.common.pipeline import LoadInfo
from dlt.common.storages.fsspec_filesystem import FileItemDict
from dlt.common.typing import TDataItems
from dlt.sources.filesystem import filesystem

from cdm_data_loaders.pipelines.core import run_cli, run_pipeline
from cdm_data_loaders.pipelines.xml_to_dict.settings import PIPELINE_NAME, XmlToDictSettings
from cdm_data_loaders.readers.xml import process_xml_file_to_dict

logger: Logger = getLogger(__name__)

SCHEMA_CONTRACT: Final[dict[str, str]] = {
    "tables": "evolve",
    "columns": "evolve",
    "data_type": "discard_row",
}


@dlt.transformer(name="xml_to_dict_reader", parallelized=True)
def xml_to_dict_reader(items: Iterator[FileItemDict], settings: XmlToDictSettings) -> Generator[TDataItems, Any, Any]:
    """Read each file in items. Yields one dict per `xml_tag` element.

    :param items: file items to read
    :type  items: Iterator[FileItemDict]
    :yield: one dict per line
    :rtype: Generator[TDataItems, Any, Any]
    """
    for file_item in items:
        yield from process_xml_file_to_dict(settings, file_path=Path(file_item.local_file_path))


def run_xml_ingest_pipeline(settings: XmlToDictSettings) -> LoadInfo | None:
    """Run the XML to dict pipeline on matching files in settings.input_dir.

    :param settings: pipeline configuration
    :type  settings: XmlToDictSettings
    """
    xml_to_dict_reader.bind(settings)

    files = filesystem(bucket_url=settings.input_dir, file_glob=settings.file_glob)

    return run_pipeline(
        settings=settings,
        resource=files | xml_to_dict_reader,
        destination_kwargs={"max_table_nesting": settings.max_table_nesting},
        pipeline_kwargs={
            "pipeline_name": PIPELINE_NAME,
            "dataset_name": settings.dataset_name,
        },
        pipeline_run_kwargs={
            "loader_file_format": settings.loader_file_format,
        },
    )


def cli() -> LoadInfo | None:
    """Command-line entry point for the XML to dictionary pipeline."""
    return run_cli(XmlToDictSettings, run_xml_ingest_pipeline)


if __name__ == "__main__":
    cli()
