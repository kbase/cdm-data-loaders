"""xml2db-based XML to relational-table pipeline for the KBase CTS."""

from collections.abc import Generator, Iterator
from logging import Logger, getLogger
from pathlib import Path
from typing import TYPE_CHECKING, Any, Final

import dlt
from dlt.common.pipeline import LoadInfo
from dlt.common.storages.fsspec_filesystem import FileItemDict
from dlt.common.typing import TDataItems
from xml2db import DataModel, load_config

from cdm_data_loaders.core.fields import PARQUET
from cdm_data_loaders.pipelines.core import filesystem_resource, run_cli, run_pipeline
from cdm_data_loaders.pipelines.xml2db.settings import PIPELINE_NAME, Xml2DbSettings
from cdm_data_loaders.readers.xml2db_doc import build_xml2db_model, process_xml_file_with_xml2db
from cdm_data_loaders.readers.xml2db_merge import MergePreparer

if TYPE_CHECKING:
    from dlt.extract import DltResource

logger: Logger = getLogger(__name__)

ICEBERG: Final[str] = "iceberg"


def _read_items(
    items: Iterator[FileItemDict],
    settings: Xml2DbSettings,
    model: DataModel,
    merge_preparer: MergePreparer | None = None,
) -> Generator[TDataItems, Any, Any]:
    """Read each file in items with xml2db. Yields pages of rows, one page per output table.

    :param items: file items to read
    :type  items: Iterator[FileItemDict]
    :param settings: pipeline config, including buffer_size and skip_xml_validation
    :type  settings: Xml2DbSettings
    :param model: the xml2db DataModel built from settings.xsd_file
    :type  model: DataModel
    :param merge_preparer: drops rows already seen earlier in the run and supplies merge hints;
        shared by every file read
    :type  merge_preparer: MergePreparer | None
    :yield: pages of table-tagged rows
    :rtype: Generator[TDataItems, Any, Any]
    """
    for file_item in items:
        yield from process_xml_file_with_xml2db(
            settings, model, file_path=Path(file_item.local_file_path), merge_preparer=merge_preparer
        )


def run_xml2db_ingest_pipeline(settings: Xml2DbSettings) -> LoadInfo | None:
    """Run the xml2db pipeline on matching files in settings.input_dir.

    Output tables are written in iceberg table format. Every table is merged on a single-column
    primary key, so rows already in a table are skipped. Repeats within the run are dropped before
    dlt sees them, because the iceberg merge fails on repeated keys.

    :param settings: pipeline configuration
    :type  settings: Xml2DbSettings
    """
    model_config = load_config(settings.xml2db_config_file) if settings.xml2db_config_file else None
    model = build_xml2db_model(settings.xsd_file, short_name=settings.short_name, model_config=model_config)

    reader = dlt.transformer(_read_items, name="xml2db_reader", parallelized=True)

    files = filesystem_resource(bucket_url=settings.input_dir, file_glob=settings.file_glob)

    xml2db_resource: DltResource = files | reader(settings, model, MergePreparer(model))

    return run_pipeline(
        settings=settings,
        resource=xml2db_resource,
        pipeline_kwargs={
            "pipeline_name": PIPELINE_NAME,
            "dataset_name": settings.dataset_name,
        },
        pipeline_run_kwargs={
            "loader_file_format": PARQUET,
            "table_format": ICEBERG,
        },
    )


def cli() -> LoadInfo | None:
    """Command-line entry point for the xml2db pipeline."""
    return run_cli(Xml2DbSettings, run_xml2db_ingest_pipeline)


if __name__ == "__main__":
    cli()
