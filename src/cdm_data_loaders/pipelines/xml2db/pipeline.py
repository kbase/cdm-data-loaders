"""xml2db-based XML to relational-table pipeline for the KBase CTS."""

from logging import Logger, getLogger
from pathlib import Path

from dlt.common.pipeline import LoadInfo
from xml2db import DataModel, load_config

from cdm_data_loaders.pipelines.core import (
    PipelineCli,
    build_file_ingest_kwargs,
    build_filesystem_resource,
    make_per_file_reader,
    run_pipeline,
)
from cdm_data_loaders.pipelines.xml2db.compaction import compact_reused_tables
from cdm_data_loaders.pipelines.xml2db.settings import PIPELINE_NAME, Xml2DbSettings
from cdm_data_loaders.readers.xml2db_doc import build_xml2db_model, process_xml_file_with_xml2db

logger: Logger = getLogger(__name__)

_read_items = make_per_file_reader(
    lambda file_item, settings, model: process_xml_file_with_xml2db(
        settings, model, file_path=Path(file_item.local_file_path)
    )
)


def run_xml2db_pipeline(settings: Xml2DbSettings) -> LoadInfo | None:
    """Run the xml2db pipeline on matching files in settings.input_dir.

    :param settings: pipeline configuration
    :type  settings: Xml2DbSettings
    """
    model_config = load_config(str(settings.xml2db_config_file)) if settings.xml2db_config_file else None
    model = build_xml2db_model(settings.xsd_file, short_name=settings.short_name, model_config=model_config)

    xml2db_resource = build_filesystem_resource(settings, _read_items, settings, model, name="xml2db_reader")

    load_info = run_pipeline(
        settings=settings,
        resource=xml2db_resource,
        **build_file_ingest_kwargs(settings, PIPELINE_NAME),
    )

    _maybe_compact(settings, model, load_info)

    return load_info


def _maybe_compact(settings: Xml2DbSettings, model: DataModel, load_info: LoadInfo | None) -> None:
    """Run post-load compaction of "reused" tables, if configured and the run succeeded.

    xml2db's content-hash deduplication of "reused" tables only ever happens within a single
    xml2db `Document` -- one per source file, or, when `settings.xml_tag` is set, one
    per chunk. Any run that parses more than one `Document` (multiple files, multiple chunks, or
    both) can leave literal duplicate rows behind in "reused" tables, so compaction runs
    regardless of whether chunking is enabled. Compaction rewrites files directly on the local
    filesystem, so it is skipped for any other destination.

    :param settings: pipeline configuration.
    :type settings: Xml2DbSettings
    :param model: the xml2db DataModel used for this run.
    :type model: DataModel
    :param load_info: the result of `pipeline.run(...)`, or None if the run failed to start.
    :type load_info: LoadInfo | None
    """
    if load_info is None or load_info.has_failed_jobs or not settings.compact_reused_tables:
        return

    dataset_dir = settings.output_dir / load_info.dataset_name
    removed = compact_reused_tables(dataset_dir, model, str(settings.loader_file_format))
    if removed:
        logger.info("Post-load compaction removed duplicate rows: %s", removed)


cli = PipelineCli(Xml2DbSettings, run_xml2db_pipeline)


if __name__ == "__main__":
    cli()
