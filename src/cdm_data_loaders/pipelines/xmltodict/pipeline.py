"""XML to dictionary pipeline for the KBase CTS."""

from pathlib import Path

from dlt.common.pipeline import LoadInfo

from cdm_data_loaders.pipelines.core import (
    PipelineCli,
    build_file_ingest_kwargs,
    build_per_file_filesystem_resource,
    run_pipeline,
)
from cdm_data_loaders.pipelines.xmltodict.settings import PIPELINE_NAME, XmlToDictSettings
from cdm_data_loaders.readers.xml import process_xml_file_to_dict


def run_xmltodict_pipeline(settings: XmlToDictSettings) -> LoadInfo | None:
    """Run the XML to dict pipeline on matching files in settings.input_dir.

    :param settings: pipeline configuration
    :type  settings: XmlToDictSettings
    """
    xmltodict_resource = build_per_file_filesystem_resource(
        settings,
        lambda file_item, cfg: process_xml_file_to_dict(cfg, file_path=Path(file_item.local_file_path)),
        settings,
        name="xmltodict_reader",
    )

    return run_pipeline(
        settings=settings,
        resource=xmltodict_resource,
        **build_file_ingest_kwargs(settings, PIPELINE_NAME),
    )


cli = PipelineCli(XmlToDictSettings, run_xmltodict_pipeline)


if __name__ == "__main__":
    cli()
