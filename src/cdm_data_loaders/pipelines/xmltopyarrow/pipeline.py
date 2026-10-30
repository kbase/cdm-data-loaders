"""XML to pyarrow pipeline for the KBase CTS."""

from pathlib import Path

from dlt.common.pipeline import LoadInfo

from cdm_data_loaders.pipelines.core import (
    PipelineCli,
    build_file_ingest_kwargs,
    build_per_file_filesystem_resource,
    run_pipeline,
)
from cdm_data_loaders.pipelines.xmltopyarrow.settings import PIPELINE_NAME, XmlToPyArrowSettings
from cdm_data_loaders.readers.xml import process_xml_file_to_pyarrow


def run_xmltopyarrow_pipeline(settings: XmlToPyArrowSettings) -> LoadInfo | None:
    """Run the XML to PyArrow pipeline on matching files in settings.input_dir.

    :param settings: pipeline configuration
    :type  settings: XmlToPyArrowSettings
    """
    xmltopyarrow_resource = build_per_file_filesystem_resource(
        settings,
        lambda file_item, cfg: process_xml_file_to_pyarrow(cfg, file_path=Path(file_item.local_file_path)),
        settings,
        name="xmltopyarrow_reader",
    )
    pipeline_name = str(settings.model_config.get("cli_prog_name") or PIPELINE_NAME)

    return run_pipeline(
        settings=settings,
        resource=xmltopyarrow_resource,
        **build_file_ingest_kwargs(settings, pipeline_name),
    )


cli = PipelineCli(XmlToPyArrowSettings, run_xmltopyarrow_pipeline)


if __name__ == "__main__":
    cli()
