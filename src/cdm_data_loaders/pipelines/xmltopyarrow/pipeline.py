"""XML to PyArrow pipeline for the KBase CTS."""

from dlt.common.pipeline import LoadInfo

from cdm_data_loaders.core.cli import CliRunner
from cdm_data_loaders.core.filesystem import build_file_ingest_kwargs, build_per_file_filesystem_resource
from cdm_data_loaders.pipelines.runner import run_pipeline
from cdm_data_loaders.pipelines.xmltopyarrow.processors import process_xml_file_to_pyarrow
from cdm_data_loaders.pipelines.xmltopyarrow.settings import (
    PIPELINE_NAME,
    XmlToPyArrowSettings,
)


def run_xmltopyarrow_pipeline(settings: XmlToPyArrowSettings) -> LoadInfo | None:
    """Run the XML to PyArrow pipeline on matching files in ``settings.input_dir``.

    :param settings: pipeline configuration
    :type settings: XmlToPyArrowSettings
    :return: DLT pipeline ``LoadInfo`` or ``None``
    :rtype: LoadInfo | None
    """
    xmltopyarrow_resource = build_per_file_filesystem_resource(
        settings,
        process_xml_file_to_pyarrow,
        name="xmltopyarrow_reader",
        parallelized=False,
    )

    return run_pipeline(
        settings=settings,
        resource=xmltopyarrow_resource,
        **build_file_ingest_kwargs(settings, PIPELINE_NAME),
    )


cli = CliRunner(XmlToPyArrowSettings, run_xmltopyarrow_pipeline)

if __name__ == "__main__":
    cli()
