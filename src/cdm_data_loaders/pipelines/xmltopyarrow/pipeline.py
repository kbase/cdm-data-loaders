"""XML to pyarrow pipeline for the KBase CTS."""

from dlt.common.pipeline import LoadInfo

from cdm_data_loaders.pipelines.core import (
    PipelineCli,
    build_file_ingest_kwargs,
    build_per_file_filesystem_resource,
    run_pipeline,
)
from cdm_data_loaders.pipelines.xmltopyarrow.settings import (
    PIPELINE_NAME,
    PubMedXmlToPyArrowSettings,
    UniProtXmlToPyArrowSettings,
    UniRef50XmlToPyArrowSettings,
    UniRef90XmlToPyArrowSettings,
    UniRef100XmlToPyArrowSettings,
    XmlToPyArrowSettings,
)
from cdm_data_loaders.readers.xml import process_xml_file_to_pyarrow


def run_xmltopyarrow_pipeline(settings: XmlToPyArrowSettings) -> LoadInfo | None:
    """Run the XML to PyArrow pipeline on matching files in settings.input_dir.

    :param settings: pipeline configuration
    :type  settings: XmlToPyArrowSettings
    :return: DLT pipeline LoadInfo or None
    :rtype : LoadInfo | None
    """
    xmltopyarrow_resource = build_per_file_filesystem_resource(
        settings,
        process_xml_file_to_pyarrow,
        name="xmltopyarrow_reader",
        parallelized=False,
    )
    pipeline_name = str(settings.model_config.get("cli_prog_name") or PIPELINE_NAME)

    return run_pipeline(
        settings=settings,
        resource=xmltopyarrow_resource,
        **build_file_ingest_kwargs(settings, pipeline_name),
    )


cli = PipelineCli(XmlToPyArrowSettings, run_xmltopyarrow_pipeline)

pubmed_cli = PipelineCli(PubMedXmlToPyArrowSettings, run_xmltopyarrow_pipeline)

uniprot_cli = PipelineCli(UniProtXmlToPyArrowSettings, run_xmltopyarrow_pipeline)

uniref50_cli = PipelineCli(UniRef50XmlToPyArrowSettings, run_xmltopyarrow_pipeline)

uniref90_cli = PipelineCli(UniRef90XmlToPyArrowSettings, run_xmltopyarrow_pipeline)

uniref100_cli = PipelineCli(UniRef100XmlToPyArrowSettings, run_xmltopyarrow_pipeline)

if __name__ == "__main__":
    cli()
