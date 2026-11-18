"""Settings class for the XML to dictionary pipeline for the KBase CTS."""

from typing import Final

from cdm_data_loaders.pipelines.xml_common import SingleTableXmlIngestSettings

PIPELINE_NAME: Final[str] = "xmltodict"


class XmlToDictSettings(SingleTableXmlIngestSettings, cli_prog_name=PIPELINE_NAME):
    """Settings for the XML ingestion pipeline."""
