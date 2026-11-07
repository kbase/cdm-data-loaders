"""Settings class for the XML to dictionary pipeline for the KBase CTS."""

from typing import Final

from pydantic_settings import SettingsConfigDict

from cdm_data_loaders.core.settings import (
    XML_CLI_SHORTCUTS,
    SingleTableXmlIngestSettings,
    default_settings_with_shortcuts,
)

PIPELINE_NAME: Final[str] = "xmltodict"


class XmlToDictSettings(SingleTableXmlIngestSettings):
    """Settings for the XML ingestion pipeline."""

    model_config: SettingsConfigDict = default_settings_with_shortcuts(
        cli_prog_name=PIPELINE_NAME,
        cli_shortcuts=XML_CLI_SHORTCUTS,
    )
