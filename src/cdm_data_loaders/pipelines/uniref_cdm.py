"""DLT pipelines to import UniRef data into KBase CDM tables via XML-to-PyArrow."""

from typing import Annotated, ClassVar, Final, Self

from pydantic import Field, model_validator
from pydantic_settings import SettingsConfigDict

from cdm_data_loaders.core.fields import LogInterval
from cdm_data_loaders.core.settings import (
    XML_CLI_SHORTCUTS,
    default_settings_with_shortcuts,
)
from cdm_data_loaders.parsers.uniprot.uniref import make_uniref_cdm_transform
from cdm_data_loaders.pipelines.core import PipelineCli
from cdm_data_loaders.pipelines.xmltopyarrow.pipeline import run_xmltopyarrow_pipeline
from cdm_data_loaders.pipelines.xmltopyarrow.settings import UniRefXmlToPyArrowSettings

UNIREF_CDM_PIPELINE_NAME: Final[str] = "uniref_cdm"
UNIREF50_CDM: Final[str] = "uniref50_cdm"
UNIREF90_CDM: Final[str] = "uniref90_cdm"
UNIREF100_CDM: Final[str] = "uniref100_cdm"
UNIREF_LOG_INTERVAL: Final[int] = 10000


class UniRefCdmSettings(UniRefXmlToPyArrowSettings):
    """Base settings for UniRef CDM XML-to-PyArrow ingestion pipelines."""

    _PROTOCOL: ClassVar[str]

    model_config: SettingsConfigDict = default_settings_with_shortcuts(
        cli_prog_name=UNIREF_CDM_PIPELINE_NAME,
        cli_shortcuts=XML_CLI_SHORTCUTS,
    )

    transforms_module: ClassVar[str | None] = None

    log_interval: Annotated[
        LogInterval,
        Field(
            default=UNIREF_LOG_INTERVAL,
        ),
    ]

    @model_validator(mode="after")
    def _configure_uniref_cdm_transform(self) -> Self:
        """Bind the variant-specific UniRef CDM TableTransform to the entry table."""
        self._table_transforms = {self.table_name: make_uniref_cdm_transform(self._PROTOCOL)}
        return self


class UniRef50CdmSettings(UniRefCdmSettings):
    """UniRef 50 CDM XML-to-PyArrow ingest settings."""

    _PROTOCOL: ClassVar[str] = "UniRef 50"

    model_config: SettingsConfigDict = default_settings_with_shortcuts(
        cli_prog_name=UNIREF50_CDM,
        cli_shortcuts=XML_CLI_SHORTCUTS,
    )

    dataset_name: ClassVar[str] = UNIREF50_CDM


class UniRef90CdmSettings(UniRefCdmSettings):
    """UniRef 90 CDM XML-to-PyArrow ingest settings."""

    _PROTOCOL: ClassVar[str] = "UniRef 90"

    model_config: SettingsConfigDict = default_settings_with_shortcuts(
        cli_prog_name=UNIREF90_CDM,
        cli_shortcuts=XML_CLI_SHORTCUTS,
    )

    dataset_name: ClassVar[str] = UNIREF90_CDM


class UniRef100CdmSettings(UniRefCdmSettings):
    """UniRef 100 CDM XML-to-PyArrow ingest settings."""

    _PROTOCOL: ClassVar[str] = "UniRef 100"

    model_config: SettingsConfigDict = default_settings_with_shortcuts(
        cli_prog_name=UNIREF100_CDM,
        cli_shortcuts=XML_CLI_SHORTCUTS,
    )

    dataset_name: ClassVar[str] = UNIREF100_CDM


uniref50_cdm_cli = PipelineCli(UniRef50CdmSettings, run_xmltopyarrow_pipeline)

uniref90_cdm_cli = PipelineCli(UniRef90CdmSettings, run_xmltopyarrow_pipeline)

uniref100_cdm_cli = PipelineCli(UniRef100CdmSettings, run_xmltopyarrow_pipeline)
