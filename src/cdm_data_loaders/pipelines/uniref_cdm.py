"""DLT pipelines to import UniRef data into KBase CDM tables via XML-to-PyArrow."""

from typing import Annotated, ClassVar, Final

import pyarrow as pa
from pydantic import Field
from pydantic_settings import SettingsConfigDict

from cdm_data_loaders.core.fields import LogInterval
from cdm_data_loaders.core.registry import TransformRegistry
from cdm_data_loaders.core.settings import (
    XML_CLI_SHORTCUTS,
    default_settings_with_shortcuts,
)
from cdm_data_loaders.parsers.uniprot.uniref import UNIREF_CDM_SCHEMA_MAP, make_uniref_cdm_transform
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
    output_schemas_module: ClassVar[str | None] = None

    log_interval: Annotated[
        LogInterval,
        Field(
            default=UNIREF_LOG_INTERVAL,
        ),
    ]

    def _resolve_settings_schema_map(self, default_pyarrow_schema: pa.Schema | None = None) -> None:
        """Bind the variant-specific UniRef CDM transform and schemas to the entry table."""
        self._table_transforms = TransformRegistry(
            registry={self.table_name: make_uniref_cdm_transform(self._PROTOCOL)}
        )
        self._unreconciled_output_schemas = UNIREF_CDM_SCHEMA_MAP
        super()._resolve_settings_schema_map(default_pyarrow_schema=default_pyarrow_schema)


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
