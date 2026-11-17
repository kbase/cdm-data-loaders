"""Preset XML-to-PyArrow CDM ingestion pipelines for UniProtKB and UniRef."""

from typing import Annotated, ClassVar, Final

import pyarrow as pa
from pydantic import Field
from pydantic_settings import SettingsConfigDict

from cdm_data_loaders.core.cli import CliRunner
from cdm_data_loaders.core.fields import LogInterval
from cdm_data_loaders.core.settings import default_settings_with_shortcuts
from cdm_data_loaders.datasets.uniprot.kb import UNIPROT_CDM_SCHEMA_MAP, make_uniprot_cdm_transform
from cdm_data_loaders.datasets.uniprot.uniref import UNIREF_CDM_SCHEMA_MAP, make_uniref_cdm_transform
from cdm_data_loaders.pipelines.presets.uniprot_xml import (
    UniProtXmlToPyArrowSettings,
    UniRefXmlToPyArrowSettings,
)
from cdm_data_loaders.pipelines.xml_common import XML_CLI_SHORTCUTS
from cdm_data_loaders.pipelines.xmltopyarrow.pipeline import run_xmltopyarrow_pipeline

PIPELINE_NAME: Final[str] = "uniprot_cdm"
UNIREF_CDM_PIPELINE_NAME: Final[str] = "uniref_cdm"
UNIREF50_CDM: Final[str] = "uniref50_cdm"
UNIREF90_CDM: Final[str] = "uniref90_cdm"
UNIREF100_CDM: Final[str] = "uniref100_cdm"
UNIPROT_LOG_INTERVAL: Final[int] = 1000
UNIREF_LOG_INTERVAL: Final[int] = 10000


class _CdmXmlPresetMixin:
    """Shared class-variable overrides disabling user-supplied transform and output-schema modules."""

    transforms_module: ClassVar[str | None] = None
    output_schemas_module: ClassVar[str | None] = None


class UniProtSettings(_CdmXmlPresetMixin, UniProtXmlToPyArrowSettings):
    """Configuration for running the UniProtKB CDM XML-to-PyArrow import pipeline."""

    model_config: SettingsConfigDict = default_settings_with_shortcuts(
        cli_prog_name=PIPELINE_NAME,
        cli_shortcuts=XML_CLI_SHORTCUTS,
    )

    dataset_name: ClassVar[str] = PIPELINE_NAME

    log_interval: Annotated[
        LogInterval,
        Field(default=UNIPROT_LOG_INTERVAL),
    ]

    def _resolve_settings_schema_map(self, default_pyarrow_schema: pa.Schema | None = None) -> None:
        """Bind the UniProtKB CDM transform and schemas to the ``entry`` table.

        :param default_pyarrow_schema: optional override for the entry reader PyArrow schema
        :type default_pyarrow_schema: pa.Schema | None
        """
        self._bind_cdm_transform_and_schemas(
            make_uniprot_cdm_transform(),
            UNIPROT_CDM_SCHEMA_MAP,
            default_pyarrow_schema=default_pyarrow_schema,
        )


class UniRefCdmSettings(_CdmXmlPresetMixin, UniRefXmlToPyArrowSettings):
    """Base settings for UniRef CDM XML-to-PyArrow ingestion pipelines."""

    PROTOCOL: ClassVar[str]

    model_config: SettingsConfigDict = default_settings_with_shortcuts(
        cli_prog_name=UNIREF_CDM_PIPELINE_NAME,
        cli_shortcuts=XML_CLI_SHORTCUTS,
    )

    log_interval: Annotated[
        LogInterval,
        Field(default=UNIREF_LOG_INTERVAL),
    ]

    def _resolve_settings_schema_map(self, default_pyarrow_schema: pa.Schema | None = None) -> None:
        """Bind the variant-specific UniRef CDM transform and schemas to the ``entry`` table.

        :param default_pyarrow_schema: optional override for the entry reader PyArrow schema
        :type default_pyarrow_schema: pa.Schema | None
        """
        self._bind_cdm_transform_and_schemas(
            make_uniref_cdm_transform(self.PROTOCOL),
            UNIREF_CDM_SCHEMA_MAP,
            default_pyarrow_schema=default_pyarrow_schema,
        )


class UniRef50CdmSettings(UniRefCdmSettings):
    """UniRef 50 CDM XML-to-PyArrow ingest settings."""

    PROTOCOL: ClassVar[str] = "UniRef 50"

    model_config: SettingsConfigDict = default_settings_with_shortcuts(
        cli_prog_name=UNIREF50_CDM,
        cli_shortcuts=XML_CLI_SHORTCUTS,
    )

    dataset_name: ClassVar[str] = UNIREF50_CDM


class UniRef90CdmSettings(UniRefCdmSettings):
    """UniRef 90 CDM XML-to-PyArrow ingest settings."""

    PROTOCOL: ClassVar[str] = "UniRef 90"

    model_config: SettingsConfigDict = default_settings_with_shortcuts(
        cli_prog_name=UNIREF90_CDM,
        cli_shortcuts=XML_CLI_SHORTCUTS,
    )

    dataset_name: ClassVar[str] = UNIREF90_CDM


class UniRef100CdmSettings(UniRefCdmSettings):
    """UniRef 100 CDM XML-to-PyArrow ingest settings."""

    PROTOCOL: ClassVar[str] = "UniRef 100"

    model_config: SettingsConfigDict = default_settings_with_shortcuts(
        cli_prog_name=UNIREF100_CDM,
        cli_shortcuts=XML_CLI_SHORTCUTS,
    )

    dataset_name: ClassVar[str] = UNIREF100_CDM


uniprot_cdm_cli = CliRunner(UniProtSettings, run_xmltopyarrow_pipeline)
uniref50_cdm_cli = CliRunner(UniRef50CdmSettings, run_xmltopyarrow_pipeline)
uniref90_cdm_cli = CliRunner(UniRef90CdmSettings, run_xmltopyarrow_pipeline)
uniref100_cdm_cli = CliRunner(UniRef100CdmSettings, run_xmltopyarrow_pipeline)
