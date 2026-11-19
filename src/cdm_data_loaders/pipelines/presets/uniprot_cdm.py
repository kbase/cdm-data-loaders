"""Preset XML-to-PyArrow CDM ingestion pipelines for UniProtKB and UniRef."""

from typing import Annotated, ClassVar, Final

import pyarrow as pa
from pydantic import Field

from cdm_data_loaders.core.cli import CliRunner
from cdm_data_loaders.core.fields import LogInterval
from cdm_data_loaders.core.transforms.settings_mixin import LockedTransformHooksMixin
from cdm_data_loaders.datasets.uniprot.uniprot_kb import (
    UNIPROT_CDM_SCHEMA_MAP,
    make_uniprot_cdm_transform,
)
from cdm_data_loaders.datasets.uniprot.uniref import UNIREF_CDM_SCHEMA_MAP, make_uniref_cdm_transform
from cdm_data_loaders.pipelines.presets.uniprot_xml import (
    UniProtXmlToPyArrowSettings,
    UniRefXmlToPyArrowSettings,
)
from cdm_data_loaders.pipelines.xmltopyarrow.pipeline import run_xmltopyarrow_pipeline

PIPELINE_NAME: Final[str] = "uniprot_cdm"
UNIREF_CDM_PIPELINE_NAME: Final[str] = "uniref_cdm"
UNIREF50_CDM: Final[str] = "uniref50_cdm"
UNIREF90_CDM: Final[str] = "uniref90_cdm"
UNIREF100_CDM: Final[str] = "uniref100_cdm"
UNIPROT_LOG_INTERVAL: Final[int] = 1000
UNIREF_LOG_INTERVAL: Final[int] = 10000


class UniProtSettings(LockedTransformHooksMixin, UniProtXmlToPyArrowSettings, cli_prog_name=PIPELINE_NAME):
    """Configuration for running the UniProtKB CDM XML-to-PyArrow import pipeline."""

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


class UniRefCdmSettings(LockedTransformHooksMixin, UniRefXmlToPyArrowSettings, cli_prog_name=UNIREF_CDM_PIPELINE_NAME):
    """Base settings for UniRef CDM XML-to-PyArrow ingestion pipelines."""

    PROTOCOL: ClassVar[str]

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


class UniRef50CdmSettings(UniRefCdmSettings, cli_prog_name=UNIREF50_CDM):
    """UniRef 50 CDM XML-to-PyArrow ingest settings."""

    PROTOCOL: ClassVar[str] = "UniRef 50"
    dataset_name: ClassVar[str] = UNIREF50_CDM


class UniRef90CdmSettings(UniRefCdmSettings, cli_prog_name=UNIREF90_CDM):
    """UniRef 90 CDM XML-to-PyArrow ingest settings."""

    PROTOCOL: ClassVar[str] = "UniRef 90"
    dataset_name: ClassVar[str] = UNIREF90_CDM


class UniRef100CdmSettings(UniRefCdmSettings, cli_prog_name=UNIREF100_CDM):
    """UniRef 100 CDM XML-to-PyArrow ingest settings."""

    PROTOCOL: ClassVar[str] = "UniRef 100"
    dataset_name: ClassVar[str] = UNIREF100_CDM


uniprot_cdm_cli = CliRunner(UniProtSettings, run_xmltopyarrow_pipeline)
uniref50_cdm_cli = CliRunner(UniRef50CdmSettings, run_xmltopyarrow_pipeline)
uniref90_cdm_cli = CliRunner(UniRef90CdmSettings, run_xmltopyarrow_pipeline)
uniref100_cdm_cli = CliRunner(UniRef100CdmSettings, run_xmltopyarrow_pipeline)
