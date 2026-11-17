"""Shared variant constants and fixtures for preset E2E tests."""

from typing import Final

from dlt.common.pipeline import LoadInfo
from frozendict import frozendict

from cdm_data_loaders.core.cli import CliRunner
from cdm_data_loaders.pipelines.presets.uniprot_cdm import (
    UNIREF50_CDM,
    UNIREF90_CDM,
    UNIREF100_CDM,
    UniRef50CdmSettings,
    UniRef90CdmSettings,
    UniRef100CdmSettings,
    UniRefCdmSettings,
    uniref50_cdm_cli,
    uniref90_cdm_cli,
    uniref100_cdm_cli,
)

type UniRefCliRunner = CliRunner[UniRefCdmSettings, LoadInfo | None]

UNIREF_CDM_VARIANTS: Final[tuple[tuple[type[UniRefCdmSettings], UniRefCliRunner, str, str], ...]] = (
    (UniRef50CdmSettings, uniref50_cdm_cli, UNIREF50_CDM, "UniRef 50"),
    (UniRef90CdmSettings, uniref90_cdm_cli, UNIREF90_CDM, "UniRef 90"),
    (UniRef100CdmSettings, uniref100_cdm_cli, UNIREF100_CDM, "UniRef 100"),
)

SAMPLE_RAW_ROWS: Final[tuple[frozendict[str, str], ...]] = (
    frozendict({"uniprot_id": "P12345", "db_reference_type": "NCBI_TaxID", "db_reference_id": "9606"}),
    frozendict({"uniprot_id": "Q8N158", "db_reference_type": "GeneID", "db_reference_id": "284697"}),
)
