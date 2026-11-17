"""Shared variant constants for UniRef CDM preset tests."""

from typing import Final

from dlt.common.pipeline import LoadInfo

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

UNIREF_CDM_SETTINGS_CLASSES: Final[tuple[type[UniRefCdmSettings], ...]] = tuple(
    cls for cls, _, _, _ in UNIREF_CDM_VARIANTS
)
