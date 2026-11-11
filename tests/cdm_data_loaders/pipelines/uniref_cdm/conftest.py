"""Shared fixtures for the UniRef CDM pipeline tests."""

from typing import Any, Final

import pytest
from frozendict import frozendict

from cdm_data_loaders.core.fields import (
    FILE_GLOB,
    LOADER_FILE_FORMAT,
    PREPROCESSORS_MODULE,
    LoaderFileFormatEnum,
)
from cdm_data_loaders.pipelines.core import PipelineCli
from cdm_data_loaders.pipelines.uniref_cdm import (
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
from cdm_data_loaders.pipelines.xmltopyarrow.settings import UNIREF_XSD_FILE

UNIREF_CDM_VARIANTS: Final[tuple[tuple[type[UniRefCdmSettings], PipelineCli, str, str], ...]] = (
    (UniRef50CdmSettings, uniref50_cdm_cli, UNIREF50_CDM, "UniRef 50"),
    (UniRef90CdmSettings, uniref90_cdm_cli, UNIREF90_CDM, "UniRef 90"),
    (UniRef100CdmSettings, uniref100_cdm_cli, UNIREF100_CDM, "UniRef 100"),
)

UNIREF_CDM_SETTINGS_CLASSES: Final[tuple[type[UniRefCdmSettings], ...]] = tuple(
    cls for cls, _, _, _ in UNIREF_CDM_VARIANTS
)


@pytest.fixture
def test_uniref_cdm_settings(test_cts_settings: frozendict[str, Any]) -> frozendict[str, Any]:
    """Return UniRef CDM settings dict extending test_cts_settings."""
    return frozendict(
        {
            **test_cts_settings,
            LOADER_FILE_FORMAT: "parquet",
            FILE_GLOB: "*.xml*",
            PREPROCESSORS_MODULE: "cdm_data_loaders.core.preprocessors",
            "xsd_file": str(UNIREF_XSD_FILE),
            "max_recursion_depth": 2,
        }
    )


@pytest.fixture
def test_uniref_cdm_settings_reconciled(test_cts_settings_reconciled: frozendict[str, Any]) -> frozendict[str, Any]:
    """Return reconciled UniRef CDM settings dict extending test_cts_settings_reconciled."""
    return frozendict(
        {
            **test_cts_settings_reconciled,
            LOADER_FILE_FORMAT: LoaderFileFormatEnum.PARQUET,
            FILE_GLOB: "*.xml*",
            PREPROCESSORS_MODULE: "cdm_data_loaders.core.preprocessors",
            "xsd_file": UNIREF_XSD_FILE,
            "max_recursion_depth": 2,
        }
    )


@pytest.fixture(params=UNIREF_CDM_VARIANTS, ids=[name for _, _, name, _ in UNIREF_CDM_VARIANTS])
def uniref_cdm_variant(
    request: pytest.FixtureRequest,
) -> tuple[type[UniRefCdmSettings], PipelineCli, str, str]:
    """Parametrized fixture over all UniRef CDM variant tuples."""
    return request.param


@pytest.fixture
def test_settings(
    uniref_cdm_variant: tuple[type[UniRefCdmSettings], PipelineCli, str, str],
    test_defaults: dict[str, Any],
) -> UniRefCdmSettings:
    """A valid UniRefCdmSettings object for each UniRef CDM variant."""
    settings_cls, _, _, _ = uniref_cdm_variant
    return settings_cls(**test_defaults)
