"""Shared fixtures and constants for the UniProt ID mapping pipeline tests."""

from typing import Any, Final

import pytest
from frozendict import frozendict

from cdm_data_loaders.core.fields import (
    FILE_GLOB,
    LOADER_FILE_FORMAT,
    PREPROCESSORS_MODULE,
    LoaderFileFormatEnum,
)
from cdm_data_loaders.pipelines.presets.uniprot_idmapping import (
    DEFAULT_IDMAPPING_FILE_GLOB,
    DEFAULT_PARTITION_BUCKETS,
    UniProtIdMappingSettings,
)
from cdm_data_loaders.schemas.uniprot.idmapping.schema import IDMAPPING_SCHEMA_FILE

SAMPLE_RAW_ROWS: Final[tuple[frozendict[str, str], ...]] = (
    frozendict({"uniprot_id": "P12345", "db_reference_type": "NCBI_TaxID", "db_reference_id": "9606"}),
    frozendict({"uniprot_id": "Q8N158", "db_reference_type": "GeneID", "db_reference_id": "284697"}),
)


@pytest.fixture
def test_settings(test_defaults: dict[str, Any]) -> UniProtIdMappingSettings:
    """Provide a minimal valid ``UniProtIdMappingSettings`` instance.

    :param test_defaults: shared minimal pipeline settings dictionary
    :type test_defaults: dict[str, Any]
    :return: validated ``UniProtIdMappingSettings`` instance
    :rtype: UniProtIdMappingSettings
    """
    return UniProtIdMappingSettings(**test_defaults)


@pytest.fixture
def test_uniprot_idmapping_settings(test_cts_settings: frozendict[str, Any]) -> frozendict[str, Any]:
    """Provide raw ``UniProtIdMappingSettings`` field values extending ``test_cts_settings``.

    :param test_cts_settings: shared raw CTS settings dictionary
    :type test_cts_settings: frozendict[str, Any]
    :return: raw settings mapping covering every field on ``UniProtIdMappingSettings``
    :rtype: frozendict[str, Any]
    """
    return frozendict(
        {
            **test_cts_settings,
            FILE_GLOB: DEFAULT_IDMAPPING_FILE_GLOB,
            LOADER_FILE_FORMAT: "parquet",
            PREPROCESSORS_MODULE: "cdm_data_loaders.core.md5_preprocessor",
            "schema_file": str(IDMAPPING_SCHEMA_FILE),
            "partition_buckets": DEFAULT_PARTITION_BUCKETS,
        }
    )


@pytest.fixture
def test_uniprot_idmapping_settings_reconciled(
    test_cts_settings_reconciled: frozendict[str, Any],
) -> frozendict[str, Any]:
    """Provide reconciled ``UniProtIdMappingSettings`` field values extending ``test_cts_settings_reconciled``.

    :param test_cts_settings_reconciled: shared reconciled CTS settings dictionary
    :type test_cts_settings_reconciled: frozendict[str, Any]
    :return: expected validated field values on ``UniProtIdMappingSettings``
    :rtype: frozendict[str, Any]
    """
    return frozendict(
        {
            **test_cts_settings_reconciled,
            FILE_GLOB: DEFAULT_IDMAPPING_FILE_GLOB,
            LOADER_FILE_FORMAT: LoaderFileFormatEnum.PARQUET,
            PREPROCESSORS_MODULE: "cdm_data_loaders.core.md5_preprocessor",
            "schema_file": IDMAPPING_SCHEMA_FILE,
            "partition_buckets": DEFAULT_PARTITION_BUCKETS,
        }
    )
