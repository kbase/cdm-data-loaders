"""Shared fixtures for ``tests/cdm_data_loaders/pipelines/presets``."""

from typing import Any

import pytest
from frozendict import frozendict

from cdm_data_loaders.core.fields import (
    FILE_GLOB,
    LOADER_FILE_FORMAT,
    PREPROCESSORS_MODULE,
    LoaderFileFormatEnum,
)
from cdm_data_loaders.pipelines.presets.uniprot_cdm import (
    UNIPROT_LOG_INTERVAL,
    UniProtSettings,
    UniRefCdmSettings,
)
from cdm_data_loaders.pipelines.presets.uniprot_idmapping import (
    DEFAULT_IDMAPPING_FILE_GLOB,
    DEFAULT_PARTITION_BUCKETS,
)
from cdm_data_loaders.pipelines.presets.uniprot_xml import UNIPROT_XSD_FILE, UNIREF_XSD_FILE
from cdm_data_loaders.schemas.uniprot.idmapping.schema import IDMAPPING_SCHEMA_FILE
from tests.cdm_data_loaders.pipelines.presets._helpers import (
    UNIREF_CDM_VARIANTS,
    UniRefCliRunner,
)


@pytest.fixture
def uniprot_settings(test_defaults: dict[str, Any]) -> UniProtSettings:
    """Provide a minimal valid ``UniProtSettings`` object.

    :param test_defaults: Shared minimal pipeline settings dictionary.
    :type test_defaults: dict[str, Any]
    :return: Validated ``UniProtSettings`` instance.
    :rtype: UniProtSettings
    """
    return UniProtSettings(**test_defaults)


@pytest.fixture
def test_uniprot_settings(test_cts_settings: frozendict[str, Any]) -> frozendict[str, Any]:
    """Return raw ``UniProtSettings`` dictionary extending ``test_cts_settings``.

    :param test_cts_settings: Shared raw CTS settings dictionary.
    :type test_cts_settings: frozendict[str, Any]
    :return: Frozen dictionary of raw field values.
    :rtype: frozendict[str, Any]
    """
    return frozendict(
        {
            **test_cts_settings,
            FILE_GLOB: "*.xml*",
            LOADER_FILE_FORMAT: "parquet",
            PREPROCESSORS_MODULE: "cdm_data_loaders.core.md5_preprocessor",
            "xsd_file": str(UNIPROT_XSD_FILE),
            "max_recursion_depth": 2,
            "log_interval": UNIPROT_LOG_INTERVAL,
        }
    )


@pytest.fixture
def test_uniprot_settings_reconciled(test_cts_settings_reconciled: frozendict[str, Any]) -> frozendict[str, Any]:
    """Return reconciled ``UniProtSettings`` dictionary extending ``test_cts_settings_reconciled``.

    :param test_cts_settings_reconciled: Shared reconciled CTS settings dictionary.
    :type test_cts_settings_reconciled: frozendict[str, Any]
    :return: Frozen dictionary of expected validated field values.
    :rtype: frozendict[str, Any]
    """
    return frozendict(
        {
            **test_cts_settings_reconciled,
            FILE_GLOB: "*.xml*",
            LOADER_FILE_FORMAT: LoaderFileFormatEnum.PARQUET,
            PREPROCESSORS_MODULE: "cdm_data_loaders.core.md5_preprocessor",
            "xsd_file": UNIPROT_XSD_FILE,
            "max_recursion_depth": 2,
            "log_interval": UNIPROT_LOG_INTERVAL,
        }
    )


@pytest.fixture
def test_uniref_cdm_settings(test_cts_settings: frozendict[str, Any]) -> frozendict[str, Any]:
    """Return raw ``UniRefCdmSettings`` dictionary extending ``test_cts_settings``.

    :param test_cts_settings: Shared raw CTS settings dictionary.
    :type test_cts_settings: frozendict[str, Any]
    :return: Frozen dictionary of raw field values.
    :rtype: frozendict[str, Any]
    """
    return frozendict(
        {
            **test_cts_settings,
            LOADER_FILE_FORMAT: "parquet",
            FILE_GLOB: "*.xml*",
            PREPROCESSORS_MODULE: "cdm_data_loaders.core.md5_preprocessor",
            "xsd_file": str(UNIREF_XSD_FILE),
            "max_recursion_depth": 2,
        }
    )


@pytest.fixture
def test_uniref_cdm_settings_reconciled(test_cts_settings_reconciled: frozendict[str, Any]) -> frozendict[str, Any]:
    """Return reconciled ``UniRefCdmSettings`` dictionary extending ``test_cts_settings_reconciled``.

    :param test_cts_settings_reconciled: Shared reconciled CTS settings dictionary.
    :type test_cts_settings_reconciled: frozendict[str, Any]
    :return: Frozen dictionary of expected validated field values.
    :rtype: frozendict[str, Any]
    """
    return frozendict(
        {
            **test_cts_settings_reconciled,
            LOADER_FILE_FORMAT: LoaderFileFormatEnum.PARQUET,
            FILE_GLOB: "*.xml*",
            PREPROCESSORS_MODULE: "cdm_data_loaders.core.md5_preprocessor",
            "xsd_file": UNIREF_XSD_FILE,
            "max_recursion_depth": 2,
        }
    )


@pytest.fixture(params=UNIREF_CDM_VARIANTS, ids=[name for _, _, name, _ in UNIREF_CDM_VARIANTS])
def uniref_cdm_variant(
    request: pytest.FixtureRequest,
) -> tuple[type[UniRefCdmSettings], UniRefCliRunner, str, str]:
    """Yield each UniRef CDM variant tuple in turn.

    :param request: Active pytest fixture request.
    :type request: pytest.FixtureRequest
    :return: Variant tuple ``(settings_cls, cli_runner, cli_name, protocol)``.
    :rtype: tuple[type[UniRefCdmSettings], UniRefCliRunner, str, str]
    """
    return request.param


@pytest.fixture
def uniref_settings(
    uniref_cdm_variant: tuple[type[UniRefCdmSettings], UniRefCliRunner, str, str],
    test_defaults: dict[str, Any],
) -> UniRefCdmSettings:
    """Provide a minimal valid ``UniRefCdmSettings`` instance for each UniRef CDM variant.

    :param uniref_cdm_variant: Active UniRef CDM variant tuple.
    :type uniref_cdm_variant: tuple[type[UniRefCdmSettings], UniRefCliRunner, str, str]
    :param test_defaults: Shared minimal pipeline settings dictionary.
    :type test_defaults: dict[str, Any]
    :return: Validated ``UniRefCdmSettings`` instance.
    :rtype: UniRefCdmSettings
    """
    settings_cls, _, _, _ = uniref_cdm_variant
    return settings_cls(**test_defaults)


@pytest.fixture
def test_uniprot_idmapping_settings(test_cts_settings: frozendict[str, Any]) -> frozendict[str, Any]:
    """Provide raw ``UniProtIdMappingSettings`` field values extending ``test_cts_settings``.

    :param test_cts_settings: Shared raw CTS settings dictionary.
    :type test_cts_settings: frozendict[str, Any]
    :return: Raw settings mapping covering every field on ``UniProtIdMappingSettings``.
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

    :param test_cts_settings_reconciled: Shared reconciled CTS settings dictionary.
    :type test_cts_settings_reconciled: frozendict[str, Any]
    :return: Expected validated field values on ``UniProtIdMappingSettings``.
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
