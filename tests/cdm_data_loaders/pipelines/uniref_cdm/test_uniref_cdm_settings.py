"""Tests for the UniRef CDM pipeline settings classes."""

from typing import Any

import pytest
from frozendict import frozendict
from pydantic_settings import CliApp, SettingsError

from cdm_data_loaders.core.registry import TransformRegistry
from cdm_data_loaders.parsers.uniprot.uniref import UNIREF_CDM_SCHEMA_MAP
from cdm_data_loaders.pipelines.uniref_cdm import (
    UNIREF_LOG_INTERVAL,
    UniRef50CdmSettings,
    UniRefCdmSettings,
)
from cdm_data_loaders.pipelines.xmltopyarrow.settings import (
    ENTRY_TABLE_NAME,
    ENTRY_XML_TAG,
    UNIREF_XSD_FILE,
)
from tests.cdm_data_loaders.conftest import check_settings
from tests.cdm_data_loaders.pipelines.uniref_cdm.conftest import (
    UNIREF_CDM_SETTINGS_CLASSES,
    UNIREF_CDM_VARIANTS,
)
from tests.helpers import assert_cli_field_roundtrips, settings_to_argv


@pytest.mark.parametrize("settings_cls", UNIREF_CDM_SETTINGS_CLASSES)
def test_uniref_settings_all_params_set(
    settings_cls: type[UniRefCdmSettings],
    test_uniref_cdm_settings: frozendict[str, Any],
    test_uniref_cdm_settings_reconciled: frozendict[str, Any],
) -> None:
    """Ensure that settings are set correctly when all args are specified."""
    s = settings_cls(**test_uniref_cdm_settings)
    check_settings(s, test_uniref_cdm_settings_reconciled)


@pytest.mark.parametrize(
    ("settings_cls", "_cli_obj", "expected_dataset_name", "expected_protocol"),
    UNIREF_CDM_VARIANTS,
)
def test_uniref_cdm_defaults_and_transform(
    settings_cls: type[UniRefCdmSettings],
    _cli_obj: object,
    expected_dataset_name: str,
    expected_protocol: str,
    test_defaults: dict[str, Any],
) -> None:
    """Ensure that defaults, locked ClassVars, and the bound transform/schema registries are configured."""
    s = settings_cls(**test_defaults)
    assert s.dataset_name == expected_dataset_name
    assert s.table_name == ENTRY_TABLE_NAME
    assert s.xml_tag == ENTRY_XML_TAG
    assert s.transforms_module is None
    assert s.output_schemas_module is None
    assert s.log_interval == UNIREF_LOG_INTERVAL
    assert s.xsd_file == UNIREF_XSD_FILE

    assert isinstance(s.table_transforms, TransformRegistry)
    transform = s.get_table_transform(ENTRY_TABLE_NAME)
    assert callable(transform)
    assert s.schema_map == UNIREF_CDM_SCHEMA_MAP

    routed = transform({"_id": "UniRef_TEST", "_updated": "2025-01-01", "name": "Cluster: Test"})
    assert routed["cluster"][0]["protocol"] == expected_protocol


@pytest.mark.parametrize("settings_cls", UNIREF_CDM_SETTINGS_CLASSES)
@pytest.mark.parametrize("field_name", list(UniRef50CdmSettings.model_fields))
def test_cli_fields_parse_correctly(
    settings_cls: type[UniRefCdmSettings],
    field_name: str,
    test_uniref_cdm_settings: frozendict[str, Any],
    test_uniref_cdm_settings_reconciled: frozendict[str, Any],
    test_defaults: dict[str, Any],
) -> None:
    """Ensure that all CLI fields round-trip on every UniRef CDM settings class."""
    assert_cli_field_roundtrips(
        settings_cls,
        field_name,
        test_uniref_cdm_settings[field_name],
        test_uniref_cdm_settings_reconciled[field_name],
        test_defaults,
    )


@pytest.mark.parametrize("settings_cls", UNIREF_CDM_SETTINGS_CLASSES)
@pytest.mark.parametrize(
    ("locked_flag", "locked_value"),
    [
        ("--dataset-name", "custom_dataset"),
        ("--table-name", "custom_table"),
        ("--xml-tag", "custom_tag"),
        ("--transforms-module", "some.module"),
        ("--output-schemas-module", "some.module"),
    ],
)
def test_locked_fields_rejected_on_cli(
    settings_cls: type[UniRefCdmSettings],
    locked_flag: str,
    locked_value: str,
    test_defaults: dict[str, Any],
) -> None:
    """Locked ClassVar fields must not be accepted as CLI flags."""
    base = settings_cls(**test_defaults)
    cli_args = [*settings_to_argv(base), locked_flag, locked_value]
    with pytest.raises(SettingsError, match=f"unrecognized arguments: {locked_flag}"):
        CliApp.run(settings_cls, cli_args=cli_args)
