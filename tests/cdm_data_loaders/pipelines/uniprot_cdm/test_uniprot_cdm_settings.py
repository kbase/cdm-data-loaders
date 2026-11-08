"""Tests for the UniProt CDM pipeline settings class."""

from typing import Any

import pytest
from frozendict import frozendict
from pydantic_settings import CliApp, SettingsError

from cdm_data_loaders.core.registry import TransformRegistry
from cdm_data_loaders.parsers.uniprot.uniprot_kb import UNIPROT_CDM_SCHEMA_MAP
from cdm_data_loaders.pipelines.uniprot_kb_cdm import (
    PIPELINE_NAME,
    UNIPROT_LOG_INTERVAL,
    UniProtSettings,
)
from cdm_data_loaders.pipelines.xmltopyarrow.settings import (
    ENTRY_TABLE_NAME,
    ENTRY_XML_TAG,
    UNIPROT_XSD_FILE,
)
from tests.cdm_data_loaders.conftest import check_settings
from tests.helpers import assert_cli_field_roundtrips, settings_to_argv


def test_uniprot_settings_all_params_set(
    test_uniprot_settings: frozendict[str, Any],
    test_uniprot_settings_reconciled: frozendict[str, Any],
) -> None:
    """Ensure that settings are set correctly when all args are specified."""
    s = UniProtSettings(**test_uniprot_settings)  # type: ignore[reportReturnType]
    check_settings(s, test_uniprot_settings_reconciled)


def test_uniprot_cdm_defaults_and_transform(
    test_defaults: dict[str, Any],
) -> None:
    """Ensure that defaults, locked ClassVars, and the bound transform/schema registries are configured."""
    s = UniProtSettings(**test_defaults)
    assert s.dataset_name == PIPELINE_NAME
    assert s.table_name == ENTRY_TABLE_NAME
    assert s.xml_tag == ENTRY_XML_TAG
    assert s.transforms_module is None
    assert s.output_schemas_module is None
    assert s.log_interval == UNIPROT_LOG_INTERVAL
    assert s.xsd_file == UNIPROT_XSD_FILE

    assert isinstance(s.table_transforms, TransformRegistry)
    transform = s.get_table_transform(ENTRY_TABLE_NAME)
    assert callable(transform)
    assert s.schema_map == UNIPROT_CDM_SCHEMA_MAP

    routed = transform({"_dataset": "Swiss-Prot", "accession": ["P12345"]})
    assert routed["entity"][0]["entity_id"] == "uniprot:P12345"


@pytest.mark.parametrize("field_name", list(UniProtSettings.model_fields))
def test_cli_fields_parse_correctly(
    field_name: str,
    test_uniprot_settings: frozendict[str, Any],
    test_uniprot_settings_reconciled: frozendict[str, Any],
    test_defaults: dict[str, Any],
) -> None:
    """Ensure that all fields and variants are parsed correctly."""
    assert_cli_field_roundtrips(
        UniProtSettings,
        field_name,
        test_uniprot_settings[field_name],
        test_uniprot_settings_reconciled[field_name],
        required=test_defaults,
    )


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
    locked_flag: str,
    locked_value: str,
    test_defaults: dict[str, Any],
) -> None:
    """Locked ClassVar fields must not be accepted as CLI flags."""
    base = UniProtSettings(**test_defaults)
    cli_args = [*settings_to_argv(base), locked_flag, locked_value]
    with pytest.raises(SettingsError, match=f"unrecognized arguments: {locked_flag}"):
        CliApp.run(UniProtSettings, cli_args=cli_args)
