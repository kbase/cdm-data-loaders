"""Unit tests for the UniProt ID mapping pipeline settings class."""

import json
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest
from frozendict import frozendict
from pydantic import ValidationError
from pydantic_settings import CliApp, SettingsError

from cdm_data_loaders.core.transforms import accepts_pyarrow_table, emit_raw_table
from cdm_data_loaders.parsers.uniprot.idmapping import (
    UNIPROT_IDMAPPING_OUTPUT_SCHEMAS,
    transform_uniprot_id_mapping,
)
from cdm_data_loaders.pipelines.uniprot_idmapping import (
    DEFAULT_IDMAPPING_FILE_GLOB,
    DEFAULT_PARTITION_BUCKETS,
    PIPELINE_NAME,
    UniProtIdMappingSettings,
)
from cdm_data_loaders.schemas.uniprot.idmapping.schema import (
    ID_MAPPING_TABLE_NAME,
    IDMAPPING_SCHEMA_FILE,
    RAW_ID_MAPPING_DESCRIPTION,
    RAW_ID_MAPPING_SCHEMA,
    RAW_ID_MAPPING_TABLE_NAME,
)
from tests.cdm_data_loaders.conftest import check_settings
from tests.conftest import LOCAL_FS
from tests.helpers import assert_cli_field_roundtrips, make_cts_settings, settings_to_argv


def test_uniprot_idmapping_settings_all_params_set(
    test_uniprot_idmapping_settings: frozendict[str, Any],
    test_uniprot_idmapping_settings_reconciled: frozendict[str, Any],
) -> None:
    """UniProtIdMappingSettings reconciles all explicit field values accurately."""
    s = UniProtIdMappingSettings(**test_uniprot_idmapping_settings)
    check_settings(s, test_uniprot_idmapping_settings_reconciled)


def test_uniprot_idmapping_settings_defaults_schema_override_and_validation(tmp_path: Path) -> None:
    """UniProtIdMappingSettings sets preset defaults, binds Arrow transforms, derives custom schema_file schemas, and validates partition_buckets."""
    input_dir = tmp_path / "in"
    input_dir.mkdir()
    settings = make_cts_settings(
        UniProtIdMappingSettings,
        tmp_path,
        input_dir=str(input_dir),
        use_destination=LOCAL_FS,
    )
    assert settings.dataset_name == PIPELINE_NAME
    assert settings.table_name == ID_MAPPING_TABLE_NAME
    assert settings.file_glob == DEFAULT_IDMAPPING_FILE_GLOB
    assert settings.schema_file == IDMAPPING_SCHEMA_FILE
    assert settings.partition_buckets == DEFAULT_PARTITION_BUCKETS
    assert settings.pyarrow_schema.equals(RAW_ID_MAPPING_SCHEMA, check_metadata=True)
    assert settings.schema_map == UNIPROT_IDMAPPING_OUTPUT_SCHEMAS
    assert settings.get_table_transforms(ID_MAPPING_TABLE_NAME) == (emit_raw_table, transform_uniprot_id_mapping)
    assert accepts_pyarrow_table(settings.get_table_transform(ID_MAPPING_TABLE_NAME)) is True

    hints_by_table = {h.table_name: h for h in settings.get_output_table_hints()}
    assert set(hints_by_table) == {RAW_ID_MAPPING_TABLE_NAME, ID_MAPPING_TABLE_NAME}
    for hint in hints_by_table.values():
        assert len(hint.iceberg_partitions) == 1
        assert hint.iceberg_partitions[0].to_dict() == {
            "transform": "bucket",
            "source_column": "uniprot_id",
            "param_value": DEFAULT_PARTITION_BUCKETS,
        }

    # custom --schema-file dynamically regenerates raw_id_mapping's PyArrow schema via convert_xsv_schema_to_pyarrow
    custom_schema_doc = json.loads(IDMAPPING_SCHEMA_FILE.read_text(encoding="utf-8"))
    custom_schema_doc["properties"]["extra_col"] = {"type": ["integer", "null"], "description": "Optional rank"}
    custom_schema_file = tmp_path / "custom_idmapping.schema.json"
    custom_schema_file.write_text(json.dumps(custom_schema_doc), encoding="utf-8")
    custom_settings = make_cts_settings(
        UniProtIdMappingSettings,
        tmp_path,
        input_dir=str(input_dir),
        use_destination=LOCAL_FS,
        schema_file=str(custom_schema_file),
    )
    assert custom_settings.schema_map is not None
    raw_override_schema = custom_settings.schema_map.registry[RAW_ID_MAPPING_TABLE_NAME]
    assert "extra_col" in raw_override_schema.names
    assert raw_override_schema.field("extra_col").type == pa.int64()
    assert raw_override_schema.metadata == {b"comment": RAW_ID_MAPPING_DESCRIPTION.encode("utf-8")}

    custom_buckets = 16
    jsonl_settings = make_cts_settings(
        UniProtIdMappingSettings,
        tmp_path,
        input_dir=str(input_dir),
        use_destination=LOCAL_FS,
        loader_file_format="jsonl",
        partition_buckets=custom_buckets,
    )
    assert jsonl_settings.schema_map is None
    assert jsonl_settings.partition_buckets == custom_buckets

    with pytest.raises(ValidationError):
        make_cts_settings(
            UniProtIdMappingSettings,
            tmp_path,
            input_dir=str(input_dir),
            use_destination=LOCAL_FS,
            partition_buckets=0,
        )


@pytest.mark.parametrize(
    "field_name",
    list(UniProtIdMappingSettings.model_fields),
    ids=list(UniProtIdMappingSettings.model_fields),
)
def test_cli_fields_parse_correctly(
    field_name: str,
    test_uniprot_idmapping_settings: frozendict[str, Any],
    test_uniprot_idmapping_settings_reconciled: frozendict[str, Any],
    test_defaults: dict[str, Any],
) -> None:
    """All CLI flags and shortcuts on UniProtIdMappingSettings round-trip cleanly."""
    assert_cli_field_roundtrips(
        UniProtIdMappingSettings,
        field_name,
        test_uniprot_idmapping_settings[field_name],
        test_uniprot_idmapping_settings_reconciled[field_name],
        required=test_defaults,
    )


@pytest.mark.parametrize(
    ("locked_flag", "locked_value"),
    [
        pytest.param("--dataset-name", "custom_dataset", id="dataset_name"),
        pytest.param("--table-name", "custom_table", id="table_name"),
        pytest.param("--transforms-module", "some.module", id="transforms_module"),
        pytest.param("--output-schemas-module", "some.module", id="output_schemas_module"),
    ],
)
def test_locked_fields_rejected_on_cli(
    locked_flag: str,
    locked_value: str,
    test_defaults: dict[str, Any],
) -> None:
    """Locked ClassVar fields on UniProtIdMappingSettings are rejected when passed on the CLI."""
    base = UniProtIdMappingSettings(**test_defaults)
    cli_args = [*settings_to_argv(base), locked_flag, locked_value]
    with pytest.raises(SettingsError, match=f"unrecognized arguments: {locked_flag}"):
        CliApp.run(UniProtIdMappingSettings, cli_args=cli_args)
