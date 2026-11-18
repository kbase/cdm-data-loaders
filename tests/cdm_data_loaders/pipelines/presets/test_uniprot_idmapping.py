"""Unit tests for ``cdm_data_loaders.pipelines.presets.uniprot_idmapping``."""

import json
from pathlib import Path
from typing import Any, Final

import pyarrow as pa
import pytest
from frozendict import frozendict
from pydantic import ValidationError
from pydantic_settings import CliApp, SettingsError

from cdm_data_loaders.core.cli import CliRunner
from cdm_data_loaders.core.transforms.introspection import accepts_pyarrow_table
from cdm_data_loaders.core.transforms.routing import emit_raw_table
from cdm_data_loaders.datasets.uniprot.idmapping import (
    UNIPROT_IDMAPPING_OUTPUT_SCHEMAS,
    transform_uniprot_id_mapping,
)
from cdm_data_loaders.pipelines.presets.uniprot_idmapping import (
    DEFAULT_IDMAPPING_FILE_GLOB,
    DEFAULT_PARTITION_BUCKETS,
    PIPELINE_NAME,
    UniProtIdMappingSettings,
    cli,
)
from cdm_data_loaders.pipelines.xsv.pipeline import run_xsv_pipeline
from cdm_data_loaders.schemas.uniprot.idmapping.schema import (
    ID_MAPPING_TABLE_NAME,
    IDMAPPING_SCHEMA_FILE,
    RAW_ID_MAPPING_DESCRIPTION,
    RAW_ID_MAPPING_SCHEMA,
    RAW_ID_MAPPING_TABLE_NAME,
)
from tests.helpers.cli_assertions import (
    assert_cli_field_roundtrips,
    check_settings,
    make_cts_settings,
    settings_to_argv,
)
from tests.helpers.paths_and_constants import LOCAL_FS


def test_cli_wires_settings_and_runner() -> None:
    """``cli`` is a ``CliRunner`` wired to ``UniProtIdMappingSettings`` and ``run_xsv_pipeline``."""
    assert isinstance(cli, CliRunner)
    assert cli.settings_cls is UniProtIdMappingSettings
    assert cli.pipeline_fn is run_xsv_pipeline
    assert UniProtIdMappingSettings.dataset_name == PIPELINE_NAME


def test_uniprot_idmapping_settings_all_params_set(
    test_uniprot_idmapping_settings: frozendict[str, Any],
    test_uniprot_idmapping_settings_reconciled: frozendict[str, Any],
) -> None:
    """``UniProtIdMappingSettings`` reconciles all explicit field values."""
    settings = UniProtIdMappingSettings(**test_uniprot_idmapping_settings)
    check_settings(settings, test_uniprot_idmapping_settings_reconciled)


CUSTOM_PARTITION_BUCKETS: Final[int] = 16


@pytest.mark.parametrize(
    ("scenario", "expected_buckets"),
    [
        pytest.param("preset_defaults", DEFAULT_PARTITION_BUCKETS, id="preset-defaults"),
        pytest.param("custom_schema_file", DEFAULT_PARTITION_BUCKETS, id="custom-schema-file"),
        pytest.param(
            "jsonl_custom_buckets",
            CUSTOM_PARTITION_BUCKETS,
            id="jsonl-custom-partition-buckets",
        ),
    ],
)
def test_uniprot_idmapping_settings_defaults_schema_override_and_validation(
    tmp_path: Path,
    scenario: str,
    expected_buckets: int,
) -> None:
    """``UniProtIdMappingSettings`` sets preset defaults, binds Arrow transforms, and derives custom schemas."""
    input_dir = tmp_path / "in"
    input_dir.mkdir()

    overrides: dict[str, object] = {}
    custom_schema_file = tmp_path / "custom_idmapping.schema.json"
    if scenario == "custom_schema_file":
        custom_schema_doc = json.loads(IDMAPPING_SCHEMA_FILE.read_text(encoding="utf-8"))
        custom_schema_doc["properties"]["extra_col"] = {"type": ["integer", "null"], "description": "Optional rank"}
        custom_schema_file.write_text(json.dumps(custom_schema_doc), encoding="utf-8")
        overrides["schema_file"] = str(custom_schema_file)
    elif scenario == "jsonl_custom_buckets":
        overrides["loader_file_format"] = "jsonl"
        overrides["partition_buckets"] = CUSTOM_PARTITION_BUCKETS

    settings = make_cts_settings(
        UniProtIdMappingSettings,
        tmp_path,
        input_dir=str(input_dir),
        use_destination=LOCAL_FS,
        **overrides,
    )
    assert settings.dataset_name == PIPELINE_NAME
    assert settings.table_name == ID_MAPPING_TABLE_NAME
    assert settings.file_glob == DEFAULT_IDMAPPING_FILE_GLOB
    assert settings.partition_buckets == expected_buckets
    assert settings.get_table_transforms(ID_MAPPING_TABLE_NAME) == (emit_raw_table, transform_uniprot_id_mapping)
    assert accepts_pyarrow_table(settings.get_table_transform(ID_MAPPING_TABLE_NAME)) is True

    hints_by_table = {h.table_name: h for h in settings.get_output_table_hints()}
    assert set(hints_by_table) == {RAW_ID_MAPPING_TABLE_NAME, ID_MAPPING_TABLE_NAME}
    for hint in hints_by_table.values():
        assert len(hint.iceberg_partitions) == 1
        assert hint.iceberg_partitions[0].to_dict() == {
            "transform": "bucket",
            "source_column": "uniprot_id",
            "param_value": expected_buckets,
        }

    if scenario == "preset_defaults":
        assert settings.schema_file == IDMAPPING_SCHEMA_FILE
        assert settings.pyarrow_schema.equals(RAW_ID_MAPPING_SCHEMA, check_metadata=True)
        assert settings.schema_map == UNIPROT_IDMAPPING_OUTPUT_SCHEMAS
    elif scenario == "custom_schema_file":
        assert settings.schema_file == custom_schema_file
        assert settings.schema_map is not None
        raw_override_schema = settings.schema_map.registry[RAW_ID_MAPPING_TABLE_NAME]
        assert "extra_col" in raw_override_schema.names
        assert raw_override_schema.field("extra_col").type == pa.int64()
        assert raw_override_schema.metadata == {b"comment": RAW_ID_MAPPING_DESCRIPTION.encode("utf-8")}
    else:
        assert settings.schema_map is None


def test_uniprot_idmapping_settings_fail_zero_partition_buckets(tmp_path: Path) -> None:
    """``UniProtIdMappingSettings`` rejects ``partition_buckets=0`` with ``ValidationError``."""
    input_dir = tmp_path / "in"
    input_dir.mkdir()
    with pytest.raises(ValidationError, match="partition_buckets"):
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
    """All CLI flags and shortcuts on ``UniProtIdMappingSettings`` round-trip cleanly."""
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
    """Locked ``ClassVar`` fields on ``UniProtIdMappingSettings`` are rejected when passed on the CLI."""
    base = UniProtIdMappingSettings(**test_defaults)
    cli_args = [*settings_to_argv(base), locked_flag, locked_value]
    with pytest.raises(SettingsError, match=f"unrecognized arguments: {locked_flag}"):
        CliApp.run(UniProtIdMappingSettings, cli_args=cli_args)
