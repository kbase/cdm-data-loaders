"""Unit tests for UniProt ID mapping schemas, table transforms, and CLI entrypoint wiring."""

import importlib
import sys

import jsonschema.validators
import pyarrow as pa
import pytest

from cdm_data_loaders.converters.facades.jsonschema_to_pyarrow import (
    JSONSchemaToPyArrow,
    convert_xsv_schema_to_pyarrow,
)
from cdm_data_loaders.core.registry import TransformContext
from cdm_data_loaders.core.transforms import emit_raw_table
from cdm_data_loaders.parsers.uniprot.idmapping import (
    OUTPUT_SCHEMAS,
    TABLE_TRANSFORMS,
    transform_uniprot_id_mapping,
)
from cdm_data_loaders.pipelines.core import PipelineCli
from cdm_data_loaders.pipelines.uniprot_idmapping import (
    PIPELINE_NAME,
    UniProtIdMappingSettings,
    cli,
)
from cdm_data_loaders.pipelines.xsv.pipeline import run_xsv_pipeline
from cdm_data_loaders.readers.jsonschema_xsv.xsv_validator.schema_utils import validate_jsonschema
from cdm_data_loaders.schemas.jsonschema import CUSTOM_META_SCHEMA_URI, get_xsv_meta_schema
from cdm_data_loaders.schemas.uniprot.idmapping.schema import (
    DB_REFERENCE_DESCRIPTION,
    ID_MAPPING_DESCRIPTION,
    ID_MAPPING_SCHEMA,
    ID_MAPPING_TABLE_NAME,
    IDMAPPING_SCHEMA_FILE,
    RAW_ID_MAPPING_DESCRIPTION,
    RAW_ID_MAPPING_SCHEMA,
    SOURCE_DESCRIPTION,
    UNIPROT_ID_DESCRIPTION,
    UNIPROT_IDMAPPING_OUTPUT_SCHEMAS,
)
from tests.cdm_data_loaders.pipelines.uniprot_idmapping.conftest import SAMPLE_RAW_ROWS


def test_uniprot_idmapping_schemas_match_jsonschema_and_descriptions() -> None:
    """RAW_ID_MAPPING_SCHEMA and ID_MAPPING_SCHEMA carry non-nullable string fields and expected descriptions."""
    derived = convert_xsv_schema_to_pyarrow(validate_jsonschema(IDMAPPING_SCHEMA_FILE)).with_metadata(
        {"comment": RAW_ID_MAPPING_DESCRIPTION}
    )
    from_file = (
        JSONSchemaToPyArrow()
        .convert_from_file(str(IDMAPPING_SCHEMA_FILE))
        .with_metadata({"comment": RAW_ID_MAPPING_DESCRIPTION})
    )
    assert RAW_ID_MAPPING_SCHEMA.equals(derived, check_metadata=True)
    assert RAW_ID_MAPPING_SCHEMA.equals(from_file, check_metadata=True)
    assert all(not field.nullable and field.type == pa.string() for field in RAW_ID_MAPPING_SCHEMA)

    assert ID_MAPPING_SCHEMA.names == ["uniprot_id", "db_reference", "source"]
    assert all(not field.nullable and field.type == pa.string() for field in ID_MAPPING_SCHEMA)
    assert ID_MAPPING_SCHEMA.metadata == {b"comment": ID_MAPPING_DESCRIPTION.encode("utf-8")}
    assert ID_MAPPING_SCHEMA.field("uniprot_id").metadata == {b"comment": UNIPROT_ID_DESCRIPTION.encode("utf-8")}
    assert ID_MAPPING_SCHEMA.field("db_reference").metadata == {b"comment": DB_REFERENCE_DESCRIPTION.encode("utf-8")}
    assert ID_MAPPING_SCHEMA.field("source").metadata == {b"comment": SOURCE_DESCRIPTION.encode("utf-8")}

    expected_transforms = {ID_MAPPING_TABLE_NAME: (emit_raw_table, transform_uniprot_id_mapping)}
    expected_schemas = dict(UNIPROT_IDMAPPING_OUTPUT_SCHEMAS.registry)
    assert expected_transforms == TABLE_TRANSFORMS
    assert expected_schemas == OUTPUT_SCHEMAS


def test_import_uniprot_idmapping_modules_does_not_mutate_jsonschema_registry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Importing UniProt ID mapping schema, parser, and pipeline modules leaves XSV metaschema state untouched."""
    target_modules = (
        "cdm_data_loaders.schemas.uniprot.idmapping.schema",
        "cdm_data_loaders.parsers.uniprot.idmapping",
        "cdm_data_loaders.pipelines.uniprot_idmapping",
    )
    for mod_name in target_modules:
        parent_name, _, child_attr = mod_name.rpartition(".")
        parent_mod = sys.modules.get(parent_name)
        if parent_mod is not None and hasattr(parent_mod, child_attr):
            monkeypatch.delattr(parent_mod, child_attr)
        monkeypatch.delitem(sys.modules, mod_name, raising=False)

    assert get_xsv_meta_schema.cache_info().currsize == 0
    assert CUSTOM_META_SCHEMA_URI not in vars(jsonschema.validators)["_VALIDATORS"]
    assert CUSTOM_META_SCHEMA_URI not in vars(jsonschema.validators)["_META_SCHEMAS"]

    importlib.import_module("cdm_data_loaders.pipelines.uniprot_idmapping")

    assert get_xsv_meta_schema.cache_info().currsize == 0
    assert CUSTOM_META_SCHEMA_URI not in vars(jsonschema.validators)["_VALIDATORS"]
    assert CUSTOM_META_SCHEMA_URI not in vars(jsonschema.validators)["_META_SCHEMAS"]


def test_cli_wires_settings_and_runner() -> None:
    """Ensure cli is a PipelineCli wired to UniProtIdMappingSettings and run_xsv_pipeline."""
    assert isinstance(cli, PipelineCli)
    assert cli.settings_cls is UniProtIdMappingSettings
    assert cli.pipeline_fn is run_xsv_pipeline
    assert UniProtIdMappingSettings.dataset_name == PIPELINE_NAME


@pytest.mark.parametrize(
    ("table", "ctx", "expected_rows"),
    [
        pytest.param(
            pa.Table.from_pylist([dict(row) for row in SAMPLE_RAW_ROWS], schema=RAW_ID_MAPPING_SCHEMA),
            TransformContext.for_source(ID_MAPPING_TABLE_NAME, "sub/HUMAN_9606_idmapping.dat.gz"),
            [
                {
                    "uniprot_id": "uniprot:P12345",
                    "db_reference": "NCBI_TaxID:9606",
                    "source": "HUMAN_9606_idmapping.dat.gz",
                },
                {
                    "uniprot_id": "uniprot:Q8N158",
                    "db_reference": "GeneID:284697",
                    "source": "HUMAN_9606_idmapping.dat.gz",
                },
            ],
            id="arrow_table_with_nested_gz_source",
        ),
        pytest.param(
            RAW_ID_MAPPING_SCHEMA.empty_table(),
            TransformContext.for_source(ID_MAPPING_TABLE_NAME, "idmapping.dat"),
            [],
            id="empty_arrow_table_returns_empty_mapping",
        ),
        pytest.param(
            pa.Table.from_pylist([dict(row) for row in SAMPLE_RAW_ROWS], schema=RAW_ID_MAPPING_SCHEMA),
            TransformContext(table_name=ID_MAPPING_TABLE_NAME, source_file="dir/idmapping.dat", file_name=None),
            [
                {"uniprot_id": "uniprot:P12345", "db_reference": "NCBI_TaxID:9606", "source": "idmapping.dat"},
                {"uniprot_id": "uniprot:Q8N158", "db_reference": "GeneID:284697", "source": "idmapping.dat"},
            ],
            id="derives_basename_from_source_file_when_file_name_none",
        ),
        pytest.param(
            pa.Table.from_pylist([dict(SAMPLE_RAW_ROWS[0])], schema=RAW_ID_MAPPING_SCHEMA),
            TransformContext(table_name=ID_MAPPING_TABLE_NAME),
            [{"uniprot_id": "uniprot:P12345", "db_reference": "NCBI_TaxID:9606", "source": ""}],
            id="missing_source_file_uses_empty_source",
        ),
    ],
)
def test_transform_uniprot_id_mapping_transforms_pyarrow_tables(
    table: pa.Table,
    ctx: TransformContext,
    expected_rows: list[dict[str, str]],
) -> None:
    """transform_uniprot_id_mapping builds standardised id_mapping PyArrow tables and drops empty batches."""
    routed = transform_uniprot_id_mapping(table, ctx)
    if not expected_rows:
        assert routed == {}
        return
    assert set(routed) == {ID_MAPPING_TABLE_NAME}
    out_table = routed[ID_MAPPING_TABLE_NAME]
    assert out_table.schema.equals(ID_MAPPING_SCHEMA, check_metadata=True)
    assert out_table.to_pylist() == expected_rows
