"""Unit and integration tests for cdm_data_loaders.cli.polaris_schema_definitions."""

import json
from collections.abc import Generator
from pathlib import Path
from typing import Any, Final

import dlt
import pyarrow as pa
import pyiceberg.catalog as pyiceberg_catalog
import pytest
from pydantic import ValidationError
from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import LongType, NestedField, StringType
from pyiceberg.utils.config import Config

from cdm_data_loaders.cli.polaris_schema_definitions import (
    load_pyarrow_schema,
)
from cdm_data_loaders.cli.polaris_schema_definitions import (
    main as cli_main,
)
from cdm_data_loaders.utils.iceberg.catalog import PolarisCatalogConfig, connect_polaris_catalog
from cdm_data_loaders.utils.iceberg.cleanup import install_iceberg_catalog_cleanup
from cdm_data_loaders.utils.iceberg.schema_sync import (
    DefinitionSyncOptions,
    SchemaDefinitionsReport,
    apply_schema_definitions,
    apply_schema_definitions_to_table,
    extract_metadata_definition,
    plan_schema_definitions,
)
from tests.dlt_config_isolation import isolated_dlt_config

CATALOG_NAME: Final[str] = "polaristest"
NAMESPACE: Final[str] = "bio_ns"


class _IcebergDocCollector:
    """Collect ``{canonical_column_path: doc}`` across all ``NestedField`` nodes in an Iceberg ``Schema``."""

    def __init__(self, schema: Schema) -> None:
        self._schema = schema
        self.docs: dict[str, str | None] = {}

    def collect(self) -> dict[str, str | None]:
        for field_id in sorted(self._schema.field_ids):
            col_name = self._schema.find_column_name(field_id)
            field = self._schema.find_field(field_id)
            if col_name is not None and field.doc is not None:
                self.docs[col_name] = field.doc
        return self.docs


def collect_iceberg_docs(schema: Schema) -> dict[str, str | None]:
    """Return all non-None ``NestedField.doc`` entries keyed by canonical Iceberg column path."""
    return _IcebergDocCollector(schema).collect()


@pytest.fixture
def polaris_like_catalog(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Generator[Catalog]:
    """Provide an isolated PyIceberg catalog with namespace ``bio_ns`` configured via PyIceberg env vars."""
    db_path = tmp_path / "polaris_meta.db"
    warehouse_dir = tmp_path / "warehouse"
    warehouse_dir.mkdir()
    monkeypatch.setenv(f"PYICEBERG_CATALOG__{CATALOG_NAME.upper()}__TYPE", "sql")
    monkeypatch.setenv(f"PYICEBERG_CATALOG__{CATALOG_NAME.upper()}__URI", f"sqlite:///{db_path.as_posix()}")
    monkeypatch.setenv(f"PYICEBERG_CATALOG__{CATALOG_NAME.upper()}__WAREHOUSE", f"file://{warehouse_dir.as_posix()}")
    monkeypatch.setattr(pyiceberg_catalog, "_ENV_CONFIG", Config())

    with load_catalog(CATALOG_NAME) as catalog:
        catalog.create_namespace(NAMESPACE)
        yield catalog


DEEP_ARROW_SCHEMA: Final[pa.Schema] = pa.schema(
    [
        pa.field("recordId", pa.int64(), nullable=False, metadata={"comment": "Primary record ID"}),
        pa.field(
            "status_code",
            pa.dictionary(pa.int8(), pa.string()),
            nullable=True,
            metadata={"description": "Dictionary-encoded status"},
        ),
        pa.field(
            "organism",
            pa.struct(
                [
                    pa.field("taxonId", pa.int64(), nullable=False, metadata={"comment": "NCBI taxonomy ID"}),
                    pa.field(
                        "lineage",
                        pa.struct(
                            [
                                pa.field("kingdom", pa.string(), metadata={"doc": "Taxonomic kingdom"}),
                                pa.field(
                                    "ranks",
                                    pa.list_(pa.field("item", pa.string(), metadata={"comment": "Rank item slot"})),
                                    metadata={"comment": "Ordered rank names"},
                                ),
                            ]
                        ),
                        metadata={"comment": "Lineage details"},
                    ),
                ]
            ),
            nullable=False,
            metadata={"comment": "Source organism"},
        ),
        pa.field(
            "features",
            pa.list_(
                pa.field(
                    "item",
                    pa.struct(
                        [
                            pa.field("kind", pa.string(), nullable=False, metadata={"comment": "Feature type"}),
                            pa.field(
                                "qualifiers",
                                pa.map_(
                                    pa.field("key", pa.string(), nullable=False, metadata={"comment": "Qualifier key"}),
                                    pa.field(
                                        "value",
                                        pa.struct(
                                            [
                                                pa.field(
                                                    "score",
                                                    pa.float64(),
                                                    metadata={"comment": "Qualifier confidence"},
                                                ),
                                            ]
                                        ),
                                        metadata={"comment": "Qualifier value slot"},
                                    ),
                                ),
                                metadata={"comment": "Qualifier map"},
                            ),
                        ]
                    ),
                    metadata={"comment": "Feature element slot"},
                )
            ),
            metadata={"comment": "Sequence features"},
        ),
        pa.field(
            "matrix",
            pa.large_list(
                pa.field(
                    "outer_item",
                    pa.list_(
                        pa.field(
                            "inner_item",
                            pa.struct([pa.field("cell", pa.int64(), metadata={"comment": "Matrix cell value"})]),
                            metadata={"comment": "Inner list slot"},
                        )
                    ),
                    metadata={"comment": "Outer list slot"},
                )
            ),
            metadata={"comment": "2D struct matrix"},
        ),
    ],
    metadata={"comment": "Curated biological entries"},
)


@pytest.mark.parametrize(
    ("metadata", "keys", "expected"),
    [
        (None, ("doc", "comment"), None),
        ({}, ("doc", "comment"), None),
        ({b"xsd_type": b"xs:string"}, ("doc", "comment"), None),
        ({b"doc": b"   ", b"comment": b"Fallback comment"}, ("doc", "comment"), "Fallback comment"),
        ({b"comment": b"From comment", b"doc": b"From doc"}, ("doc", "comment"), "From doc"),
        ({b"comment": b"From comment", b"doc": b"From doc"}, (b"comment", b"doc"), "From comment"),
        ({b"description": b"From description"}, ("doc", "comment", "description"), "From description"),
    ],
    ids=[
        "none_meta",
        "empty_meta",
        "other_key_only",
        "blank_skipped",
        "doc_priority",
        "comment_priority",
        "description_fallback",
    ],
)
def test_extract_metadata_definition_cases(
    metadata: dict[bytes, bytes] | None,
    keys: tuple[bytes | str, ...],
    expected: str | None,
) -> None:
    """extract_metadata_definition respects key priority and skips missing or whitespace-only entries."""
    assert extract_metadata_definition(metadata, keys) == expected


@pytest.mark.parametrize(
    ("invalid_keys", "match"),
    [
        ((), "at least one key"),
        (("   ",), "must not be empty"),
        ("doc", "Input should be a valid tuple"),
    ],
    ids=["empty_sequence", "blank_key", "bare_string"],
)
def test_definition_sync_options_invalid_metadata_keys_raise(invalid_keys: object, match: str) -> None:
    """DefinitionSyncOptions rejects empty, blank, or non-sequence metadata_keys."""
    with pytest.raises(ValidationError, match=match):
        DefinitionSyncOptions.model_validate({"metadata_keys": invalid_keys})


@pytest.mark.parametrize(
    ("config", "expected_kwargs"),
    [
        (
            PolarisCatalogConfig(catalog="polaris"),
            {},
        ),
        (
            PolarisCatalogConfig(
                catalog="polaris",
                uri="http://polaris:8181/api/catalog",
                warehouse="tenant_refdata",
                credential="cid:secret",
                scope="PRINCIPAL_ROLE:ALL",
                properties={"header.X-Iceberg-Access-Delegation": "vended-credentials"},
            ),
            {
                "header.X-Iceberg-Access-Delegation": "vended-credentials",
                "uri": "http://polaris:8181/api/catalog",
                "warehouse": "tenant_refdata",
                "credential": "cid:secret",
                "scope": "PRINCIPAL_ROLE:ALL",
                "type": "rest",
            },
        ),
        (
            PolarisCatalogConfig.model_validate(
                {
                    "catalog": "custom",
                    "uri": "https://polaris.example.org/api/catalog",
                    "token": "tok-123",
                    "properties": {"type": "rest"},
                }
            ),
            {
                "type": "rest",
                "uri": "https://polaris.example.org/api/catalog",
                "token": "tok-123",
            },
        ),
    ],
    ids=["yaml_defaults_only", "http_rest_with_oauth", "https_with_bearer_token"],
)
def test_polaris_catalog_config_to_load_catalog_kwargs(
    config: PolarisCatalogConfig,
    expected_kwargs: dict[str, str],
) -> None:
    """PolarisCatalogConfig builds PyIceberg load_catalog overrides and infers type='rest' for HTTP(S) URIs."""
    assert config.to_load_catalog_kwargs() == expected_kwargs


def test_apply_schema_definitions_recursive_and_idempotent_on_dlt_loaded_table(
    tmp_path: Path,
    polaris_like_catalog: Catalog,
) -> None:
    """apply_schema_definitions populates table comment and nested docs after dlt Iceberg load, and is idempotent on re-run."""
    warehouse_dir = tmp_path / "warehouse"
    db_path = tmp_path / "polaris_meta.db"
    dlt_cfg: dict[str, Any] = {
        "destination": {"filesystem": {"bucket_url": warehouse_dir.as_posix()}},
        "iceberg_catalog": {
            "iceberg_catalog_name": CATALOG_NAME,
            "iceberg_catalog_type": "sql",
            "iceberg_catalog_config": {
                "type": "sql",
                "uri": f"sqlite:///{db_path.as_posix()}",
                "warehouse": f"file://{warehouse_dir.as_posix()}",
            },
        },
    }
    rows = [
        {
            "recordId": 1,
            "status_code": "active",
            "organism": {"taxonId": 9606, "lineage": {"kingdom": "Metazoa", "ranks": ["Eukaryota", "Chordata"]}},
            "features": [{"kind": "domain", "qualifiers": [("pfam", {"score": 0.99})]}],
            "matrix": [[{"cell": 42}]],
        }
    ]
    arrow_input = pa.Table.from_pylist(rows, schema=DEEP_ARROW_SCHEMA)

    install_iceberg_catalog_cleanup()
    with isolated_dlt_config(config=dlt_cfg):
        pipeline = dlt.pipeline(
            pipeline_name="polaris_defs_pipe",
            destination="filesystem",
            dataset_name=NAMESPACE,
            pipelines_dir=(tmp_path / "pipelines").as_posix(),
        )
        load_info = pipeline.run(
            dlt.resource(arrow_input, name="entries"),
            loader_file_format="parquet",
            table_format="iceberg",
        )
        assert not load_info.has_failed_jobs

    table_id = (NAMESPACE, "entries")
    before_table = polaris_like_catalog.load_table(table_id)
    # dlt drops pa.Schema.metadata and b"comment" field metadata when creating the Iceberg table
    assert "comment" not in before_table.properties
    # Only the single b"doc" field survived initial table creation
    assert collect_iceberg_docs(before_table.schema()) == {"organism.lineage.kingdom": "Taxonomic kingdom"}

    # Dry run computes full diff without mutating the catalog
    dry_report = apply_schema_definitions(
        polaris_like_catalog,
        table_id,
        DEEP_ARROW_SCHEMA,
        options=DefinitionSyncOptions(dry_run=True),
    )
    assert dry_report.dry_run is True
    assert dry_report.table_comment_applied == "Curated biological entries"
    assert dry_report.unchanged_fields == ("organism.lineage.kingdom",)
    assert "comment" not in polaris_like_catalog.load_table(table_id).properties

    # Real run commits table comment and all nested struct field docs (matching snake_case root record_id)
    report = apply_schema_definitions(polaris_like_catalog, table_id, DEEP_ARROW_SCHEMA)
    expected_updated = {
        "record_id": "Primary record ID",
        "status_code": "Dictionary-encoded status",
        "organism": "Source organism",
        "organism.taxonId": "NCBI taxonomy ID",
        "organism.lineage": "Lineage details",
        "organism.lineage.ranks": "Ordered rank names",
        "features": "Sequence features",
        "features.element.kind": "Feature type",
        "features.element.qualifiers": "Qualifier map",
        "features.element.qualifiers.value.score": "Qualifier confidence",
        "matrix": "2D struct matrix",
        "matrix.element.element.cell": "Matrix cell value",
    }
    assert report == SchemaDefinitionsReport(
        table_identifier=table_id,
        dry_run=False,
        table_comment_applied="Curated biological entries",
        table_comment_unchanged=False,
        updated_fields=expected_updated,
        unchanged_fields=("organism.lineage.kingdom",),
        unmatched_arrow_fields=(),
        skipped_non_struct_fields={
            "organism.lineage.ranks.item": "Rank item slot",
            "features.item": "Feature element slot",
            "features.item.qualifiers.key": "Qualifier key",
            "features.item.qualifiers.value": "Qualifier value slot",
            "matrix.outer_item": "Outer list slot",
            "matrix.outer_item.inner_item": "Inner list slot",
        },
    )

    refreshed = polaris_like_catalog.load_table(table_id)
    assert refreshed.properties["comment"] == "Curated biological entries"
    assert collect_iceberg_docs(refreshed.schema()) == {
        **expected_updated,
        "organism.lineage.kingdom": "Taxonomic kingdom",
    }

    # Re-running with identical definitions makes no further changes
    second_report = apply_schema_definitions(polaris_like_catalog, table_id, DEEP_ARROW_SCHEMA)
    assert second_report.table_comment_applied is None
    assert second_report.table_comment_unchanged is True
    assert second_report.updated_fields == {}
    assert set(second_report.unchanged_fields) == set(expected_updated) | {"organism.lineage.kingdom"}


def test_plan_and_strict_mode_handle_unmatched_fields_and_container_mismatches(
    polaris_like_catalog: Catalog,
) -> None:
    """Unmatched root/nested fields, ambiguous root normalizations, and flattened container columns are reported or rejected in strict mode."""
    iceberg_schema = Schema(
        NestedField(1, "foo_bar", StringType(), required=False),
        NestedField(2, "payload_struct", StringType(), required=False),
        NestedField(3, "payload_list", StringType(), required=False),
        NestedField(4, "payload_map", StringType(), required=False),
    )
    table = polaris_like_catalog.create_table((NAMESPACE, "mismatched"), schema=iceberg_schema)

    arrow_schema = pa.schema(
        [
            # Two distinct Arrow columns that both snake_case-normalize to 'foo_bar' -> ambiguous, neither guessed
            pa.field("fooBar", pa.string(), metadata={"comment": "Camel foo bar"}),
            pa.field("FooBar", pa.string(), metadata={"comment": "Pascal foo bar"}),
            # Container columns flattened to StringType in Iceberg
            pa.field(
                "payload_struct",
                pa.struct([pa.field("inner_a", pa.int64(), metadata={"comment": "Inner A"})]),
                metadata={"comment": "JSON serialized struct"},
            ),
            pa.field(
                "payload_list",
                pa.list_(
                    pa.field(
                        "item",
                        pa.list_(pa.field("inner_item", pa.struct([pa.field("inner_b", pa.int64())]))),
                    )
                ),
                metadata={"comment": "JSON serialized list"},
            ),
            pa.field(
                "payload_map",
                pa.map_(
                    pa.field(
                        "key",
                        pa.struct(
                            [
                                pa.field("k_id", pa.string(), metadata={"comment": "Key struct id"}),
                                pa.field(
                                    "k_tags",
                                    pa.list_(pa.field("item", pa.string(), metadata={"comment": "Key tag item"})),
                                ),
                                pa.field(
                                    "k_attrs",
                                    pa.map_(
                                        pa.field(
                                            "key", pa.string(), nullable=False, metadata={"comment": "Nested key"}
                                        ),
                                        pa.field("value", pa.string(), metadata={"comment": "Nested val"}),
                                    ),
                                ),
                            ]
                        ),
                        nullable=False,
                    ),
                    pa.field(
                        "value",
                        pa.map_(
                            pa.field("key", pa.string(), nullable=False),
                            pa.field("value", pa.struct([pa.field("inner_c", pa.int64())])),
                        ),
                    ),
                ),
                metadata={"comment": "JSON serialized map"},
            ),
            pa.field("extra_col", pa.int64(), metadata={"comment": "Not in Iceberg"}),
        ]
    )

    plan = plan_schema_definitions(
        arrow_schema,
        iceberg_schema,
        DefinitionSyncOptions(table_description="Explicit table override"),
    )
    assert plan.table_comment == "Explicit table override"
    assert plan.skipped_non_struct_fields == {
        "payload_map.key.k_id": "Key struct id",
        "payload_map.key.k_tags.item": "Key tag item",
        "payload_map.key.k_attrs.key": "Nested key",
        "payload_map.key.k_attrs.value": "Nested val",
    }
    assert plan.unmatched_arrow_fields == (
        "fooBar",
        "FooBar",
        "payload_struct.inner_a",
        "payload_list.item.inner_item.inner_b",
        "payload_map.value.value.inner_c",
        "extra_col",
    )

    with pytest.raises(ValueError, match="Unmatched PyArrow field"):
        apply_schema_definitions_to_table(table, arrow_schema, DefinitionSyncOptions(strict=True))

    non_strict_report = apply_schema_definitions_to_table(
        table,
        arrow_schema,
        DefinitionSyncOptions(table_description="Explicit table override", strict=False),
    )
    assert non_strict_report.table_comment_applied == "Explicit table override"
    assert non_strict_report.updated_fields == {
        "payload_struct": "JSON serialized struct",
        "payload_list": "JSON serialized list",
        "payload_map": "JSON serialized map",
    }
    assert non_strict_report.unmatched_arrow_fields == plan.unmatched_arrow_fields


def test_normalize_root_names_disabled_skips_snake_case_fallback() -> None:
    """Disabling normalize_root_names requires exact root column name matches."""
    iceberg_schema = Schema(NestedField(1, "record_id", LongType(), required=True))
    arrow_schema = pa.schema([pa.field("recordId", pa.int64(), nullable=False, metadata={"comment": "ID"})])

    plan = plan_schema_definitions(arrow_schema, iceberg_schema, DefinitionSyncOptions(normalize_root_names=False))
    assert plan.field_docs == {}
    assert plan.unmatched_arrow_fields == ("recordId",)


def test_load_pyarrow_schema_sources_and_errors(tmp_path: Path) -> None:
    """load_pyarrow_schema loads dotted modules, .py files, registries, mappings, and JSON Schemas, and rejects invalid inputs."""
    # 1. Dotted module + attribute
    id_mapping_schema = load_pyarrow_schema("cdm_data_loaders.schemas.uniprot.idmapping.schema:ID_MAPPING_SCHEMA")
    assert extract_metadata_definition(id_mapping_schema.metadata) == "Standardised mapping data from UniProt"

    # 2. Dotted module + ArrowSchemaRegistry + schema_key
    reg_schema = load_pyarrow_schema(
        "cdm_data_loaders.datasets.uniprot.idmapping:UNIPROT_IDMAPPING_OUTPUT_SCHEMAS",
        schema_key="id_mapping",
    )
    assert reg_schema.equals(id_mapping_schema, check_metadata=True)

    # 3. Local .py file with default SCHEMA and SCHEMAS mapping
    py_file = tmp_path / "custom_schemas.py"
    py_file.write_text(
        "import pyarrow as pa\n"
        "SCHEMA = pa.schema([pa.field('id', pa.int64(), metadata={'comment': 'File ID'})], metadata={'comment': 'File table'})\n"
        "SCHEMAS = {'alpha': SCHEMA}\n"
        "BAD_MAP = {'alpha': 123}\n"
        "NOT_A_SCHEMA = 42\n",
        encoding="utf-8",
    )
    loaded_default = load_pyarrow_schema(str(py_file))
    assert extract_metadata_definition(loaded_default.metadata) == "File table"
    loaded_keyed = load_pyarrow_schema(str(py_file), schema_key="alpha")
    assert loaded_keyed.equals(loaded_default, check_metadata=True)

    # 4. JSON Schema file with top-level description and property descriptions
    json_file = tmp_path / "sample.schema.json"
    json_file.write_text(
        json.dumps(
            {
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": "object",
                "description": "JSON Schema table description",
                "properties": {
                    "gene_id": {"type": "string", "description": "Gene identifier"},
                },
                "required": ["gene_id"],
            }
        ),
        encoding="utf-8",
    )
    loaded_json = load_pyarrow_schema(str(json_file))
    assert extract_metadata_definition(loaded_json.metadata) == "JSON Schema table description"
    assert extract_metadata_definition(loaded_json.field("gene_id").metadata) == "Gene identifier"

    # Error cases
    bad_json = tmp_path / "list_root.json"
    bad_json.write_text("[1, 2, 3]", encoding="utf-8")
    empty_mod = tmp_path / "empty_mod.py"
    empty_mod.write_text("X = 1\n", encoding="utf-8")

    with pytest.raises(ValueError, match="has no attribute"):
        load_pyarrow_schema(f"{py_file}:MISSING_ATTR")
    with pytest.raises(ValueError, match=r"already a single pyarrow\.Schema"):
        load_pyarrow_schema(f"{py_file}:SCHEMA", schema_key="alpha")
    with pytest.raises(TypeError, match=r"must be a pyarrow\.Schema"):
        load_pyarrow_schema(f"{py_file}:NOT_A_SCHEMA")
    with pytest.raises(ValueError, match="pass schema_key"):
        load_pyarrow_schema(f"{py_file}:SCHEMAS")
    with pytest.raises(ValueError, match="not found"):
        load_pyarrow_schema(f"{py_file}:SCHEMAS", schema_key="missing")
    with pytest.raises(TypeError, match=r"must be a pyarrow\.Schema"):
        load_pyarrow_schema(f"{py_file}:BAD_MAP", schema_key="alpha")
    with pytest.raises(ValueError, match="neither 'OUTPUT_SCHEMAS' nor 'SCHEMAS'"):
        load_pyarrow_schema(str(empty_mod), schema_key="alpha")
    with pytest.raises(ValueError, match="no default 'SCHEMA'"):
        load_pyarrow_schema(str(empty_mod))
    with pytest.raises(ValueError, match="not supported when loading JSON Schema"):
        load_pyarrow_schema(str(json_file), schema_key="alpha")
    with pytest.raises(TypeError, match="JSON object at root"):
        load_pyarrow_schema(str(bad_json))
    with pytest.raises(FileNotFoundError):
        load_pyarrow_schema(r"C:\nonexistent\windows_schema.py")


def test_cli_main_applies_definitions_and_prints_json_report(
    polaris_like_catalog: Catalog,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """CLI main() connects via connect_polaris_catalog, applies definitions, and prints the JSON report."""
    polaris_like_catalog.create_table(
        (NAMESPACE, "id_mapping"),
        schema=Schema(
            NestedField(1, "uniprot_id", StringType(), required=True),
            NestedField(2, "db_reference", StringType(), required=True),
            NestedField(3, "source", StringType(), required=True),
        ),
    )

    exit_code = cli_main(
        [
            "--catalog",
            CATALOG_NAME,
            "--table",
            f"{NAMESPACE}.id_mapping",
            "--schema-source",
            "cdm_data_loaders.datasets.uniprot.idmapping:UNIPROT_IDMAPPING_OUTPUT_SCHEMAS",
            "--schema-key",
            "id_mapping",
        ]
    )
    assert exit_code == 0

    captured = capsys.readouterr()
    parsed_stdout = json.loads(captured.out[captured.out.index("{\n") :])
    assert parsed_stdout["table_comment_applied"] == "Standardised mapping data from UniProt"
    assert parsed_stdout["updated_fields"] == {
        "uniprot_id": "Standardised identifier for the protein in the UniProt database",
        "db_reference": "Database cross reference for the mapped entity",
        "source": "Mapping source",
    }

    with connect_polaris_catalog(PolarisCatalogConfig(catalog=CATALOG_NAME)) as verify_cat:
        updated_tbl = verify_cat.load_table((NAMESPACE, "id_mapping"))
        assert updated_tbl.properties["comment"] == "Standardised mapping data from UniProt"
        assert collect_iceberg_docs(updated_tbl.schema()) == parsed_stdout["updated_fields"]
