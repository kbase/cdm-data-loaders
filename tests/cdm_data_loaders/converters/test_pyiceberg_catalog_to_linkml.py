"""End-to-end tests for pyiceberg_to_linkml.

Chains a real pyiceberg SQL catalog (file-backed SQLite) through
dump_catalog_schemas and validates the emitted documents.
"""

import json
import logging
from datetime import UTC, datetime
from pathlib import Path

import pytest
from linkml.linter.linter import Linter
from linkml_runtime.linkml_model.meta import ClassDefinition, SchemaDefinition, SlotDefinition
from linkml_runtime.loaders import yaml_loader
from pyiceberg import catalog as pyiceberg_catalog
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    DateType,
    DecimalType,
    DoubleType,
    IntegerType,
    ListType,
    LongType,
    MapType,
    NestedField,
    StringType,
    StructType,
    TimestamptzType,
)
from pyiceberg.utils.config import Config

from cdm_data_loaders.converters.pyiceberg_catalog_to_linkml import (
    IcebergToLinkMLSettings,
    dump_catalog_schemas,
)

CATALOG_NAME = "testcatlinkml"


@pytest.fixture
def catalog_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Point the testcat catalog at a file-backed SQLite DB under tmp_path."""
    db_path = tmp_path / "catalog_linkml.db"
    warehouse = tmp_path / "warehouse_linkml"
    warehouse.mkdir()
    monkeypatch.setenv(f"PYICEBERG_CATALOG__{CATALOG_NAME.upper()}__TYPE", "sql")
    monkeypatch.setenv(f"PYICEBERG_CATALOG__{CATALOG_NAME.upper()}__URI", f"sqlite:///{db_path}")
    monkeypatch.setenv(f"PYICEBERG_CATALOG__{CATALOG_NAME.upper()}__WAREHOUSE", f"file://{warehouse}")
    monkeypatch.setattr(pyiceberg_catalog, "_ENV_CONFIG", Config())
    return tmp_path


@pytest.fixture
def wide_schema() -> Schema:
    """A schema exercising scalars, nested structs, lists, and maps."""
    return Schema(
        NestedField(1, "id", LongType(), required=True, doc="primary key"),
        NestedField(2, "name", StringType(), required=True),
        NestedField(3, "count", IntegerType(), required=False),
        NestedField(4, "ratio", DoubleType(), required=False),
        NestedField(5, "as_of", TimestamptzType(), required=False),
        NestedField(6, "born_on", DateType(), required=False),
        NestedField(7, "rate", DecimalType(12, 4), required=False),
        NestedField(8, "tags", ListType(element_id=9, element=StringType(), element_required=False), required=False),
        NestedField(
            10,
            "attrs",
            MapType(key_id=11, key_type=StringType(), value_id=12, value_type=DoubleType(), value_required=False),
            required=False,
        ),
        NestedField(
            13,
            "address",
            StructType(
                NestedField(14, "street", StringType(), required=True),
                NestedField(15, "city", StringType(), required=False),
            ),
            required=False,
        ),
    )


def test_dump_catalog_schemas_pass_yaml_models(catalog_env: Path, simple_schema: Schema, wide_schema: Schema) -> None:
    """Write YAML schemas with valid LinkML models and a shared timezone-aware timestamp."""
    catalog = load_catalog(CATALOG_NAME)
    catalog.create_namespace("ns")
    catalog.create_table(("ns", "simple"), schema=simple_schema)
    catalog.create_table(("ns", "wide"), schema=wide_schema)

    out_dir = catalog_env / "schemas"
    settings = IcebergToLinkMLSettings(catalog=CATALOG_NAME, output_dir=str(out_dir))  # pyright: ignore[reportCallIssue]
    dump_catalog_schemas(settings)

    files = sorted(out_dir.iterdir())
    assert [f.name for f in files] == ["ns.simple.schema.yaml", "ns.wide.schema.yaml"]

    simple_doc = yaml_loader.load(str(files[0]), target_class=SchemaDefinition)
    assert simple_doc.name == "simple"
    assert simple_doc.id == "urn:iceberg:ns.simple"
    assert simple_doc.classes == {
        "simple": ClassDefinition(
            name="simple",
            attributes={
                "id": SlotDefinition(name="id", range="integer", required=True, description="primary key"),
                "name": SlotDefinition(name="name", range="string", description="display name"),
            },
        )
    }

    wide_doc = yaml_loader.load(str(files[1]), target_class=SchemaDefinition)
    assert set(wide_doc.classes) == {"wide", "wide_attrs_map", "wide_address"}
    assert wide_doc.classes["wide"].attributes == {
        "id": SlotDefinition(name="id", range="integer", required=True, description="primary key"),
        "name": SlotDefinition(name="name", range="string", required=True),
        "count": SlotDefinition(name="count", range="integer"),
        "ratio": SlotDefinition(name="ratio", range="float"),
        "as_of": SlotDefinition(name="as_of", range="datetime"),
        "born_on": SlotDefinition(name="born_on", range="date"),
        "rate": SlotDefinition(name="rate", range="decimal"),
        "tags": SlotDefinition(name="tags", range="string", multivalued=True),
        "attrs": SlotDefinition(name="attrs", range="wide_attrs_map", multivalued=True),
        "address": SlotDefinition(name="address", range="wide_address"),
    }
    assert wide_doc.classes["wide_attrs_map"].attributes == {
        "key": SlotDefinition(name="key", range="string", required=True),
        "value": SlotDefinition(name="value", range="float"),
    }
    assert wide_doc.classes["wide_address"].attributes == {
        "street": SlotDefinition(name="street", range="string", required=True),
        "city": SlotDefinition(name="city", range="string"),
    }
    generated_at = simple_doc.annotations["generated_at"].value
    assert wide_doc.annotations["generated_at"].value == generated_at
    assert datetime.fromisoformat(generated_at).tzinfo == UTC

    for schema_path in files:
        with pytest.raises(json.JSONDecodeError):
            json.loads(schema_path.read_text(encoding="utf-8"))
        assert list(Linter.validate_schema(str(schema_path))) == []


def test_dump_catalog_schemas_pass_empty_catalog(catalog_env: Path) -> None:
    """An empty catalog (no namespaces) writes no files and raises nothing."""
    load_catalog(CATALOG_NAME)
    out_dir = catalog_env / "schemas"
    settings = IcebergToLinkMLSettings(catalog=CATALOG_NAME, output_dir=str(out_dir))  # pyright: ignore[reportCallIssue]
    dump_catalog_schemas(settings)
    assert list(out_dir.iterdir()) == []


def test_dump_catalog_schemas_pass_skips_failing_table_and_logs(
    catalog_env: Path, simple_schema: Schema, caplog: pytest.LogCaptureFixture
) -> None:
    """A table that fails to convert is logged and skipped; other tables still succeed."""
    catalog = load_catalog(CATALOG_NAME)
    catalog.create_namespace("ns")
    bad_table = catalog.create_table(("ns", "bad"), schema=simple_schema)
    bad_table.io.delete(bad_table.metadata_location)
    catalog.create_table(("ns", "good"), schema=simple_schema)

    out_dir = catalog_env / "schemas"
    settings = IcebergToLinkMLSettings(catalog=CATALOG_NAME, output_dir=str(out_dir))  # pyright: ignore[reportCallIssue]
    with caplog.at_level(logging.WARNING):
        dump_catalog_schemas(settings)

    assert [f.name for f in sorted(out_dir.iterdir())] == ["ns.good.schema.yaml"]
    assert "Failed to convert table ('ns', 'bad'); skipping" in caplog.text
    assert "Completed with 1 table(s) skipped due to conversion failures" in caplog.text


def test_dump_catalog_schemas_pass_overwrites_existing_file_with_warning(
    catalog_env: Path, simple_schema: Schema, caplog: pytest.LogCaptureFixture
) -> None:
    """A pre-existing output file for a table is overwritten, with a warning logged first."""
    catalog = load_catalog(CATALOG_NAME)
    catalog.create_namespace("ns")
    catalog.create_table(("ns", "simple"), schema=simple_schema)

    out_dir = catalog_env / "schemas"
    out_dir.mkdir(parents=True)
    stale_file = out_dir / "ns.simple.schema.yaml"
    stale_file.write_text("stale")

    settings = IcebergToLinkMLSettings(catalog=CATALOG_NAME, output_dir=str(out_dir))  # pyright: ignore[reportCallIssue]
    with caplog.at_level(logging.WARNING):
        dump_catalog_schemas(settings)

    assert "Overwriting existing schema file" in caplog.text
    written = yaml_loader.load(str(stale_file), target_class=SchemaDefinition)
    assert written.name == "simple"
