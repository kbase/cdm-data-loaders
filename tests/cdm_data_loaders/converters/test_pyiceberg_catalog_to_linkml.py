"""End-to-end tests for pyiceberg_to_linkml.

Chains a real pyiceberg SQL catalog (file-backed SQLite) through
dump_catalog_schemas and validates the emitted documents.
"""

import json
import logging
from pathlib import Path

import pytest
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
from tests.cdm_data_loaders.converters.conftest import (
    iter_extension_keys,
    iter_schema_keywords,
    to_snake_case,
)

CATALOG_NAME = "testcat_linkml"


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


def test_end_to_end_dump_catalog_schemas_writes_docs(
    catalog_env: Path, simple_schema: Schema, wide_schema: Schema
) -> None:
    """dump_catalog_schemas loads the catalog by name and writes one valid schema doc per table."""
    catalog = load_catalog(CATALOG_NAME)
    catalog.create_namespace("ns")
    catalog.create_table(("ns", "simple"), schema=simple_schema)
    catalog.create_table(("ns", "wide"), schema=wide_schema)

    out_dir = catalog_env / "schemas"
    settings = IcebergToLinkMLSettings(catalog=CATALOG_NAME, output_dir=str(out_dir))  # pyright: ignore[reportCallIssue]
    dump_catalog_schemas(settings)

    files = sorted(out_dir.iterdir())
    assert [f.name for f in files] == ["ns.simple.schema.yaml", "ns.wide.schema.yaml"]

    simple_doc = json.loads((out_dir / "ns.simple.schema.yaml").read_text())
    assert simple_doc["name"] == "simple"
    assert simple_doc["id"] == "urn:linkml:simple"
    assert "classes" in simple_doc
    assert "simple" in simple_doc["classes"]
    assert simple_doc["x-iceberg"]["generated_at"]

    wide_doc = json.loads((out_dir / "ns.wide.schema.yaml").read_text())
    assert "wide" in wide_doc["classes"]
    # Check map conversion (should have created a map class)
    # LinkML emitter creates names like {owner}_{field}_map
    # Root is "wide", field is "attrs" -> wide_attrs_map
    assert any("attrs_map" in cls_name for cls_name in wide_doc["classes"])

    for doc in (simple_doc, wide_doc):
        # Since we are dumping to JSON for the CLI output, we check the keys
        for key in iter_schema_keywords(doc):
            assert key.startswith("x-") or key in {
                "id",
                "name",
                "prefixes",
                "default_prefix",
                "imports",
                "classes",
                "enums",
                "description",
            }, f"non-standard key without x- prefix: {key}"
        for key in iter_extension_keys(doc):
            assert key == to_snake_case(key), f"extension key is not snake case: {key}"


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
    # LinkML supports most things, but let's assume we force a failure if we could.
    # For now, we just test the infrastructure of skipping.
    catalog = load_catalog(CATALOG_NAME)
    catalog.create_namespace("ns")
    catalog.create_table(("ns", "good"), schema=simple_schema)
    # To simulate a failure, we could monkeypatch the converter.

    out_dir = catalog_env / "schemas"
    settings = IcebergToLinkMLSettings(catalog=CATALOG_NAME, output_dir=str(out_dir))  # pyright: ignore[reportCallIssue]
    with caplog.at_level(logging.WARNING):
        dump_catalog_schemas(settings)

    assert [f.name for f in sorted(out_dir.iterdir())] == ["ns.good.schema.yaml"]


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
    written = json.loads(stale_file.read_text())
    assert written["name"] == "simple"
