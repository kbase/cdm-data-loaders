"""End-to-end tests for pyiceberg_to_jsonschema.

Chains a real pyiceberg SQL catalog (file-backed SQLite) through
dump_catalog_schemas and validates the emitted documents with jsonschema's
Draft 2020-12 validator.
"""

import json
import logging
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator, FormatChecker
from pyiceberg import catalog as pyiceberg_catalog
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    BinaryType,
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

from cdm_data_loaders.converters.pyiceberg_catalog_to_jsonschema import (
    IcebergToJsonSchemaSettings,
    dump_catalog_schemas,
)
from tests.cdm_data_loaders.converters.conftest import (
    JSON_SCHEMA_KEYWORDS,
    iter_extension_keys,
    iter_schema_keywords,
    to_snake_case,
)

CATALOG_NAME = "testcat"


@pytest.fixture
def catalog_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Point the testcat catalog at a file-backed SQLite DB under tmp_path."""
    db_path = tmp_path / "catalog.db"
    warehouse = tmp_path / "warehouse"
    warehouse.mkdir()
    monkeypatch.setenv("PYICEBERG_CATALOG__TESTCAT__TYPE", "sql")
    monkeypatch.setenv("PYICEBERG_CATALOG__TESTCAT__URI", f"sqlite:///{db_path}")
    monkeypatch.setenv("PYICEBERG_CATALOG__TESTCAT__WAREHOUSE", f"file://{warehouse}")
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


def test_end_to_end_dump_catalog_schemas_writes_valid_docs(
    catalog_env: Path, simple_schema: Schema, wide_schema: Schema
) -> None:
    """dump_catalog_schemas loads the catalog by name and writes one valid schema doc per table."""
    catalog = load_catalog(CATALOG_NAME)
    catalog.create_namespace("ns")
    catalog.create_table(("ns", "simple"), schema=simple_schema)
    catalog.create_table(("ns", "wide"), schema=wide_schema)

    out_dir = catalog_env / "schemas"
    settings = IcebergToJsonSchemaSettings(catalog=CATALOG_NAME, output_dir=str(out_dir))  # pyright: ignore[reportCallIssue]
    dump_catalog_schemas(settings)

    files = sorted(out_dir.iterdir())
    assert [f.name for f in files] == ["ns.simple.schema.json", "ns.wide.schema.json"]

    simple_doc = json.loads((out_dir / "ns.simple.schema.json").read_text())
    assert simple_doc["$schema"] == "https://json-schema.org/draft/2020-12/schema"
    assert simple_doc["$id"] == "urn:iceberg:ns.simple"
    assert simple_doc["title"] == "simple"
    assert simple_doc["x-iceberg"]["generated_at"]
    Draft202012Validator.check_schema(simple_doc)
    simple_validator = Draft202012Validator(simple_doc, format_checker=FormatChecker())
    assert simple_validator.is_valid({"id": 1, "name": "n"})
    assert not simple_validator.is_valid({"name": "n"})
    assert not simple_validator.is_valid({"id": "not-an-int", "name": "n"})

    wide_doc = json.loads((out_dir / "ns.wide.schema.json").read_text())
    Draft202012Validator.check_schema(wide_doc)
    wide_validator = Draft202012Validator(wide_doc, format_checker=FormatChecker())
    assert wide_validator.is_valid(
        {
            "id": 1,
            "name": "n",
            "count": 3,
            "ratio": 0.5,
            "as_of": "2026-09-22T00:00:00+00:00",
            "born_on": "2026-09-22",
            "rate": "1234.5678",
            "tags": ["a"],
            "attrs": [{"key": "k", "value": 1.5}],
            "address": {"street": "s", "city": "c"},
        }
    )
    assert wide_validator.is_valid({"id": 1, "name": "n", "address": {"street": "s"}})
    assert not wide_validator.is_valid({"id": 1, "name": "n", "address": {"city": "c"}})
    assert not wide_validator.is_valid({"id": 1, "name": "n", "rate": 1234.5678})
    assert not wide_validator.is_valid({"id": 1, "name": "n", "tags": "not-an-array"})
    assert not wide_validator.is_valid({"id": 1, "name": "n", "attrs": [{"key": "k", "value": "x"}]})

    for doc in (simple_doc, wide_doc):
        for key in iter_schema_keywords(doc):
            assert key in JSON_SCHEMA_KEYWORDS or key.startswith("x-"), f"non-standard key without x- prefix: {key}"
        for key in iter_extension_keys(doc):
            assert key == to_snake_case(key), f"extension key is not snake case: {key}"


def test_dump_catalog_schemas_pass_empty_catalog(catalog_env: Path) -> None:
    """An empty catalog (no namespaces) writes no files and raises nothing."""
    load_catalog(CATALOG_NAME)
    out_dir = catalog_env / "schemas"
    settings = IcebergToJsonSchemaSettings(catalog=CATALOG_NAME, output_dir=str(out_dir))  # pyright: ignore[reportCallIssue]
    dump_catalog_schemas(settings)
    assert list(out_dir.iterdir()) == []


def test_dump_catalog_schemas_pass_skips_failing_table_and_logs(
    catalog_env: Path, simple_schema: Schema, caplog: pytest.LogCaptureFixture
) -> None:
    """A table that fails to convert is logged and skipped; other tables still succeed."""
    unsupported_schema = Schema(NestedField(1, "value", BinaryType(), required=True))
    catalog = load_catalog(CATALOG_NAME)
    catalog.create_namespace("ns")
    catalog.create_table(("ns", "good"), schema=simple_schema)
    catalog.create_table(("ns", "bad"), schema=unsupported_schema)

    out_dir = catalog_env / "schemas"
    settings = IcebergToJsonSchemaSettings(catalog=CATALOG_NAME, output_dir=str(out_dir))  # pyright: ignore[reportCallIssue]
    with caplog.at_level(logging.WARNING):
        dump_catalog_schemas(settings)

    assert [f.name for f in sorted(out_dir.iterdir())] == ["ns.good.schema.json"]
    assert "Failed to convert table" in caplog.text
    assert "('ns', 'bad')" in caplog.text
    assert "1 table(s) skipped" in caplog.text


def test_dump_catalog_schemas_pass_overwrites_existing_file_with_warning(
    catalog_env: Path, simple_schema: Schema, caplog: pytest.LogCaptureFixture
) -> None:
    """A pre-existing output file for a table is overwritten, with a warning logged first."""
    catalog = load_catalog(CATALOG_NAME)
    catalog.create_namespace("ns")
    catalog.create_table(("ns", "simple"), schema=simple_schema)

    out_dir = catalog_env / "schemas"
    out_dir.mkdir(parents=True)
    stale_file = out_dir / "ns.simple.schema.json"
    stale_file.write_text("stale")

    settings = IcebergToJsonSchemaSettings(catalog=CATALOG_NAME, output_dir=str(out_dir))  # pyright: ignore[reportCallIssue]
    with caplog.at_level(logging.WARNING):
        dump_catalog_schemas(settings)

    assert "Overwriting existing schema file" in caplog.text
    written = json.loads(stale_file.read_text())
    assert written["title"] == "simple"
