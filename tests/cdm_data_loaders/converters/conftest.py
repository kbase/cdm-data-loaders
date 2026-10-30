"""Shared fixtures and helpers for the converters test suite."""

import re
from collections.abc import Generator, Iterator
from pathlib import Path
from typing import Any, Final

import pytest
from pyiceberg import catalog as pyiceberg_catalog
from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.partitioning import PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.table.metadata import new_table_metadata
from pyiceberg.table.sorting import SortOrder
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

from cdm_data_loaders.converters.dlt_to_jsonschema import DltToJSONSchema
from cdm_data_loaders.converters.jsonschema_to_dlt import JSONSchemaToDlt
from cdm_data_loaders.converters.jsonschema_to_pyarrow import JSONSchemaToPyArrow
from cdm_data_loaders.converters.jsonschema_to_pyspark import JSONSchemaToPySpark

CATALOG_NAME: Final[str] = "testcat"

JSON_SCHEMA_KEYWORDS: Final = frozenset(
    {
        "$schema",
        "$id",
        "$defs",
        "title",
        "description",
        "type",
        "format",
        "pattern",
        "items",
        "properties",
        "required",
        "additionalProperties",
        "anyOf",
        "minimum",
        "maximum",
        "contentEncoding",
    }
)


@pytest.fixture
def dlt_to_jsonschema_converter() -> DltToJSONSchema:
    """A DltToJSONSchema instance configured with all default settings."""
    return DltToJSONSchema()


@pytest.fixture
def jsonschema_to_dlt_converter() -> JSONSchemaToDlt:
    """A JSONSchemaToDlt instance configured with all default settings."""
    return JSONSchemaToDlt(schema_name="test_source")


@pytest.fixture
def nested_json_converter() -> JSONSchemaToDlt:
    """A JSONSchemaToDlt instance that keeps nested structures as json columns."""
    return JSONSchemaToDlt(schema_name="test_source", skip_nested_types=True)


@pytest.fixture
def jsonschema_to_pyspark_converter() -> JSONSchemaToPySpark:
    """A JSONSchemaToPySpark instance configured with all default settings."""
    return JSONSchemaToPySpark()


@pytest.fixture
def jsonschema_to_pyarrow_converter() -> JSONSchemaToPyArrow:
    """A JSONSchemaToPyArrow instance configured with all default settings."""
    return JSONSchemaToPyArrow()


def base_stored_schema(tables: dict[str, Any]) -> dict[str, Any]:
    """Build a minimal stored-schema dict wrapping the given tables."""
    return {"name": "test_source", "tables": tables}


def base_table(**overrides: Any) -> dict[str, Any]:  # noqa: ANN401
    """Build a minimal TTableSchema with a single non-nullable text column, applying overrides."""
    table: dict[str, Any] = {
        "columns": {"col": {"name": "col", "data_type": "text", "nullable": False}},
    }
    table.update(overrides)
    return table


def base_object_schema(**overrides: Any) -> dict[str, Any]:  # noqa: ANN401
    """Build a minimal valid root object schema, applying keyword overrides."""
    schema: dict[str, Any] = {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "properties": {},
    }
    schema.update(overrides)
    return schema


def make_table(
    identifier: tuple[str, ...],
    schema: Schema,
    partition_spec: PartitionSpec | None = None,
    properties: dict[str, str] | None = None,
) -> Table:
    """Build a real pyiceberg Table backed by freshly generated metadata."""
    metadata = new_table_metadata(
        schema,
        partition_spec if partition_spec is not None else PartitionSpec(),
        SortOrder(),
        f"file:///tmp/{'/'.join(identifier)}",
        properties or {},
    )
    return Table(identifier, metadata, f"{metadata.location}/metadata/00000.metadata.json", None, None)


@pytest.fixture
def simple_schema() -> Schema:
    """A schema with one required and one optional field."""
    return Schema(
        NestedField(1, "id", LongType(), required=True, doc="primary key"),
        NestedField(2, "name", StringType(), doc="display name"),
    )


@pytest.fixture
def catalog_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Point the testcat catalog at a file-backed SQLite DB under tmp_path."""
    db_path = tmp_path / "catalog.db"
    warehouse = tmp_path / "warehouse"
    warehouse.mkdir()
    monkeypatch.setenv(f"PYICEBERG_CATALOG__{CATALOG_NAME.upper()}__TYPE", "sql")
    monkeypatch.setenv(f"PYICEBERG_CATALOG__{CATALOG_NAME.upper()}__URI", f"sqlite:///{db_path}")
    monkeypatch.setenv(f"PYICEBERG_CATALOG__{CATALOG_NAME.upper()}__WAREHOUSE", f"file://{warehouse}")
    monkeypatch.setattr(pyiceberg_catalog, "_ENV_CONFIG", Config())
    return tmp_path


@pytest.fixture
def catalog(catalog_env: Path) -> Generator[Catalog]:  # noqa: ARG001
    """Load the testcat SQL catalog and close its database connections at teardown."""
    with load_catalog(CATALOG_NAME) as loaded_catalog:
        yield loaded_catalog


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


def iter_schema_keywords(node: dict) -> Iterator[str]:
    """Yield every schema keyword key in a schema document, descending into subschemas."""
    for key, value in node.items():
        yield key
        if key in {"properties", "$defs"} and isinstance(value, dict):
            for subschema in value.values():
                yield from iter_schema_keywords(subschema)
        elif key == "items" and isinstance(value, dict):
            yield from iter_schema_keywords(value)
        elif key == "anyOf" and isinstance(value, list):
            for subschema in value:
                if isinstance(subschema, dict):
                    yield from iter_schema_keywords(subschema)


def iter_property_names(node: dict) -> Iterator[str]:
    """Yield every property name in a schema document, descending into subschemas."""
    for key, value in node.items():
        if key == "properties" and isinstance(value, dict):
            for name, subschema in value.items():
                yield name
                yield from iter_property_names(subschema)
        elif key == "items" and isinstance(value, dict):
            yield from iter_property_names(value)
        elif key == "anyOf" and isinstance(value, list):
            for subschema in value:
                if isinstance(subschema, dict):
                    yield from iter_property_names(subschema)


def iter_extension_keys(node: dict) -> Iterator[str]:
    """Yield every x- prefixed key and every key inside extension blocks."""
    for key, value in node.items():
        if key.startswith("x-"):
            yield key
            if isinstance(value, dict):
                yield from iter_extension_keys(value)
        elif isinstance(value, dict):
            yield from iter_extension_keys(value)
        elif isinstance(value, list):
            for item in value:
                if isinstance(item, dict):
                    yield from iter_extension_keys(item)


def to_snake_case(key: str) -> str:
    """Return the key unchanged if already snake case."""
    return re.sub(r"(?<!^)(?=[A-Z])", "_", key).lower()
