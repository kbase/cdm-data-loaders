"""Shared fixtures for the pyiceberg_to_jsonschema test suite."""

import re
from collections.abc import Iterator
from typing import Final

import pytest
from pyiceberg.partitioning import PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.table.metadata import new_table_metadata
from pyiceberg.table.sorting import SortOrder
from pyiceberg.types import LongType, NestedField, StringType

JSON_SCHEMA_KEYWORDS: Final = frozenset(
    {
        "$schema",
        "$id",
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


def iter_schema_keywords(node: dict) -> Iterator[str]:
    """Yield every schema keyword key in a schema document, descending into subschemas."""
    for key, value in node.items():
        yield key
        if key == "properties" and isinstance(value, dict):
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
