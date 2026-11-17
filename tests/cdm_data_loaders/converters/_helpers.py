"""Shared schema construction and keyword inspection helpers for converter and CLI tests."""

import re
from collections.abc import Iterator
from typing import Any, Final

JSON_SCHEMA_KEYWORDS: Final[frozenset[str]] = frozenset(
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
        "minItems",
        "maxItems",
        "contentEncoding",
    }
)


def base_stored_schema(tables: dict[str, Any]) -> dict[str, Any]:
    """Build a minimal dlt stored-schema dictionary wrapping ``tables``.

    :param tables: Mapping of table names to dlt table definitions.
    :type tables: dict[str, Any]
    :return: Stored-schema dictionary.
    :rtype: dict[str, Any]
    """
    return {"name": "test_source", "tables": tables}


def base_table(**overrides: object) -> dict[str, Any]:
    """Build a minimal dlt table schema with a single non-nullable text column and optional overrides.

    :param overrides: Field overrides merged into the table dictionary.
    :type overrides: object
    :return: Table schema dictionary.
    :rtype: dict[str, Any]
    """
    table: dict[str, Any] = {
        "columns": {"col": {"name": "col", "data_type": "text", "nullable": False}},
    }
    table.update(overrides)
    return table


def base_object_schema(**overrides: object) -> dict[str, Any]:
    """Build a minimal valid Draft 2020-12 root object schema with optional overrides.

    :param overrides: Keyword overrides merged into the root schema dictionary.
    :type overrides: object
    :return: JSON Schema dictionary.
    :rtype: dict[str, Any]
    """
    schema: dict[str, Any] = {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "properties": {},
    }
    schema.update(overrides)
    return schema


def iter_schema_keywords(node: dict[str, Any]) -> Iterator[str]:
    """Yield every JSON Schema keyword key in ``node``, descending into subschemas.

    :param node: JSON Schema dictionary.
    :type node: dict[str, Any]
    :return: Iterator of keyword strings.
    :rtype: Iterator[str]
    """
    for key, value in node.items():
        yield key
        if key in {"properties", "$defs"} and isinstance(value, dict):
            for subschema in value.values():
                if isinstance(subschema, dict):
                    yield from iter_schema_keywords(subschema)
        elif key == "items" and isinstance(value, dict):
            yield from iter_schema_keywords(value)
        elif key == "anyOf" and isinstance(value, list):
            for subschema in value:
                if isinstance(subschema, dict):
                    yield from iter_schema_keywords(subschema)


def iter_extension_keys(node: dict[str, Any]) -> Iterator[str]:
    """Yield every ``x-`` prefixed key and nested extension block key in ``node``.

    :param node: JSON Schema dictionary.
    :type node: dict[str, Any]
    :return: Iterator of extension key strings.
    :rtype: Iterator[str]
    """
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
    """Convert camelCase/PascalCase segments in ``key`` to ``snake_case``.

    :param key: Identifier string.
    :type key: str
    :return: Snake-cased string.
    :rtype: str
    """
    return re.sub(r"(?<!^)(?=[A-Z])", "_", key).lower()
