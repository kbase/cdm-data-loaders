"""Shared JSON Schema keyword sets and facet extractors."""

from collections.abc import Mapping
from typing import Any, Final

from cdm_data_loaders.converters.core.extensions import ExtensionRegistry, Extensions

TYPES: Final[frozenset[str]] = frozenset({"object", "array", "string", "integer", "number", "boolean", "null"})
ANNOTATIONS: Final[frozenset[str]] = frozenset(
    {
        "$schema",
        "$id",
        "$anchor",
        "$dynamicAnchor",
        "$comment",
        "$vocabulary",
        "title",
        "description",
        "default",
        "examples",
        "readOnly",
        "writeOnly",
        "deprecated",
    }
)
SCHEMA_KEYS: Final[frozenset[str]] = frozenset(
    {
        "additionalItems",
        "unevaluatedItems",
        "contains",
        "unevaluatedProperties",
        "propertyNames",
        "not",
        "if",
        "then",
        "else",
        "contentSchema",
    }
)
SCHEMA_MAPS: Final[frozenset[str]] = frozenset({"$defs", "definitions", "dependentSchemas"})
STRUCTURE: Final[frozenset[str]] = (
    SCHEMA_KEYS
    | SCHEMA_MAPS
    | {
        "$ref",
        "allOf",
        "type",
        "properties",
        "required",
        "items",
        "prefixItems",
        "patternProperties",
        "additionalProperties",
        "anyOf",
        "oneOf",
        "dependencies",
    }
)


def extract_extensions(schema: Mapping[str, Any], registry: ExtensionRegistry) -> Extensions:
    """Extract and validate ``x-*`` vendor extensions from a JSON Schema mapping.

    :param schema: JSON Schema node mapping.
    :type schema: Mapping[str, Any]
    :param registry: Extension registry used for validation.
    :type registry: ExtensionRegistry
    :return: Validated ``Extensions`` container.
    :rtype: Extensions
    """
    return Extensions(
        {key: value for key, value in schema.items() if key.startswith("x-")},
        registry,
    )


def extract_constraints(schema: Mapping[str, Any]) -> dict[str, Any]:
    """Extract non-structural, non-annotation, non-extension constraint keywords from ``schema``.

    :param schema: JSON Schema node mapping.
    :type schema: Mapping[str, Any]
    :return: Dictionary of constraint keywords and values.
    :rtype: dict[str, Any]
    """
    return {
        key: value
        for key, value in schema.items()
        if key not in STRUCTURE and key not in ANNOTATIONS and not key.startswith("x-")
    }
