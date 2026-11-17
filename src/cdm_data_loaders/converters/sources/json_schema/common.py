"""Shared JSON Schema extension and constraint facet extractors."""

from collections.abc import Mapping
from typing import Any

from cdm_data_loaders.converters.core.extensions import ExtensionRegistry, Extensions
from cdm_data_loaders.validation.jsonschema.keywords import ANNOTATIONS, STRUCTURE


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
