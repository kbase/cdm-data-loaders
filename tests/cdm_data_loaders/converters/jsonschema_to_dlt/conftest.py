"""Shared fixtures for the jsonschema_to_dlt test suite."""

from typing import Any

import pytest

from cdm_data_loaders.converters.jsonschema_to_dlt.converter import JSONSchemaToDlt


@pytest.fixture
def converter() -> JSONSchemaToDlt:
    """A JSONSchemaToDlt instance configured with all default settings."""
    return JSONSchemaToDlt(schema_name="test_source")


@pytest.fixture
def nested_json_converter() -> JSONSchemaToDlt:
    """A JSONSchemaToDlt instance that keeps nested structures as json columns."""
    return JSONSchemaToDlt(schema_name="test_source", skip_nested_types=True)


def base_object_schema(**overrides: Any) -> dict[str, Any]:  # noqa: ANN401
    """Build a minimal valid root object schema, applying keyword overrides."""
    schema: dict[str, Any] = {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "properties": {},
    }
    schema.update(overrides)
    return schema
