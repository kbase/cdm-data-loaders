"""Shared fixtures for the dlt_to_jsonschema test suite."""

from typing import Any

import pytest

from cdm_data_loaders.converters.dlt_to_jsonschema.converter import DltToJSONSchema


@pytest.fixture
def converter() -> DltToJSONSchema:
    """A DltToJSONSchema instance configured with all default settings."""
    return DltToJSONSchema()


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
