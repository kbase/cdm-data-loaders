"""Shared fixtures for ``tests/cdm_data_loaders/converters``."""

import pytest
from pyiceberg.schema import Schema

from cdm_data_loaders.converters.facades.dlt_to_jsonschema import DltToJSONSchema
from cdm_data_loaders.converters.facades.jsonschema_to_dlt import JSONSchemaToDlt
from cdm_data_loaders.converters.facades.jsonschema_to_pyarrow import JSONSchemaToPyArrow
from cdm_data_loaders.converters.facades.jsonschema_to_pyspark import JSONSchemaToPySpark
from tests.helpers.iceberg_catalog import SIMPLE_ICEBERG_SCHEMA, WIDE_ICEBERG_SCHEMA


@pytest.fixture
def dlt_to_jsonschema_converter() -> DltToJSONSchema:
    """Return a ``DltToJSONSchema`` instance configured with default settings.

    :return: Default ``DltToJSONSchema`` converter.
    :rtype: DltToJSONSchema
    """
    return DltToJSONSchema()


@pytest.fixture
def jsonschema_to_dlt_converter() -> JSONSchemaToDlt:
    """Return a ``JSONSchemaToDlt`` instance configured with default settings.

    :return: Default ``JSONSchemaToDlt`` converter.
    :rtype: JSONSchemaToDlt
    """
    return JSONSchemaToDlt(schema_name="test_source")


@pytest.fixture
def nested_json_converter() -> JSONSchemaToDlt:
    """Return a ``JSONSchemaToDlt`` instance that preserves nested structures as JSON columns.

    :return: ``JSONSchemaToDlt`` with ``skip_nested_types=True``.
    :rtype: JSONSchemaToDlt
    """
    return JSONSchemaToDlt(schema_name="test_source", skip_nested_types=True)


@pytest.fixture
def jsonschema_to_pyspark_converter() -> JSONSchemaToPySpark:
    """Return a ``JSONSchemaToPySpark`` instance configured with default settings.

    :return: Default ``JSONSchemaToPySpark`` converter.
    :rtype: JSONSchemaToPySpark
    """
    return JSONSchemaToPySpark()


@pytest.fixture
def jsonschema_to_pyarrow_converter() -> JSONSchemaToPyArrow:
    """Return a ``JSONSchemaToPyArrow`` instance configured with default settings.

    :return: Default ``JSONSchemaToPyArrow`` converter.
    :rtype: JSONSchemaToPyArrow
    """
    return JSONSchemaToPyArrow()


@pytest.fixture
def simple_schema() -> Schema:
    """Return a minimal two-column PyIceberg ``Schema``.

    :return: Simple PyIceberg schema.
    :rtype: Schema
    """
    return SIMPLE_ICEBERG_SCHEMA


@pytest.fixture
def wide_schema() -> Schema:
    """Return a PyIceberg ``Schema`` exercising scalars, structs, lists, and maps.

    :return: Wide PyIceberg schema.
    :rtype: Schema
    """
    return WIDE_ICEBERG_SCHEMA
