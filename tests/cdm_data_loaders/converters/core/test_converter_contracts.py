"""Integration tests for shared helpers and direction-specific conversion policies."""

import json
import logging
from typing import Any

import pytest
from pyspark.sql.types import ArrayType, DataType, DoubleType, LongType, NullType, StringType, StructType

from cdm_data_loaders.converters.core import ConversionError, inference
from cdm_data_loaders.converters.dlt_to_jsonschema import converter as reverse_module
from cdm_data_loaders.converters.jsonschema_to_dlt import converter as dlt_module
from cdm_data_loaders.converters.jsonschema_to_dlt.converter import _decimal_places as dlt_decimal_places
from cdm_data_loaders.converters.jsonschema_to_dlt.converter import _infer_implicit_type as dlt_infer_implicit_type
from cdm_data_loaders.converters.jsonschema_to_pyspark import converter as spark_module
from cdm_data_loaders.converters.jsonschema_to_pyspark.converter import _decimal_places as spark_decimal_places
from cdm_data_loaders.converters.jsonschema_to_pyspark.converter import (
    _infer_implicit_type as spark_infer_implicit_type,
)


def property_schema(fragment: dict[str, Any]) -> dict[str, Any]:
    """Wrap a property fragment in a declared object schema."""
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "properties": {"value": fragment},
    }


@pytest.mark.parametrize(
    "error",
    [
        dlt_module.JSONSchemaToDltError,
        dlt_module.InvalidJSONSchemaError,
        spark_module.JSONSchemaToPySparkError,
        spark_module.InvalidJSONSchemaError,
        reverse_module.DltToJSONSchemaError,
    ],
    ids=["dlt", "invalid-dlt", "pyspark", "invalid-pyspark", "reverse-dlt"],
)
def test_conversion_error_pass_shared_base(error: type[ConversionError]) -> None:
    """All direction-specific exceptions remain catchable as conversion errors and ValueError."""
    assert issubclass(error, ConversionError)
    assert issubclass(error, ValueError)
    assert str(error("invalid schema")) == "invalid schema"


@pytest.mark.parametrize(
    ("converter", "error", "other_error"),
    [
        (
            dlt_module.JSONSchemaToDlt(schema_name="test"),
            dlt_module.InvalidJSONSchemaError,
            spark_module.InvalidJSONSchemaError,
        ),
        (spark_module.JSONSchemaToPySpark(), spark_module.InvalidJSONSchemaError, dlt_module.InvalidJSONSchemaError),
    ],
    ids=["dlt", "pyspark"],
)
def test_convert_fail_direction_specific_invalid_schema(
    converter: dlt_module.JSONSchemaToDlt | spark_module.JSONSchemaToPySpark,
    error: type[ConversionError],
    other_error: type[ConversionError],
) -> None:
    """Invalid-schema errors retain distinct identities in the two forward converters."""
    with pytest.raises(error) as caught:
        converter.convert({"type": "object"})
    assert type(caught.value) is error
    assert not isinstance(caught.value, other_error)


def test_private_helpers_pass_shared_aliases() -> None:
    """Private helper imports and keyword exports refer to their shared implementations."""
    assert dlt_decimal_places is inference.decimal_places
    assert spark_decimal_places is inference.decimal_places
    assert dlt_infer_implicit_type is inference.infer_implicit_type
    assert spark_infer_implicit_type is inference.infer_implicit_type
    assert spark_module.IMPLICIT_OBJECT_KEYWORDS is inference.IMPLICIT_OBJECT_KEYWORDS
    assert spark_module.IMPLICIT_ARRAY_KEYWORDS is inference.IMPLICIT_ARRAY_KEYWORDS
    assert spark_module.IMPLICIT_STRING_KEYWORDS is inference.IMPLICIT_STRING_KEYWORDS
    assert spark_module.IMPLICIT_NUMBER_KEYWORDS is inference.IMPLICIT_NUMBER_KEYWORDS


@pytest.mark.parametrize(
    "converter",
    [dlt_module.JSONSchemaToDlt(schema_name="test"), spark_module.JSONSchemaToPySpark()],
    ids=["dlt", "pyspark"],
)
def test_convert_from_string_fail_forward_yaml(
    converter: dlt_module.JSONSchemaToDlt | spark_module.JSONSchemaToPySpark,
) -> None:
    """Forward string entry points reject even otherwise valid YAML schemas."""
    with pytest.raises(json.JSONDecodeError):
        converter.convert_from_string("$schema: https://json-schema.org/draft/2020-12/schema\ntype: object\n")


@pytest.mark.parametrize("combiner", ["oneOf", "anyOf"], ids=["one-of", "any-of"])
@pytest.mark.parametrize(
    ("fragment", "dlt_type", "spark_type", "dlt_warns", "spark_warns"),
    [
        ({"type": "string"}, "bigint", StringType(), True, False),
        ({"type": "object", "properties": {}}, "bigint", StructType([]), True, False),
        ({"type": "array", "items": {"type": "string"}}, "bigint", ArrayType(StringType()), True, False),
        ({"type": ["string", "null"]}, "bigint", StringType(), True, False),
        ({"type": ["null"]}, "text", NullType(), False, False),
        ({"type": ["integer", "string"]}, "text", StringType(), False, False),
        ({"properties": {}}, "json", StructType([]), False, False),
        ({"pattern": "x"}, "text", StringType(), False, False),
        ({"minimum": 0}, "double", DoubleType(), False, False),
        ({"enum": ["x"]}, "text", StringType(), False, False),
        ({}, "bigint", LongType(), True, True),
    ],
    ids=[
        "explicit-string",
        "explicit-object",
        "explicit-array",
        "nullable-string",
        "null-union",
        "mixed-union",
        "implicit-object",
        "implicit-string",
        "implicit-number",
        "enum",
        "combiner-only",
    ],
)
def test_convert_type_pass_combiner_precedence(
    combiner: str,
    fragment: dict[str, Any],
    dlt_type: str,
    spark_type: DataType,
    dlt_warns: bool,
    spark_warns: bool,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Preserve distinct type-versus-combiner precedence and module-owned warnings."""
    schema = property_schema({**fragment, combiner: [{"type": "integer"}, {"type": "string"}]})
    dlt_converter = dlt_module.JSONSchemaToDlt(schema_name="test", skip_nested_types=True)
    spark_converter = spark_module.JSONSchemaToPySpark()
    with caplog.at_level(logging.WARNING):
        assert dlt_converter.convert(schema)["tables"]["test"]["columns"]["value"] == {
            "name": "value",
            "data_type": dlt_type,
            "nullable": True,
        }
        assert spark_converter.convert(schema)["value"].dataType == spark_type
    warning_modules = {record.name for record in caplog.records if "Approximating" in record.message}
    assert ("cdm_data_loaders.converters.emitters.dlt" in warning_modules) == dlt_warns
    assert ("cdm_data_loaders.converters.emitters.pyspark" in warning_modules) == spark_warns


@pytest.mark.parametrize(
    "schema",
    [
        {},
        {"type": "unknown"},
        {"type": ["integer", "string"]},
        {"oneOf": []},
        {"anyOf": []},
        {"oneOf": [True]},
        {"anyOf": [False]},
        {"oneOf": [{"anyOf": [True]}]},
    ],
    ids=[
        "empty",
        "unknown",
        "mixed-union",
        "empty-one-of",
        "empty-any-of",
        "true-branch",
        "false-branch",
        "nested-boolean",
    ],
)
def test_convert_type_fail_strict_pyspark_fallback(schema: dict[str, Any]) -> None:
    """Keep dlt text fallback and PySpark's optional rejection of unsupported schemas."""
    document = property_schema(schema)
    stored = dlt_module.JSONSchemaToDlt(schema_name="test").convert(document)
    assert stored["tables"]["test"]["columns"]["value"] == {"name": "value", "data_type": "text", "nullable": True}
    permissive = spark_module.JSONSchemaToPySpark()
    strict = spark_module.JSONSchemaToPySpark(treat_unknown_as_string=False)
    assert permissive.convert(document)["value"].dataType == StringType()
    with pytest.raises(spark_module.JSONSchemaToPySparkError):
        strict.convert(document)


def test_convert_type_pass_one_of_before_any_of() -> None:
    """Both dispatchers prefer the first oneOf branch when both combiners are present."""
    schema = property_schema({"oneOf": [{"type": "integer"}], "anyOf": [{"type": "string"}]})
    stored = dlt_module.JSONSchemaToDlt(schema_name="test").convert(schema)
    assert stored["tables"]["test"]["columns"]["value"] == {"name": "value", "data_type": "bigint", "nullable": True}
    converter = spark_module.JSONSchemaToPySpark()
    assert converter.convert(schema)["value"].dataType == LongType()
