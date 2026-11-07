"""Unit tests for jsonschema_to_pyspark."""

from typing import Any

import pytest
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    IntegerType,
    LongType,
    MapType,
    NullType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from cdm_data_loaders.converters.jsonschema_to_pyspark import (
    JSONSchemaToPySpark,
    JSONSchemaToPySparkError,
)
from cdm_data_loaders.utils.jsonschema.dereferencer import dereference_schema
from tests.cdm_data_loaders.converters.conftest import base_object_schema


def test_convert_fail_root_resolves_to_maptype(jsonschema_to_pyspark_converter: JSONSchemaToPySpark) -> None:
    """convert() rejects a root schema that has no 'properties' and resolves to a MapType."""
    schema = base_object_schema(patternProperties={"^x-": {"type": "string"}})
    del schema["properties"]
    with pytest.raises(JSONSchemaToPySparkError, match="did not resolve to a StructType"):
        jsonschema_to_pyspark_converter.convert(schema)


def test_convert_pass_simple_object(jsonschema_to_pyspark_converter: JSONSchemaToPySpark) -> None:
    """convert() produces a StructType with correct nullability and empty metadata for a flat object schema."""
    schema = base_object_schema(
        properties={
            "name": {"type": "string"},
            "age": {"type": "integer"},
        },
        required=["name"],
    )
    result = jsonschema_to_pyspark_converter.convert(schema)
    assert result == StructType(
        [
            StructField("name", StringType(), nullable=False, metadata={}),
            StructField("age", LongType(), nullable=True, metadata={}),
        ]
    )


def test_convert_pass_root_schema_without_type_keyword_infers_object(
    jsonschema_to_pyspark_converter: JSONSchemaToPySpark,
) -> None:
    """convert() succeeds for a root schema that omits 'type' entirely but declares 'properties' (valid JSON Schema)."""
    schema = {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "properties": {"a": {"type": "string"}},
    }
    result = jsonschema_to_pyspark_converter.convert(schema)
    assert result == StructType([StructField("a", StringType(), nullable=True, metadata={})])


@pytest.mark.parametrize("value", [["a"], ("a",), {"a"}])
def test_json_schema_to_pyspark_pass_extra_metadata_keywords_accepts_list_tuple_set(
    value: list[str] | tuple[str] | set[str],
) -> None:
    """JSONSchemaToPySpark(extra_metadata_keywords=...) accepts list/tuple/set and stores a frozenset."""
    converter = JSONSchemaToPySpark(extra_metadata_keywords=value)  # pyright: ignore[reportArgumentType]
    assert converter.extra_metadata_keywords == frozenset({"a"})


"""End-to-end test covering every JSON Schema construct the converter supports,
in a single schema, asserting the exact resulting StructType.
"""


def _all_types_schema() -> dict[str, Any]:
    """Build a schema covering every JSON Schema construct handled by the converter."""
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "title": "AllTypes",
        "properties": {
            # primitives
            "a_string": {"type": "string", "title": "A String", "description": "a plain string"},
            "a_formatted_date": {"type": "string", "format": "date"},
            "a_formatted_datetime": {"type": "string", "format": "date-time"},
            "a_formatted_uuid": {"type": "string", "format": "uuid"},
            "a_small_integer": {"type": "integer", "minimum": 0, "maximum": 100},
            "a_large_integer": {"type": "integer", "minimum": 0, "maximum": 99_999_999_999},
            "an_unbounded_integer": {"type": "integer"},
            "a_number": {"type": "number"},
            "a_decimal_number": {"type": "number", "multipleOf": 0.01},
            "a_boolean": {"type": "boolean"},
            "a_null": {"type": "null"},
            # an enum (typeless)
            "a_string_enum": {"enum": ["red", "green", "blue"]},
            "an_integer_enum": {"enum": [1, 2, 3]},
            "a_boolean_enum": {"enum": [True, False]},
            # nullable union
            "a_nullable_string": {"type": ["string", "null"]},
            # array
            "a_string_array": {"type": "array", "items": {"type": "string"}},
            "a_nested_array": {
                "type": "array",
                "items": {"type": "array", "items": {"type": "integer"}},
            },
            # nested object (fixed properties)
            "a_nested_object": {
                "type": "object",
                "properties": {
                    "inner_field": {"type": "string"},
                },
                "required": ["inner_field"],
            },
            # object -> MapType (dynamic keys)
            "a_string_map": {"type": "object", "additionalProperties": {"type": "string"}},
            "a_pattern_property_map": {
                "type": "object",
                "patternProperties": {"^x-": {"type": "integer"}},
            },
            # oneOf / anyOf approximated by first branch
            "a_one_of_field": {"oneOf": [{"type": "integer"}, {"type": "string"}]},
            "an_any_of_field": {"anyOf": [{"type": "boolean"}, {"type": "string"}]},
        },
        "required": [
            "a_string",
            "a_small_integer",
        ],
    }


def test_convert_e2e_pass_all_supported_types_produce_expected_structtype() -> None:
    """A schema exercising every supported JSON Schema construct converts to the exact expected StructType."""
    schema = _all_types_schema()
    dereferenced = dereference_schema(schema)
    result = JSONSchemaToPySpark().convert(dereferenced)

    expected = StructType(
        [
            StructField(
                "a_string",
                StringType(),
                nullable=False,
                metadata={"jsonschema": {"title": "A String"}, "comment": "a plain string"},
            ),
            StructField("a_formatted_date", DateType(), nullable=True, metadata={}),
            StructField("a_formatted_datetime", TimestampType(), nullable=True, metadata={}),
            StructField("a_formatted_uuid", StringType(), nullable=True, metadata={}),
            StructField("a_small_integer", IntegerType(), nullable=False, metadata={}),
            StructField("a_large_integer", LongType(), nullable=True, metadata={}),
            StructField("an_unbounded_integer", LongType(), nullable=True, metadata={}),
            StructField("a_number", DoubleType(), nullable=True, metadata={}),
            StructField("a_decimal_number", DecimalType(38, 2), nullable=True, metadata={}),
            StructField("a_boolean", BooleanType(), nullable=True, metadata={}),
            StructField("a_null", NullType(), nullable=True, metadata={}),
            StructField("a_string_enum", StringType(), nullable=True, metadata={}),
            StructField("an_integer_enum", LongType(), nullable=True, metadata={}),
            StructField("a_boolean_enum", BooleanType(), nullable=True, metadata={}),
            StructField("a_nullable_string", StringType(), nullable=True, metadata={}),
            StructField("a_string_array", ArrayType(StringType(), containsNull=True), nullable=True, metadata={}),
            StructField(
                "a_nested_array",
                ArrayType(ArrayType(LongType(), containsNull=True), containsNull=True),
                nullable=True,
                metadata={},
            ),
            StructField(
                "a_nested_object",
                StructType([StructField("inner_field", StringType(), nullable=False, metadata={})]),
                nullable=True,
                metadata={},
            ),
            StructField(
                "a_string_map", MapType(StringType(), StringType(), valueContainsNull=True), nullable=True, metadata={}
            ),
            StructField(
                "a_pattern_property_map",
                MapType(StringType(), LongType(), valueContainsNull=True),
                nullable=True,
                metadata={},
            ),
            StructField("a_one_of_field", LongType(), nullable=True, metadata={}),
            StructField("an_any_of_field", BooleanType(), nullable=True, metadata={}),
        ]
    )
    assert result == expected


def test_convert_e2e_pass_all_supported_types_field_names_match_schema_properties() -> None:
    """The resulting StructType's field names exactly match the schema's 'properties' keys, in order."""
    schema = _all_types_schema()
    result = JSONSchemaToPySpark().convert(schema)
    assert result.fieldNames() == list(schema["properties"].keys())


def test_convert_e2e_pass_all_supported_types_required_fields_are_non_nullable() -> None:
    """Every field listed in the schema's root 'required' array is non-nullable in the result; all others are nullable."""
    schema = _all_types_schema()
    result = JSONSchemaToPySpark().convert(schema)
    required = set(schema["required"])
    for field in result.fields:
        assert field.nullable is (field.name not in required)


def test_convert_e2e_pass_extra_metadata_keywords_take_effect_via_convert() -> None:
    """A converter's extra_metadata_keywords reach _build_metadata correctly when driven through convert()."""
    converter = JSONSchemaToPySpark(extra_metadata_keywords={"x-pii"})  # pyright: ignore[reportArgumentType]
    schema = base_object_schema(properties={"ssn": {"type": "string", "x-pii": True}})
    result = converter.convert(schema)
    assert result["ssn"].metadata == {"jsonschema": {"x-pii": True}}


def test_convert_e2e_pass_property_without_type_keyword_infers_object() -> None:
    """A nested property schema that omits 'type' but declares 'properties' still converts to a StructType."""
    schema = {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "properties": {
            "address": {
                "properties": {"city": {"type": "string"}},
                "required": ["city"],
            },
        },
    }
    result = JSONSchemaToPySpark().convert(schema)
    assert result["address"].dataType == StructType([StructField("city", StringType(), nullable=False, metadata={})])
