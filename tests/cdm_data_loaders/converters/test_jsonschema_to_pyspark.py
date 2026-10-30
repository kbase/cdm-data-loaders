"""Unit tests for jsonschema_to_pyspark."""

# ruff: noqa: SLF001
import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest
import yaml
from pydantic import ValidationError
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
    InvalidJSONSchemaError,
    JSONSchemaToPySpark,
    JSONSchemaToPySparkError,
)
from cdm_data_loaders.utils.jsonschema.dereferencer import dereference_schema
from tests.cdm_data_loaders.converters.conftest import base_object_schema


def test_json_schema_to_pyspark_fail_instance_is_frozen(jsonschema_to_pyspark_converter: JSONSchemaToPySpark) -> None:
    """Assigning to a JSONSchemaToPySpark instance attribute raises ValidationError (frozen model)."""
    with pytest.raises(ValidationError, match="Instance is frozen"):
        jsonschema_to_pyspark_converter.treat_unknown_as_string = False


"""convert"""


def test_convert_fail_missing_schema_keyword(jsonschema_to_pyspark_converter: JSONSchemaToPySpark) -> None:
    """convert() rejects a schema with no top-level '$schema' keyword."""
    schema = {"type": "object", "properties": {}}
    with pytest.raises(InvalidJSONSchemaError, match="missing a '\\$schema'"):
        jsonschema_to_pyspark_converter.convert(schema)


@pytest.mark.parametrize(
    "root_type",
    ["string", "array", "integer", "number", "boolean", "null"],
)
def test_convert_fail_non_object_root_type(
    jsonschema_to_pyspark_converter: JSONSchemaToPySpark, root_type: str
) -> None:
    """convert() rejects any root schema whose declared 'type' isn't 'object'."""
    schema = base_object_schema(type=root_type)
    with pytest.raises(JSONSchemaToPySparkError, match="must be of type 'object'"):
        jsonschema_to_pyspark_converter.convert(schema)


def test_convert_fail_unresolved_ref_at_root(jsonschema_to_pyspark_converter: JSONSchemaToPySpark) -> None:
    """convert() refuses a root schema containing an un-dereferenced $ref."""
    schema = base_object_schema(**{"$ref": "#/$defs/Foo"})
    del schema["type"]
    with pytest.raises(JSONSchemaToPySparkError, match="unresolved \\$ref"):
        jsonschema_to_pyspark_converter.convert(schema)


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


def test_convert_fail_list_form_object_root_type_rejected(jsonschema_to_pyspark_converter: JSONSchemaToPySpark) -> None:
    """convert() rejects a list-form root 'type' (e.g. ['object', 'null']) even though it contains 'object'.

    `JsonSchemaReader.read()` itself accepts this nullable-root form when used
    directly, but the facade's `require_object_root` guard is intentionally
    stricter and runs first, so this form is only usable via the reader.
    """
    schema = base_object_schema(type=["object", "null"])
    with pytest.raises(JSONSchemaToPySparkError, match="must be of type 'object'"):
        jsonschema_to_pyspark_converter.convert(schema)


"""convert_from_string / convert_from_file"""


def test_convert_from_string_pass_valid_json(jsonschema_to_pyspark_converter: JSONSchemaToPySpark) -> None:
    """convert_from_string() parses JSON text and converts it identically to convert()."""
    schema = base_object_schema(properties={"id": {"type": "integer"}})
    result = jsonschema_to_pyspark_converter.convert_from_string(json.dumps(schema))
    assert result == jsonschema_to_pyspark_converter.convert(schema)


def test_convert_from_string_fail_invalid_json(jsonschema_to_pyspark_converter: JSONSchemaToPySpark) -> None:
    """convert_from_string() propagates a JSONDecodeError for malformed JSON text."""
    with pytest.raises(json.JSONDecodeError):
        jsonschema_to_pyspark_converter.convert_from_string("{not valid json")


@pytest.mark.parametrize(
    ("suffix", "dumper"),
    [(".json", json.dumps), (".yaml", yaml.safe_dump)],
)
def test_convert_from_file_pass_supported_extensions(
    jsonschema_to_pyspark_converter: JSONSchemaToPySpark,
    tmp_path: Path,
    suffix: str,
    dumper: Callable[[dict[str, Any]], str],
) -> None:
    """convert_from_file() loads both .json and non-.json (YAML) files correctly."""
    schema = base_object_schema(properties={"id": {"type": "integer"}})
    path = tmp_path / f"schema{suffix}"
    path.write_text(dumper(schema))
    result = jsonschema_to_pyspark_converter.convert_from_file(str(path))
    assert result == jsonschema_to_pyspark_converter.convert(schema)


def test_convert_from_file_fail_missing_file(
    jsonschema_to_pyspark_converter: JSONSchemaToPySpark, tmp_path: Path
) -> None:
    """convert_from_file() raises FileNotFoundError for a nonexistent path."""
    missing = tmp_path / "does_not_exist.json"
    with pytest.raises(FileNotFoundError):
        jsonschema_to_pyspark_converter.convert_from_file(str(missing))


"""_merge_with_builtin_format_map"""


def test_merge_with_builtin_format_map_pass_merges_user_overrides_over_defaults() -> None:
    """_merge_with_builtin_format_map() merges a non-empty user mapping on top of DEFAULT_FORMAT_MAP."""
    result = JSONSchemaToPySpark._merge_with_builtin_format_map({"date-time": StringType()})
    assert result["date-time"] == StringType()  # pyright: ignore[reportIndexIssue]

    # untouched default entry survives
    assert result["date"] == DateType()  # pyright: ignore[reportIndexIssue]


@pytest.mark.parametrize("value", [None, {}, "", 0])
def test_merge_with_builtin_format_map_pass_falsy_value_passed_through_unchanged(
    value: dict | str | int | None,
) -> None:
    """_merge_with_builtin_format_map() passes falsy values straight through without merging."""
    assert JSONSchemaToPySpark._merge_with_builtin_format_map(value) == value


def test_merge_with_builtin_format_map_pass_non_dict_value_passed_through_unchanged() -> None:
    """_merge_with_builtin_format_map() passes non-dict/non-frozendict values through unchanged."""
    sentinel = object()
    assert JSONSchemaToPySpark._merge_with_builtin_format_map(sentinel) is sentinel


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ({"a", "b"}, frozenset({"a", "b"})),
        (["a", "b"], frozenset({"a", "b"})),
        (("a", "b"), frozenset({"a", "b"})),
        (frozenset({"a", "b"}), frozenset({"a", "b"})),
    ],
)
def test_coerce_extra_metadata_keywords_pass_coerces_set_list_tuple_to_frozenset(
    value: list | tuple | set | frozenset, expected: frozenset[str]
) -> None:
    """_coerce_extra_metadata_keywords() coerces set/list/tuple inputs (and passes frozenset through) unchanged in value."""
    assert JSONSchemaToPySpark._coerce_extra_metadata_keywords(value) == expected


def test_coerce_extra_metadata_keywords_pass_frozenset_identity_preserved() -> None:
    """_coerce_extra_metadata_keywords() returns the exact same frozenset object when already a frozenset."""
    value = frozenset({"a"})
    assert JSONSchemaToPySpark._coerce_extra_metadata_keywords(value) is value


def test_coerce_extra_metadata_keywords_pass_other_type_passed_through_unchanged() -> None:
    """_coerce_extra_metadata_keywords() passes non-set/list/tuple/frozenset values through unchanged."""
    sentinel = object()
    assert JSONSchemaToPySpark._coerce_extra_metadata_keywords(sentinel) is sentinel


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
