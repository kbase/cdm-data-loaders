"""Unit tests for jsonschema_to_pyarrow."""

import pyarrow as pa
import pytest

from cdm_data_loaders.converters.jsonschema_to_pyarrow import (
    JSONSchemaToPyArrow,
    JSONSchemaToPyArrowError,
)
from cdm_data_loaders.utils.jsonschema.dereferencer import dereference_schema
from tests.cdm_data_loaders.converters.conftest import base_object_schema


def test_convert_fail_root_resolves_to_map(jsonschema_to_pyarrow_converter: JSONSchemaToPyArrow) -> None:
    """convert() rejects a root schema that has no 'properties' and resolves to a map."""
    schema = base_object_schema(patternProperties={"^x-": {"type": "string"}})
    del schema["properties"]
    with pytest.raises(JSONSchemaToPyArrowError, match="did not resolve to a struct"):
        jsonschema_to_pyarrow_converter.convert(schema)


def test_convert_fail_strict_unknown_type_is_wrapped() -> None:
    """Emitter errors surface as the facade's own error type."""
    schema = base_object_schema(properties={"value": {"type": ["string", "integer"]}})
    with pytest.raises(JSONSchemaToPyArrowError, match="multi-type union"):
        JSONSchemaToPyArrow(treat_unknown_as_string=False).convert(schema)


def test_convert_pass_simple_object(jsonschema_to_pyarrow_converter: JSONSchemaToPyArrow) -> None:
    """convert() produces a schema with correct nullability and no metadata for a flat object."""
    schema = base_object_schema(properties={"name": {"type": "string"}, "age": {"type": "integer"}}, required=["name"])
    assert jsonschema_to_pyarrow_converter.convert(schema) == pa.schema(
        [pa.field("name", pa.string(), nullable=False), pa.field("age", pa.int64())]
    )


def test_convert_pass_root_without_type_infers_object(jsonschema_to_pyarrow_converter: JSONSchemaToPyArrow) -> None:
    """convert() accepts a root that omits 'type' but declares 'properties'."""
    schema = {"$schema": "https://json-schema.org/draft/2020-12/schema", "properties": {"a": {"type": "string"}}}
    assert jsonschema_to_pyarrow_converter.convert(schema) == pa.schema([pa.field("a", pa.string())])


def test_convert_pass_nested_schema() -> None:
    """Nested objects, arrays, dynamic maps, formats and decimals convert together."""
    schema = base_object_schema(
        properties={
            "id": {"type": "integer", "minimum": 0, "maximum": 10, "title": "Identifier"},
            "created": {"type": "string", "format": "date-time"},
            "price": {"type": "number", "multipleOf": 0.01},
            "tags": {"type": "array", "items": {"type": "string"}},
            "labels": {"type": "object", "additionalProperties": {"type": "string"}},
            "owner": {
                "type": "object",
                "properties": {"name": {"type": "string"}, "active": {"type": "boolean"}},
                "required": ["name"],
            },
        },
        required=["id"],
    )
    schema_out = JSONSchemaToPyArrow().convert(schema)
    assert schema_out == pa.schema(
        [
            pa.field("id", pa.int32(), nullable=False),
            pa.field("created", pa.timestamp("us", tz="UTC")),
            pa.field("price", pa.decimal128(38, 2)),
            pa.field("tags", pa.list_(pa.string())),
            pa.field("labels", pa.map_(pa.string(), pa.string())),
            pa.field(
                "owner",
                pa.struct([pa.field("name", pa.string(), nullable=False), pa.field("active", pa.bool_())]),
            ),
        ]
    )
    assert schema_out.field("id").metadata == {b"jsonschema": b'{"title": "Identifier"}'}


def test_convert_pass_dereferenced_schema() -> None:
    """A dereferenced $ref schema converts using the referenced definition."""
    schema = base_object_schema(
        properties={"owner": {"$ref": "#/$defs/Person"}},
        **{"$defs": {"Person": {"type": "object", "properties": {"name": {"type": "string"}}}}},
    )
    result = JSONSchemaToPyArrow().convert(dereference_schema(schema))
    assert result == pa.schema([pa.field("owner", pa.struct([pa.field("name", pa.string())]))])


def test_convert_to_table_pass_empty_table(jsonschema_to_pyarrow_converter: JSONSchemaToPyArrow) -> None:
    """convert_to_table() returns a zero-row table with the converted schema."""
    schema = base_object_schema(properties={"id": {"type": "integer"}, "tags": {"type": "array"}}, required=["id"])
    table = jsonschema_to_pyarrow_converter.convert_to_table(schema)
    assert table.num_rows == 0
    assert table.schema == jsonschema_to_pyarrow_converter.convert(schema)
    assert table.schema.field("id").nullable is False
    assert table.append_column("extra", pa.array([], pa.string())).column_names == ["id", "tags", "extra"]


def test_convert_to_table_pass_accepts_rows(jsonschema_to_pyarrow_converter: JSONSchemaToPyArrow) -> None:
    """The emitted schema can build a populated table with the matching nested values."""
    schema = base_object_schema(
        properties={"id": {"type": "integer"}, "tags": {"type": "array", "items": {"type": "string"}}},
    )
    arrow_schema = jsonschema_to_pyarrow_converter.convert(schema)
    table = pa.Table.from_pylist([{"id": 1, "tags": ["a", "b"]}, {"id": 2, "tags": None}], schema=arrow_schema)
    assert table.to_pylist() == [{"id": 1, "tags": ["a", "b"]}, {"id": 2, "tags": None}]


def test_jsonschema_to_pyarrow_pass_options_reach_emitter() -> None:
    """Format overrides, metadata keywords, union metadata and nesting limits configure the emitter."""
    converter = JSONSchemaToPyArrow(
        format_map={"date-time": pa.string()},
        extra_metadata_keywords=["pattern"],
        emit_unions_as_json=True,
        max_nesting=1,
    )
    schema = base_object_schema(
        properties={
            "when": {"type": "string", "format": "date-time", "pattern": "^2"},
            "either": {"type": ["string", "integer"]},
            "nested": {"type": "object", "properties": {"x": {"type": "integer"}}},
        }
    )
    result = converter.convert(schema)
    assert converter.extra_metadata_keywords == frozenset({"pattern"})
    assert result.field("when").type == pa.string()
    assert result.field("when").metadata == {b"jsonschema": b'{"pattern": "^2"}'}
    assert result.field("either").metadata == {b"original_union": b'["string", "integer"]'}
    assert result.field("nested").type == pa.string()
