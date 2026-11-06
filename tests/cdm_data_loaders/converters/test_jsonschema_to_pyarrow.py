"""Unit tests for jsonschema_to_pyarrow."""

import json
from datetime import UTC, date, datetime, time
from decimal import Decimal
from typing import Any

import pyarrow as pa
import pytest

from cdm_data_loaders.converters.jsonschema_to_pyarrow import (
    JSONSchemaToPyArrow,
    JSONSchemaToPyArrowError,
    coerce_record_for_pyarrow,
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


def test_coerce_record_for_pyarrow_pass_temporal_decimal_and_nested_types() -> None:
    """JSON-parsed values are coerced to match temporal, decimal, numeric, struct, list, map, and string fields."""
    pa_schema = pa.schema(
        [
            pa.field("day", pa.date32()),
            pa.field("ts_z", pa.timestamp("us", tz="UTC")),
            pa.field("ts_naive", pa.timestamp("us", tz="UTC")),
            pa.field("tod", pa.time64("us")),
            pa.field("price", pa.decimal128(38, 2)),
            pa.field("ratio", pa.float64()),
            pa.field("whole", pa.int64()),
            pa.field("collapsed_obj", pa.string()),
            pa.field("collapsed_num", pa.string()),
            pa.field("nested", pa.struct([pa.field("when", pa.date32())])),
            pa.field("items", pa.list_(pa.decimal128(38, 1))),
            pa.field("attrs", pa.map_(pa.string(), pa.date32())),
        ]
    )
    raw_record: dict[str, Any] = {
        "day": "2024-02-29",
        "ts_z": "2024-02-29T12:30:00Z",
        "ts_naive": "2024-02-29T12:30:00",
        "tod": "08:15:30",
        "price": 19.95,
        "ratio": 2,
        "whole": 7.0,
        "collapsed_obj": {"a": 1},
        "collapsed_num": 42,
        "nested": {"when": "2024-03-01", "extra": "dropped"},
        "items": [1.5, 2],
        "attrs": {"start": "2024-01-01"},
        "undeclared_top": "dropped",
    }

    coerced = coerce_record_for_pyarrow(raw_record, pa_schema)

    assert coerced == {
        "day": date(2024, 2, 29),
        "ts_z": datetime(2024, 2, 29, 12, 30, tzinfo=UTC),
        "ts_naive": datetime(2024, 2, 29, 12, 30, tzinfo=UTC),
        "tod": time(8, 15, 30),
        "price": Decimal("19.95"),
        "ratio": 2.0,
        "whole": 7,
        "collapsed_obj": json.dumps({"a": 1}),
        "collapsed_num": "42",
        "nested": {"when": date(2024, 3, 1)},
        "items": [Decimal("1.5"), Decimal(2)],
        "attrs": {"start": date(2024, 1, 1)},
    }
    table = pa.Table.from_pylist([coerced], schema=pa_schema)
    assert table.num_rows == 1
    assert table.schema == pa_schema


BOUNDARY_PA_SCHEMA = pa.schema(
    [
        pa.field("day", pa.date32()),
        pa.field("ts_naive_target", pa.timestamp("us")),
        pa.field("tod", pa.time64("us")),
        pa.field("zero_dec", pa.decimal128(38, 2)),
        pa.field("neg_dec", pa.decimal128(38, 2)),
        pa.field("neg_float", pa.float64()),
        pa.field("neg_int", pa.int64()),
        pa.field("large_text", pa.large_string()),
        pa.field("collapsed_list", pa.large_string()),
        pa.field("nested", pa.struct([pa.field("when", pa.date32())])),
        pa.field("large_items", pa.large_list(pa.decimal128(38, 1))),
        pa.field("attrs", pa.map_(pa.string(), pa.date32())),
    ]
)


@pytest.mark.parametrize(
    ("record", "expected"),
    [
        pytest.param({}, {}, id="empty-dict"),
        pytest.param(
            dict.fromkeys(BOUNDARY_PA_SCHEMA.names),
            dict.fromkeys(BOUNDARY_PA_SCHEMA.names),
            id="all-nulls",
        ),
        pytest.param(
            {
                "day": date(2024, 1, 1),
                "ts_naive_target": "2024-02-29T12:30:00",
                "tod": time(0, 0, 0),
                "zero_dec": 0,
                "neg_dec": "-0.05",
                "neg_float": -3,
                "neg_int": -7.0,
                "large_text": "already-string",
                "collapsed_list": [1, "two"],
                "nested": {"when": None},
                "large_items": [None, Decimal("-1.5")],
                "attrs": {"none_val": None},
            },
            {
                "day": date(2024, 1, 1),
                "ts_naive_target": datetime(2024, 2, 29, 12, 30),  # noqa: DTZ001
                "tod": time(0, 0, 0),
                "zero_dec": Decimal(0),
                "neg_dec": Decimal("-0.05"),
                "neg_float": -3.0,
                "neg_int": -7,
                "large_text": "already-string",
                "collapsed_list": json.dumps([1, "two"]),
                "nested": {"when": None},
                "large_items": [None, Decimal("-1.5")],
                "attrs": {"none_val": None},
            },
            id="boundary-values",
        ),
        pytest.param(
            {"nested": {}, "large_items": [], "attrs": {}},
            {"nested": {}, "large_items": [], "attrs": {}},
            id="empty-containers",
        ),
    ],
)
def test_coerce_record_for_pyarrow_pass_nulls_empty_and_boundary_values(
    record: dict[str, Any],
    expected: dict[str, Any],
) -> None:
    """Nulls, empty containers, zero/negative numbers, tz-naive timestamps, large types, and pre-coerced values pass."""
    coerced = coerce_record_for_pyarrow(record, BOUNDARY_PA_SCHEMA)
    assert coerced == expected
    table = pa.Table.from_pylist([coerced], schema=BOUNDARY_PA_SCHEMA)
    assert table.num_rows == 1


def test_coerce_record_for_pyarrow_pass_preserves_boolean_and_non_integral_float_guards() -> None:
    """Booleans are not coerced to numeric/decimal types, and non-integral floats are not truncated to int."""
    pa_schema = pa.schema(
        [
            pa.field("dec", pa.decimal128(38, 2)),
            pa.field("flt", pa.float64()),
            pa.field("integer_from_bool", pa.int64()),
            pa.field("integer_from_frac", pa.int64()),
        ]
    )
    fractional_value = 7.5
    raw = {
        "dec": True,
        "flt": False,
        "integer_from_bool": True,
        "integer_from_frac": fractional_value,
    }
    coerced = coerce_record_for_pyarrow(raw, pa_schema)
    assert coerced == raw
    assert isinstance(coerced["dec"], bool)
    assert isinstance(coerced["flt"], bool)
    assert isinstance(coerced["integer_from_bool"], bool)
    assert coerced["integer_from_frac"] == fractional_value
