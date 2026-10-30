"""Pure schema-object checks for the Arrow emitter."""

import json
from decimal import Decimal
from typing import Any

import pyarrow as pa
import pytest
from jsonschema import Draft202012Validator
from pyiceberg import types as iceberg_types
from pyiceberg.schema import Schema

from cdm_data_loaders.converters.core.ir import Field, NodeHints, NodeType, Provenance, SchemaDocument, TypedNode
from cdm_data_loaders.converters.emitters.pyarrow import (
    PyArrowEmitter,
    PyArrowEmitterError,
    infer_type_from_enum,
    merge_format_map,
)
from cdm_data_loaders.converters.readers.dlt import DltReader
from cdm_data_loaders.converters.readers.iceberg import IcebergReader
from cdm_data_loaders.converters.readers.json_schema import JsonSchemaReader

DIALECT = "https://json-schema.org/draft/2020-12/schema"


def _document(schema: dict[str, Any] | bool) -> SchemaDocument:
    """Wrap a fragment in an object-compatible JSON document."""
    return JsonSchemaReader().read({"$schema": DIALECT, "properties": {"value": schema}})


def _value_schema(data_type: pa.DataType) -> pa.Schema:
    """Build the expected one-column schema produced by `_document`."""
    return pa.schema([pa.field("value", data_type, nullable=True)])


def _list(element: pa.DataType, *, nullable: bool = True) -> pa.ListType:
    """Build a list type with the emitter's element field name."""
    return pa.list_(pa.field("item", element, nullable=nullable))


def _map(value: pa.DataType, *, nullable: bool = True) -> pa.MapType:
    """Build a string-keyed map type with the emitter's value field name."""
    return pa.map_(pa.string(), pa.field("value", value, nullable=nullable))


def test_emit_pass_simple_object() -> None:
    """A flat object produces fields with presence-driven nullability and no metadata."""
    document = JsonSchemaReader().read(
        {
            "$schema": DIALECT,
            "type": "object",
            "properties": {"name": {"type": "string"}, "age": {"type": "integer"}},
            "required": ["name"],
        }
    )
    schema = PyArrowEmitter().emit(document)
    assert schema == pa.schema([pa.field("name", pa.string(), nullable=False), pa.field("age", pa.int64())])
    assert [schema.field(name).metadata for name in schema.names] == [None, None]


def test_emit_table_pass_empty_table_with_schema() -> None:
    """emit_table returns a zero-row table whose schema equals the emitted schema."""
    document = JsonSchemaReader().read(
        {"$schema": DIALECT, "properties": {"id": {"type": "integer"}, "tags": {"type": "array"}}, "required": ["id"]}
    )
    emitter = PyArrowEmitter()
    table = emitter.emit_table(document)
    assert table.num_rows == 0
    assert table.schema == emitter.emit(document)
    assert table.schema.field("id").nullable is False


def test_emit_pass_ipc_round_trip() -> None:
    """Serialized emitted schemas, including field metadata, deserialize through Arrow's IPC reader."""
    document = JsonSchemaReader().read(
        {
            "$schema": DIALECT,
            "type": "object",
            "properties": {
                "id": {"type": "integer", "title": "Identifier"},
                "tags": {"type": "array", "items": {"type": "string"}},
                "attrs": {"type": "object", "additionalProperties": {"type": "string"}},
            },
            "required": ["id"],
        }
    )
    emitted = PyArrowEmitter().emit(document)
    restored = pa.ipc.read_schema(pa.py_buffer(emitted.serialize()))
    assert restored.equals(emitted, check_metadata=True)


@pytest.mark.parametrize("strict", [False, True], ids=["fallback-policy", "strict-policy"])
@pytest.mark.parametrize(
    ("schema", "expected"),
    [
        pytest.param({"type": "string"}, pa.string(), id="string"),
        pytest.param({"type": "boolean"}, pa.bool_(), id="boolean"),
        pytest.param({"type": "null"}, pa.null(), id="null"),
        pytest.param({"type": "integer"}, pa.int64(), id="integer"),
        pytest.param({"type": "number"}, pa.float64(), id="number"),
        pytest.param({"type": "object"}, pa.struct([]), id="empty-object"),
        pytest.param({"type": ["null", "string"]}, pa.string(), id="nullable-string"),
        pytest.param({"type": ["integer"]}, pa.int64(), id="single-type-list"),
        pytest.param({"type": ["null", "null"]}, pa.null(), id="null-only-list"),
        pytest.param({"type": []}, pa.null(), id="empty-type-list"),
        pytest.param({"enum": [True, False]}, pa.bool_(), id="boolean-enum"),
        pytest.param({"enum": [1, 2]}, pa.int64(), id="integer-enum"),
        pytest.param({"enum": [1, 2.5]}, pa.float64(), id="number-enum"),
        pytest.param({"enum": ["a", "b"]}, pa.string(), id="string-enum"),
        pytest.param({"enum": []}, pa.string(), id="empty-enum"),
        pytest.param({"enum": [True, 1]}, pa.string(), id="bool-is-not-integer-enum"),
        pytest.param({"enum": [1], "minimum": 0, "oneOf": [{"type": "string"}]}, pa.int64(), id="enum-precedence"),
        pytest.param({"type": "string", "enum": [1]}, pa.string(), id="declared-over-enum"),
        pytest.param({"oneOf": [{"type": "integer"}, {"type": "string"}]}, pa.int64(), id="oneof-first"),
        pytest.param({"anyOf": [{"type": "null"}, {"type": "string"}]}, pa.null(), id="anyof-null-first"),
        pytest.param(
            {"oneOf": [{"type": "boolean"}], "anyOf": [{"type": "integer"}]}, pa.bool_(), id="oneof-over-anyof"
        ),
        pytest.param({"type": "mystery", "anyOf": [{"type": "boolean"}]}, pa.bool_(), id="unknown-with-combiner"),
        pytest.param({"format": "date", "oneOf": [False]}, pa.date32(), id="inferred-over-combiner"),
        pytest.param({"items": {"type": "integer"}, "pattern": "a"}, _list(pa.int64()), id="array-before-string"),
        pytest.param({"pattern": "a", "multipleOf": 0.01}, pa.string(), id="string-before-number"),
        pytest.param({"minimum": 0, "maximum": 10}, pa.float64(), id="numeric-inference"),
        pytest.param({"multipleOf": 0.01}, pa.decimal128(38, 2), id="decimal-inference"),
        pytest.param({"required": ["missing"]}, pa.struct([]), id="required-implies-object"),
        pytest.param({"uniqueItems": True}, _list(pa.string()), id="uniqueness-implies-array"),
        pytest.param({"type": "array", "prefixItems": []}, _list(pa.string()), id="empty-prefix"),
        pytest.param({"type": "array", "prefixItems": [False]}, _list(pa.string()), id="false-prefix"),
        pytest.param({"type": "array", "prefixItems": [{}]}, _list(pa.string()), id="empty-prefix-schema"),
        pytest.param(
            {"type": "array", "prefixItems": [{"type": "integer"}, False], "items": True},
            _list(pa.int64()),
            id="prefix-before-items",
        ),
        pytest.param({"type": "array", "items": [{"type": "boolean"}, False]}, _list(pa.bool_()), id="tuple-items"),
        pytest.param({"type": "array", "items": [False]}, _list(pa.string()), id="false-tuple"),
        pytest.param(
            {"type": "array", "items": {"type": "array", "items": {"type": "integer"}}},
            _list(_list(pa.int64())),
            id="nested-arrays",
        ),
        pytest.param({"type": "object", "additionalProperties": True}, pa.struct([]), id="additional-true"),
        pytest.param({"type": "object", "additionalProperties": False}, pa.struct([]), id="additional-false"),
        pytest.param(
            {"type": "object", "additionalProperties": {"type": "integer"}},
            _map(pa.int64()),
            id="additional-schema",
        ),
        pytest.param(
            {"patternProperties": {"a": {"type": "boolean"}, "b": False}, "additionalProperties": False},
            _map(pa.bool_()),
            id="first-pattern",
        ),
        pytest.param(
            {"patternProperties": {"a": {"type": "integer"}}, "additionalProperties": {"type": "boolean"}},
            _map(pa.int64()),
            id="pattern-before-additional",
        ),
        pytest.param(
            {"properties": {"name": {"type": "string"}}, "patternProperties": {"a": False}, "additionalProperties": {}},
            pa.struct([pa.field("name", pa.string())]),
            id="fixed-before-dynamic",
        ),
        pytest.param({"type": "integer", "minimum": 0, "maximum": 100}, pa.int32(), id="bounded-int"),
        pytest.param(
            {"type": "integer", "minimum": -2_147_483_648, "maximum": 2_147_483_647},
            pa.int32(),
            id="int32-boundaries",
        ),
        pytest.param({"type": "integer", "minimum": -2_147_483_649, "maximum": 100}, pa.int64(), id="below-int32"),
        pytest.param({"type": "integer", "minimum": 0, "maximum": 2_147_483_648}, pa.int64(), id="above-int32"),
        pytest.param({"type": "integer", "minimum": 0}, pa.int64(), id="missing-maximum"),
        pytest.param({"type": "integer", "maximum": 0}, pa.int64(), id="missing-minimum"),
        pytest.param(
            {"type": "integer", "exclusiveMinimum": 0, "exclusiveMaximum": 100}, pa.int32(), id="exclusive-bounds"
        ),
        pytest.param({"type": "number", "multipleOf": 1}, pa.decimal128(38, 0), id="integral-multiple"),
        pytest.param({"type": "number", "multipleOf": 0.001}, pa.decimal128(38, 3), id="fractional-multiple"),
        pytest.param({"type": "number", "multipleOf": 1e-9}, pa.decimal128(38, 9), id="exponent-multiple"),
        pytest.param({"type": "number", "multipleOf": "0.01"}, pa.float64(), id="nonnumeric-multiple"),
        pytest.param({"type": "number", "multipleOf": Decimal("0.0100")}, pa.decimal128(38, 4), id="decimal-multiple"),
        pytest.param({"type": "string", "format": "date"}, pa.date32(), id="date-format"),
        pytest.param({"type": "string", "format": "date-time"}, pa.timestamp("us", tz="UTC"), id="date-time-format"),
        pytest.param({"type": "string", "format": "uuid"}, pa.string(), id="uuid-format"),
        pytest.param({"type": "string", "format": "unknown"}, pa.string(), id="unknown-format"),
    ],
)
def test_emit_pass_json_dispatch(schema: dict[str, Any], expected: pa.DataType, strict: bool) -> None:
    """Typed dispatch matches fixed expected schemas under both unknown-type policies."""
    emitter = PyArrowEmitter(treat_unknown_as_string=not strict)
    assert emitter.emit(_document(schema)) == _value_schema(expected)


def test_emit_node_pass_map_node() -> None:
    """Map nodes keep their distinct key and value types and value nullability."""
    node = TypedNode(
        type="map",
        key_type=TypedNode(type="integer", hints=NodeHints(logical_type="int", bit_width=32)),
        value_type=TypedNode(type="string", nullable=False, provenance=Provenance("iceberg")),
    )
    result = PyArrowEmitter().emit_node(node)
    assert result == pa.map_(pa.int32(), pa.field("value", pa.string(), nullable=False))
    assert result.item_field.nullable is False


@pytest.mark.parametrize(
    ("hints", "node_type", "expected"),
    [
        pytest.param(
            NodeHints(logical_type="decimal", precision=20, scale=8), "number", pa.decimal128(20, 8), id="dec"
        ),
        pytest.param(
            NodeHints(logical_type="decimal", precision=38, scale=0), "number", pa.decimal128(38, 0), id="d38"
        ),
        pytest.param(
            NodeHints(logical_type="decimal", precision=39, scale=2), "number", pa.decimal256(39, 2), id="d39"
        ),
        pytest.param(NodeHints(logical_type="decimal"), "number", pa.decimal128(38, 0), id="decimal-no-hints"),
        pytest.param(NodeHints(logical_type="wei"), "integer", pa.decimal256(76, 0), id="wei"),
        pytest.param(NodeHints(logical_type="timestamp", timezone=False), "string", pa.timestamp("us"), id="naive"),
        pytest.param(
            NodeHints(logical_type="timestamp", timezone=True), "string", pa.timestamp("us", tz="UTC"), id="aware"
        ),
        pytest.param(
            NodeHints(logical_type="timestamp"), "string", pa.timestamp("us", tz="UTC"), id="unknown-timezone"
        ),
        pytest.param(NodeHints(logical_type="timestamp-without-tz"), "string", pa.timestamp("us"), id="iceberg-naive"),
        pytest.param(
            NodeHints(logical_type="timestamp-with-tz", timezone=True),
            "string",
            pa.timestamp("us", tz="UTC"),
            id="iceberg-aware",
        ),
        pytest.param(NodeHints(logical_type="timestamp", precision=0), "string", pa.timestamp("s", "UTC"), id="ts-s"),
        pytest.param(NodeHints(logical_type="timestamp", precision=1), "string", pa.timestamp("ms", "UTC"), id="ts-1"),
        pytest.param(NodeHints(logical_type="timestamp", precision=3), "string", pa.timestamp("ms", "UTC"), id="ts-3"),
        pytest.param(NodeHints(logical_type="timestamp", precision=4), "string", pa.timestamp("us", "UTC"), id="ts-4"),
        pytest.param(NodeHints(logical_type="timestamp", precision=6), "string", pa.timestamp("us", "UTC"), id="ts-6"),
        pytest.param(NodeHints(logical_type="timestamp", precision=7), "string", pa.timestamp("ns", "UTC"), id="ts-7"),
        pytest.param(NodeHints(logical_type="timestamp", precision=9), "string", pa.timestamp("ns", "UTC"), id="ts-9"),
        pytest.param(NodeHints(logical_type="bigint", bit_width=0), "integer", pa.int8(), id="int-0"),
        pytest.param(NodeHints(logical_type="bigint", bit_width=8), "integer", pa.int8(), id="int-8"),
        pytest.param(NodeHints(logical_type="bigint", bit_width=9), "integer", pa.int16(), id="int-9"),
        pytest.param(NodeHints(logical_type="bigint", bit_width=16), "integer", pa.int16(), id="int-16"),
        pytest.param(NodeHints(logical_type="int", bit_width=17), "integer", pa.int32(), id="int-17"),
        pytest.param(NodeHints(logical_type="int", bit_width=32), "integer", pa.int32(), id="int-32"),
        pytest.param(NodeHints(logical_type="long", bit_width=33), "integer", pa.int64(), id="int-33"),
        pytest.param(NodeHints(logical_type="long", bit_width=64), "integer", pa.int64(), id="int-64"),
        pytest.param(NodeHints(logical_type="float", bit_width=32), "number", pa.float32(), id="float"),
        pytest.param(NodeHints(bit_width=32), "number", pa.float32(), id="float-by-width"),
        pytest.param(NodeHints(logical_type="double", bit_width=64), "number", pa.float64(), id="double"),
        pytest.param(NodeHints(logical_type="date"), "string", pa.date32(), id="date"),
        pytest.param(NodeHints(logical_type="time"), "string", pa.time64("us"), id="time"),
        pytest.param(NodeHints(logical_type="binary"), "string", pa.binary(), id="binary"),
        pytest.param(NodeHints(logical_type="fixed", length=16), "string", pa.binary(16), id="fixed"),
        pytest.param(NodeHints(logical_type="fixed"), "string", pa.binary(), id="fixed-no-length"),
        pytest.param(NodeHints(logical_type="uuid"), "string", pa.string(), id="uuid"),
    ],
)
def test_emit_node_pass_logical_hints(hints: NodeHints, node_type: NodeType, expected: pa.DataType) -> None:
    """Logical source facts produce native Arrow types."""
    assert PyArrowEmitter().emit_node(TypedNode(type=node_type, hints=hints)) == expected


@pytest.mark.parametrize(
    "hints",
    [
        NodeHints(logical_type="decimal", precision=0),
        NodeHints(logical_type="decimal", precision=77),
        NodeHints(logical_type="decimal", precision=5, scale=6),
        NodeHints(logical_type="wei", precision=77),
    ],
    ids=["zero-precision", "precision-above-256-bit", "scale-above-precision", "wei-above-256-bit"],
)
def test_emit_node_fail_invalid_decimal_hints(hints: NodeHints) -> None:
    """Decimal hints outside Arrow's range raise."""
    with pytest.raises(PyArrowEmitterError, match="Unsupported Arrow decimal precision/scale"):
        PyArrowEmitter().emit_node(TypedNode(type="number", hints=hints))


def test_emit_pass_iceberg_types() -> None:
    """Iceberg types keep their widths, logical types, nullability and nested structure."""
    schema = Schema(
        iceberg_types.NestedField(1, "id", iceberg_types.LongType(), required=True),
        iceberg_types.NestedField(2, "small", iceberg_types.IntegerType()),
        iceberg_types.NestedField(3, "ratio", iceberg_types.FloatType()),
        iceberg_types.NestedField(4, "score", iceberg_types.DoubleType()),
        iceberg_types.NestedField(5, "price", iceberg_types.DecimalType(10, 2)),
        iceberg_types.NestedField(6, "born", iceberg_types.DateType()),
        iceberg_types.NestedField(7, "at", iceberg_types.TimeType()),
        iceberg_types.NestedField(8, "naive", iceberg_types.TimestampType()),
        iceberg_types.NestedField(9, "aware", iceberg_types.TimestamptzType()),
        iceberg_types.NestedField(10, "blob", iceberg_types.BinaryType()),
        iceberg_types.NestedField(11, "hash", iceberg_types.FixedType(16)),
        iceberg_types.NestedField(12, "uid", iceberg_types.UUIDType()),
        iceberg_types.NestedField(13, "flag", iceberg_types.BooleanType()),
        iceberg_types.NestedField(
            14, "tags", iceberg_types.ListType(15, iceberg_types.StringType(), element_required=False)
        ),
        iceberg_types.NestedField(
            16, "ids", iceberg_types.ListType(17, iceberg_types.LongType(), element_required=True)
        ),
        iceberg_types.NestedField(
            18,
            "attrs",
            iceberg_types.MapType(19, iceberg_types.StringType(), 20, iceberg_types.LongType(), value_required=True),
        ),
        iceberg_types.NestedField(
            21,
            "inner",
            iceberg_types.StructType(iceberg_types.NestedField(22, "x", iceberg_types.IntegerType(), required=True)),
        ),
    )
    expected = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("small", pa.int32()),
            pa.field("ratio", pa.float32()),
            pa.field("score", pa.float64()),
            pa.field("price", pa.decimal128(10, 2)),
            pa.field("born", pa.date32()),
            pa.field("at", pa.time64("us")),
            pa.field("naive", pa.timestamp("us")),
            pa.field("aware", pa.timestamp("us", tz="UTC")),
            pa.field("blob", pa.binary()),
            pa.field("hash", pa.binary(16)),
            pa.field("uid", pa.string()),
            pa.field("flag", pa.bool_()),
            pa.field("tags", _list(pa.string(), nullable=True)),
            pa.field("ids", _list(pa.int64(), nullable=False)),
            pa.field("attrs", _map(pa.int64(), nullable=False)),
            pa.field("inner", pa.struct([pa.field("x", pa.int32(), nullable=False)])),
        ]
    )
    actual = PyArrowEmitter().emit(IcebergReader().read(schema))
    assert actual == expected
    assert actual.field("ids").type.value_field.nullable is False
    assert actual.field("tags").type.value_field.nullable is True
    assert actual.field("attrs").type.item_field.nullable is False


def test_emit_pass_dlt_columns() -> None:
    """Dlt column hints select widths, decimals, timestamp units and nullability."""
    columns = {
        "id": {"data_type": "bigint", "precision": 16, "nullable": False},
        "amount": {"data_type": "decimal", "precision": 20, "scale": 8, "nullable": False},
        "balance": {"data_type": "wei"},
        "seen": {"data_type": "timestamp", "precision": 3},
        "naive": {"data_type": "timestamp", "timezone": False},
        "day": {"data_type": "date"},
        "clock": {"data_type": "time"},
        "raw": {"data_type": "binary"},
        "label": {"data_type": "text"},
        "ok": {"data_type": "bool"},
        "score": {"data_type": "double"},
        "payload": {"data_type": "json"},
    }
    document = DltReader().read({"records": {"columns": columns}})["records"]
    assert PyArrowEmitter().emit(document) == pa.schema(
        [
            pa.field("id", pa.int16(), nullable=False),
            pa.field("amount", pa.decimal128(20, 8), nullable=False),
            pa.field("balance", pa.decimal256(76, 0)),
            pa.field("seen", pa.timestamp("ms", tz="UTC")),
            pa.field("naive", pa.timestamp("us")),
            pa.field("day", pa.date32()),
            pa.field("clock", pa.time64("us")),
            pa.field("raw", pa.binary()),
            pa.field("label", pa.string()),
            pa.field("ok", pa.bool_()),
            pa.field("score", pa.float64()),
            pa.field("payload", pa.string()),
        ]
    )


def test_emit_fail_dlt_json_column_strict() -> None:
    """A dlt json column has no Arrow type under the strict policy."""
    document = DltReader().read({"records": {"columns": {"payload": {"data_type": "json"}}}})["records"]
    with pytest.raises(PyArrowEmitterError, match="Unsupported/unknown schema type: 'any'"):
        PyArrowEmitter(treat_unknown_as_string=False).emit(document)


def test_emit_pass_metadata() -> None:
    """Titles, descriptions and requested keywords are stored as string field metadata."""
    document = JsonSchemaReader().read(
        {
            "$schema": DIALECT,
            "properties": {
                "name": {"type": "string", "title": "Name", "description": "A name", "pattern": "^a", "x-pii": True}
            },
        }
    )
    emitter = PyArrowEmitter(extra_metadata_keywords=frozenset({"pattern", "description", "x-pii"}))
    metadata = emitter.emit(document).field("name").metadata
    assert set(metadata) == {b"jsonschema", b"comment"}
    assert json.loads(metadata[b"jsonschema"]) == {
        "title": "Name",
        "description": "A name",
        "pattern": "^a",
        "x-pii": True,
    }
    assert metadata[b"comment"] == b"A name"


def test_build_metadata_pass_decimal_encoded_as_string() -> None:
    """Exact decimals are encoded as strings, never as binary floats."""
    emitter = PyArrowEmitter(extra_metadata_keywords=frozenset({"multipleOf"}))
    node = TypedNode(type="number", constraints={"multipleOf": Decimal("0.10")})
    metadata = emitter.build_metadata(node, emitter.build_context(Draft202012Validator))
    assert metadata == {"jsonschema": '{"multipleOf": "0.10"}'}


def test_build_metadata_pass_original_union() -> None:
    """Collapsed unions record their non-null members when requested."""
    document = JsonSchemaReader().read(
        {"$schema": DIALECT, "properties": {"value": {"type": ["string", "integer", "null"]}}}
    )
    emitter = PyArrowEmitter(emit_unions_as_json=True)
    schema = emitter.emit(document)
    assert schema.field("value").type == pa.string()
    assert json.loads(schema.field("value").metadata[b"original_union"]) == ["string", "integer"]
    assert PyArrowEmitter().emit(document).field("value").metadata is None


def test_build_metadata_pass_field_annotations_override_node() -> None:
    """Field-level annotations take precedence over node annotations."""
    emitter = PyArrowEmitter()
    node = TypedNode(type="string", annotations={"title": "Node", "description": "node text"})
    field = Field("value", node, annotations={"title": "Field"})
    metadata = emitter.build_metadata(node, emitter.build_context(Draft202012Validator), field)
    assert metadata == {"jsonschema": '{"title": "Field"}', "comment": "node text"}


@pytest.mark.parametrize(
    ("values", "expected"),
    [
        pytest.param([True], pa.bool_(), id="boolean"),
        pytest.param([1, 2], pa.int64(), id="integer"),
        pytest.param([1, 2.5], pa.float64(), id="number"),
        pytest.param(["a"], pa.string(), id="string"),
        pytest.param([], pa.string(), id="empty"),
        pytest.param([1, "a"], pa.string(), id="mixed"),
    ],
)
def test_infer_type_from_enum_pass(values: list[object], expected: pa.DataType) -> None:
    """Enum members map to the narrowest shared Arrow scalar type."""
    assert infer_type_from_enum(values) == expected


def test_merge_format_map_pass_non_mapping_unchanged() -> None:
    """Non-mapping values pass through for model validation to reject."""
    assert merge_format_map("text") == "text"
