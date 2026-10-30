"""Pure schema-object checks for the Spark emitter."""

import json
from dataclasses import replace
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest
from jsonschema import Draft7Validator, Draft202012Validator
from pyiceberg import types as iceberg_types
from pyiceberg.schema import Schema
from pyspark.sql.types import (
    ArrayType,
    BinaryType,
    BooleanType,
    DataType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    MapType,
    NullType,
    StringType,
    StructField,
    StructType,
    TimestampNTZType,
    TimestampType,
)

from cdm_data_loaders.converters.core.extensions import DEFAULT_EXTENSIONS, ExtensionError, ExtensionSpec
from cdm_data_loaders.converters.core.ir import Field, NodeHints, NodeType, SchemaDocument, TypedNode
from cdm_data_loaders.converters.core.metadata import (
    REF_AND_IDENTITY_KEYWORDS,
    STRUCTURAL_OR_COMPOSITIONAL_KEYWORDS,
    ConversionContext,
    get_known_jsonschema_keywords,
    metadata_keys_for,
)
from cdm_data_loaders.converters.emitters.pyspark import (
    PySparkEmitter,
    PySparkEmitterError,
    infer_type_from_enum,
)
from cdm_data_loaders.converters.readers.dlt import DltReader
from cdm_data_loaders.converters.readers.iceberg import IcebergReader
from cdm_data_loaders.converters.readers.json_schema import JsonSchemaReader

DIALECT = "https://json-schema.org/draft/2020-12/schema"


def _document(schema: dict[str, Any] | bool) -> SchemaDocument:
    """Wrap a fragment in an object-compatible JSON document."""
    return JsonSchemaReader().read({"$schema": DIALECT, "properties": {"value": schema}})


@pytest.mark.parametrize(
    "items",
    [False, {}, [], None],
    ids=["false-schema", "empty-schema", "empty-tuple", "absent-items"],
)
def test_emit_pass_strict_empty_array_items(items: object) -> None:
    """Empty and false array item schemas produce string elements in strict mode."""
    array: dict[str, Any] = {"type": "array"}
    if items is not None:
        array["items"] = items
    document = JsonSchemaReader().read({"$schema": DIALECT, "properties": {"values": array}})
    assert PySparkEmitter(treat_unknown_as_string=False).emit(document).jsonValue() == {
        "type": "struct",
        "fields": [
            {
                "name": "values",
                "type": ArrayType(StringType(), containsNull=True).jsonValue(),
                "nullable": True,
                "metadata": {},
            }
        ],
    }


def test_emit_pass_json_round_trip() -> None:
    """Serialized emitted schemas deserialize through PySpark's native parser."""
    document = JsonSchemaReader().read(
        {
            "$schema": DIALECT,
            "type": "object",
            "properties": {
                "id": {"type": "integer"},
                "tags": {"type": "array", "items": {"type": "string"}},
                "metadata": {"type": "object", "additionalProperties": {"type": "string"}},
            },
            "required": ["id"],
        }
    )
    emitted = PySparkEmitter().emit(document)

    assert StructType.fromJson(emitted.jsonValue()) == emitted


@pytest.mark.parametrize(
    ("hints", "expected"),
    [
        (NodeHints(logical_type="decimal", precision=20, scale=8), DecimalType(20, 8)),
        (NodeHints(logical_type="timestamp", timezone=False), TimestampNTZType()),
        (NodeHints(logical_type="timestamp", timezone=True), TimestampType()),
        (NodeHints(logical_type="timestamp"), TimestampType()),
    ],
    ids=["decimal", "naive-timestamp", "aware-timestamp", "unknown-timezone"],
)
def test_emit_node_pass_logical_hints(hints: NodeHints, expected: DataType) -> None:
    """Logical source facts produce native Spark types."""
    node = TypedNode(type="number" if hints.logical_type == "decimal" else "string", hints=hints)
    assert PySparkEmitter().emit_node(node).jsonValue() == expected.jsonValue()


def test_emit_pass_metadata() -> None:
    """Titles, descriptions and requested assertion keywords retain their exact values."""
    document = JsonSchemaReader().read(
        {
            "$schema": DIALECT,
            "properties": {
                "name": {"type": "string", "title": "Name", "description": "A name", "pattern": "^a", "x-pii": True}
            },
        }
    )
    emitter = PySparkEmitter(extra_metadata_keywords=frozenset({"pattern", "description", "x-pii"}))
    assert emitter.emit(document)["name"].metadata == {
        "jsonschema": {"title": "Name", "description": "A name", "pattern": "^a", "x-pii": True},
        "comment": "A name",
    }


@pytest.mark.parametrize("strict", [False, True], ids=["fallback-policy", "strict-policy"])
@pytest.mark.parametrize(
    ("schema", "expected"),
    [
        pytest.param({"type": "string"}, StringType(), id="string"),
        pytest.param({"type": "boolean"}, BooleanType(), id="boolean"),
        pytest.param({"type": "null"}, NullType(), id="null"),
        pytest.param({"type": "integer"}, LongType(), id="integer"),
        pytest.param({"type": "number"}, DoubleType(), id="number"),
        pytest.param({"type": "object"}, StructType([]), id="empty-object"),
        pytest.param({"type": ["null", "string"]}, StringType(), id="nullable-string"),
        pytest.param({"type": ["integer"]}, LongType(), id="single-type-list"),
        pytest.param({"type": ["null", "null"]}, NullType(), id="null-only-list"),
        pytest.param({"type": []}, NullType(), id="empty-type-list"),
        pytest.param({"enum": [True, False]}, BooleanType(), id="boolean-enum"),
        pytest.param({"enum": [1, 2]}, LongType(), id="integer-enum"),
        pytest.param({"enum": [1, 2.5]}, DoubleType(), id="number-enum"),
        pytest.param({"enum": ["a", "b"]}, StringType(), id="string-enum"),
        pytest.param({"enum": []}, StringType(), id="empty-enum"),
        pytest.param({"enum": [None]}, StringType(), id="null-enum"),
        pytest.param({"enum": [True, 1]}, StringType(), id="bool-is-not-integer-enum"),
        pytest.param({"enum": [1, "a"]}, StringType(), id="mixed-enum"),
        pytest.param({"enum": [{"type": "integer"}]}, StringType(), id="object-enum"),
        pytest.param({"enum": [Decimal("1.1")]}, StringType(), id="decimal-enum"),
        pytest.param({"enum": [1], "minimum": 0, "oneOf": [{"type": "string"}]}, LongType(), id="enum-precedence"),
        pytest.param({"type": "string", "enum": [1]}, StringType(), id="declared-over-enum"),
        pytest.param({"oneOf": [{"type": "integer"}, {"type": "string"}]}, LongType(), id="oneof-first"),
        pytest.param({"anyOf": [{"type": "null"}, {"type": "string"}]}, NullType(), id="anyof-null-first"),
        pytest.param(
            {"oneOf": [{"type": "boolean"}], "anyOf": [{"type": "integer"}]}, BooleanType(), id="oneof-over-anyof"
        ),
        pytest.param({"oneOf": [], "anyOf": [{"type": "integer"}]}, LongType(), id="empty-oneof-before-anyof"),
        pytest.param({"type": "mystery", "anyOf": [{"type": "boolean"}]}, BooleanType(), id="unknown-with-combiner"),
        pytest.param(
            {"type": "string", "oneOf": [False], "anyOf": [False], "not": False},
            StringType(),
            id="declared-over-combiners",
        ),
        pytest.param({"format": "date", "oneOf": [False]}, DateType(), id="inferred-over-combiner"),
        pytest.param({"properties": {}, "items": False}, StructType([]), id="object-inference-first"),
        pytest.param({"items": {"type": "integer"}, "pattern": "a"}, ArrayType(LongType()), id="array-before-string"),
        pytest.param({"pattern": "a", "multipleOf": 0.01}, StringType(), id="string-before-number"),
        pytest.param({"minimum": 0, "maximum": 10}, DoubleType(), id="numeric-inference"),
        pytest.param({"multipleOf": 0.01}, DecimalType(38, 2), id="decimal-inference"),
        pytest.param({"required": ["missing"]}, StructType([]), id="required-implies-object"),
        pytest.param({"uniqueItems": True}, ArrayType(StringType()), id="uniqueness-implies-array"),
        pytest.param({"type": "array", "prefixItems": []}, ArrayType(StringType()), id="empty-prefix"),
        pytest.param({"type": "array", "prefixItems": [False]}, ArrayType(StringType()), id="false-prefix"),
        pytest.param({"type": "array", "prefixItems": [{}]}, ArrayType(StringType()), id="empty-prefix-schema"),
        pytest.param(
            {"type": "array", "prefixItems": [{"type": "integer"}, False], "items": True},
            ArrayType(LongType()),
            id="prefix-before-items",
        ),
        pytest.param(
            {"type": "array", "items": [{"type": "boolean"}, False]}, ArrayType(BooleanType()), id="tuple-items"
        ),
        pytest.param({"type": "array", "items": [False]}, ArrayType(StringType()), id="false-tuple"),
        pytest.param(
            {"type": "array", "items": {"type": "array", "items": {"type": "integer"}}},
            ArrayType(ArrayType(LongType())),
            id="nested-arrays",
        ),
        pytest.param({"type": "object", "additionalProperties": True}, StructType([]), id="boolean-additional-true"),
        pytest.param({"type": "object", "additionalProperties": False}, StructType([]), id="boolean-additional-false"),
        pytest.param(
            {"type": "object", "additionalProperties": {"type": "integer"}},
            MapType(StringType(), LongType()),
            id="additional-schema",
        ),
        pytest.param(
            {"patternProperties": {"a": {"type": "boolean"}, "b": False}, "additionalProperties": False},
            MapType(StringType(), BooleanType()),
            id="first-pattern",
        ),
        pytest.param(
            {"patternProperties": {"a": {"type": "integer"}}, "additionalProperties": {"type": "boolean"}},
            MapType(StringType(), LongType()),
            id="pattern-before-additional",
        ),
        pytest.param(
            {"properties": {"name": {"type": "string"}}, "patternProperties": {"a": False}, "additionalProperties": {}},
            StructType([StructField("name", StringType())]),
            id="fixed-before-dynamic",
        ),
        pytest.param({"type": "integer", "minimum": 0, "maximum": 100}, IntegerType(), id="bounded-int"),
        pytest.param(
            {"type": "integer", "minimum": -2_147_483_648, "maximum": 2_147_483_647},
            IntegerType(),
            id="int32-boundaries",
        ),
        pytest.param({"type": "integer", "minimum": -2_147_483_649, "maximum": 100}, LongType(), id="below-int32"),
        pytest.param({"type": "integer", "minimum": 0, "maximum": 2_147_483_648}, LongType(), id="above-int32"),
        pytest.param({"type": "integer", "minimum": 0}, LongType(), id="missing-maximum"),
        pytest.param({"type": "integer", "maximum": 0}, LongType(), id="missing-minimum"),
        pytest.param(
            {"type": "integer", "exclusiveMinimum": 0, "exclusiveMaximum": 100}, IntegerType(), id="exclusive-bounds"
        ),
        pytest.param(
            {"type": "integer", "minimum": None, "exclusiveMinimum": 0, "maximum": 100},
            LongType(),
            id="null-minimum-does-not-use-exclusive",
        ),
        pytest.param({"type": "number", "multipleOf": 1}, DecimalType(38, 0), id="integral-multiple"),
        pytest.param({"type": "number", "multipleOf": 1.0}, DecimalType(38, 1), id="float-integral-multiple"),
        pytest.param({"type": "number", "multipleOf": 0.001}, DecimalType(38, 3), id="fractional-multiple"),
        pytest.param({"type": "number", "multipleOf": 1e-9}, DecimalType(38, 9), id="exponent-multiple"),
        pytest.param({"type": "number", "multipleOf": "0.01"}, DoubleType(), id="nonnumeric-multiple"),
        pytest.param(
            {"type": "number", "multipleOf": Decimal("0.0100")}, DecimalType(38, 4), id="decimal-multiple-parity"
        ),
    ],
)
def test_emit_pass_json_dispatch(schema: dict[str, Any], expected: DataType, strict: bool) -> None:
    """Typed dispatch matches fixed expected schemas."""
    document = _document(schema)
    emitter = PySparkEmitter(treat_unknown_as_string=not strict)
    actual = emitter.emit(document)
    assert actual.jsonValue() == {
        "type": "struct",
        "fields": [{"name": "value", "type": expected.jsonValue(), "nullable": True, "metadata": {}}],
    }


@pytest.mark.parametrize(
    "case_id", ["json-spark-nested-meta", "json-spark-union-dynamic"], ids=["nested-meta", "union-map"]
)
def test_emit_pass_fixed_golden(case_id: str) -> None:
    """Spark output matches the independently stored JSON schema golden."""
    path = Path(__file__).parents[3] / "data" / "converters" / "ir" / "legacy_outputs.json"
    cases = json.loads(path.read_text(encoding="utf-8"))
    case = next(case for case in cases if case["id"] == case_id)
    emitter = PySparkEmitter.model_validate(case["options"])
    assert emitter.emit(JsonSchemaReader().read(case["input"])).jsonValue() == case["expected"]


def test_emit_pass_root_union_and_inference() -> None:
    """Struct emission accepts an inferred or nullable object without a facade declaration guard."""
    for declaration in (None, ["object", "null"]):
        schema: dict[str, Any] = {"$schema": DIALECT, "properties": {"name": {"type": "string"}}}
        if declaration is not None:
            schema["type"] = declaration
        assert PySparkEmitter().emit(JsonSchemaReader().read(schema)) == StructType([StructField("name", StringType())])


@pytest.mark.parametrize("validator", [Draft7Validator, Draft202012Validator], ids=["draft7", "draft2020"])
def test_conversion_context_pass_keyword_policy(validator: type) -> None:
    """Metadata helpers exclude structural keywords and preserve recognized assertions and annotations."""
    context = ConversionContext(validator, frozenset({"pattern", "x-custom", "type", "$id", "typo"}))
    known = get_known_jsonschema_keywords(validator)
    assert {"title", "description", "pattern"} <= known
    assert not known & REF_AND_IDENTITY_KEYWORDS
    assert context.metadata_keys == frozenset(known - STRUCTURAL_OR_COMPOSITIONAL_KEYWORDS)
    assert context.allowed_extra_metadata_keywords == frozenset({"pattern", "x-custom"})
    assert context.invalid_extra_metadata_keywords == frozenset({"type", "$id", "typo"})
    # ensure that exactly the same object is returned (i.e. that the caching functionality is working)
    assert metadata_keys_for(validator) is metadata_keys_for(validator)


@pytest.mark.parametrize("dialect", ["http://json-schema.org/draft-07/schema#", DIALECT], ids=["draft7", "draft2020"])
def test_emit_pass_invalid_metadata_warns_once(dialect: str, caplog: pytest.LogCaptureFixture) -> None:
    """Use the reader's detected draft even when the document's dialect label is removed."""
    document = JsonSchemaReader().read(
        {
            "$schema": dialect,
            "properties": {"nested": {"properties": {"value": {"type": "string", "typo": "ignored", "pattern": "a"}}}},
        }
    )
    emitter = PySparkEmitter(extra_metadata_keywords=frozenset({"typo", "type", "properties", "$id", "pattern"}))
    result = emitter.emit(replace(document, dialect=None))
    assert result["nested"].dataType["value"].metadata == {"jsonschema": {"pattern": "a"}}
    assert caplog.text.count("Ignoring invalid extra_metadata_keywords") == 1
    assert ("Draft7Validator" if "draft-07" in dialect else "Draft202012Validator") in caplog.text


@pytest.mark.parametrize(
    ("annotations", "expected"),
    [
        ({}, {}),
        ({"title": "Title"}, {"jsonschema": {"title": "Title"}}),
        ({"description": "Description"}, {"comment": "Description"}),
        ({"description": None}, {"comment": None}),
        ({"title": None, "description": ""}, {"jsonschema": {"title": None}, "comment": ""}),
    ],
    ids=["absent", "title", "description", "null-description", "null-title-empty-description"],
)
def test_build_metadata_pass_annotation_presence(annotations: dict[str, Any], expected: dict[str, Any]) -> None:
    """Absent, null and empty annotations remain distinct."""
    node = TypedNode(type="string", annotations=annotations)
    assert PySparkEmitter().build_metadata(node, ConversionContext(Draft202012Validator)) == expected


def test_emit_pass_registered_extensions_and_metadata_isolation() -> None:
    """Registered extension payloads and exact decimals are copied without sharing mutable output."""
    registry = DEFAULT_EXTENSIONS.register(
        ExtensionSpec(
            "x-custom", {"type": "object", "properties": {"values": {"type": "array"}}, "additionalProperties": False}
        )
    )
    schema = {
        "$schema": DIALECT,
        "title": "Root title",
        "properties": {
            "value": {
                "type": "string",
                "default": {"exact": Decimal("0.1000000000000000000001")},
                "examples": ["a", "b"],
                "x-custom": {"values": [1, None]},
            }
        },
    }
    with pytest.raises(ExtensionError):
        JsonSchemaReader().read(schema)
    document = JsonSchemaReader(extension_registry=registry).read(schema)
    emitter = PySparkEmitter(extra_metadata_keywords=frozenset({"x-custom", "examples", "default"}))
    expected = {
        "jsonschema": {
            "default": {"exact": Decimal("0.1000000000000000000001")},
            "examples": ["a", "b"],
            "x-custom": {"values": [1, None]},
        }
    }
    result = emitter.emit(document)
    assert result["value"].metadata == expected
    result["value"].metadata["jsonschema"]["x-custom"]["values"].append("changed")
    result["value"].metadata["jsonschema"]["examples"].clear()
    assert emitter.emit(document)["value"].metadata == expected


def test_emit_pass_field_metadata_overrides_node() -> None:
    """Field annotations and extensions take precedence over type-scoped metadata."""
    node = TypedNode(type="string", annotations={"title": "Node", "description": "node"}, extensions={"x-pii": False})
    field = Field("value", node, annotations={"description": "field"}, extensions={"x-pii": True})
    document = SchemaDocument(root=TypedNode(type="object", properties=(field,)))
    assert PySparkEmitter(extra_metadata_keywords=frozenset({"x-pii"})).emit(document)["value"].metadata == {
        "jsonschema": {"title": "Node", "x-pii": True},
        "comment": "field",
    }


@pytest.mark.parametrize(
    ("values", "expected"),
    [([], StringType()), ([True], BooleanType()), ([1], LongType()), ([1, 2.1], DoubleType()), ([None], StringType())],
    ids=["empty", "boolean", "integer", "number", "null"],
)
def test_infer_type_from_enum_pass_scalars(values: list[object], expected: DataType) -> None:
    """Enum inference preserves the JSON scalar distinctions."""
    assert infer_type_from_enum(values) == expected


@pytest.mark.parametrize(
    ("kind", "hints", "expected"),
    [
        ("number", NodeHints(logical_type="decimal"), DecimalType(38, 0)),
        ("integer", NodeHints(logical_type="wei"), DecimalType(38, 0)),
        ("integer", NodeHints(bit_width=16), IntegerType()),
        ("integer", NodeHints(bit_width=32), IntegerType()),
        ("integer", NodeHints(bit_width=64), LongType()),
        ("number", NodeHints(logical_type="float"), FloatType()),
        ("number", NodeHints(bit_width=32), FloatType()),
        ("number", NodeHints(bit_width=64), DoubleType()),
        ("string", NodeHints(logical_type="date"), DateType()),
        ("string", NodeHints(logical_type="binary"), BinaryType()),
        ("string", NodeHints(logical_type="fixed", length=12), BinaryType()),
        ("string", NodeHints(logical_type="timestamp-without-tz"), TimestampNTZType()),
        ("string", NodeHints(logical_type="timestamp-with-tz"), TimestampType()),
        ("string", NodeHints(logical_type="uuid"), StringType()),
        ("string", NodeHints(logical_type="time"), StringType()),
    ],
    ids=[
        "decimal-default",
        "wei",
        "int16",
        "int32",
        "int64",
        "float",
        "float-width",
        "double",
        "date",
        "binary",
        "fixed",
        "naive",
        "aware",
        "uuid",
        "time",
    ],
)
def test_emit_node_pass_direct_logical_types(kind: NodeType, hints: NodeHints, expected: DataType) -> None:
    """Direct typed source hints retain logical values and conservative physical widths."""
    assert PySparkEmitter().emit_node(TypedNode(type=kind, hints=hints)) == expected


@pytest.mark.parametrize(
    ("precision", "scale"),
    [(0, 0), (39, 0), (2, 3)],
    ids=["zero-precision", "excessive-precision", "scale-above-precision"],
)
def test_emit_node_fail_unrepresentable_decimal(precision: int, scale: int) -> None:
    """Logical decimals outside Spark's precision limits raise instead of degrading to floating point."""
    node = TypedNode(type="number", hints=NodeHints(logical_type="decimal", precision=precision, scale=scale))
    with pytest.raises(PySparkEmitterError, match="Unsupported Spark decimal precision/scale"):
        PySparkEmitter().emit_node(node)


def test_emit_pass_json_multiple_of_scale_within_limit_uses_decimal() -> None:
    """A 'multipleOf' scale within Spark's precision limit produces a matching DecimalType."""
    document = _document({"multipleOf": Decimal("1e-38")})
    assert PySparkEmitter().emit(document).fields[0].dataType == DecimalType(38, 38)


def test_emit_pass_direct_dlt_document() -> None:
    """Dlt decimals, timestamp timezone hints, widths and metadata emit directly."""
    document = DltReader().read(
        {
            "records": {
                "columns": {
                    "id": {"data_type": "bigint", "precision": 32, "nullable": False},
                    "amount": {"data_type": "decimal", "precision": 20, "scale": 8, "description": "Exact"},
                    "local": {"data_type": "timestamp", "timezone": False},
                    "aware": {"data_type": "timestamp", "timezone": True},
                    "binary": {"data_type": "binary"},
                    "unknown": {"data_type": "json"},
                }
            }
        }
    )["records"]
    assert PySparkEmitter().emit(document).jsonValue() == {
        "type": "struct",
        "fields": [
            {"name": "id", "type": "integer", "nullable": False, "metadata": {}},
            {"name": "amount", "type": "decimal(20,8)", "nullable": True, "metadata": {"comment": "Exact"}},
            {"name": "local", "type": "timestamp_ntz", "nullable": True, "metadata": {}},
            {"name": "aware", "type": "timestamp", "nullable": True, "metadata": {}},
            {"name": "binary", "type": "binary", "nullable": True, "metadata": {}},
            {"name": "unknown", "type": "string", "nullable": True, "metadata": {}},
        ],
    }


def test_emit_pass_direct_iceberg_document() -> None:
    """Iceberg field metadata and typed map keys remain separate from node type facts."""
    document = IcebergReader().read(
        Schema(
            iceberg_types.NestedField(1, "amount", iceberg_types.DecimalType(12, 4), required=True, doc="Amount"),
            iceberg_types.NestedField(
                2,
                "lookup",
                iceberg_types.MapType(
                    3, iceberg_types.IntegerType(), 4, iceberg_types.TimestampType(), value_required=True
                ),
            ),
            iceberg_types.NestedField(
                5, "values", iceberg_types.ListType(6, iceberg_types.FloatType(), element_required=True)
            ),
        )
    )
    assert PySparkEmitter().emit(document).jsonValue() == {
        "type": "struct",
        "fields": [
            {"name": "amount", "type": "decimal(12,4)", "nullable": False, "metadata": {"comment": "Amount"}},
            {
                "name": "lookup",
                "type": {"type": "map", "keyType": "integer", "valueType": "timestamp_ntz", "valueContainsNull": True},
                "nullable": True,
                "metadata": {"comment": None},
            },
            {
                "name": "values",
                "type": {"type": "array", "elementType": "float", "containsNull": True},
                "nullable": True,
                "metadata": {"comment": None},
            },
        ],
    }


def test_emit_node_pass_explicit_context_and_branch_metadata() -> None:
    """An explicit context controls metadata without copying annotations from a selected combiner branch."""
    document = _document(
        {"title": "Outer", "oneOf": [{"type": "integer", "title": "Inner", "description": "Branch"}], "default": 1}
    )
    context = ConversionContext(Draft7Validator, frozenset({"default"}))
    actual = PySparkEmitter().emit_node(document.root, context)
    assert actual.jsonValue() == {
        "type": "struct",
        "fields": [
            {
                "name": "value",
                "type": "long",
                "nullable": True,
                "metadata": {"jsonschema": {"title": "Outer", "default": 1}},
            }
        ],
    }
