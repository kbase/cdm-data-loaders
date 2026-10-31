"""Shared unit tests for tabular emitters (_TabularEmitterBase: PyArrow and PySpark)."""

import json
import logging
from collections import UserDict
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

import pyarrow as pa
import pytest
from frozendict import frozendict
from jsonschema import Draft202012Validator
from pydantic import ValidationError
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DataType,
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

from cdm_data_loaders.converters.core.errors import ConversionError
from cdm_data_loaders.converters.core.ir import Field, NodeHints, Provenance, SchemaDocument, TypedNode
from cdm_data_loaders.converters.emitters._tabular_base import _TabularEmitterBase
from cdm_data_loaders.converters.emitters.pyarrow import (
    DEFAULT_FORMAT_MAP as ARROW_FORMAT_MAP,
)
from cdm_data_loaders.converters.emitters.pyarrow import (
    PyArrowEmitter,
    PyArrowEmitterError,
)
from cdm_data_loaders.converters.emitters.pyarrow import (
    infer_type_from_enum as arrow_infer_type_from_enum,
)
from cdm_data_loaders.converters.emitters.pyspark import (
    DEFAULT_FORMAT_MAP as SPARK_FORMAT_MAP,
)
from cdm_data_loaders.converters.emitters.pyspark import (
    PySparkEmitter,
    PySparkEmitterError,
)
from cdm_data_loaders.converters.emitters.pyspark import (
    infer_type_from_enum as spark_infer_type_from_enum,
)
from cdm_data_loaders.converters.readers.json_schema import JsonSchemaReader

DIALECT = "https://json-schema.org/draft/2020-12/schema"


def _document(schema: dict[str, Any] | bool) -> SchemaDocument:
    """Wrap a fragment in an object-compatible JSON document."""
    return JsonSchemaReader().read({"$schema": DIALECT, "properties": {"value": schema}})


def _arrow_field_metadata(schema: pa.Schema, name: str) -> dict[str, Any]:
    """Decode PyArrow byte-keyed field metadata into a Python dict matching Spark's shape."""
    raw = schema.field(name).metadata or {}
    decoded: dict[str, Any] = {k.decode(): v.decode() for k, v in raw.items()}
    if "jsonschema" in decoded:
        decoded["jsonschema"] = json.loads(decoded["jsonschema"])
    return decoded


def _assert_arrow_single_value(actual: pa.Schema, expected: pa.DataType) -> None:
    assert actual == pa.schema([pa.field("value", expected, nullable=True)])


def _assert_spark_single_value(actual: StructType, expected: DataType) -> None:
    assert actual.jsonValue() == {
        "type": "struct",
        "fields": [{"name": "value", "type": expected.jsonValue(), "nullable": True, "metadata": {}}],
    }


@dataclass(frozen=True, slots=True)
class TabularTargetSpec:
    """Target-specific expectations for shared tabular emitter tests."""

    emitter_cls: type[_TabularEmitterBase[Any]]
    error_cls: type[ConversionError]
    logger_name: str
    target_name: str
    decimal_target_label: str
    empty_struct_warning: str
    non_struct_root_message: str
    default_format_map: Mapping[str, Any]
    string_type: Any
    bool_type: Any
    null_type: Any
    int32_type: Any
    int64_type: Any
    date_type: Any
    timestamp_type: Any
    float64_type: Any
    decimal_type: Callable[[int, int], Any]
    list_type: Callable[[Any], Any]
    map_type: Callable[[Any], Any]
    struct_type: Callable[[list[tuple[str, Any]]], Any]
    assert_single_value_schema: Callable[[Any, Any], None]
    infer_enum: Callable[[Sequence[object]], Any]
    extract_field_metadata: Callable[[Any, str], dict[str, Any]]
    field_nullabilities: Callable[[Any], list[tuple[str, bool]]]
    unwrap_single_struct_field: Callable[[Any], Any]


ARROW_SPEC = TabularTargetSpec(
    emitter_cls=PyArrowEmitter,
    error_cls=PyArrowEmitterError,
    logger_name="cdm_data_loaders.converters.emitters.pyarrow",
    target_name="Arrow",
    decimal_target_label="Arrow",
    empty_struct_warning="empty struct",
    non_struct_root_message="did not resolve to a struct",
    default_format_map=ARROW_FORMAT_MAP,
    string_type=pa.string(),
    bool_type=pa.bool_(),
    null_type=pa.null(),
    int32_type=pa.int32(),
    int64_type=pa.int64(),
    date_type=pa.date32(),
    timestamp_type=pa.timestamp("us", tz="UTC"),
    float64_type=pa.float64(),
    decimal_type=pa.decimal128,
    list_type=lambda element: pa.list_(pa.field("item", element, nullable=True)),
    map_type=lambda value: pa.map_(pa.string(), pa.field("value", value, nullable=True)),
    struct_type=lambda fields: pa.struct([pa.field(name, dt) for name, dt in fields]),
    assert_single_value_schema=_assert_arrow_single_value,
    infer_enum=arrow_infer_type_from_enum,
    extract_field_metadata=_arrow_field_metadata,
    field_nullabilities=lambda schema: [(field.name, field.nullable) for field in schema],
    unwrap_single_struct_field=lambda dt: dt.field(0).type if pa.types.is_struct(dt) else None,
)

SPARK_SPEC = TabularTargetSpec(
    emitter_cls=PySparkEmitter,
    error_cls=PySparkEmitterError,
    logger_name="cdm_data_loaders.converters.emitters.pyspark",
    target_name="PySpark",
    decimal_target_label="Spark",
    empty_struct_warning="empty StructType",
    non_struct_root_message="did not resolve to a StructType",
    default_format_map=SPARK_FORMAT_MAP,
    string_type=StringType(),
    bool_type=BooleanType(),
    null_type=NullType(),
    int32_type=IntegerType(),
    int64_type=LongType(),
    date_type=DateType(),
    timestamp_type=TimestampType(),
    float64_type=DoubleType(),
    decimal_type=DecimalType,
    list_type=ArrayType,
    map_type=lambda value: MapType(StringType(), value),
    struct_type=lambda fields: StructType([StructField(name, dt) for name, dt in fields]),
    assert_single_value_schema=_assert_spark_single_value,
    infer_enum=spark_infer_type_from_enum,
    extract_field_metadata=lambda schema, name: schema[name].metadata,
    field_nullabilities=lambda schema: [(field.name, field.nullable) for field in schema.fields],
    unwrap_single_struct_field=lambda dt: dt.fields[0].dataType if isinstance(dt, StructType) else None,
)


@pytest.fixture(params=[ARROW_SPEC, SPARK_SPEC], ids=["pyarrow", "pyspark"])
def spec(request: pytest.FixtureRequest) -> TabularTargetSpec:
    """Parametrize shared tabular emitter tests across PyArrow and PySpark."""
    return request.param


def test_emit_pass_field_presence_controls_nullable(spec: TabularTargetSpec) -> None:
    """Required presence controls field nullability independently of value nullability."""
    document = SchemaDocument(
        root=TypedNode(
            type="object",
            properties=(
                Field("required", TypedNode(type="string", nullable=True), required=True),
                Field("optional", TypedNode(type="string", nullable=False)),
            ),
        )
    )
    schema = spec.emitter_cls().emit(document)
    assert spec.field_nullabilities(schema) == [("required", False), ("optional", True)]


def test_emit_pass_nesting_limit(spec: TabularTargetSpec, caplog: pytest.LogCaptureFixture) -> None:
    """Objects at max_nesting collapse to string."""

    def make_deep_node(depth: int) -> TypedNode:
        if depth <= 0:
            return TypedNode(type="string")
        return TypedNode(type="object", properties=(Field("child", make_deep_node(depth - 1), required=True),))

    with caplog.at_level(logging.WARNING, logger=spec.logger_name):
        current = spec.emitter_cls(max_nesting=10).emit_node(make_deep_node(15))

    for _ in range(10):
        next_type = spec.unwrap_single_struct_field(current)
        assert next_type is not None
        current = next_type
    assert current == spec.string_type
    assert "Max nesting limit (10) reached" in caplog.text


@pytest.mark.parametrize("strict", [False, True], ids=["fallback-policy", "strict-policy"])
@pytest.mark.parametrize(
    ("schema", "message_template"),
    [
        pytest.param(True, "no {target} equivalent", id="true-schema"),
        pytest.param(False, "no {target} equivalent", id="false-schema"),
        pytest.param({}, "Unsupported/unknown schema type", id="empty-schema"),
        pytest.param({"type": "alien"}, "Unsupported/unknown schema type", id="unknown-type"),
        pytest.param({"description": "anything"}, "Unsupported/unknown schema type", id="annotation-only"),
        pytest.param({"const": 1}, "Unsupported/unknown schema type", id="const-only"),
        pytest.param({"oneOf": []}, "Unsupported/unknown schema type", id="empty-oneof"),
        pytest.param({"anyOf": []}, "Unsupported/unknown schema type", id="empty-anyof"),
        pytest.param({"oneOf": [False]}, "no {target} equivalent", id="false-combiner-branch"),
        pytest.param({"type": ["string", "integer"]}, "multi-type union", id="mixed-type-list"),
        pytest.param({"type": ["string", "string"]}, "multi-type union", id="duplicate-non-null-types"),
        pytest.param({"not": {"type": "string"}}, "Unsupported/unknown schema type", id="not"),
        pytest.param({"if": {"type": "string"}}, "Unsupported/unknown schema type", id="if"),
        pytest.param({"then": {"type": "string"}}, "Unsupported/unknown schema type", id="then"),
        pytest.param({"else": {"type": "string"}}, "Unsupported/unknown schema type", id="else"),
    ],
)
def test_emit_node_pass_unsupported_policy(
    spec: TabularTargetSpec, schema: dict[str, Any] | bool, message_template: str, strict: bool
) -> None:
    """Unsupported nodes either raise their specific error or use a string fallback."""
    emitter = spec.emitter_cls(treat_unknown_as_string=not strict)
    node = _document(schema).root.properties[0].node
    message = message_template.format(target=spec.target_name)
    if strict:
        with pytest.raises(spec.error_cls, match=message):
            emitter.emit_node(node)
    else:
        assert emitter.emit_node(node) == spec.string_type


@pytest.mark.parametrize(
    "schema",
    [
        {"type": "array", "items": True},
        {"type": "array", "items": {"description": "unknown"}},
        {"type": "array", "items": {"oneOf": []}},
        {"type": "array", "prefixItems": [True]},
        {"type": "object", "additionalProperties": {}},
        {"type": "object", "patternProperties": {"a": False}},
    ],
    ids=["true-items", "nonempty-items", "empty-combiner-items", "true-prefix", "empty-map-value", "false-map-value"],
)
def test_emit_fail_strict_nested_unknown(spec: TabularTargetSpec, schema: dict[str, Any]) -> None:
    """Nonempty unknown array schemas and dynamic map schemas use strict dispatch."""
    with pytest.raises(spec.error_cls):
        spec.emitter_cls(treat_unknown_as_string=False).emit(_document(schema))


@pytest.mark.parametrize(
    ("schema", "warning"),
    [
        pytest.param({"oneOf": [{"type": "integer"}]}, "Approximating 'oneOf'", id="oneof"),
        pytest.param({"anyOf": [{"type": "integer"}]}, "Approximating 'anyOf'", id="anyof"),
        pytest.param({"not": False}, "Ignoring unsupported conditional keyword 'not'", id="not"),
        pytest.param({"if": False}, "Ignoring unsupported conditional keyword 'if'", id="if"),
        pytest.param({"then": False}, "Ignoring unsupported conditional keyword 'then'", id="then"),
        pytest.param({"else": False}, "Ignoring unsupported conditional keyword 'else'", id="else"),
        pytest.param({"type": ["string", "integer"]}, "Collapsing multi-type union", id="union"),
        pytest.param(
            {"properties": {"value": {"type": "string"}}, "additionalProperties": {}},
            "dynamic keys are ignored",
            id="fixed-dynamic",
        ),
        pytest.param(
            {"patternProperties": {"a": {"type": "integer"}, "b": False}},
            "only the first pattern's schema",
            id="patterns",
        ),
        pytest.param({"type": "object"}, "{empty_struct}", id="empty-object"),
        pytest.param({"type": "array", "prefixItems": []}, "prefixItems", id="prefix"),
        pytest.param({"type": "array", "items": []}, "tuple-style 'items'", id="tuple"),
    ],
)
def test_emit_pass_warnings(
    spec: TabularTargetSpec, schema: dict[str, Any], warning: str, caplog: pytest.LogCaptureFixture
) -> None:
    """Lossy schema approximations report the selected policy."""
    expected_warning = warning.format(empty_struct=spec.empty_struct_warning)
    with caplog.at_level(logging.WARNING, logger=spec.logger_name):
        spec.emitter_cls().emit(_document(schema))
    assert expected_warning in caplog.text


@pytest.mark.parametrize(
    "schema",
    [
        {"type": "string", "oneOf": [False], "not": False},
        {"format": "date", "anyOf": [False], "if": False},
        {"properties": {"value": {"type": "string"}}, "additionalProperties": True},
    ],
    ids=["declared", "inferred", "boolean-additional"],
)
def test_emit_pass_recognized_types_ignore_combiners_silently(
    spec: TabularTargetSpec, schema: dict[str, Any], caplog: pytest.LogCaptureFixture
) -> None:
    """Recognized types ignore irrelevant combiners without warning."""
    spec.emitter_cls(treat_unknown_as_string=False).emit(_document(schema))
    assert not [record for record in caplog.records if record.name == spec.logger_name]


@pytest.mark.parametrize(
    "node",
    [
        TypedNode(type="string"),
        TypedNode(type="object", additional_properties=TypedNode(type="string")),
        TypedNode(type="array", items=TypedNode(type="string")),
        TypedNode(type="unknown"),
    ],
    ids=["scalar", "dynamic-map", "array", "unknown"],
)
def test_emit_fail_non_struct_root(spec: TabularTargetSpec, node: TypedNode) -> None:
    """Table emission rejects every non-struct root result."""
    with pytest.raises(spec.error_cls, match=spec.non_struct_root_message):
        spec.emitter_cls().emit(SchemaDocument(root=node))


@pytest.mark.parametrize(
    "node",
    [
        TypedNode(type="map"),
        TypedNode(type="map", key_type=TypedNode(type="string")),
        TypedNode(type="map", value_type=TypedNode(type="number")),
    ],
    ids=["no-key-or-value", "no-value", "no-key"],
)
def test_emit_node_fail_incomplete_map(spec: TabularTargetSpec, node: TypedNode) -> None:
    """Map nodes need both key and value types."""
    with pytest.raises(spec.error_cls, match="Map nodes require both key_type and value_type"):
        spec.emitter_cls().emit_node(node)


def test_emit_node_pass_json_source_ignores_hints(spec: TabularTargetSpec) -> None:
    """Hints on JSON Schema nodes are ignored in favor of keyword dispatch."""
    node = TypedNode(
        type="number",
        hints=NodeHints(logical_type="decimal", precision=10, scale=2),
        provenance=Provenance("json-schema"),
    )
    assert spec.emitter_cls().emit_node(node) == spec.float64_type


def test_emit_node_fail_decimal_scale_exceeds_precision_limit(spec: TabularTargetSpec) -> None:
    """A multipleOf scale beyond 38 digits cannot be a decimal128."""
    node = TypedNode(type="number", constraints={"multipleOf": Decimal("0." + "0" * 39 + "1")})
    expected_msg = rf"Unsupported {spec.decimal_target_label} decimal precision/scale: \(38, 40\)"
    with pytest.raises(spec.error_cls, match=expected_msg):
        spec.emitter_cls().emit_node(node)


@pytest.mark.parametrize("strict", [False, True], ids=["fallback-policy", "strict-policy"])
def test_emit_node_pass_malformed_enum_policy(spec: TabularTargetSpec, strict: bool) -> None:
    """Non-sequence enum constraints follow the unsupported-node policy."""
    node = TypedNode(type="unknown", constraints={"enum": None})
    emitter = spec.emitter_cls(treat_unknown_as_string=not strict)
    if strict:
        with pytest.raises(spec.error_cls, match="Unsupported/unknown schema type"):
            emitter.emit_node(node)
    else:
        assert emitter.emit_node(node) == spec.string_type


def test_build_context_pass_warns_about_invalid_keywords(
    spec: TabularTargetSpec, caplog: pytest.LogCaptureFixture
) -> None:
    """Structural and unknown extra keywords are reported and ignored."""
    emitter = spec.emitter_cls(extra_metadata_keywords=frozenset({"type", "typo", "x-ok"}))
    with caplog.at_level(logging.WARNING, logger=spec.logger_name):
        context = emitter.build_context(Draft202012Validator)
    assert context.allowed_extra_metadata_keywords == frozenset({"x-ok"})
    assert "['type', 'typo']" in caplog.text


def test_emit_node_pass_formats(spec: TabularTargetSpec) -> None:
    """Every built-in format, unknown format, and non-string format resolves to the expected type."""
    emitter = spec.emitter_cls()
    for format_name, expected in [
        *spec.default_format_map.items(),
        ("unknown", spec.string_type),
        ("", spec.string_type),
    ]:
        node = TypedNode(type="string", constraints={"format": format_name})
        assert emitter.emit_node(node) == expected
    assert emitter.emit_node(TypedNode(type="string", constraints={"format": 1})) == spec.string_type


@pytest.mark.parametrize("use_user_dict", [False, True], ids=["dict-override", "mapping-override"])
def test_emitter_pass_merged_frozen_formats(spec: TabularTargetSpec, use_user_dict: bool) -> None:
    """Format overrides are copied and merged while the emitter's configuration remains frozen."""
    raw = {"date-time": spec.string_type}
    overrides: dict[str, Any] | UserDict[str, Any] = UserDict(raw) if use_user_dict else raw
    expected = {**spec.default_format_map, **overrides}
    emitter = spec.emitter_cls.model_validate({"format_map": overrides})
    overrides["date"] = spec.bool_type
    assert emitter.format_map == frozendict(expected)
    assert isinstance(emitter.format_map, frozendict)
    with pytest.raises(ValidationError, match="Instance is frozen"):
        emitter.treat_unknown_as_string = False
    with pytest.raises(TypeError):
        emitter.format_map["date"] = spec.bool_type  # type: ignore[index]
    assert emitter.emit_node(TypedNode(type="string", constraints={"format": "date-time"})) == spec.string_type


@pytest.mark.parametrize("bad_kind", ["none", "text", "value", "key"])
def test_emitter_fail_invalid_format_map(spec: TabularTargetSpec, bad_kind: str) -> None:
    """Invalid format map containers, keys and values fail model validation."""
    value: object = {
        "none": None,
        "text": "bad",
        "value": {"date": "date"},
        "key": {1: spec.date_type},
    }[bad_kind]
    with pytest.raises(ValidationError):
        spec.emitter_cls.model_validate({"format_map": value})


def test_emitter_fail_invalid_max_nesting(spec: TabularTargetSpec) -> None:
    """max_nesting must be positive."""
    with pytest.raises(ValidationError):
        spec.emitter_cls(max_nesting=0)


@pytest.mark.parametrize(
    "value", [["pattern"], ("pattern",), {"pattern"}, frozenset({"pattern"})], ids=["list", "tuple", "set", "frozen"]
)
def test_emitter_pass_metadata_keyword_coercion(spec: TabularTargetSpec, value: object) -> None:
    """Metadata keyword collections are normalized into frozensets."""
    emitter = spec.emitter_cls.model_validate({"extra_metadata_keywords": value})
    assert emitter.extra_metadata_keywords == frozenset({"pattern"})
    assert isinstance(emitter.extra_metadata_keywords, frozenset)


@pytest.mark.parametrize(
    "items",
    [False, {}, [], None],
    ids=["false-schema", "empty-schema", "empty-tuple", "absent-items"],
)
def test_emit_pass_strict_empty_array_items(spec: TabularTargetSpec, items: object) -> None:
    """Empty and false array item schemas produce string elements in strict mode."""
    array: dict[str, Any] = {"type": "array"}
    if items is not None:
        array["items"] = items
    document = JsonSchemaReader().read({"$schema": DIALECT, "properties": {"value": array}})
    actual = spec.emitter_cls(treat_unknown_as_string=False).emit(document)
    spec.assert_single_value_schema(actual, spec.list_type(spec.string_type))


def test_emit_pass_metadata(spec: TabularTargetSpec) -> None:
    """Titles, descriptions and requested keywords retain their values in field metadata."""
    document = JsonSchemaReader().read(
        {
            "$schema": DIALECT,
            "properties": {
                "name": {"type": "string", "title": "Name", "description": "A name", "pattern": "^a", "x-pii": True}
            },
        }
    )
    emitter = spec.emitter_cls(extra_metadata_keywords=frozenset({"pattern", "description", "x-pii"}))
    assert spec.extract_field_metadata(emitter.emit(document), "name") == {
        "jsonschema": {"title": "Name", "description": "A name", "pattern": "^a", "x-pii": True},
        "comment": "A name",
    }


def test_emit_pass_field_metadata_overrides_node(spec: TabularTargetSpec) -> None:
    """Field annotations and extensions take precedence over type-scoped metadata."""
    node = TypedNode(type="string", annotations={"title": "Node", "description": "node"}, extensions={"x-pii": False})
    field = Field("value", node, annotations={"description": "field"}, extensions={"x-pii": True})
    document = SchemaDocument(root=TypedNode(type="object", properties=(field,)))
    emitted = spec.emitter_cls(extra_metadata_keywords=frozenset({"x-pii"})).emit(document)
    assert spec.extract_field_metadata(emitted, "value") == {
        "jsonschema": {"title": "Node", "x-pii": True},
        "comment": "field",
    }


@pytest.mark.parametrize(
    ("values", "expected_getter"),
    [
        pytest.param([True], lambda s: s.bool_type, id="boolean"),
        pytest.param([1, 2], lambda s: s.int64_type, id="integer"),
        pytest.param([1, 2.5], lambda s: s.float64_type, id="number"),
        pytest.param(["a"], lambda s: s.string_type, id="string"),
        pytest.param([], lambda s: s.string_type, id="empty"),
        pytest.param([None], lambda s: s.string_type, id="null"),
        pytest.param([1, "a"], lambda s: s.string_type, id="mixed"),
    ],
)
def test_infer_type_from_enum_pass(
    spec: TabularTargetSpec,
    values: list[object],
    expected_getter: Callable[[TabularTargetSpec], Any],
) -> None:
    """Enum members map to the narrowest shared target scalar type."""
    assert spec.infer_enum(values) == expected_getter(spec)


@pytest.mark.parametrize("strict", [False, True], ids=["fallback-policy", "strict-policy"])
@pytest.mark.parametrize(
    ("schema", "expected_getter"),
    [
        pytest.param({"type": "string"}, lambda s: s.string_type, id="string"),
        pytest.param({"type": "boolean"}, lambda s: s.bool_type, id="boolean"),
        pytest.param({"type": "null"}, lambda s: s.null_type, id="null"),
        pytest.param({"type": "integer"}, lambda s: s.int64_type, id="integer"),
        pytest.param({"type": "number"}, lambda s: s.float64_type, id="number"),
        pytest.param({"type": "object"}, lambda s: s.struct_type([]), id="empty-object"),
        pytest.param({"type": ["null", "string"]}, lambda s: s.string_type, id="nullable-string"),
        pytest.param({"type": ["integer"]}, lambda s: s.int64_type, id="single-type-list"),
        pytest.param({"type": ["null", "null"]}, lambda s: s.null_type, id="null-only-list"),
        pytest.param({"type": []}, lambda s: s.null_type, id="empty-type-list"),
        pytest.param({"enum": [True, False]}, lambda s: s.bool_type, id="boolean-enum"),
        pytest.param({"enum": [1, 2]}, lambda s: s.int64_type, id="integer-enum"),
        pytest.param({"enum": [1, 2.5]}, lambda s: s.float64_type, id="number-enum"),
        pytest.param({"enum": ["a", "b"]}, lambda s: s.string_type, id="string-enum"),
        pytest.param({"enum": []}, lambda s: s.string_type, id="empty-enum"),
        pytest.param({"enum": [None]}, lambda s: s.string_type, id="null-enum"),
        pytest.param({"enum": [True, 1]}, lambda s: s.string_type, id="bool-is-not-integer-enum"),
        pytest.param({"enum": [1, "a"]}, lambda s: s.string_type, id="mixed-enum"),
        pytest.param({"enum": [{"type": "integer"}]}, lambda s: s.string_type, id="object-enum"),
        pytest.param({"enum": [Decimal("1.1")]}, lambda s: s.string_type, id="decimal-enum"),
        pytest.param(
            {"enum": [1], "minimum": 0, "oneOf": [{"type": "string"}]},
            lambda s: s.int64_type,
            id="enum-precedence",
        ),
        pytest.param({"type": "string", "enum": [1]}, lambda s: s.string_type, id="declared-over-enum"),
        pytest.param(
            {"oneOf": [{"type": "integer"}, {"type": "string"}]},
            lambda s: s.int64_type,
            id="oneof-first",
        ),
        pytest.param(
            {"anyOf": [{"type": "null"}, {"type": "string"}]},
            lambda s: s.null_type,
            id="anyof-null-first",
        ),
        pytest.param(
            {"oneOf": [{"type": "boolean"}], "anyOf": [{"type": "integer"}]},
            lambda s: s.bool_type,
            id="oneof-over-anyof",
        ),
        pytest.param(
            {"oneOf": [], "anyOf": [{"type": "integer"}]},
            lambda s: s.int64_type,
            id="empty-oneof-before-anyof",
        ),
        pytest.param(
            {"type": "mystery", "anyOf": [{"type": "boolean"}]},
            lambda s: s.bool_type,
            id="unknown-with-combiner",
        ),
        pytest.param(
            {"type": "string", "oneOf": [False], "anyOf": [False], "not": False},
            lambda s: s.string_type,
            id="declared-over-combiners",
        ),
        pytest.param({"format": "date", "oneOf": [False]}, lambda s: s.date_type, id="inferred-over-combiner"),
        pytest.param({"properties": {}, "items": False}, lambda s: s.struct_type([]), id="object-inference-first"),
        pytest.param(
            {"items": {"type": "integer"}, "pattern": "a"},
            lambda s: s.list_type(s.int64_type),
            id="array-before-string",
        ),
        pytest.param({"pattern": "a", "multipleOf": 0.01}, lambda s: s.string_type, id="string-before-number"),
        pytest.param({"minimum": 0, "maximum": 10}, lambda s: s.float64_type, id="numeric-inference"),
        pytest.param({"multipleOf": 0.01}, lambda s: s.decimal_type(38, 2), id="decimal-inference"),
        pytest.param({"required": ["missing"]}, lambda s: s.struct_type([]), id="required-implies-object"),
        pytest.param({"uniqueItems": True}, lambda s: s.list_type(s.string_type), id="uniqueness-implies-array"),
        pytest.param({"type": "array", "prefixItems": []}, lambda s: s.list_type(s.string_type), id="empty-prefix"),
        pytest.param(
            {"type": "array", "prefixItems": [False]},
            lambda s: s.list_type(s.string_type),
            id="false-prefix",
        ),
        pytest.param(
            {"type": "array", "prefixItems": [{}]},
            lambda s: s.list_type(s.string_type),
            id="empty-prefix-schema",
        ),
        pytest.param(
            {"type": "array", "prefixItems": [{"type": "integer"}, False], "items": True},
            lambda s: s.list_type(s.int64_type),
            id="prefix-before-items",
        ),
        pytest.param(
            {"type": "array", "items": [{"type": "boolean"}, False]},
            lambda s: s.list_type(s.bool_type),
            id="tuple-items",
        ),
        pytest.param({"type": "array", "items": [False]}, lambda s: s.list_type(s.string_type), id="false-tuple"),
        pytest.param(
            {"type": "array", "items": {"type": "array", "items": {"type": "integer"}}},
            lambda s: s.list_type(s.list_type(s.int64_type)),
            id="nested-arrays",
        ),
        pytest.param(
            {"type": "object", "additionalProperties": True},
            lambda s: s.struct_type([]),
            id="boolean-additional-true",
        ),
        pytest.param(
            {"type": "object", "additionalProperties": False},
            lambda s: s.struct_type([]),
            id="boolean-additional-false",
        ),
        pytest.param(
            {"type": "object", "additionalProperties": {"type": "integer"}},
            lambda s: s.map_type(s.int64_type),
            id="additional-schema",
        ),
        pytest.param(
            {"patternProperties": {"a": {"type": "boolean"}, "b": False}, "additionalProperties": False},
            lambda s: s.map_type(s.bool_type),
            id="first-pattern",
        ),
        pytest.param(
            {"patternProperties": {"a": {"type": "integer"}}, "additionalProperties": {"type": "boolean"}},
            lambda s: s.map_type(s.int64_type),
            id="pattern-before-additional",
        ),
        pytest.param(
            {"properties": {"name": {"type": "string"}}, "patternProperties": {"a": False}, "additionalProperties": {}},
            lambda s: s.struct_type([("name", s.string_type)]),
            id="fixed-before-dynamic",
        ),
        pytest.param({"type": "integer", "minimum": 0, "maximum": 100}, lambda s: s.int32_type, id="bounded-int"),
        pytest.param(
            {"type": "integer", "minimum": -2_147_483_648, "maximum": 2_147_483_647},
            lambda s: s.int32_type,
            id="int32-boundaries",
        ),
        pytest.param(
            {"type": "integer", "minimum": -2_147_483_649, "maximum": 100},
            lambda s: s.int64_type,
            id="below-int32",
        ),
        pytest.param(
            {"type": "integer", "minimum": 0, "maximum": 2_147_483_648},
            lambda s: s.int64_type,
            id="above-int32",
        ),
        pytest.param({"type": "integer", "minimum": 0}, lambda s: s.int64_type, id="missing-maximum"),
        pytest.param({"type": "integer", "maximum": 0}, lambda s: s.int64_type, id="missing-minimum"),
        pytest.param(
            {"type": "integer", "exclusiveMinimum": 0, "exclusiveMaximum": 100},
            lambda s: s.int32_type,
            id="exclusive-bounds",
        ),
        pytest.param(
            {"type": "integer", "minimum": None, "exclusiveMinimum": 0, "maximum": 100},
            lambda s: s.int64_type,
            id="null-minimum-does-not-use-exclusive",
        ),
        pytest.param({"type": "number", "multipleOf": 1}, lambda s: s.decimal_type(38, 0), id="integral-multiple"),
        pytest.param(
            {"type": "number", "multipleOf": 1.0},
            lambda s: s.decimal_type(38, 1),
            id="float-integral-multiple",
        ),
        pytest.param(
            {"type": "number", "multipleOf": 0.001},
            lambda s: s.decimal_type(38, 3),
            id="fractional-multiple",
        ),
        pytest.param({"type": "number", "multipleOf": 1e-9}, lambda s: s.decimal_type(38, 9), id="exponent-multiple"),
        pytest.param({"type": "number", "multipleOf": "0.01"}, lambda s: s.float64_type, id="nonnumeric-multiple"),
        pytest.param(
            {"type": "number", "multipleOf": Decimal("0.0100")},
            lambda s: s.decimal_type(38, 4),
            id="decimal-multiple-parity",
        ),
        pytest.param({"type": "string", "format": "date"}, lambda s: s.date_type, id="date-format"),
        pytest.param({"type": "string", "format": "date-time"}, lambda s: s.timestamp_type, id="date-time-format"),
        pytest.param({"type": "string", "format": "uuid"}, lambda s: s.string_type, id="uuid-format"),
        pytest.param({"type": "string", "format": "unknown"}, lambda s: s.string_type, id="unknown-format"),
    ],
)
def test_emit_pass_json_dispatch(
    spec: TabularTargetSpec,
    schema: dict[str, Any],
    expected_getter: Callable[[TabularTargetSpec], Any],
    strict: bool,
) -> None:
    """Typed dispatch matches fixed expected schemas under both unknown-type policies."""
    emitter = spec.emitter_cls(treat_unknown_as_string=not strict)
    spec.assert_single_value_schema(emitter.emit(_document(schema)), expected_getter(spec))
