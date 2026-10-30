"""Shared unit tests for tabular emitters (_TabularEmitterBase: PyArrow and PySpark)."""

import logging
from collections import UserDict
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

import pyarrow as pa
import pytest
from frozendict import frozendict
from jsonschema import Draft202012Validator
from pydantic import ValidationError
from pyspark.sql.types import BooleanType, DateType, DoubleType, StringType, StructType

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
from cdm_data_loaders.converters.emitters.pyspark import (
    DEFAULT_FORMAT_MAP as SPARK_FORMAT_MAP,
)
from cdm_data_loaders.converters.emitters.pyspark import (
    PySparkEmitter,
    PySparkEmitterError,
)
from cdm_data_loaders.converters.readers.json_schema import JsonSchemaReader

DIALECT = "https://json-schema.org/draft/2020-12/schema"


def _document(schema: dict[str, Any] | bool) -> SchemaDocument:
    """Wrap a fragment in an object-compatible JSON document."""
    return JsonSchemaReader().read({"$schema": DIALECT, "properties": {"value": schema}})


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
    string_type: object
    bool_type: object
    date_type: object
    float64_type: object
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
    date_type=pa.date32(),
    float64_type=pa.float64(),
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
    date_type=DateType(),
    float64_type=DoubleType(),
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
    for format_name, expected in [*spec.default_format_map.items(), ("unknown", spec.string_type), ("", spec.string_type)]:
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
