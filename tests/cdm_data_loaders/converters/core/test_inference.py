"""Tests for target-independent type inference."""

from decimal import Decimal
from typing import Any, Final

import pytest

from cdm_data_loaders.converters.core.inference import (
    IMPLICIT_ARRAY_KEYWORDS,
    IMPLICIT_NUMBER_KEYWORDS,
    IMPLICIT_OBJECT_KEYWORDS,
    IMPLICIT_STRING_KEYWORDS,
    decimal_pattern,
    decimal_places,
    infer_implicit_type,
    is_unconstrained_node,
    json_type_from_enum,
    nodes_same_shape,
    unify_scalar_specs,
)
from cdm_data_loaders.converters.core.ir import (
    SPEC_FLOAT64,
    SPEC_STRING,
    SPEC_TS_UTC,
    Field,
    NodeHints,
    TypedNode,
)

DEC_10_2: Final[tuple[str, NodeHints, str]] = (
    "number",
    NodeHints(logical_type="decimal", precision=10, scale=2),
    "Decimal(10, 2)",
)
DEC_12_4: Final[tuple[str, NodeHints, str]] = (
    "number",
    NodeHints(logical_type="decimal", precision=12, scale=4),
    "Decimal(12, 4)",
)
DEC_38_0: Final[tuple[str, NodeHints, str]] = (
    "number",
    NodeHints(logical_type="decimal", precision=38, scale=0),
    "Decimal(38, 0)",
)
LONG_SPEC: Final[tuple[str, NodeHints, str]] = ("integer", NodeHints(logical_type="long", bit_width=64), "int")
TS_TZ_SPEC: Final[tuple[str, NodeHints, str]] = (
    "string",
    NodeHints(logical_type="timestamp-with-tz", timezone=True),
    "AwareDT",
)
TS_NAIVE_SPEC: Final[tuple[str, NodeHints, str]] = (
    "string",
    NodeHints(logical_type="timestamp-without-tz", timezone=False),
    "NaiveDT",
)
BOOL_SPEC: Final[tuple[str, NodeHints, str]] = ("boolean", NodeHints(logical_type="boolean"), "bool")

STR_NODE: Final[TypedNode] = TypedNode("string")
NULLABLE_STR_NODE: Final[TypedNode] = TypedNode("string", nullable=True)
INT_NODE: Final[TypedNode] = TypedNode("integer")
DATE_STR_NODE: Final[TypedNode] = TypedNode("string", hints=NodeHints(logical_type="date"))
ARRAY_STR_NODE: Final[TypedNode] = TypedNode("array", items=STR_NODE)
MAP_STR_INT_NODE: Final[TypedNode] = TypedNode("map", key_type=STR_NODE, value_type=INT_NODE)
OBJ_X_REQ_NODE: Final[TypedNode] = TypedNode("object", properties=(Field("x", STR_NODE, required=True),))


@pytest.mark.parametrize(
    ("node", "expected"),
    [
        pytest.param(TypedNode(type="never"), True, id="never"),
        pytest.param(TypedNode(type="unknown"), True, id="bare-unknown"),
        pytest.param(TypedNode(type="any"), False, id="any"),
        pytest.param(TypedNode(type="string"), False, id="string"),
        pytest.param(TypedNode(type="unknown", declared_type="alien"), False, id="declared-type"),
        pytest.param(TypedNode(type="unknown", inferred_type="string"), False, id="inferred-type"),
        pytest.param(TypedNode(type="unknown", source_keywords=("type",)), False, id="source-keyword"),
        pytest.param(TypedNode(type="unknown", constraints={"const": 1}), False, id="constraint"),
        pytest.param(TypedNode(type="unknown", annotations={"description": "d"}), False, id="annotation"),
        pytest.param(TypedNode(type="unknown", one_of=()), False, id="empty-one-of"),
        pytest.param(TypedNode(type="unknown", any_of=()), False, id="empty-any-of"),
        pytest.param(
            TypedNode(type="unknown", schema_keywords={"not": TypedNode(type="never")}), False, id="schema-keyword"
        ),
        pytest.param(
            TypedNode(type="unknown", schema_maps={"$defs": {"a": TypedNode(type="string")}}), False, id="schema-map"
        ),
    ],
)
def test_is_unconstrained_node_pass(node: TypedNode, expected: bool) -> None:
    """Only a never node or a keyword-free unknown node counts as unconstrained."""
    assert is_unconstrained_node(node) is expected


@pytest.mark.parametrize(
    ("keyword", "expected"),
    [
        pytest.param(keyword, json_type, id=f"{json_type}-{keyword}")
        for json_type, keywords in (
            ("object", IMPLICIT_OBJECT_KEYWORDS),
            ("array", IMPLICIT_ARRAY_KEYWORDS),
            ("string", IMPLICIT_STRING_KEYWORDS),
            ("number", IMPLICIT_NUMBER_KEYWORDS),
        )
        for keyword in sorted(keywords)
    ],
)
def test_infer_implicit_type_pass_keywords(keyword: str, expected: str) -> None:
    """Infer each keyword family from presence, independent of its value."""
    assert infer_implicit_type({keyword: None}) == expected


@pytest.mark.parametrize(
    ("schema", "expected"),
    [
        ({}, None),
        ({"title": "label", "description": "text"}, None),
        ({"const": 1}, None),
        ({"type": "integer"}, None),
        ({"required": [], "items": {}, "pattern": "", "minimum": 0}, "object"),
        ({"items": {}, "pattern": "", "minimum": 0}, "array"),
        ({"pattern": "", "minimum": 0}, "string"),
        ({"multipleOf": 1}, "number"),
    ],
    ids=[
        "empty",
        "annotations",
        "const",
        "explicit-type-only",
        "object-first",
        "array-first",
        "string-first",
        "numeric",
    ],
)
def test_infer_implicit_type_pass_precedence(schema: dict[str, Any], expected: str | None) -> None:
    """Preserve keyword precedence and leave unconstrained fragments uninferred."""
    original = schema.copy()
    assert infer_implicit_type(schema) == expected
    assert schema == original


@pytest.mark.parametrize(
    ("values", "expected"),
    [
        ([], "string"),
        ([True, False], "boolean"),
        ([-1, 0, 2], "integer"),
        ([0.0, 1.5], "number"),
        ([1, 2.5], "number"),
        ([1.0, 2.0], "number"),
        (["", "text"], "string"),
        ([True, 1], "string"),
        ([False, 1.5], "string"),
        ([1, "text"], "string"),
        ([None], "string"),
        ([1, None], "string"),
        ([{}], "string"),
        ([[]], "string"),
    ],
    ids=[
        "empty",
        "booleans",
        "integers",
        "floats",
        "mixed-numbers",
        "integral-floats",
        "strings",
        "bool-is-not-int",
        "bool-is-not-number",
        "heterogeneous",
        "null",
        "nullable-int",
        "objects",
        "arrays",
    ],
)
def test_json_type_from_enum_pass_scalar_inference(values: list[Any], expected: str) -> None:
    """Infer scalar enum types without treating booleans as integers or numbers."""
    assert json_type_from_enum(values) == expected


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (0, 0),
        (100, 0),
        (-10, 0),
        (0.01, 2),
        (-0.125, 3),
        (1e-7, 7),
        (Decimal("1.2300"), 4),
        (Decimal("1E+3"), 0),
        (Decimal("0.000"), 3),
        (Decimal("1.0000000000000000000000000001"), 28),
        (float("inf"), 0),
        (float("-inf"), 0),
        (float("nan"), 0),
        (Decimal("sNaN"), 0),
    ],
    ids=[
        "zero",
        "integer",
        "negative-integer",
        "hundredth",
        "negative-fraction",
        "scientific",
        "trailing-zeros",
        "positive-exponent",
        "scaled-zero",
        "exact-decimal",
        "infinity",
        "negative-infinity",
        "nan",
        "signaling-nan",
    ],
)
def test_decimal_places_pass_scale(value: float | Decimal, expected: int) -> None:
    """Count decimal scale without converting exact decimals through binary floats."""
    assert decimal_places(value) == expected


@pytest.mark.parametrize(
    ("precision", "scale", "expected"),
    [
        (10, 2, r"^-?\d{1,8}(\.\d{1,2})?$"),
        (5, 2, r"^-?\d{1,3}(\.\d{1,2})?$"),
        (2, 2, r"^-?0(\.\d{1,2})?$"),
        (10, 0, r"^-?\d{1,10}$"),
        (4, 0, r"^-?\d{1,4}$"),
    ],
    ids=["standard", "small-standard", "zero-before-decimal", "no-scale", "small-no-scale"],
)
def test_decimal_pattern_pass_regex(precision: int, scale: int, expected: str) -> None:
    """Build precise regex patterns for decimal constraints."""
    assert decimal_pattern(precision, scale) == expected


@pytest.mark.parametrize(
    ("specs", "expected"),
    [
        pytest.param([DEC_10_2, DEC_10_2], ("number", DEC_10_2[1], "Decimal(10, 2)"), id="identical-decimals"),
        pytest.param(
            [DEC_10_2, DEC_12_4],
            ("number", NodeHints(logical_type="decimal", precision=12, scale=4), "Decimal(10, 2) | Decimal(12, 4)"),
            id="widened-decimals",
        ),
        pytest.param(
            [DEC_38_0, DEC_12_4],
            (*SPEC_FLOAT64, "Decimal(38, 0) | Decimal(12, 4)"),
            id="decimal-precision-overflow",
        ),
        pytest.param([LONG_SPEC, DEC_10_2], (*SPEC_FLOAT64, "int | Decimal(10, 2)"), id="integer-and-decimal"),
        pytest.param([TS_TZ_SPEC, TS_NAIVE_SPEC], (*SPEC_TS_UTC, "AwareDT | NaiveDT"), id="mixed-timestamps"),
        pytest.param([LONG_SPEC, BOOL_SPEC], (*SPEC_STRING, "int | bool"), id="incompatible-fallback"),
    ],
)
def test_unify_scalar_specs_pass(
    specs: list[tuple[str, NodeHints, str]],
    expected: tuple[str, NodeHints, str],
) -> None:
    """unify_scalar_specs unifies identical specs, decimals, numeric or timestamp groups, and fallbacks."""
    assert unify_scalar_specs(specs) == expected


@pytest.mark.parametrize(
    ("left", "right", "expected"),
    [
        pytest.param(STR_NODE, NULLABLE_STR_NODE, True, id="scalar-ignores-nullability"),
        pytest.param(STR_NODE, INT_NODE, False, id="scalar-different-type"),
        pytest.param(STR_NODE, DATE_STR_NODE, False, id="scalar-different-hints"),
        pytest.param(ARRAY_STR_NODE, TypedNode("array", items=STR_NODE), True, id="array-same-items"),
        pytest.param(ARRAY_STR_NODE, TypedNode("array", items=NULLABLE_STR_NODE), False, id="array-item-nullability"),
        pytest.param(ARRAY_STR_NODE, TypedNode("array", items=INT_NODE), False, id="array-item-type"),
        pytest.param(ARRAY_STR_NODE, TypedNode("array", items=None), False, id="array-missing-items"),
        pytest.param(ARRAY_STR_NODE, TypedNode("array", items=(STR_NODE,)), False, id="array-tuple-items"),
        pytest.param(
            MAP_STR_INT_NODE,
            TypedNode("map", key_type=STR_NODE, value_type=INT_NODE),
            True,
            id="map-same-types",
        ),
        pytest.param(
            MAP_STR_INT_NODE,
            TypedNode("map", key_type=None, value_type=INT_NODE),
            False,
            id="map-missing-key",
        ),
        pytest.param(
            MAP_STR_INT_NODE,
            TypedNode("map", key_type=STR_NODE, value_type=None),
            False,
            id="map-missing-value",
        ),
        pytest.param(
            MAP_STR_INT_NODE,
            TypedNode("map", key_type=INT_NODE, value_type=INT_NODE),
            False,
            id="map-different-key",
        ),
        pytest.param(
            MAP_STR_INT_NODE,
            TypedNode("map", key_type=STR_NODE, value_type=STR_NODE),
            False,
            id="map-different-value",
        ),
        pytest.param(
            MAP_STR_INT_NODE,
            TypedNode("map", key_type=STR_NODE, value_type=TypedNode("integer", nullable=True)),
            False,
            id="map-value-nullability",
        ),
        pytest.param(
            OBJ_X_REQ_NODE,
            TypedNode("object", properties=(Field("x", STR_NODE, required=True),)),
            True,
            id="object-same-properties",
        ),
        pytest.param(OBJ_X_REQ_NODE, TypedNode("object", properties=()), False, id="object-property-count"),
        pytest.param(
            OBJ_X_REQ_NODE,
            TypedNode("object", properties=(Field("y", STR_NODE, required=True),)),
            False,
            id="object-property-name",
        ),
        pytest.param(
            OBJ_X_REQ_NODE,
            TypedNode("object", properties=(Field("x", STR_NODE, required=False),)),
            False,
            id="object-property-required",
        ),
        pytest.param(
            OBJ_X_REQ_NODE,
            TypedNode("object", properties=(Field("x", INT_NODE, required=True),)),
            False,
            id="object-property-type",
        ),
    ],
)
def test_nodes_same_shape_pass(left: TypedNode, right: TypedNode, *, expected: bool) -> None:
    """nodes_same_shape compares types, hints, arrays, maps, and objects recursively."""
    assert nodes_same_shape(left, right) is expected
