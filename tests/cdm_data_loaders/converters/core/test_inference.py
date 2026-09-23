"""Tests for target-independent type inference."""

from decimal import Decimal
from typing import Any

import pytest

from cdm_data_loaders.converters.core.inference import (
    IMPLICIT_ARRAY_KEYWORDS,
    IMPLICIT_NUMBER_KEYWORDS,
    IMPLICIT_OBJECT_KEYWORDS,
    IMPLICIT_STRING_KEYWORDS,
    decimal_places,
    infer_implicit_type,
    json_type_from_enum,
)


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
