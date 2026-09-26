"""Target-independent JSON Schema type inference and decimal scale helpers."""

from decimal import Decimal
from typing import Any, Final, Literal

IMPLICIT_OBJECT_KEYWORDS: Final[frozenset[str]] = frozenset(
    {
        "properties",
        "patternProperties",
        "additionalProperties",
        "unevaluatedProperties",
        "required",
        "propertyNames",
        "minProperties",
        "maxProperties",
        "dependentSchemas",
        "dependentRequired",
        "dependencies",
    }
)
IMPLICIT_ARRAY_KEYWORDS: Final[frozenset[str]] = frozenset(
    {
        "items",
        "prefixItems",
        "additionalItems",
        "unevaluatedItems",
        "contains",
        "minItems",
        "maxItems",
        "uniqueItems",
        "minContains",
        "maxContains",
    }
)
IMPLICIT_STRING_KEYWORDS: Final[frozenset[str]] = frozenset(
    {
        "pattern",
        "minLength",
        "maxLength",
        "format",
        "contentEncoding",
        "contentMediaType",
        "contentSchema",
    }
)
IMPLICIT_NUMBER_KEYWORDS: Final[frozenset[str]] = frozenset(
    {"minimum", "maximum", "exclusiveMinimum", "exclusiveMaximum", "multipleOf"}
)


def infer_implicit_type(schema: dict[str, Any]) -> str | None:
    """Approximate a type-less schema from its type-specific keywords.

    Precedence is object, array, string, number. These keywords constrain
    values of their own type but do not exclude other JSON types.
    """
    if schema.keys() & IMPLICIT_OBJECT_KEYWORDS:
        return "object"
    if schema.keys() & IMPLICIT_ARRAY_KEYWORDS:
        return "array"
    if schema.keys() & IMPLICIT_STRING_KEYWORDS:
        return "string"
    if schema.keys() & IMPLICIT_NUMBER_KEYWORDS:
        return "number"
    return None


def json_type_from_enum(values: list[Any]) -> Literal["boolean", "integer", "number", "string"]:
    """Infer a scalar JSON type, using string for empty or heterogeneous enums."""
    if not values:
        return "string"
    if all(isinstance(value, bool) for value in values):
        return "boolean"
    if all(isinstance(value, int) and not isinstance(value, bool) for value in values):
        return "integer"
    if all(isinstance(value, (int, float)) and not isinstance(value, bool) for value in values):
        return "number"
    return "string"


def decimal_places(value: float | Decimal) -> int:
    """Count fractional digits in the decimal representation; non-finite values return zero."""
    exponent = Decimal(str(value)).as_tuple().exponent
    if isinstance(exponent, int):
        return max(-exponent, 0)
    return 0
