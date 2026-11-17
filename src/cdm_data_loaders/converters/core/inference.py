"""Target-independent JSON Schema type inference, scalar unification, and decimal scale helpers."""

from collections.abc import Sequence
from decimal import Decimal
from typing import Any, Final, Literal

from cdm_data_loaders.converters.core.constants import DECIMAL128_MAX_PRECISION
from cdm_data_loaders.converters.core.ir import (
    SPEC_FLOAT64,
    SPEC_STRING,
    SPEC_TS_UTC,
    NodeHints,
    NodeType,
    TypedNode,
)

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

    :param schema: JSON Schema fragment dictionary.
    :type schema: dict[str, Any]
    :return: Inferred JSON Schema type name or ``None``.
    :rtype: str | None
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
    """Infer a scalar JSON type, using string for empty or heterogeneous enums.

    :param values: Enum member values.
    :type values: list[Any]
    :return: Inferred JSON scalar type name.
    :rtype: Literal["boolean", "integer", "number", "string"]
    """
    if not values:
        return "string"
    if all(isinstance(value, bool) for value in values):
        return "boolean"
    if all(isinstance(value, int) and not isinstance(value, bool) for value in values):
        return "integer"
    if all(isinstance(value, (int, float)) and not isinstance(value, bool) for value in values):
        return "number"
    return "string"


def is_unconstrained_node(node: TypedNode) -> bool:
    """Identify false or empty JSON schemas that carry no type information.

    :param node: the schema node to inspect
    :type node: TypedNode
    :return: True for a ``never`` node or an unknown node with no keywords, metadata or combiners
    :rtype: bool
    """
    if node.type == "never":
        return True
    return (
        node.type == "unknown"
        and node.declared_type is None
        and node.inferred_type is None
        and not node.source_keywords
        and not node.constraints
        and not node.annotations
        and not node.extensions
        and node.one_of is None
        and node.any_of is None
        and not node.schema_keywords
        and not node.schema_maps
    )


def decimal_places(value: float | Decimal) -> int:
    """Count fractional digits in the decimal representation; non-finite values return zero.

    :param value: Numeric ``multipleOf`` factor.
    :type value: float | Decimal
    :return: Count of fractional decimal digits.
    :rtype: int
    """
    exponent = Decimal(str(value)).as_tuple().exponent
    if isinstance(exponent, int):
        return max(-exponent, 0)
    return 0


def decimal_pattern(precision: int, scale: int) -> str:
    """Build the decimal string pattern from integer precision and scale.

    :param precision: Total decimal digits.
    :type precision: int
    :param scale: Fractional decimal digits.
    :type scale: int
    :return: Regular expression pattern matching decimal strings within ``(precision, scale)``.
    :rtype: str
    """
    digits_before = precision - scale
    if scale > 0:
        if digits_before == 0:
            return rf"^-?0(\.\d{{1,{scale}}})?$"
        return rf"^-?\d{{1,{digits_before}}}(\.\d{{1,{scale}}})?$"
    return rf"^-?\d{{1,{digits_before}}}$"


def unify_scalar_specs(
    resolved: Sequence[tuple[NodeType, NodeHints, str]],
) -> tuple[NodeType, NodeHints, str]:
    """Unify multiple scalar ``(NodeType, NodeHints, type_label)`` tuples under ``R-UNION-SCALAR``.

    :param resolved: Sequence of resolved scalar branch ``(NodeType, NodeHints, label)`` tuples.
    :type resolved: Sequence[tuple[NodeType, NodeHints, str]]
    :return: Unified ``(NodeType, NodeHints, joined_label)`` tuple.
    :rtype: tuple[NodeType, NodeHints, str]
    """
    unique_labels = list(dict.fromkeys(label for _, _, label in resolved))
    joined_label = " | ".join(unique_labels)

    unique_specs = list(dict.fromkeys((nt, h) for nt, h, _ in resolved))
    if len(unique_specs) == 1:
        return unique_specs[0][0], unique_specs[0][1], joined_label

    if all(h.logical_type == "decimal" for _, h in unique_specs):
        new_scale = max(h.scale or 0 for _, h in unique_specs)
        max_int_digits = max((h.precision or 0) - (h.scale or 0) for _, h in unique_specs)
        new_precision = max_int_digits + new_scale
        if new_precision <= DECIMAL128_MAX_PRECISION:
            return (
                "number",
                NodeHints(logical_type="decimal", precision=new_precision, scale=new_scale),
                joined_label,
            )
        return *SPEC_FLOAT64, joined_label

    if all(h.logical_type in {"long", "double", "decimal"} for _, h in unique_specs):
        return *SPEC_FLOAT64, joined_label

    if all(h.logical_type in {"timestamp-with-tz", "timestamp-without-tz"} for _, h in unique_specs):
        return *SPEC_TS_UTC, joined_label

    return *SPEC_STRING, joined_label


def nodes_same_shape(a: TypedNode, b: TypedNode) -> bool:
    """Return ``True`` when two IR nodes emit identical structural and scalar shapes.

    :param a: First IR node.
    :type a: TypedNode
    :param b: Second IR node.
    :type b: TypedNode
    :return: ``True`` if both nodes have identical recursive shape and hints.
    :rtype: bool
    """
    if a.type != b.type or a.hints != b.hints:
        return False
    if a.type == "array":
        return (
            a.items is not None
            and b.items is not None
            and not isinstance(a.items, tuple)
            and not isinstance(b.items, tuple)
            and a.items.nullable == b.items.nullable
            and nodes_same_shape(a.items, b.items)
        )
    if a.type == "map":
        return (
            a.key_type is not None
            and b.key_type is not None
            and a.value_type is not None
            and b.value_type is not None
            and a.value_type.nullable == b.value_type.nullable
            and nodes_same_shape(a.key_type, b.key_type)
            and nodes_same_shape(a.value_type, b.value_type)
        )
    if a.type == "object":
        return len(a.properties) == len(b.properties) and all(
            fa.name == fb.name and fa.required == fb.required and nodes_same_shape(fa.node, fb.node)
            for fa, fb in zip(a.properties, b.properties, strict=True)
        )
    return True
