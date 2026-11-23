"""Unit tests for strict JSON Schema combinator merging and node classification."""

from typing import Any, Final

import pytest

from cdm_data_loaders.converters.core.errors import ConversionError
from cdm_data_loaders.converters.core.extensions import DEFAULT_EXTENSIONS
from cdm_data_loaders.converters.sources.json_schema.strict_combinators import (
    classify_node_kind,
    classify_resolved_node_kind,
    extract_choice_branches_into_object,
    extract_object_parts_with_refs,
    merge_property_definitions,
    merge_ref_target_and_siblings,
)
from cdm_data_loaders.converters.sources.json_schema.strict_refs import StrictBuildContext

SINGLE_PROP_DEF: Final[dict[str, Any]] = {"type": "string"}
IDENTICAL_PROP_A: Final[dict[str, Any]] = {"type": "integer", "minimum": 1}
IDENTICAL_PROP_B: Final[dict[str, Any]] = {"minimum": 1, "type": "integer"}


def _ctx(defs: dict[str, Any] | None = None) -> StrictBuildContext:
    return StrictBuildContext(
        defs=defs or {},
        ref_counts={},
        max_recursion_depth=3,
        extension_registry=DEFAULT_EXTENSIONS,
        error_type=ConversionError,
        error_by_code={},
    )


@pytest.mark.parametrize(
    ("definitions", "expected", "expect_same_identity"),
    [
        pytest.param([SINGLE_PROP_DEF], SINGLE_PROP_DEF, True, id="single-definition"),
        pytest.param([IDENTICAL_PROP_A, IDENTICAL_PROP_B], IDENTICAL_PROP_A, True, id="identical-reordered-keys"),
        pytest.param(
            [{"type": "integer"}, {"type": "string", "description": "str branch"}],
            {
                "anyOf": [{"type": "integer"}, {"type": "string", "description": "str branch"}],
                "description": "str branch",
            },
            False,
            id="differing-definitions-wrapped-in-anyof",
        ),
    ],
)
def test_merge_property_definitions_variants(
    definitions: list[dict[str, Any]],
    expected: dict[str, Any],
    expect_same_identity: bool,
) -> None:
    """Verify merge_property_definitions deduplicates identical schemas and wraps differing ones in anyOf."""
    result = merge_property_definitions(definitions)

    if expect_same_identity:
        assert result is expected
    else:
        assert result == expected


def test_merge_ref_target_and_siblings_combines_fields() -> None:
    """Verify merge_ref_target_and_siblings strips $id/$ref and merges properties, required, and allOf."""
    target = {
        "$id": "urn:target",
        "type": "object",
        "properties": {"a": {"type": "integer"}, "b": {"type": "string"}},
        "required": ["a"],
        "allOf": [{"properties": {"c": {"type": "boolean"}}}],
    }
    node = {
        "$ref": "#/$defs/T",
        "properties": {"a": {"type": "number"}, "d": {"type": "string"}},
        "required": ["a", "d"],
        "allOf": [{"properties": {"e": {"type": "integer"}}}],
        "description": "merged",
    }

    merged = merge_ref_target_and_siblings(target, node)

    assert "$id" not in merged
    assert "$ref" not in merged
    assert merged["required"] == ["a", "d"]
    assert merged["description"] == "merged"
    expected_all_of_len = 2
    assert len(merged["allOf"]) == expected_all_of_len
    assert merged["properties"]["a"] == {"anyOf": [{"type": "integer"}, {"type": "number"}]}
    assert merged["properties"]["d"] == {"type": "string"}


@pytest.mark.parametrize(
    ("schema", "expected_kind"),
    [
        pytest.param({"anyOf": [{"type": "string"}]}, "union", id="anyof-union"),
        pytest.param({"allOf": [{"properties": {"x": {"type": "int"}}}]}, "object", id="allof-object"),
        pytest.param({"type": ["integer", "number"]}, "scalar", id="multi-scalar-type"),
        pytest.param(
            {"type": "object", "additionalProperties": {"type": "string"}},
            "map",
            id="additional-properties-map",
        ),
        pytest.param(
            {"type": "object", "properties": {"a": {"type": "string"}}, "additionalProperties": {"type": "string"}},
            "object",
            id="properties-and-additional-properties-object",
        ),
        pytest.param({"prefixItems": [{"type": "string"}]}, "array", id="prefix-items-array"),
    ],
)
def test_classify_resolved_node_kind_valid(schema: dict[str, Any], expected_kind: str) -> None:
    """Verify classify_resolved_node_kind classifies union, object, scalar, map, and array schemas."""
    assert classify_resolved_node_kind(schema, error_type=ConversionError, error_by_code={}) == expected_kind


def test_classify_resolved_node_kind_mixed_object_and_scalar_raises() -> None:
    """Verify classify_resolved_node_kind rejects mixed object and scalar type arrays."""
    with pytest.raises(ConversionError):
        classify_resolved_node_kind({"type": ["object", "string"]}, error_type=ConversionError, error_by_code={})


def test_classify_node_kind_ref_cycle_raises() -> None:
    """Verify classify_node_kind raises ConversionError on cyclic $ref definitions."""
    defs = {"A": {"$ref": "#/$defs/B"}, "B": {"$ref": "#/$defs/A"}}

    with pytest.raises(ConversionError):
        classify_node_kind(
            {"$ref": "#/$defs/A"},
            defs,
            extension_registry=DEFAULT_EXTENSIONS,
            error_type=ConversionError,
            error_by_code={},
        )


def test_extract_object_parts_with_refs_merges_ref_and_conditionals() -> None:
    """Verify extract_object_parts_with_refs merges $ref targets, dependentSchemas, and then/else branches."""
    defs = {
        "Base": {
            "type": "object",
            "required": ["id"],
            "properties": {"id": {"type": "string"}},
        }
    }

    props, req, scalars = extract_object_parts_with_refs(
        {
            "$ref": "#/$defs/Base",
            "required": ["extra"],
            "properties": {"extra": {"type": "integer"}},
            "dependentSchemas": {"extra": {"properties": {"dep": {"type": "boolean"}}}},
            "then": {"properties": {"then_prop": {"type": "string"}}},
            "else": {"properties": {"else_prop": {"type": "string"}}},
        },
        _ctx(defs),
    )

    assert req == {"id", "extra"}
    assert scalars == []
    assert set(props) == {"id", "extra", "dep", "then_prop", "else_prop"}


def test_extract_object_parts_with_refs_non_object_allof_raises() -> None:
    """Verify extract_object_parts_with_refs rejects non-object allOf branches."""
    with pytest.raises(ConversionError):
        extract_object_parts_with_refs({"allOf": [{"type": "array", "items": {"type": "string"}}]}, _ctx())


@pytest.mark.parametrize(
    ("branches", "keyword"),
    [
        pytest.param([{"type": "null"}], "anyOf", id="null-only-anyof"),
        pytest.param([{"type": "array", "items": {"type": "string"}}], "oneOf", id="array-branch-oneof"),
    ],
)
def test_extract_choice_branches_into_object_invalid_raises(
    branches: list[dict[str, Any]],
    keyword: str,
) -> None:
    """Verify extract_choice_branches_into_object rejects null-only and array choice branches."""
    raw_props: dict[str, Any] = {}
    scalar_list: list[dict[str, Any]] = []

    with pytest.raises(ConversionError):
        extract_choice_branches_into_object(branches, keyword, _ctx(), raw_props, scalar_list)
