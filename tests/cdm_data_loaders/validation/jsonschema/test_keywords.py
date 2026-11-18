"""Unit tests for ``cdm_data_loaders.validation.jsonschema.keywords``."""

from typing import Any

import pytest

from cdm_data_loaders.validation.jsonschema.keywords import (
    ANNOTATION_ONLY_KEYWORDS,
    ANNOTATIONS,
    DECLARATION_ONLY_KEYWORDS,
    JSONSCHEMA_PRIMITIVE_TYPES,
    REF_AND_IDENTITY_KEYWORDS,
    REF_TARGET_IDENTITY_KEYWORDS,
    REFERENCE_KEYWORDS,
    SCHEMA_KEYS,
    SCHEMA_MAPS,
    SINGLE_SCHEMA_KEYWORDS,
    STRUCTURAL_OR_COMPOSITIONAL_KEYWORDS,
    STRUCTURE,
    iter_child_subschemas,
    strip_ref_target_identity,
)


def test_keyword_sets_invariants() -> None:
    """Verify canonical JSON Schema keyword sets compose without unintended overlap."""
    assert len(JSONSCHEMA_PRIMITIVE_TYPES) == len({"object", "array", "string", "integer", "number", "boolean", "null"})
    assert ANNOTATIONS == REF_TARGET_IDENTITY_KEYWORDS | ANNOTATION_ONLY_KEYWORDS
    assert REF_AND_IDENTITY_KEYWORDS == DECLARATION_ONLY_KEYWORDS | REF_TARGET_IDENTITY_KEYWORDS | REFERENCE_KEYWORDS
    assert SINGLE_SCHEMA_KEYWORDS - {"additionalProperties"} == SCHEMA_KEYS
    assert DECLARATION_ONLY_KEYWORDS | {"dependentSchemas"} == SCHEMA_MAPS
    assert "dependentRequired" in STRUCTURAL_OR_COMPOSITIONAL_KEYWORDS
    assert "dependentRequired" not in STRUCTURE
    assert "$ref" in STRUCTURE


def test_strip_ref_target_identity_removes_all_identity_keywords() -> None:
    """Strip every resource identity and dialect keyword while keeping structural and annotation keys."""
    raw: dict[str, Any] = {
        "$id": "https://example.org/x.json",
        "id": "legacy-id",
        "$anchor": "x",
        "$dynamicAnchor": "dx",
        "$recursiveAnchor": True,
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$vocabulary": {},
        "type": "string",
        "description": "Kept description.",
    }
    assert strip_ref_target_identity(raw) == {
        "type": "string",
        "description": "Kept description.",
    }


@pytest.mark.parametrize(
    ("node", "expected"),
    [
        pytest.param(
            {
                "$defs": {"IgnoredDef": {"type": "string", "format": "date"}},
                "definitions": {"IgnoredLegacyDef": {"type": "integer"}},
                "default": {"format": "date", "$ref": "#/$defs/Ignored"},
                "examples": [{"contentEncoding": "base64"}],
                "const": {"format": "date"},
                "enum": [{"format": "date"}],
                "dependentRequired": {"a": ["b"]},
                "x-custom": {"format": "date"},
                "properties": {"p1": {"type": "string"}},
                "patternProperties": {"^s_": {"type": "integer"}},
                "dependentSchemas": {"p1": {"type": "object"}},
                "allOf": [{"minimum": 0}],
                "anyOf": [{"type": "null"}],
                "oneOf": [{"type": "boolean"}],
                "prefixItems": [{"type": "number"}],
                "items": {"type": "string"},
                "dependencies": {
                    "schema_dep": {"required": ["p1"]},
                    "bool_dep": True,
                    "list_dep": ["p1"],
                },
                "additionalProperties": False,
                "not": {"type": "null"},
            },
            [
                {"type": "string"},
                {"type": "integer"},
                {"type": "object"},
                {"minimum": 0},
                {"type": "null"},
                {"type": "boolean"},
                {"type": "number"},
                {"type": "string"},
                {"required": ["p1"]},
                True,
                False,
                {"type": "null"},
            ],
            id="schema-bearing-and-ignored-keywords",
        ),
        pytest.param(
            {"items": [{"type": "string"}, False], "items_non_schema": 123},
            [{"type": "string"}, False],
            id="draft07-tuple-items",
        ),
        pytest.param(
            {"items": "invalid-non-schema"},
            [],
            id="non-schema-items-ignored",
        ),
    ],
)
def test_iter_child_subschemas_pass_yields_schema_children_and_skips_literals(
    node: dict[str, Any],
    expected: list[dict[str, Any] | bool],
) -> None:
    """Yield subschemas across schema-bearing keywords and skip literal, declaration, and extension payloads."""
    assert list(iter_child_subschemas(node)) == expected
