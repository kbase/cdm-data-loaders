"""Canonical JSON Schema keyword classifications and subschema traversal helpers."""

from collections.abc import Iterator, Mapping
from typing import Any, Final

JSONSCHEMA_PRIMITIVE_TYPES: Final[frozenset[str]] = frozenset(
    {"object", "array", "string", "integer", "number", "boolean", "null"}
)

DECLARATION_ONLY_KEYWORDS: Final[frozenset[str]] = frozenset({"$defs", "definitions"})

REF_TARGET_IDENTITY_KEYWORDS: Final[frozenset[str]] = frozenset(
    {"$anchor", "$dynamicAnchor", "$id", "$recursiveAnchor", "$schema", "$vocabulary", "id"}
)

REFERENCE_KEYWORDS: Final[frozenset[str]] = frozenset({"$ref", "$dynamicRef", "$recursiveRef"})

REF_AND_IDENTITY_KEYWORDS: Final[frozenset[str]] = (
    DECLARATION_ONLY_KEYWORDS | REF_TARGET_IDENTITY_KEYWORDS | REFERENCE_KEYWORDS
)

ANNOTATION_ONLY_KEYWORDS: Final[frozenset[str]] = frozenset(
    {"$comment", "title", "description", "default", "examples", "readOnly", "writeOnly", "deprecated"}
)

ANNOTATIONS: Final[frozenset[str]] = REF_TARGET_IDENTITY_KEYWORDS | ANNOTATION_ONLY_KEYWORDS

DICT_OF_SCHEMAS_KEYWORDS: Final[frozenset[str]] = frozenset({"properties", "patternProperties", "dependentSchemas"})

LIST_OF_SCHEMAS_KEYWORDS: Final[frozenset[str]] = frozenset({"anyOf", "oneOf", "prefixItems"})

SINGLE_SCHEMA_KEYWORDS: Final[frozenset[str]] = frozenset(
    {
        "additionalProperties",
        "additionalItems",
        "contains",
        "propertyNames",
        "contentSchema",
        "not",
        "if",
        "then",
        "else",
        "unevaluatedItems",
        "unevaluatedProperties",
    }
)

SCHEMA_KEYS: Final[frozenset[str]] = SINGLE_SCHEMA_KEYWORDS - {"additionalProperties"}

SCHEMA_MAPS: Final[frozenset[str]] = DECLARATION_ONLY_KEYWORDS | {"dependentSchemas"}

STRUCTURAL_OR_COMPOSITIONAL_KEYWORDS: Final[frozenset[str]] = (
    DICT_OF_SCHEMAS_KEYWORDS
    | LIST_OF_SCHEMAS_KEYWORDS
    | SINGLE_SCHEMA_KEYWORDS
    | {"allOf", "dependencies", "dependentRequired", "items", "required", "type"}
)

STRUCTURE: Final[frozenset[str]] = (
    (STRUCTURAL_OR_COMPOSITIONAL_KEYWORDS - {"dependentRequired"}) | DECLARATION_ONLY_KEYWORDS | {"$ref"}
)


def strip_ref_target_identity(target: Mapping[str, Any]) -> dict[str, Any]:
    """Return a shallow copy of ``target`` without resource identity or dialect keywords.

    :param target: Resolved ``$ref`` target schema mapping.
    :type target: Mapping[str, Any]
    :return: Dictionary with ``REF_TARGET_IDENTITY_KEYWORDS`` omitted.
    :rtype: dict[str, Any]
    """
    return {k: v for k, v in target.items() if k not in REF_TARGET_IDENTITY_KEYWORDS}


def iter_child_subschemas(node: Mapping[str, Any]) -> Iterator[object]:
    """Yield direct schema-valued children of ``node``, skipping literal payloads and ``$defs``.

    Skips ``$defs``, ``definitions``, literal values (``const``, ``enum``, ``default``,
    ``examples``, ``dependentRequired``, string-array ``dependencies``), and ``x-*`` extensions.

    :param node: JSON Schema mapping node to inspect.
    :type node: Mapping[str, Any]
    :return: Iterator yielding child schema objects or booleans.
    :rtype: Iterator[object]
    """
    for key, value in node.items():
        if key in DECLARATION_ONLY_KEYWORDS:
            continue
        if key in DICT_OF_SCHEMAS_KEYWORDS and isinstance(value, Mapping):
            yield from value.values()
        elif (key in LIST_OF_SCHEMAS_KEYWORDS or key == "allOf") and isinstance(value, list):
            yield from value
        elif key == "items":
            if isinstance(value, list):
                yield from value
            elif isinstance(value, (Mapping, bool)):
                yield value
        elif key == "dependencies" and isinstance(value, Mapping):
            for dep_val in value.values():
                if isinstance(dep_val, (Mapping, bool)):
                    yield dep_val
        elif key in SINGLE_SCHEMA_KEYWORDS and isinstance(value, (Mapping, bool)):
            yield value
