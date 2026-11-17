"""JSON Schema ``$ref`` and ``allOf`` dereferencing utilities."""

import logging
from collections.abc import Mapping
from dataclasses import dataclass
from dataclasses import field as dataclass_field
from typing import Any, Final, Literal, cast

from referencing import Registry, Resource
from referencing._core import Resolver
from referencing.exceptions import CannotDetermineSpecification, Unresolvable
from referencing.jsonschema import DRAFT201909, DRAFT202012, specification_with

logger: logging.Logger = logging.getLogger(__name__)

RefSiblingMode = Literal["auto", "always", "never"]

REF_SIBLING_AWARE_SPECS: Final[tuple[Any, ...]] = (DRAFT202012, DRAFT201909)
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
DECLARATION_ONLY_KEYWORDS: Final[frozenset[str]] = frozenset({"$defs", "definitions"})
REF_TARGET_IDENTITY_KEYWORDS: Final[frozenset[str]] = frozenset(
    {"$anchor", "$dynamicAnchor", "$id", "$schema", "$vocabulary", "id"}
)


class DereferencingError(ValueError):
    """Raised when a JSON Schema document cannot be dereferenced."""


@dataclass
class DereferenceContext:
    """Mutable state for a single ``dereference_schema()`` call."""

    resolver: Resolver
    spec: Any
    max_ref_depth: int
    ref_sibling_mode: RefSiblingMode
    active_refs: set[str] = dataclass_field(default_factory=set)


def dereference_schema(
    schema: dict[str, Any],
    additional_resources: Mapping[str, dict[str, Any]] | None = None,
    *,
    max_ref_depth: int = 50,
    ref_sibling_mode: RefSiblingMode = "auto",
) -> dict[str, Any]:
    """Resolve every ``$ref`` and merge ``allOf`` branches in ``schema``.

    :param schema: JSON Schema document to dereference
    :type schema: dict[str, Any]
    :param additional_resources: mapping of URI to schema dict for external ``$ref`` targets
    :type additional_resources: Mapping[str, dict[str, Any]] | None
    :param max_ref_depth: maximum depth of a ``$ref`` chain before raising ``DereferencingError``
    :type max_ref_depth: int
    :param ref_sibling_mode: sibling keyword handling alongside ``$ref`` (``'auto'``, ``'always'``, or ``'never'``)
    :type ref_sibling_mode: RefSiblingMode
    :return: ``$ref``-free and ``allOf``-free JSON Schema document
    :rtype: dict[str, Any]
    :raises DereferencingError: if a ``$ref`` is unresolvable, circular, or exceeds ``max_ref_depth``,
        or if a dialect cannot be determined for the root schema or an external resource
    """
    additional_resources = additional_resources or {}

    dialect_id = schema.get("$schema")
    spec = specification_with(dialect_id, default=DRAFT202012) if dialect_id else DRAFT202012

    try:
        root_resource = Resource.from_contents(schema, default_specification=spec)
    except CannotDetermineSpecification as exc:
        err_msg = f"Could not determine a JSON Schema dialect for the root schema: {exc}"
        raise DereferencingError(err_msg) from exc

    try:
        resources = [
            (uri, Resource.from_contents(doc, default_specification=spec)) for uri, doc in additional_resources.items()
        ]
    except CannotDetermineSpecification as exc:
        err_msg = f"Could not determine a JSON Schema dialect for an external resource in `additional_resources`: {exc}"
        raise DereferencingError(err_msg) from exc

    registry: Registry = Registry().with_resources(resources)
    resolver: Resolver[Any] = registry.resolver_with_root(root_resource)

    ctx = DereferenceContext(
        resolver=resolver,
        spec=spec,
        max_ref_depth=max_ref_depth,
        ref_sibling_mode=ref_sibling_mode,
    )
    return cast("dict[str, Any]", _dereference(schema, ctx))


def _should_apply_ref_siblings(ctx: DereferenceContext, *, has_siblings: bool) -> bool:
    """Decide whether keywords declared alongside a ``$ref`` apply under ``ctx``.

    :param ctx: active dereference context
    :type ctx: DereferenceContext
    :param has_siblings: whether sibling keywords exist alongside ``$ref``
    :type has_siblings: bool
    :return: ``True`` if sibling keywords merge into the resolved schema
    :rtype: bool
    """
    if not has_siblings or ctx.ref_sibling_mode == "never":
        return False
    if ctx.ref_sibling_mode == "always":
        return True
    return ctx.spec in REF_SIBLING_AWARE_SPECS


def _resolve_ref_and_all_of(schema: dict[str, Any], ctx: DereferenceContext) -> dict[str, Any]:
    """Resolve ``allOf`` and ``$ref`` references at a single schema level.

    :param schema: schema fragment to resolve at this level
    :type schema: dict[str, Any]
    :param ctx: active dereference context
    :type ctx: DereferenceContext
    :return: schema with ``$ref`` and ``allOf`` resolved at this level
    :rtype: dict[str, Any]
    :raises DereferencingError: if a ``$ref`` is unresolvable, circular, boolean, or exceeds ``ctx.max_ref_depth``
    """
    if "$ref" in schema:
        ref = schema["$ref"]

        if len(ctx.active_refs) >= ctx.max_ref_depth:
            msg = f"$ref chain exceeded max depth ({ctx.max_ref_depth}) near {ref!r}."
            raise DereferencingError(msg)

        if ref in ctx.active_refs:
            err_msg = (
                f"Circular $ref detected at {ref!r}. PySpark schemas must be finite; JSON Schemas with "
                "self-referential structures (e.g. recursive tree/linked-list) cannot be dereferenced."
            )
            raise DereferencingError(err_msg)

        try:
            resolved = ctx.resolver.lookup(ref)
        except Unresolvable as exc:
            err_msg = f"Unable to resolve $ref {ref!r}: {exc}"
            raise DereferencingError(err_msg) from exc

        if isinstance(resolved.contents, bool):
            err_msg = (
                f"$ref {ref!r} resolves to a boolean schema ({resolved.contents!r}). Boolean schemas "
                "reached via $ref are not supported by the dereferencer; inline the boolean "
                "schema at the reference site instead."
            )
            raise DereferencingError(err_msg)

        sibling_keys = {k: v for k, v in schema.items() if k != "$ref"}
        apply_siblings = _should_apply_ref_siblings(ctx, has_siblings=bool(sibling_keys))

        merged = {k: v for k, v in resolved.contents.items() if k not in REF_TARGET_IDENTITY_KEYWORDS}
        if apply_siblings:
            merged.update(sibling_keys)
        elif sibling_keys:
            logger.warning(
                "Ignoring keyword(s) %s declared alongside $ref %r, per JSON Schema draft semantics "
                "(siblings of $ref are not applied in this draft). Pass ref_sibling_mode='always' to "
                "override.",
                sorted(sibling_keys),
                ref,
            )

        previous_resolver = ctx.resolver
        ctx.resolver = resolved.resolver
        ctx.active_refs.add(ref)
        try:
            return _resolve_ref_and_all_of(merged, ctx)
        finally:
            ctx.active_refs.discard(ref)
            ctx.resolver = previous_resolver

    if "allOf" in schema:
        return _merge_all_of(schema, ctx)

    return schema


def _merge_all_of(schema: dict[str, Any], ctx: DereferenceContext) -> dict[str, Any]:
    """Merge an ``allOf`` keyword's branches into a single flattened schema.

    :param schema: schema fragment containing ``allOf``
    :type schema: dict[str, Any]
    :param ctx: active dereference context
    :type ctx: DereferenceContext
    :return: schema with ``allOf`` merged
    :rtype: dict[str, Any]
    """
    merged: dict[str, Any] = {}
    merged_properties: dict[str, Any] = {}
    merged_required: list[str] = []
    merged_pattern_properties: dict[str, Any] = {}

    for sub_schema in schema["allOf"]:
        resolved_sub = _resolve_ref_and_all_of(sub_schema, ctx)
        merged_properties.update(resolved_sub.get("properties", {}))
        merged_required.extend(resolved_sub.get("required", []))
        merged_pattern_properties.update(resolved_sub.get("patternProperties", {}))
        merged.update(resolved_sub)

    sibling_schema = {k: v for k, v in schema.items() if k != "allOf"}
    merged_properties.update(sibling_schema.get("properties", {}))
    merged_required.extend(sibling_schema.get("required", []))
    merged_pattern_properties.update(sibling_schema.get("patternProperties", {}))
    merged.update(sibling_schema)

    if merged_properties:
        merged["properties"] = merged_properties
    if merged_required:
        merged["required"] = list(dict.fromkeys(merged_required))
    if merged_pattern_properties:
        merged["patternProperties"] = merged_pattern_properties

    return merged


def _dereference(schema: object, ctx: DereferenceContext) -> object:
    """Resolve ``$ref`` and ``allOf`` across every nested level of ``schema``.

    :param schema: schema fragment or boolean schema to dereference
    :type schema: object
    :param ctx: active dereference context
    :type ctx: DereferenceContext
    :return: dereferenced schema fragment
    :rtype: object
    :raises DereferencingError: if any nested ``$ref`` is unresolvable, circular, or exceeds ``ctx.max_ref_depth``
    """
    if isinstance(schema, bool) or not isinstance(schema, dict):
        return schema

    resolved = _resolve_ref_and_all_of(schema, ctx)

    result: dict[str, Any] = {}
    for key, value in resolved.items():
        if key in DECLARATION_ONLY_KEYWORDS:
            continue
        if key in DICT_OF_SCHEMAS_KEYWORDS and isinstance(value, dict):
            result[key] = {k: _dereference(v, ctx) for k, v in value.items()}
        elif (key in LIST_OF_SCHEMAS_KEYWORDS or key == "items") and isinstance(value, list):
            result[key] = [_dereference(v, ctx) for v in value]
        elif key == "dependencies" and isinstance(value, dict):
            result[key] = {k: (_dereference(v, ctx) if isinstance(v, (dict, bool)) else v) for k, v in value.items()}
        elif key in SINGLE_SCHEMA_KEYWORDS or key == "items":
            result[key] = _dereference(value, ctx) if isinstance(value, (dict, bool)) else value
        else:
            result[key] = value
    return result
