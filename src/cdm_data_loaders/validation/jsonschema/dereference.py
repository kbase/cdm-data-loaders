"""JSON Schema ``$ref`` and ``allOf`` dereferencing utilities."""

import logging
from collections.abc import Mapping
from dataclasses import dataclass
from dataclasses import field as dataclass_field
from typing import Any, Final, Literal, cast

from referencing import Registry, Resource
from referencing._core import Resolved, Resolver
from referencing.exceptions import CannotDetermineSpecification, Unresolvable
from referencing.jsonschema import DRAFT201909, DRAFT202012, specification_with

from cdm_data_loaders.utils.recursion import validate_positive_int_param
from cdm_data_loaders.validation.jsonschema.keywords import (
    DECLARATION_ONLY_KEYWORDS,
    DICT_OF_SCHEMAS_KEYWORDS,
    LIST_OF_SCHEMAS_KEYWORDS,
    SINGLE_SCHEMA_KEYWORDS,
    strip_ref_target_identity,
)

logger: logging.Logger = logging.getLogger(__name__)

RefSiblingMode = Literal["auto", "always", "never"]

REF_SIBLING_AWARE_SPECS: Final[tuple[Any, ...]] = (DRAFT202012, DRAFT201909)


class DereferencingError(ValueError):
    """Raised when a JSON Schema document cannot be dereferenced."""


@dataclass
class DereferenceContext:
    """Mutable state for a single ``dereference_schema()`` call."""

    resolver: Resolver[Any]
    spec: Any
    max_ref_depth: int
    ref_sibling_mode: RefSiblingMode
    active_refs: set[str] = dataclass_field(default_factory=set)
    active_target_ids: set[int] = dataclass_field(default_factory=set)


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
    :raises TypeError: if ``max_ref_depth`` is a boolean or not an integer
    :raises ValueError: if ``max_ref_depth`` is less than ``1``
    :raises DereferencingError: if a ``$ref`` is unresolvable, circular, or exceeds ``max_ref_depth``,
        or if a dialect cannot be determined for the root schema or an external resource
    """
    validate_positive_int_param(max_ref_depth, "max_ref_depth")
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

    registry: Registry[Any] = Registry().with_resources(resources)
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


def _merge_schema_pair(base: Mapping[str, Any], overlay: Mapping[str, Any]) -> dict[str, Any]:
    """Deep-merge ``overlay`` onto ``base`` while preserving nested schema maps and ``required`` sets.

    :param base: Base schema dictionary.
    :type base: Mapping[str, Any]
    :param overlay: Overlay schema dictionary taking scalar precedence over ``base``.
    :type overlay: Mapping[str, Any]
    :return: Merged schema dictionary.
    :rtype: dict[str, Any]
    """
    merged: dict[str, Any] = dict(base)
    for key, value in overlay.items():
        if key in DICT_OF_SCHEMAS_KEYWORDS and isinstance(merged.get(key), Mapping) and isinstance(value, Mapping):
            sub_map: dict[str, Any] = dict(merged[key])
            for sub_key, sub_val in value.items():
                existing = sub_map.get(sub_key)
                if isinstance(existing, Mapping) and isinstance(sub_val, Mapping):
                    sub_map[sub_key] = _merge_schema_pair(existing, sub_val)
                else:
                    sub_map[sub_key] = sub_val
            merged[key] = sub_map
        elif key == "required" and isinstance(value, list):
            prior_req = merged["required"] if isinstance(merged.get("required"), list) else []
            merged["required"] = list(dict.fromkeys([*prior_req, *value]))
        elif key == "allOf" and isinstance(merged.get("allOf"), list) and isinstance(value, list):
            merged["allOf"] = [*merged["allOf"], *value]
        else:
            merged[key] = value
    return merged


def _lookup_ref_target(ref: object, ctx: DereferenceContext) -> tuple[str, Resolved[Any], int]:
    """Validate and look up ``ref`` against ``ctx.resolver`` while checking cycle and depth limits.

    :param ref: Raw ``$ref`` keyword value.
    :type ref: object
    :param ctx: Active dereference context.
    :type ctx: DereferenceContext
    :return: Tuple of ``(ref_str, resolved_lookup, target_object_id)``.
    :rtype: tuple[str, Resolved[Any], int]
    :raises DereferencingError: If ``ref`` is non-string, circular, too deep, unresolvable, or non-object.
    """
    if not isinstance(ref, str):
        err_msg = f"Invalid non-string $ref {ref!r}."
        raise DereferencingError(err_msg)

    if ref in ctx.active_refs:
        err_msg = (
            f"Circular $ref detected at {ref!r}. JSON Schemas with self-referential "
            "structures (e.g. recursive tree/linked-list) cannot be dereferenced."
        )
        raise DereferencingError(err_msg)

    if len(ctx.active_refs) >= ctx.max_ref_depth:
        msg = f"$ref chain exceeded max depth ({ctx.max_ref_depth}) near {ref!r}."
        raise DereferencingError(msg)

    try:
        resolved = ctx.resolver.lookup(ref)
    except Unresolvable as exc:
        err_msg = f"Unable to resolve $ref {ref!r}: {exc}"
        raise DereferencingError(err_msg) from exc
    except (AttributeError, TypeError) as exc:
        err_msg = f"$ref {ref!r} resolves to a non-object schema: {exc}"
        raise DereferencingError(err_msg) from exc

    if isinstance(resolved.contents, bool):
        err_msg = (
            f"$ref {ref!r} resolves to a boolean schema ({resolved.contents!r}). Boolean schemas "
            "reached via $ref are not supported by the dereferencer; inline the boolean "
            "schema at the reference site instead."
        )
        raise DereferencingError(err_msg)

    if not isinstance(resolved.contents, Mapping):
        err_msg = f"$ref {ref!r} resolves to a non-object schema ({resolved.contents!r})."
        raise DereferencingError(err_msg)

    target_id = id(resolved.contents)
    if target_id in ctx.active_target_ids:
        err_msg = (
            f"Circular $ref detected at {ref!r}. JSON Schemas with self-referential "
            "structures (e.g. recursive tree/linked-list) cannot be dereferenced."
        )
        raise DereferencingError(err_msg)

    return ref, resolved, target_id


def _warn_ignored_ref_siblings(ref: str, sibling_keys: Mapping[str, Any]) -> None:
    """Emit a warning when sibling keywords alongside ``ref`` are ignored by draft rules.

    :param ref: ``$ref`` URI string.
    :type ref: str
    :param sibling_keys: Ignored sibling keyword mapping.
    :type sibling_keys: Mapping[str, Any]
    """
    logger.warning(
        "Ignoring keyword(s) %s declared alongside $ref %r, per JSON Schema draft semantics "
        "(siblings of $ref are not applied in this draft). Pass ref_sibling_mode='always' to "
        "override.",
        sorted(sibling_keys),
        ref,
    )


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
        ref, resolved, target_id = _lookup_ref_target(schema["$ref"], ctx)
        sibling_keys = {k: v for k, v in schema.items() if k != "$ref"}
        apply_siblings = _should_apply_ref_siblings(ctx, has_siblings=bool(sibling_keys))

        target_body = strip_ref_target_identity(resolved.contents)
        if apply_siblings:
            merged = _merge_schema_pair(target_body, sibling_keys)
        else:
            if sibling_keys:
                _warn_ignored_ref_siblings(ref, sibling_keys)
            merged = target_body

        previous_resolver = ctx.resolver
        ctx.resolver = resolved.resolver
        ctx.active_refs.add(ref)
        ctx.active_target_ids.add(target_id)
        try:
            return _resolve_ref_and_all_of(merged, ctx)
        finally:
            ctx.active_refs.discard(ref)
            ctx.active_target_ids.discard(target_id)
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
    :return: schema with ``allOf`` merged and nested ``$ref``/``allOf`` resolved
    :rtype: dict[str, Any]
    :raises DereferencingError: if any ``allOf`` branch is ``False``, non-mapping, or fails dereferencing
    """
    branches = schema["allOf"]
    if not isinstance(branches, list):
        err_msg = f"'allOf' must be a list of schemas, got {branches!r}."
        raise DereferencingError(err_msg)

    merged: dict[str, Any] = {}
    for sub_schema in branches:
        if isinstance(sub_schema, bool):
            if sub_schema:
                continue
            err_msg = (
                "allOf branch is boolean schema False, which makes the combined schema unsatisfiable "
                "and cannot be flattened into an object/scalar schema."
            )
            raise DereferencingError(err_msg)
        if not isinstance(sub_schema, dict):
            err_msg = f"allOf branch must be a schema object or boolean True, got {sub_schema!r}."
            raise DereferencingError(err_msg)
        deref_sub = cast("dict[str, Any]", _dereference(sub_schema, ctx))
        merged = _merge_schema_pair(merged, deref_sub)

    sibling_schema = {k: v for k, v in schema.items() if k != "allOf"}
    if sibling_schema:
        deref_siblings = cast("dict[str, Any]", _dereference(sibling_schema, ctx))
        merged = _merge_schema_pair(merged, deref_siblings)
    return merged


def _dereference_ref_node(schema: dict[str, Any], ctx: DereferenceContext) -> dict[str, Any]:
    """Dereference a ``$ref`` node while keeping ancestor cycle state and target resolver scope active.

    :param schema: Schema mapping containing ``$ref``.
    :type schema: dict[str, Any]
    :param ctx: Active dereference context.
    :type ctx: DereferenceContext
    :return: Dereferenced schema dictionary.
    :rtype: dict[str, Any]
    """
    ref, resolved, target_id = _lookup_ref_target(schema["$ref"], ctx)
    sibling_keys = {k: v for k, v in schema.items() if k != "$ref"}
    apply_siblings = _should_apply_ref_siblings(ctx, has_siblings=bool(sibling_keys))
    if sibling_keys and not apply_siblings:
        _warn_ignored_ref_siblings(ref, sibling_keys)

    caller_resolver = ctx.resolver
    ctx.active_refs.add(ref)
    ctx.active_target_ids.add(target_id)
    try:
        ctx.resolver = resolved.resolver
        deref_target = cast("dict[str, Any]", _dereference(strip_ref_target_identity(resolved.contents), ctx))
        ctx.resolver = caller_resolver
        if apply_siblings:
            deref_siblings = cast("dict[str, Any]", _dereference(sibling_keys, ctx))
            return _merge_schema_pair(deref_target, deref_siblings)
        return deref_target
    finally:
        ctx.active_refs.discard(ref)
        ctx.active_target_ids.discard(target_id)
        ctx.resolver = caller_resolver


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

    if "$ref" in schema:
        return _dereference_ref_node(schema, ctx)

    if "allOf" in schema:
        return _merge_all_of(schema, ctx)

    result: dict[str, Any] = {}
    for key, value in schema.items():
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
