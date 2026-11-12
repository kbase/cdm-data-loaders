"""JSON Schema validation, custom KBase XSV metaschema, and $ref/allOf dereferencing utilities."""

import importlib.resources
import json
import logging
from collections.abc import Mapping
from dataclasses import dataclass
from dataclasses import field as dataclass_field
from functools import cache
from importlib.resources.abc import Traversable
from pathlib import Path
from typing import Any, Final, Literal

import jsonschema
import jsonschema.exceptions
import jsonschema.protocols
import jsonschema.validators
from frozendict import frozendict
from jsonschema import validators
from pydantic import BaseModel, field_validator
from referencing import Registry, Resource
from referencing._core import Resolver
from referencing.exceptions import CannotDetermineSpecification, Unresolvable
from referencing.jsonschema import DRAFT201909, DRAFT202012, specification_with

from cdm_data_loaders.utils.file_transfer.sync_client import get_httpx_client

logger: logging.Logger = logging.getLogger(__name__)

XSV_CONFIG_SCHEMA_REL_PATH: Final[str] = "xsv_config/0.1.0/schema.json"
CUSTOM_META_SCHEMA_URI: Final[str] = f"https://kbase.github.io/cdm-data-loaders/schemas/{XSV_CONFIG_SCHEMA_REL_PATH}"
X_XSV_CONFIG_KEY: Final[str] = "x-xsv-config"


def _read_raw_xsv_meta_schema(
    local_schema_path: Path | Traversable | None = None,
    remote_url: str = CUSTOM_META_SCHEMA_URI,
) -> object:
    """Read the raw XSV custom metaschema JSON from the bundled package file or remote URL.

    Loads from ``schemas/xsv_config/0.1.0/schema.json`` when available locally, falling
    back to fetching ``remote_url`` over HTTP when the local file is not present.

    :param local_schema_path: optional local schema path or ``Traversable`` override
    :type local_schema_path: Path | Traversable | None
    :param remote_url: fallback URL to fetch the metaschema from when unavailable locally
    :type remote_url: str
    :return: parsed JSON value of the custom metaschema
    :rtype: object
    :raises httpx.HTTPError: if the fallback HTTP request fails
    :raises json.JSONDecodeError: if the schema content is not valid JSON
    """
    resource: Path | Traversable | None = local_schema_path
    if resource is None:
        try:
            resource = importlib.resources.files("cdm_data_loaders.schemas").joinpath(
                *XSV_CONFIG_SCHEMA_REL_PATH.split("/")
            )
        except (FileNotFoundError, ModuleNotFoundError):
            resource = None

    is_local_file = False
    if resource is not None:
        try:
            is_local_file = resource.is_file()
        except (FileNotFoundError, ModuleNotFoundError):
            is_local_file = False

    if is_local_file and resource is not None:
        return json.loads(resource.read_text(encoding="utf-8"))

    with get_httpx_client() as client:
        response = client.get(remote_url)
        response.raise_for_status()
        return response.json()


def load_xsv_meta_schema(
    local_schema_path: Path | Traversable | None = None,
    remote_url: str = CUSTOM_META_SCHEMA_URI,
) -> frozendict[str, Any]:
    """Load the custom XSV metaschema and validate it against its declared metaschema.

    Reads the custom metaschema from ``schemas/xsv_config/0.1.0/schema.json`` if available,
    or from ``https://kbase.github.io/cdm-data-loaders/schemas/xsv_config/0.1.0/schema.json``
    otherwise, and validates the document against its own ``$schema`` metaschema.

    :param local_schema_path: optional local schema path or ``Traversable`` override
    :type local_schema_path: Path | Traversable | None
    :param remote_url: fallback URL to fetch the metaschema from when unavailable locally
    :type remote_url: str
    :return: validated custom metaschema as an immutable mapping
    :rtype: frozendict[str, Any]
    :raises TypeError: if the document is not a JSON object
    :raises ValueError: if the document omits ``$schema``, declares itself as its own metaschema,
        or uses an unsupported JSON Schema draft
    :raises jsonschema.exceptions.SchemaError: if the custom metaschema fails validation against its metaschema
    """
    raw_schema = _read_raw_xsv_meta_schema(local_schema_path=local_schema_path, remote_url=remote_url)
    if not isinstance(raw_schema, dict):
        err_msg = "Custom metaschema must be a JSON object"
        raise TypeError(err_msg)

    declared = raw_schema.get("$schema")
    if not isinstance(declared, str) or not declared:
        err_msg = "JSON Schema is missing the $schema keyword"
        raise ValueError(err_msg)

    if declared in {CUSTOM_META_SCHEMA_URI, remote_url}:
        err_msg = f"Custom metaschema cannot declare itself as its own $schema: {declared}"
        raise ValueError(err_msg)

    meta_validator = jsonschema.validators.validator_for(raw_schema, default=None)  # type: ignore[arg-type]
    if meta_validator is None:
        err_msg = f"Unsupported JSON Schema draft: {declared}"
        raise ValueError(err_msg)

    meta_validator.check_schema(raw_schema)
    return frozendict(raw_schema)


@cache
def get_xsv_meta_schema() -> frozendict[str, Any]:
    """Lazily load, validate, and cache the custom KBase XSV metaschema.

    :return: validated custom metaschema mapping
    :rtype: frozendict[str, Any]
    """
    return load_xsv_meta_schema()


def get_xsv_config_schema() -> frozendict[str, Any]:
    """Lazily extract the ``x-xsv-config`` extension schema from the custom XSV metaschema.

    :return: immutable ``x-xsv-config`` JSON Schema mapping
    :rtype: frozendict[str, Any]
    :raises ValueError: if the custom metaschema does not define ``properties['x-xsv-config']``
    :raises TypeError: if ``properties['x-xsv-config']`` is not a mapping
    """
    meta_schema = get_xsv_meta_schema()
    properties = meta_schema.get("properties")
    xsv_config_schema = properties.get(X_XSV_CONFIG_KEY) if isinstance(properties, Mapping) else None
    if xsv_config_schema is None:
        err_msg = f"Custom metaschema is missing '{X_XSV_CONFIG_KEY}' object schema under 'properties'"
        raise ValueError(err_msg)
    if not isinstance(xsv_config_schema, Mapping):
        err_msg = f"Custom metaschema '{X_XSV_CONFIG_KEY}' schema must be a mapping"
        raise TypeError(err_msg)
    return frozendict(xsv_config_schema)


def register_xsv_validator() -> None:
    """Lazily load, validate, and register the custom validator for the KBase XSV metaschema.

    :return: None
    :rtype: None
    """
    meta_schema = get_xsv_meta_schema()
    effective_meta_schema = (
        meta_schema
        if meta_schema.get("$id") == CUSTOM_META_SCHEMA_URI
        else frozendict({**meta_schema, "$id": CUSTOM_META_SCHEMA_URI})
    )
    existing = jsonschema.validators.validator_for({"$schema": CUSTOM_META_SCHEMA_URI}, default=None)  # type: ignore[arg-type]
    if existing is not None and getattr(existing, "META_SCHEMA", None) == effective_meta_schema:
        return
    base_validator = jsonschema.validators.validator_for({**meta_schema})
    xsv_validator = validators.extend(base_validator)
    xsv_validator.META_SCHEMA = effective_meta_schema
    validators.validates(CUSTOM_META_SCHEMA_URI)(xsv_validator)


class ValidatedSchema(BaseModel):
    """A validated JSON Schema."""

    jsonschema: dict[str, Any]

    @field_validator("jsonschema", mode="after")
    @classmethod
    def validate_jsonschema(cls, schema: dict[str, Any]) -> dict[str, Any]:
        """Validate the schema in the jsonschema field.

        :param schema: a JSON schema
        :type schema: dict[str, Any]
        :raises ValueError: if there is no ``$schema`` field to identify the metaschema used
        :raises jsonschema.exceptions.SchemaError: if the schema is invalid
        :raises jsonschema.exceptions.ValidationError: if the schema fails validation
        :return: validated schema
        :rtype: dict[str, Any]
        """
        if "$schema" not in schema or not isinstance(schema["$schema"], str):
            err_msg = "JSON Schema is missing the $schema keyword"
            raise ValueError(err_msg)

        declared = schema["$schema"]
        if declared == CUSTOM_META_SCHEMA_URI:
            register_xsv_validator()

        validator = jsonschema.validators.validator_for(schema, default=None)  # type: ignore[arg-type]
        if validator is None:
            err_msg = f"Unsupported JSON Schema draft: {declared}"
            raise ValueError(err_msg)

        try:
            validator.check_schema(schema)
        except (jsonschema.exceptions.SchemaError, jsonschema.exceptions.ValidationError):
            logger.exception("Error validating JSON Schema")
            raise
        return schema


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
    """Raised when a JSON Schema document cannot be fully dereferenced."""


@dataclass
class DereferenceContext:
    """Mutable state for a single ``dereference_schema()`` call.

    :param resolver: the ``referencing`` resolver rooted at the schema currently being dereferenced
    :type resolver: Resolver
    :param spec: the JSON Schema specification/dialect in effect
    :type spec: Any
    :param max_ref_depth: maximum depth of a ``$ref`` chain before giving up
    :type max_ref_depth: int
    :param ref_sibling_mode: how to handle keywords declared alongside ``$ref``
    :type ref_sibling_mode: RefSiblingMode
    :param active_refs: the set of ``$ref`` values currently being resolved, used for circular-reference detection
    :type active_refs: set[str]
    """

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
    """Resolve every ``$ref`` in a schema using ``additional_resources`` where needed.

    Merges any ``allOf`` tags to return a fully self-contained schema.

    :param schema: the JSON Schema document to dereference
    :type schema: dict[str, Any]
    :param additional_resources: mapping of URI to schema dict, used to resolve external ``$ref``s
    :type additional_resources: Mapping[str, dict[str, Any]] | None
    :param max_ref_depth: maximum depth of a ``$ref`` chain before giving up
    :type max_ref_depth: int
    :param ref_sibling_mode: how to handle keywords declared alongside ``$ref``:
        ``'auto'`` (spec-compliant per detected draft), ``'always'`` (permissive), or
        ``'never'`` (strict pre-2019-09 semantics)
    :type ref_sibling_mode: RefSiblingMode
    :return: an equivalent, ``$ref``/``allOf``-free JSON Schema document
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
    return _dereference(schema, ctx)  # type: ignore[return-value]


def _should_apply_ref_siblings(ctx: DereferenceContext, has_siblings: bool) -> bool:  # noqa: FBT001
    """Decide whether keywords declared alongside a ``$ref`` should be applied.

    :param ctx: the active dereference context
    :type ctx: DereferenceContext
    :param has_siblings: whether the schema declares any keywords alongside ``$ref``
    :type has_siblings: bool
    :return: True if sibling keywords should be merged into the resolved schema
    :rtype: bool
    """
    if not has_siblings or ctx.ref_sibling_mode == "never":
        return False
    if ctx.ref_sibling_mode == "always":
        return True
    return ctx.spec in REF_SIBLING_AWARE_SPECS


def _resolve_ref_and_all_of(schema: dict[str, Any], ctx: DereferenceContext) -> dict[str, Any]:
    """Resolve ``allOf`` and ``$ref`` references at a single schema level.

    Does not descend into nested sub-schemas (e.g. ``properties`` values).

    :param schema: the schema fragment to resolve at this level
    :type schema: dict[str, Any]
    :param ctx: the active dereference context
    :type ctx: DereferenceContext
    :return: the schema with ``$ref``/``allOf`` resolved at this level (not recursively)
    :rtype: dict[str, Any]
    :raises DereferencingError: if a ``$ref`` is unresolvable, circular, or the chain
        exceeds ``ctx.max_ref_depth``
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
                "reached via $ref are not currently supported by the dereferencer; inline the boolean "
                "schema directly at the reference site instead."
            )
            raise DereferencingError(err_msg)

        sibling_keys = {k: v for k, v in schema.items() if k != "$ref"}
        apply_siblings = _should_apply_ref_siblings(ctx, bool(sibling_keys))

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

    ``properties``/``required``/``patternProperties`` are unioned across all ``allOf``
    branches and any sibling declaration of the same keywords; other sibling
    keywords take final precedence over their ``allOf``-branch counterparts.

    :param schema: the schema fragment containing the ``allOf`` keyword to merge
    :type schema: dict[str, Any]
    :param ctx: the active dereference context
    :type ctx: DereferenceContext
    :return: the schema with ``allOf`` merged away
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


def _dereference(schema: dict[str, Any] | bool, ctx: DereferenceContext) -> dict[str, Any] | bool:  # noqa: FBT001
    """Recursively resolve ``$ref``/``allOf`` at every level of ``schema``.

    :param schema: the schema fragment (or boolean schema) to dereference
    :type schema: dict[str, Any] | bool
    :param ctx: the active dereference context
    :type ctx: DereferenceContext
    :return: the fully dereferenced schema fragment, with nested schemas recursively dereferenced
    :rtype: dict[str, Any] | bool
    :raises DereferencingError: if any nested ``$ref`` is unresolvable, circular, or exceeds ``ctx.max_ref_depth``
    """
    if isinstance(schema, bool):
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
