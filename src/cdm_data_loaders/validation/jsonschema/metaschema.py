"""Custom KBase XSV JSON Schema metaschema loading, caching, and validator registration."""

import importlib.resources
import json
from collections.abc import Mapping
from functools import cache
from importlib.resources.abc import Traversable
from pathlib import Path
from typing import Any, Final

import httpx
import jsonschema.validators
from frozendict import frozendict
from jsonschema import validators

XSV_CONFIG_SCHEMA_REL_PATH: Final[str] = "xsv_config/0.1.0/schema.json"
CUSTOM_META_SCHEMA_URI: Final[str] = f"https://kbase.github.io/cdm-data-loaders/schemas/{XSV_CONFIG_SCHEMA_REL_PATH}"
X_XSV_CONFIG_KEY: Final[str] = "x-xsv-config"
REMOTE_METASCHEMA_TIMEOUT_SECONDS: Final[float] = 30.0


def _read_raw_xsv_meta_schema(
    local_schema_path: Path | Traversable | None = None,
    remote_url: str = CUSTOM_META_SCHEMA_URI,
) -> object:
    """Read the raw XSV custom metaschema JSON from the bundled package file or remote URL.

    Loads from ``schemas/xsv_config/0.1.0/schema.json`` when present locally, falling back
    to fetching ``remote_url`` over HTTP when unavailable locally.

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

    with httpx.Client(timeout=httpx.Timeout(REMOTE_METASCHEMA_TIMEOUT_SECONDS), follow_redirects=True) as client:
        response = client.get(remote_url)
        response.raise_for_status()
        return response.json()


def load_xsv_meta_schema(
    local_schema_path: Path | Traversable | None = None,
    remote_url: str = CUSTOM_META_SCHEMA_URI,
) -> frozendict[str, Any]:
    """Load the custom XSV metaschema and validate it against its declared metaschema.

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
    """Load, validate, and cache the custom KBase XSV metaschema on first call.

    :return: validated custom metaschema mapping
    :rtype: frozendict[str, Any]
    """
    return load_xsv_meta_schema()


def get_xsv_config_schema() -> frozendict[str, Any]:
    """Extract the ``x-xsv-config`` extension schema from the custom XSV metaschema.

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
    """Load, validate, and register the custom validator for the KBase XSV metaschema."""
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
