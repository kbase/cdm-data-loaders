"""Schema input parsing, file writing, and extra metadata keyword coercion."""

import json
from logging import Logger
from pathlib import Path
from typing import Annotated, Any

import yaml
from pydantic import BeforeValidator


def _require_mapping(document: object, source: str) -> dict[str, Any]:
    """Reject a parsed document that is not a top-level object.

    :param document: Parsed JSON or YAML value.
    :type document: object
    :param source: Diagnostic source label.
    :type source: str
    :return: Validated top-level mapping.
    :rtype: dict[str, Any]
    :raises TypeError: If ``document`` is not a ``dict``.
    """
    if not isinstance(document, dict):
        msg = f"Parsed schema {source} must be a JSON/YAML object, got {type(document).__name__}"
        raise TypeError(msg)
    return document


def load_schema_text(text: str | bytes, *, allow_yaml: bool = False) -> dict[str, Any]:
    """Parse JSON text, falling back to YAML when ``allow_yaml`` is enabled.

    :param text: Raw JSON or YAML string or bytes.
    :type text: str | bytes
    :param allow_yaml: Whether to fall back to ``yaml.safe_load`` on invalid JSON.
    :type allow_yaml: bool
    :return: Parsed top-level schema mapping.
    :rtype: dict[str, Any]
    """
    try:
        document = json.loads(text)
    except json.JSONDecodeError:
        if not allow_yaml:
            raise
        document = yaml.safe_load(text)
    return _require_mapping(document, "text")


def load_schema_file(path: str | Path) -> dict[str, Any]:
    """Load a JSON or YAML schema from a file, choosing by the ``.json`` extension.

    :param path: path to the schema file
    :type path: str | Path
    :return: the parsed schema document
    :rtype: dict[str, Any]
    """
    path = Path(path)
    loader = json.loads if path.name.endswith(".json") else yaml.safe_load
    return _require_mapping(loader(path.read_bytes()), f"from {path}")


def write_schema_text(text: str, out_file: Path, log: Logger) -> None:
    """Write serialized schema text to disk, warning before replacing an existing file.

    :param text: serialized schema text to write
    :type text: str
    :param out_file: destination file path
    :type out_file: Path
    :param log: logger instance
    :type log: Logger
    """
    if out_file.exists():
        log.warning("Overwriting existing schema file %s", out_file)
    out_file.write_text(text, encoding="utf-8")
    log.info("Wrote schema to %s", out_file)


def coerce_extra_metadata_keywords(value: object) -> object:
    """Normalize supported metadata keyword collections into a frozenset.

    :param value: candidate keyword collection or raw input value
    :type value: object
    :return: normalized ``frozenset`` when ``value`` is a ``set``, ``list``, or ``tuple``, else ``value`` unchanged
    :rtype: object
    """
    return frozenset(value) if isinstance(value, (set, list, tuple)) else value


ExtraMetadataKeywords = Annotated[frozenset[str], BeforeValidator(coerce_extra_metadata_keywords)]
