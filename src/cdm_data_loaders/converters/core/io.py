"""Schema input parsing and shared converter facade base class."""

import json
from pathlib import Path
from typing import Any, ClassVar

import yaml
from pydantic import BaseModel, ConfigDict, InstanceOf

from cdm_data_loaders.converters.core.extensions import DEFAULT_EXTENSIONS, ExtensionRegistry


def _require_mapping(document: object, source: str) -> dict[str, Any]:
    """Reject a parsed document that is not a top-level object."""
    if not isinstance(document, dict):
        msg = f"Parsed schema {source} must be a JSON/YAML object, got {type(document).__name__}"
        raise TypeError(msg)
    return document


def load_schema_text(text: str | bytes, *, allow_yaml: bool = False) -> dict[str, Any]:
    """Parse JSON text, falling back to YAML only when explicitly enabled."""
    try:
        document = json.loads(text)
    except json.JSONDecodeError:
        if not allow_yaml:
            raise
        document = yaml.safe_load(text)
    return _require_mapping(document, "text")


def load_schema_file(path: str | Path) -> dict[str, Any]:
    """Load a JSON or YAML schema from a file, choosing by the `.json` extension.

    :param path: path to the schema file
    :type path: str | Path
    :return: the parsed schema document
    :rtype: dict[str, Any]
    """
    path = Path(path)
    loader = json.loads if path.name.endswith(".json") else yaml.safe_load
    return _require_mapping(loader(path.read_bytes()), f"from {path}")


def coerce_extra_metadata_keywords(value: object) -> object:
    """Normalize supported metadata keyword collections into a frozenset."""
    return frozenset(value) if isinstance(value, (set, list, tuple)) else value


class JsonSchemaConverterBase[OutputT](BaseModel):
    """Shared base class for schema converter facades supporting string and file input."""

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    _allow_yaml_in_string: ClassVar[bool] = False

    extension_registry: InstanceOf[ExtensionRegistry] = DEFAULT_EXTENSIONS

    def convert(self, schema: dict[str, Any]) -> OutputT:
        """Convert a parsed schema mapping to the target representation."""
        raise NotImplementedError

    def convert_from_string(self, schema_str: str) -> OutputT:
        """Convert a schema string according to the converter's text parsing policy."""
        return self.convert(load_schema_text(schema_str, allow_yaml=self._allow_yaml_in_string))

    def convert_from_file(self, path: str) -> OutputT:
        """Convert a JSON or YAML schema file selected by its extension."""
        return self.convert(load_schema_file(path))
