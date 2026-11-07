"""Schema input parsing and shared converter facade base class."""

import json
from pathlib import Path
from typing import Any, ClassVar

import yaml
from pydantic import BaseModel, ConfigDict, InstanceOf

from cdm_data_loaders.converters.core.errors import ConversionError
from cdm_data_loaders.converters.core.extensions import DEFAULT_EXTENSIONS, ExtensionRegistry
from cdm_data_loaders.converters.core.guards import require_object_root, require_schema_keyword
from cdm_data_loaders.converters.core.ir import Emitter, SchemaDocument
from cdm_data_loaders.converters.readers.json_schema import JsonSchemaReader


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
    _error_cls: ClassVar[type[ConversionError]] = ConversionError
    _invalid_schema_error_cls: ClassVar[type[ConversionError]] = ConversionError
    _converter_name: ClassVar[str] = ""
    _root_target_label: ClassVar[str] = ""
    _dereference_function: ClassVar[str] = "cdm_data_loaders.utils.jsonschema.dereferencer.dereference_schema"

    extension_registry: InstanceOf[ExtensionRegistry] = DEFAULT_EXTENSIONS

    def _reader(self) -> JsonSchemaReader:
        """Configure reader diagnostics for this public facade."""
        return JsonSchemaReader(
            self.extension_registry,
            error_type=self._error_cls,
            converter_name=self._converter_name or type(self).__name__,
            dereference_function=self._dereference_function,
        )

    def _emitter(self) -> Emitter[OutputT]:
        """Return the configured emitter for this facade."""
        raise NotImplementedError

    def _read(self, schema: dict[str, Any]) -> SchemaDocument:
        """Preserve facade guards and error identity around the reader."""
        converter_name = self._converter_name or type(self).__name__
        require_schema_keyword(schema, self._invalid_schema_error_cls, converter_name=converter_name)
        require_object_root(schema, self._error_cls, target=self._root_target_label)
        try:
            return self._reader().read(schema)
        except ConversionError as error:
            if isinstance(error, self._error_cls):
                raise
            raise self._error_cls(str(error)) from error

    def convert(self, schema: dict[str, Any]) -> OutputT:
        """Convert a parsed schema mapping to the target representation."""
        try:
            return self._emitter().emit(self._read(schema))
        except ConversionError as error:
            if isinstance(error, self._error_cls):
                raise
            raise self._error_cls(str(error)) from error

    def convert_from_string(self, schema_str: str) -> OutputT:
        """Convert a schema string according to the converter's text parsing policy."""
        return self.convert(load_schema_text(schema_str, allow_yaml=self._allow_yaml_in_string))

    def convert_from_file(self, path: str) -> OutputT:
        """Convert a JSON or YAML schema file selected by its extension."""
        return self.convert(load_schema_file(path))
