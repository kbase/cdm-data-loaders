"""Shared base class for JSON Schema and YAML converter facades."""

from collections.abc import Mapping
from typing import Any, ClassVar, Protocol

from pydantic import BaseModel, ConfigDict, InstanceOf

from cdm_data_loaders.converters.core.errors import ConversionError
from cdm_data_loaders.converters.core.extensions import DEFAULT_EXTENSIONS, ExtensionRegistry
from cdm_data_loaders.converters.core.guards import require_object_root, require_schema_keyword
from cdm_data_loaders.converters.core.io import load_schema_file, load_schema_text
from cdm_data_loaders.converters.core.ir import Emitter, SchemaDocument
from cdm_data_loaders.converters.sources.json_schema.permissive import JsonSchemaReader


class JsonSchemaIRReader(Protocol):
    """Protocol for JSON Schema IR readers accepted by ``JsonSchemaConverterBase``."""

    def read(self, source: Mapping[str, Any]) -> SchemaDocument:
        """Read a JSON Schema mapping into a ``SchemaDocument``.

        :param source: JSON Schema mapping.
        :type source: Mapping[str, Any]
        :return: Parsed ``SchemaDocument``.
        :rtype: SchemaDocument
        """
        ...


class JsonSchemaConverterBase[OutputT](BaseModel):
    """Shared base class for schema converter facades supporting string and file input."""

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    _allow_yaml_in_string: ClassVar[bool] = False
    _run_root_guards: ClassVar[bool] = True
    _error_cls: ClassVar[type[ConversionError]] = ConversionError
    _invalid_schema_error_cls: ClassVar[type[ConversionError]] = ConversionError
    _converter_name: ClassVar[str] = ""
    _root_target_label: ClassVar[str] = ""
    _dereference_function: ClassVar[str] = "cdm_data_loaders.validation.jsonschema.dereference.dereference_schema"

    extension_registry: InstanceOf[ExtensionRegistry] = DEFAULT_EXTENSIONS

    def _reader(self) -> JsonSchemaIRReader:
        """Configure reader diagnostics for this public facade.

        :return: Configured ``JsonSchemaReader`` instance.
        :rtype: JsonSchemaIRReader
        """
        return JsonSchemaReader(
            self.extension_registry,
            error_type=self._error_cls,
            converter_name=self._converter_name or type(self).__name__,
            dereference_function=self._dereference_function,
        )

    def _emitter(self) -> Emitter[OutputT]:
        """Return the configured emitter for this facade.

        :return: Target emitter instance.
        :rtype: Emitter[OutputT]
        :raises NotImplementedError: If not overridden by a concrete subclass.
        """
        raise NotImplementedError

    def _read(self, schema: dict[str, Any]) -> SchemaDocument:
        """Run facade root guards and lower ``schema`` into a ``SchemaDocument``.

        :param schema: Parsed schema mapping.
        :type schema: dict[str, Any]
        :return: Lowered ``SchemaDocument``.
        :rtype: SchemaDocument
        """
        if self._run_root_guards:
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
        """Convert a parsed schema mapping to the target representation.

        :param schema: Parsed schema mapping.
        :type schema: dict[str, Any]
        :return: Converted target schema.
        :rtype: OutputT
        """
        try:
            return self._emitter().emit(self._read(schema))
        except ConversionError as error:
            if isinstance(error, self._error_cls):
                raise
            raise self._error_cls(str(error)) from error

    def convert_from_string(self, schema_str: str) -> OutputT:
        """Convert a schema string according to the converter's text parsing policy.

        :param schema_str: Raw JSON or YAML schema text.
        :type schema_str: str
        :return: Converted target schema.
        :rtype: OutputT
        """
        return self.convert(load_schema_text(schema_str, allow_yaml=self._allow_yaml_in_string))

    def convert_from_file(self, path: str) -> OutputT:
        """Convert a JSON or YAML schema file selected by its extension.

        :param path: Path to the schema file.
        :type path: str
        :return: Converted target schema.
        :rtype: OutputT
        """
        return self.convert(load_schema_file(path))
