"""Convert JSON Schema through the shared reader and PySpark emitter."""

from typing import Any

from frozendict import frozendict
from pydantic import Field, field_validator
from pyspark.sql.types import DataType, StructType

from cdm_data_loaders.converters.core.errors import ConversionError
from cdm_data_loaders.converters.core.guards import (
    require_object_root,
    require_schema_keyword,
)
from cdm_data_loaders.converters.core.io import JsonSchemaConverterBase, coerce_extra_metadata_keywords
from cdm_data_loaders.converters.emitters.pyspark import (
    DEFAULT_FORMAT_MAP,
    PySparkEmitter,
    merge_format_map,
)
from cdm_data_loaders.converters.readers.json_schema import JsonSchemaReader


class JSONSchemaToPySparkError(ConversionError):
    """Raised when a JSON Schema construct cannot be converted."""


class InvalidJSONSchemaError(JSONSchemaToPySparkError):
    """Raised when the input document is not a valid JSON Schema."""


class JSONSchemaToPySpark(JsonSchemaConverterBase[StructType]):
    """Read dereferenced JSON Schema and emit pure Python Spark schema objects."""

    format_map: frozendict[str, DataType] = Field(default_factory=lambda: DEFAULT_FORMAT_MAP)
    treat_unknown_as_string: bool = True
    extra_metadata_keywords: frozenset[str] = Field(default_factory=frozenset)

    @field_validator("format_map", mode="before")
    @classmethod
    def _merge_with_builtin_format_map(cls, value: object) -> object:
        """Delegate nonempty format overrides to the emitter."""
        return merge_format_map(value) if value else value

    @field_validator("extra_metadata_keywords", mode="before")
    @classmethod
    def _coerce_extra_metadata_keywords(cls, value: object) -> object:
        """Normalize supported metadata keyword collections."""
        return coerce_extra_metadata_keywords(value)

    def _reader(self) -> JsonSchemaReader:
        """Configure reader diagnostics for this public facade."""
        return JsonSchemaReader(
            self.extension_registry,
            error_type=JSONSchemaToPySparkError,
            converter_name="JSONSchemaToPySpark",
            dereference_function="jsonschema_to_pyspark.dereferencing.dereference_schema",
        )

    def _emitter(self) -> PySparkEmitter:
        """Pass target policy to the emitter without serializing arbitrary types."""
        return PySparkEmitter(
            format_map=self.format_map,
            treat_unknown_as_string=self.treat_unknown_as_string,
            extra_metadata_keywords=self.extra_metadata_keywords,
        )

    def convert(self, schema: dict[str, Any]) -> StructType:
        """Convert an object document, preserving direction-specific guards."""
        require_schema_keyword(schema, InvalidJSONSchemaError, converter_name="JSONSchemaToPySpark")
        require_object_root(schema, JSONSchemaToPySparkError, target="a StructType")
        try:
            return self._emitter().emit(self._reader().read(schema))
        except ConversionError as error:
            raise JSONSchemaToPySparkError(str(error)) from error
