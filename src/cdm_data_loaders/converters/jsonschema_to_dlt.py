"""Convert dereferenced JSON Schema through the shared reader and dlt emitter."""

from typing import TYPE_CHECKING, Any, ClassVar

from dlt.common.schema.typing import TStoredSchema
from pydantic import Field, field_validator

from cdm_data_loaders.converters.core.errors import ConversionError
from cdm_data_loaders.converters.core.io import JsonSchemaConverterBase
from cdm_data_loaders.converters.emitters.dlt import DltEmitter, validate_non_empty_schema_name

if TYPE_CHECKING:
    from dlt.common.schema import Schema


class JSONSchemaToDltError(ConversionError):
    """Raised when a JSON Schema construct cannot be converted."""


class InvalidJSONSchemaError(JSONSchemaToDltError):
    """Raised when the input document is not a valid JSON Schema."""


class JSONSchemaToDlt(JsonSchemaConverterBase[TStoredSchema]):
    """Read a dereferenced JSON Schema and emit dlt tables."""

    _error_cls: ClassVar[type[ConversionError]] = JSONSchemaToDltError
    _invalid_schema_error_cls: ClassVar[type[ConversionError]] = InvalidJSONSchemaError
    _converter_name: ClassVar[str] = "JSONSchemaToDlt"
    _root_target_label: ClassVar[str] = "a dlt table"
    _dereference_function: ClassVar[str] = "jsonschema_to_dlt.dereferencing.dereference_schema"

    schema_name: str
    skip_nested_types: bool = False
    max_nesting: int = Field(default=10, ge=1)
    write_disposition: str | None = None
    flatten_scalars: bool = True

    @field_validator("schema_name")
    @classmethod
    def _validate_schema_name(cls, value: str) -> str:
        """Reject empty schema names."""
        return validate_non_empty_schema_name(value)

    def _emitter(self) -> DltEmitter:
        """Pass target policy to the emitter."""
        return DltEmitter(**self.model_dump(exclude={"extension_registry"}))

    def to_yaml(self, schema: dict[str, Any]) -> str:
        """Emit a dlt stored schema as YAML."""
        return self._emitter().to_yaml(self._read(schema))

    def to_schema(self, schema: dict[str, Any]) -> "Schema":
        """Emit a live dlt Schema with parent tables installed first."""
        return self._emitter().to_schema(self._read(schema))
