"""Convert dlt schemas through the typed reader and JSON Schema emitter."""

from typing import Any, ClassVar, Literal

from cdm_data_loaders.converters.core.errors import ConversionError
from cdm_data_loaders.converters.emitters.json_schema.emitter import JsonSchemaEmitter
from cdm_data_loaders.converters.exceptions.facades.dlt_to_jsonschema import DltToJSONSchemaError
from cdm_data_loaders.converters.facades._base import JsonSchemaConverterBase
from cdm_data_loaders.converters.sources.dlt import DltReader


class DltToJSONSchema(JsonSchemaConverterBase[dict[str, dict[str, Any]]]):
    """Read root table documents and emit their JSON Schema equivalents."""

    _allow_yaml_in_string: ClassVar[bool] = True

    child_table_mode: Literal["object", "array"] = "object"
    include_dlt_columns: bool = False
    include_variant_columns: bool = False
    preserve_unknown_hints: bool = True

    def convert(self, schema: dict[str, Any]) -> dict[str, dict[str, Any]]:
        """Convert a stored schema or table mapping into root JSON documents."""
        reader = DltReader(
            include_dlt_columns=self.include_dlt_columns,
            include_variant_columns=self.include_variant_columns,
            child_table_mode=self.child_table_mode,
            extension_registry=self.extension_registry,
        )
        emitter = JsonSchemaEmitter(preserve_unknown_hints=self.preserve_unknown_hints)
        try:
            return {name: emitter.emit(document) for name, document in reader.read(schema).items()}
        except ConversionError as error:
            raise DltToJSONSchemaError(str(error)) from error
