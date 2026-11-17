"""Exception types raised by the JSON-Schema-to-dlt converter facade."""

from cdm_data_loaders.converters.core.errors import ConversionError


class JSONSchemaToDltError(ConversionError):
    """Raised when a JSON Schema construct cannot be converted."""


class InvalidJSONSchemaError(JSONSchemaToDltError):
    """Raised when the input document is not a valid JSON Schema."""
