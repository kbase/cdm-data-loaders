"""Exception types raised by the JSON-Schema-to-PySpark converter facade."""

from cdm_data_loaders.converters.core.errors import ConversionError


class JSONSchemaToPySparkError(ConversionError):
    """Raised when a JSON Schema construct cannot be converted."""


class InvalidJSONSchemaError(JSONSchemaToPySparkError):
    """Raised when the input document is not a valid JSON Schema."""
