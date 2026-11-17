"""Exception types raised by the dlt-to-JSON-Schema converter facade."""

from cdm_data_loaders.converters.core.errors import ConversionError


class DltToJSONSchemaError(ConversionError):
    """Raised when a dlt schema construct cannot be converted."""
