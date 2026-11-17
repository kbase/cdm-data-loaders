"""Exception types raised by the PyArrow converter source reader."""

from cdm_data_loaders.converters.core.errors import ConversionError


class PyArrowReaderError(ConversionError):
    """Raised when a PyArrow schema, field, or data type cannot be converted into the converter IR."""
