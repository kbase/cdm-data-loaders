"""Exception types raised by the PyArrow schema emitter."""

from cdm_data_loaders.converters.core.errors import ConversionError


class PyArrowEmitterError(ConversionError):
    """A typed schema cannot be represented by the configured Arrow policy."""
