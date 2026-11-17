"""Exception types raised by the PySpark schema emitter."""

from cdm_data_loaders.converters.core.errors import ConversionError


class PySparkEmitterError(ConversionError):
    """A typed schema cannot be represented by the configured Spark policy."""
