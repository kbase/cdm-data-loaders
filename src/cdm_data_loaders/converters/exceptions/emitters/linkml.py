"""Exception hierarchy for the LinkML schema emitter."""

from cdm_data_loaders.converters.core.errors import ConversionError


class LinkMLEmitterError(ConversionError):
    """Raised when a typed node has no faithful LinkML representation."""


class EmptyLinkMLSlotNameError(LinkMLEmitterError):
    """Raised when a field has an empty or whitespace-only name that cannot form a LinkML slot."""


class NestedLinkMLMultivaluedError(LinkMLEmitterError):
    """Raised when an array contains nested array or map items that cannot be represented as a flat LinkML slot."""
