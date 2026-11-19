"""Draft-specific JSON Schema keyword selection for field metadata."""

from functools import cache
from typing import Final

from jsonschema import Draft7Validator
from pydantic import BaseModel, ConfigDict, Field

from cdm_data_loaders.validation.jsonschema.keywords import (
    REF_AND_IDENTITY_KEYWORDS,
    STRUCTURAL_OR_COMPOSITIONAL_KEYWORDS,
)

DEFAULT_METADATA_KEYWORDS: Final[frozenset[str]] = frozenset({"title"})


def get_known_jsonschema_keywords(validator_cls: type) -> set[str]:
    """Combine draft-specific assertions with the Draft-07 annotation vocabulary."""
    keywords = set(Draft7Validator.META_SCHEMA["properties"]) | set(validator_cls.VALIDATORS)
    return keywords - REF_AND_IDENTITY_KEYWORDS


@cache
def metadata_keys_for(validator_cls: type) -> frozenset[str]:
    """Find nonstructural field metadata keywords for a validator class."""
    return frozenset(get_known_jsonschema_keywords(validator_cls) - STRUCTURAL_OR_COMPOSITIONAL_KEYWORDS)


class ConversionContext(BaseModel):
    """Draft-specific metadata selection for a single emission."""

    model_config = ConfigDict(arbitrary_types_allowed=True, extra="forbid", frozen=True)

    validator_cls: type
    extra_metadata_keywords: frozenset[str] = Field(default_factory=frozenset)

    @property
    def metadata_keys(self) -> frozenset[str]:
        """Return the active draft's eligible metadata keywords."""
        return metadata_keys_for(self.validator_cls)

    @property
    def allowed_extra_metadata_keywords(self) -> frozenset[str]:
        """Select standard keywords and explicitly requested extension namespaces."""
        return frozenset(
            key for key in self.extra_metadata_keywords if key in self.metadata_keys or key.startswith("x-")
        )

    @property
    def invalid_extra_metadata_keywords(self) -> frozenset[str]:
        """Return ignored structural, identity and unknown keywords."""
        return self.extra_metadata_keywords - self.allowed_extra_metadata_keywords
