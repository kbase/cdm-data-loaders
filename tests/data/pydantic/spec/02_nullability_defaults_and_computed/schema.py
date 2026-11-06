"""Spec 02: Field nullability, non-None defaults, default_factory, and @computed_field."""

from functools import partial

from pydantic import BaseModel, Field, computed_field


class NullabilityAndDefaultsRecord(BaseModel):
    id: str
    required_score: int
    defaulted_retries: int = 3
    default_tags: list[str] = Field(default_factory=partial(list, ("general",)))
    optional_alias: str | None = None
    explicit_nullable: int | None

    @computed_field(description="Synthesized identifier and retry label")
    @property
    def summary_label(self) -> str:
        return f"{self.id}:r{self.defaulted_retries}"


MODEL = NullabilityAndDefaultsRecord
