"""Spec 08: Python field names vs. Field(alias=...) and Field(serialization_alias=...)."""

# ruff: noqa: D101, INP001

from pydantic import AliasChoices, BaseModel, ConfigDict, Field


class AliasedChild(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    internal_code: str = Field(alias="code")
    score_val: float = Field(serialization_alias="serialized_score")


class AliasedRecord(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    record_id: str = Field(alias="recordId", description="Record identifier")
    display_name: str = Field(serialization_alias="displayName")
    alt_input: int = Field(validation_alias=AliasChoices("alt_input", "altInput"))
    child: AliasedChild


MODEL = AliasedRecord
