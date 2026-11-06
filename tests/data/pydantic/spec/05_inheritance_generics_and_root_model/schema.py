"""Spec 05: Model inheritance, Generic[T] parameterization, and RootModel[T] unwrapping."""

from pydantic import BaseModel, Field, RootModel


class BaseEntity(BaseModel):
    id: str
    version: int | str = 1


class AuditedEntity(BaseEntity):
    version: int = Field(default=1, description="Overridden integer version")
    updated_by: str


class SamplePayload(BaseModel):
    sample_name: str
    depth_m: float


class GenericEnvelope[PayloadT](AuditedEntity):
    payload: PayloadT
    secondary_payload: PayloadT | None = None


class RootEnvelopeModel(RootModel[GenericEnvelope[SamplePayload]]):
    pass


MODEL = RootEnvelopeModel
