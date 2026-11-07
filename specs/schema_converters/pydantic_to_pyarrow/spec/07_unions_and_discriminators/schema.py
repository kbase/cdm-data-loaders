"""Spec 07: Scalar unions, BaseModel unions, discriminated unions, and scalar + model unions."""

from typing import Annotated, Literal

from pydantic import BaseModel, Field


class ShortReadSource(BaseModel):
    shared_id: str = Field(description="Shared source identifier")
    platform: str
    read_length: int
    metric: int


class LongReadSource(BaseModel):
    shared_id: str = Field(description="Ignored second description")
    chemistry: str
    metric: float


class BlastHit(BaseModel):
    hit_type: Literal["blast"]
    accession: str
    evalue: float


class HmmHit(BaseModel):
    hit_type: Literal["hmm"]
    accession: str
    bit_score: float


class StructuredCitation(BaseModel):
    pmid: int
    title: str


class UnionShowcaseRecord(BaseModel):
    id: str
    numeric_union: int | float
    scalar_fallback_union: int | str | bool
    source: ShortReadSource | LongReadSource
    top_hit: Annotated[BlastHit | HmmHit, Field(discriminator="hit_type")]
    citation: StructuredCitation | str


MODEL = UnionShowcaseRecord
