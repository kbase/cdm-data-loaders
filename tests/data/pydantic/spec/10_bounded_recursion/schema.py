"""Spec 10: Self-recursive and mutually recursive BaseModels."""

from pydantic import BaseModel, Field


class TaxonNode(BaseModel):
    rank: str
    name: str
    children: list["TaxonNode"] = Field(default_factory=list)


class PathwayStep(BaseModel):
    step_id: str
    reaction: "ReactionNode | None" = None


class ReactionNode(BaseModel):
    ec_number: str
    next_step: PathwayStep | None = None


class RecursiveCladeRecord(BaseModel):
    root_taxon: TaxonNode
    entry_step: PathwayStep


MODEL = RecursiveCladeRecord
