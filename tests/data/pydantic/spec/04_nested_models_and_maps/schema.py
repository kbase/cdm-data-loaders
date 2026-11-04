"""Spec 04: Nested BaseModel, TypedDict, stdlib dataclass, and dict[str, V] maps."""

# ruff: noqa: D101, INP001

from dataclasses import dataclass
from typing import TypedDict

from pydantic import BaseModel


class OrganismInfo(BaseModel):
    """Taxonomic organism metadata."""

    taxon_id: int
    scientific_name: str
    strain: str | None = None


class AssemblyStatsDict(TypedDict):
    contigs: int
    gc_percent: float


@dataclass
class SubmitterContact:
    """Submitting institution contact."""

    institution: str
    department: str | None = None


class NestedModelsAndMapsRecord(BaseModel):
    id: str
    organism: OrganismInfo
    stats: AssemblyStatsDict
    submitter: SubmitterContact | None = None
    feature_counts: dict[str, int]
    optional_metrics: dict[str, float | None]
    extra_organisms: dict[str, OrganismInfo]


MODEL = NestedModelsAndMapsRecord
