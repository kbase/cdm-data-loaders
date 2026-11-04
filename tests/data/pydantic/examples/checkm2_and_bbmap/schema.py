"""Example 3: CheckM2 quality metrics and BBMap stats.sh contigset statistics."""

# ruff: noqa: D101, D102, INP001

from decimal import Decimal
from pathlib import Path
from typing import Annotated

from pydantic import BaseModel, Field, computed_field


class CheckM2Metrics(BaseModel):
    """CheckM2 completeness and contamination quality metrics."""

    completeness: Annotated[Decimal, Field(max_digits=5, decimal_places=2, description="Estimated completeness (%)")]
    contamination: Annotated[Decimal, Field(max_digits=5, decimal_places=2, description="Estimated contamination (%)")]
    coding_density: float | None = None
    translation_table: int = 11


class BbmapContigStats(BaseModel):
    """BBMap stats.sh assembly summary metrics."""

    filename: Path
    scaffolds: int
    contigs: int
    scaf_bp: int
    contig_bp: int
    gc_avg: float
    gc_std: float
    scaf_n50: int
    scaf_l50: int
    ctg_n50: int
    ctg_l50: int
    scaf_max: int
    ctg_max: int


class GenomeQualitySummaryRecord(BaseModel):
    sample_id: str = Field(description="Sample / bin identifier")
    checkm2: CheckM2Metrics
    bbmap: BbmapContigStats
    extra_scores: dict[str, float] = Field(default_factory=dict)

    @computed_field(description="MIMAG quality tier (high, medium, or low)")
    @property
    def mimag_tier(self) -> str:
        if self.checkm2.completeness >= Decimal("90.00") and self.checkm2.contamination < Decimal("5.00"):
            return "high"
        if self.checkm2.completeness >= Decimal("50.00") and self.checkm2.contamination < Decimal("10.00"):
            return "medium"
        return "low"


MODEL = GenomeQualitySummaryRecord
