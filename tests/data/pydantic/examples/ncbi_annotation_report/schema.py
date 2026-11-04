"""Example 1: NCBI Datasets API Genome Annotation Report model."""

# ruff: noqa: D101, INP001

from enum import StrEnum

from pydantic import BaseModel, Field


class GeneOrientation(StrEnum):
    PLUS = "plus"
    MINUS = "minus"


class GenomicInterval(BaseModel):
    """Genomic interval coordinates on a sequence accession."""

    accession_version: str
    begin: int
    end: int
    orientation: GeneOrientation


class TranscriptSummary(BaseModel):
    accession_version: str
    name: str | None = None
    length: int
    cds_range: GenomicInterval | None = None


class NcbiAnnotationFeatureReport(BaseModel):
    gene_id: int = Field(description="NCBI Entrez Gene identifier")
    symbol: str = Field(description="Official gene symbol")
    description: str | None = Field(default=None, description="Gene product description")
    gene_type: str
    tax_id: int
    synonyms: list[str] = Field(default_factory=list)
    genomic_regions: list[GenomicInterval]
    transcripts: list[TranscriptSummary] = Field(default_factory=list)


MODEL = NcbiAnnotationFeatureReport
