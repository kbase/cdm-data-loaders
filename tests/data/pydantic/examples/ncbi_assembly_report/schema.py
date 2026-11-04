"""Example 2: NCBI Datasets API Assembly Dataset Report model."""

# ruff: noqa: D101, INP001

import datetime as dt
from enum import StrEnum

from pydantic import AwareDatetime, BaseModel, Field


class AssemblyLevel(StrEnum):
    COMPLETE_GENOME = "Complete Genome"
    CHROMOSOME = "Chromosome"
    SCAFFOLD = "Scaffold"
    CONTIG = "Contig"


class AssemblyOrganism(BaseModel):
    """Organism taxonomy and infraspecific metadata."""

    tax_id: int
    organism_name: str
    strain: str | None = None


class AssemblyStatsSummary(BaseModel):
    total_sequence_length: int
    number_of_contigs: int
    contig_n50: int
    gc_percent: float


class AnnotationMetadata(BaseModel):
    name: str
    provider: str
    release_date: dt.date
    total_gene_count: int


class NcbiAssemblyDatasetReport(BaseModel):
    accession: str = Field(description="Assembly accession (GCF_ or GCA_)")
    paired_accession: str | None = None
    source_database: str
    assembly_level: AssemblyLevel
    release_date: dt.date
    updated_at: AwareDatetime
    organism: AssemblyOrganism
    assembly_stats: AssemblyStatsSummary
    annotation_info: AnnotationMetadata | None = None
    bioproject_accessions: list[str] = Field(default_factory=list)


MODEL = NcbiAssemblyDatasetReport
