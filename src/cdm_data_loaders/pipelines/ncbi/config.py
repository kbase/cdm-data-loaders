"""Model for storing NCBI Assembly Summary release info."""

from pathlib import Path
from typing import Annotated, Final

from pydantic import BaseModel, ConfigDict, Field, HttpUrl

IntStr = Annotated[str, Field(pattern=r"^\d+$")]

DecimalStr = Annotated[str, Field(pattern=r"^\d+(\.\d+)?$")]

S3Path = Annotated[str, Field(pattern=r"^s3://")]

ASS_SUM_README: Final[str] = "README_assembly_summary.txt"
ASS_SUM_GENBANK: Final[str] = "assembly_summary_genbank.txt"
ASS_SUM_GENBANK_HIST: Final[str] = "assembly_summary_genbank_historical.txt"
ASS_SUM_REFSEQ: Final[str] = "assembly_summary_refseq.txt"
ASS_SUM_REFSEQ_HIST: Final[str] = "assembly_summary_refseq_historical.txt"

NCBI_FILES: Final[list[str]] = [
    ASS_SUM_README,
    ASS_SUM_GENBANK,
    ASS_SUM_GENBANK_HIST,
    ASS_SUM_REFSEQ,
    ASS_SUM_REFSEQ_HIST,
]


class Release(BaseModel):
    """Release information for the Genbank and RefSeq files."""

    model_config = ConfigDict(extra="forbid")

    genbank: DecimalStr
    refseq: IntStr


class FileEntryPartial(BaseModel):
    """Paths for a file, raw dir only."""

    model_config = ConfigDict(extra="forbid")

    url: str
    s3_raw_data_dir: str


class FileEntryFull(FileEntryPartial):
    """Paths for a file, saved to raw and derived dirs."""

    model_config = ConfigDict(extra="forbid")

    s3_derived_dir: str


class Files(BaseModel):
    """List of files in the NCBI assembly summary release."""

    model_config = ConfigDict(extra="forbid")

    assembly_summary_genbank_historical: FileEntryFull
    assembly_summary_genbank: FileEntryFull
    assembly_summary_refseq_historical: FileEntryFull
    assembly_summary_refseq: FileEntryFull
    README_assembly_summary: FileEntryPartial


class NcbiReleaseMetadata(BaseModel):
    """Metadata for an NCBI assembly summary release."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    date: str = Field(pattern=r"^(19|20)\d{2}-(0[1-9]|1[0-2])-[0-3][0-9]$")
    date_yyyy_mm: str = Field(pattern=r"^(19|20)\d{2}-(0[1-9]|1[0-2])$")
    release: Release
    files: Files
    local_raw_data_dir: Path
    local_validated_data_dir: Path
