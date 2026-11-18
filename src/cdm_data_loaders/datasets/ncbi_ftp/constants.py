"""Constants, source enums, and compiled regular expressions for NCBI FTP assembly paths and S3 prefixes."""

import re
from enum import StrEnum
from pathlib import PurePosixPath
from types import MappingProxyType
from typing import Final


class Source(StrEnum):
    """NCBI assembly database source."""

    REFSEQ = "refseq"
    GENBANK = "genbank"


DATABASE_ACC_PREFIX: Final[MappingProxyType[str, str]] = MappingProxyType(
    {
        Source.REFSEQ: "GCF_",
        Source.GENBANK: "GCA_",
    }
)

FTP_HOST: Final[str] = "ftp.ncbi.nlm.nih.gov"

# Matches a bare accession (GCF_000001215.4) or an assembly directory name (GCF_000001215.4_Release_6...)
ACCESSION_PARTS_REGEX: Final[re.Pattern[str]] = re.compile(
    r"^(?P<accession>(?P<db>GC[AF])_(?P<p1>\d{3})(?P<p2>\d{3})(?P<p3>\d{3})\.\d+)(?:_(?P<name>[^/]+))?$"
)

# Matches an assembly directory name with a required suffix after the versioned accession
ASSEMBLY_DIR_REGEX: Final[re.Pattern[str]] = re.compile(r"^(?P<accession>GC[AF]_\d{9}\.\d+)_(?P<name>[^/]+)$")

# Extracts the assembly directory, accession, and database (GCA or GCF) from an FTP or S3 path
ASSEMBLY_PATH_REGEX: Final[re.Pattern[str]] = re.compile(
    r"(?:^|/)(?P<assembly_dir>(?P<accession>(?P<db>GC[AF])_\d{9}\.\d+)_[^/]+)(?:/.*)?$"
)

DEFAULT_STAGING_BUCKET: Final[PurePosixPath] = PurePosixPath("cts")
DEFAULT_STAGING_KEY_PREFIX: Final[PurePosixPath] = PurePosixPath("staging")
DEFAULT_DESTINATION_BUCKET: Final[PurePosixPath] = PurePosixPath("cdm-lake")
DEFAULT_DESTINATION_PREFIX: Final[PurePosixPath] = PurePosixPath("tenant-general-warehouse/kbase/datasets/ncbi")
DEFAULT_TRANSFER_MANIFEST_FILE: Final[PurePosixPath] = PurePosixPath("transfer_manifest.txt")
