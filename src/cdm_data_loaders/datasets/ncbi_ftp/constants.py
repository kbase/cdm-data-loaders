"""Constants and compiled regular expressions for NCBI FTP assembly paths and S3 prefixes."""

import re
from pathlib import PurePosixPath
from typing import Final

FTP_HOST: Final[str] = "ftp.ncbi.nlm.nih.gov"

# Extracts the database (GCF or GCA) and 3-digit parts from an accession ID
ACCESSION_PARTS_REGEX: Final[re.Pattern[str]] = re.compile(r"(GC[AF])_(\d{3})(\d{3})(\d{3})\.\d+.*")

# Extracts the full assembly directory, accession, and database (GCA or GCF) from an FTP path
ASSEMBLY_PATH_REGEX: Final[re.Pattern[str]] = re.compile(r"(((GC[AF])_\d{9}\.\d+)_[^/]+)/?.*$")

DEFAULT_STAGING_BUCKET: Final[PurePosixPath] = PurePosixPath("cts")
DEFAULT_STAGING_KEY_PREFIX: Final[PurePosixPath] = PurePosixPath("staging")
DEFAULT_DESTINATION_BUCKET: Final[PurePosixPath] = PurePosixPath("cdm-lake")
DEFAULT_DESTINATION_PREFIX: Final[PurePosixPath] = PurePosixPath("tenant-general-warehouse/kbase/datasets/ncbi")
DEFAULT_TRANSFER_MANIFEST_FILE: Final[PurePosixPath] = PurePosixPath("transfer_manifest.txt")
