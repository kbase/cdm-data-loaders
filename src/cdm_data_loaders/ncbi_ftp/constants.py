"""Constants, globals, and utility functions for NCBI FTP modules."""

import re

FTP_HOST = "ftp.ncbi.nlm.nih.gov"

# Pre-compiled regex patterns

# Extracts the database (GCF or GCA) and 3-digit parts from an accession ID
# (e.g., "GCF_000001215.4" -> ("GCF, "000", "001", "215"))
ACCESSION_PARTS_REGEX = re.compile(r"(GC[AF])_(\d{3})(\d{3})(\d{3})\.\d+.*")

# Extracts the full assembly directory, accession, and database (GCA or GCF) from an FTP path
# (e.g. "/GCF/000/001/215/GCF_000001215.4_Release_6_plus_ISO1_MT/"
#   → ("GCF_000001215.4_Release_6_plus_ISO1_MT", "GCF_000001215.4", "GCF"))
ASSEMBLY_PATH_REGEX = re.compile(r"(((GC[AF])_\d{9}\.\d+)_[^/]+)/?.*$")
