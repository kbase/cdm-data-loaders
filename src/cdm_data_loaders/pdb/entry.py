"""PDB archive domain logic.

Provides constants, path helpers, ID parsing, and the :class:`PDBRecord`
dataclass for PDB entries in the wwPDB Beta archive (extended ID format
``pdb_00001abc``).

The Beta archive uses 12-character extended IDs (``pdb_`` prefix followed by
8 lower-case hex digits).  Each entry lives under a 2-character hash directory
derived from the penultimate two characters of the ID suffix.

Example::

    pdb_id   = "pdb_00001abc"
    hash_dir = "ab"   # pdb_id[-3:-1]
    path     = "raw_data/ab/pdb_00001abc/"
"""

import re
from dataclasses import dataclass, field

# ── Rsync / holdings constants ──────────────────────────────────────────

RSYNC_HOST = "rsync-beta.rcsb.org"
RSYNC_PORT = 32382
RSYNC_MODULE = "pdb_data"
RSYNC_ENTRIES_PATH = "entries"

HOLDINGS_BASE_URL = "https://files-beta.rcsb.org/pub/wwpdb/pdb/holdings"

DEFAULT_LAKEHOUSE_KEY_PREFIX = "tenant-general-warehouse/kbase/datasets/pdb/"

# ── File-type subdirectories ────────────────────────────────────────────

# Mapping of logical file-type name to the subdirectory name used in the
# wwPDB Beta archive under each entry directory.
FILE_TYPE_DIRS: dict[str, str] = {
    "structures": "structures",
    "experimental_data": "experimental_data",
    "validation_reports": "validation_reports",
    "assemblies": "assemblies",
}

ALL_FILE_TYPES: tuple[str, ...] = tuple(FILE_TYPE_DIRS.keys())

# ── PDB ID pattern ──────────────────────────────────────────────────────

# Extended PDB ID: "pdb_" followed by exactly 8 lower-case alphanumeric characters.
# Classic PDB IDs are [0-9A-Z]{4}; in extended format they are zero-padded to 8
# chars and lowercased, giving [0-9a-z]{8} — NOT just hex [0-9a-f]{8}.
_PDB_ID_RE = re.compile(r"^pdb_[0-9a-z]{8}$", re.IGNORECASE)
_PDB_ID_SEARCH_RE = re.compile(r"pdb_[0-9a-z]{8}", re.IGNORECASE)


# ── Data structures ─────────────────────────────────────────────────────


@dataclass
class PDBRecord:
    """A single PDB entry as represented in the holdings inventory.

    :param pdb_id: extended PDB ID, e.g. ``"pdb_00001abc"``
    :param last_modified: ISO-8601 date of last modification, e.g. ``"2024-01-15"``
    :param file_types: list of content types present in the archive for this entry
    """

    pdb_id: str
    last_modified: str
    file_types: list[str] = field(default_factory=list)


# ── Path helpers ────────────────────────────────────────────────────────


def pdb_id_hash(pdb_id: str) -> str:
    """Return the 2-character directory hash for a PDB extended ID.

    The hash is formed from the penultimate two characters of the full ID
    (characters at positions ``[-3:-1]``).  For example::

        pdb_id_hash("pdb_00001abc")  # → "ab"
        pdb_id_hash("pdb_0000abcd")  # → "bc"

    :param pdb_id: extended PDB ID, e.g. ``"pdb_00001abc"``
    :return: 2-character lowercase hash, e.g. ``"ab"``
    :raises ValueError: if *pdb_id* does not match the expected format
    """
    normed = pdb_id.lower()
    if not _PDB_ID_RE.match(normed):
        msg = f"Invalid PDB ID {pdb_id!r}: expected 'pdb_' followed by 8 alphanumeric characters."
        raise ValueError(msg)
    return normed[-3:-1]


def build_entry_path(pdb_id: str) -> str:
    """Build the relative S3 output path for a PDB entry.

    Produces ``raw_data/<hash>/<pdb_id>/``, for example::

        build_entry_path("pdb_00001abc")  # → "raw_data/ab/pdb_00001abc/"

    :param pdb_id: extended PDB ID, e.g. ``"pdb_00001abc"``
    :return: relative path string, e.g. ``"raw_data/ab/pdb_00001abc/"``
    :raises ValueError: if *pdb_id* does not match the expected format
    """
    normed = pdb_id.lower()
    return f"raw_data/{pdb_id_hash(normed)}/{normed}/"


def extract_pdb_id_from_s3_key(key: str) -> str | None:
    """Extract a PDB extended ID from an S3 object key.

    Returns the *first* match found (lower-cased), or ``None`` if the key
    contains no extended PDB ID.

    :param key: S3 object key
    :return: lower-cased PDB ID, e.g. ``"pdb_00001abc"``, or ``None``
    """
    m = _PDB_ID_SEARCH_RE.search(key)
    return m.group(0).lower() if m else None
