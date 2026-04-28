"""Tests for pdb.entry module — path helpers, ID parsing, constants."""

import pytest

from cdm_data_loaders.pdb.entry import (
    ALL_FILE_TYPES,
    FILE_TYPE_DIRS,
    build_entry_path,
    extract_pdb_id_from_s3_key,
    pdb_id_hash,
)


# ── pdb_id_hash ──────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("pdb_id", "expected"),
    [
        pytest.param("pdb_00001abc", "ab", id="standard"),
        pytest.param("pdb_00000012", "01", id="numeric_suffix"),
        pytest.param("PDB_00001ABC", "ab", id="uppercase_normalised"),
        pytest.param("pdb_0000abcd", "bc", id="another_id"),
    ],
)
def test_pdb_id_hash(pdb_id: str, expected: str) -> None:
    """Verify 2-character hash is derived from the penultimate two chars of the ID."""
    assert pdb_id_hash(pdb_id) == expected


@pytest.mark.parametrize(
    "pdb_id",
    [
        pytest.param("pdb_001", id="too_short"),
        pytest.param("pdb_0000-xyz", id="non_alphanumeric"),
        pytest.param("00001abc", id="missing_prefix"),
    ],
)
def test_pdb_id_hash_invalid(pdb_id: str) -> None:
    """Verify ValueError is raised for malformed PDB IDs."""
    with pytest.raises(ValueError, match="Invalid PDB ID"):
        pdb_id_hash(pdb_id)


# ── build_entry_path ─────────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("pdb_id", "expected"),
    [
        pytest.param("pdb_00001abc", "raw_data/ab/pdb_00001abc/", id="standard"),
        pytest.param("PDB_00001ABC", "raw_data/ab/pdb_00001abc/", id="uppercase_normalised"),
    ],
)
def test_build_entry_path(pdb_id: str, expected: str) -> None:
    """Verify relative S3 output path is constructed correctly."""
    assert build_entry_path(pdb_id) == expected


def test_build_entry_path_hash_matches_pdb_id_hash() -> None:
    """Verify path uses the same hash as pdb_id_hash."""
    pdb_id = "pdb_00002def"
    assert build_entry_path(pdb_id).startswith(f"raw_data/{pdb_id_hash(pdb_id)}/")


def test_build_entry_path_invalid_raises() -> None:
    """Verify ValueError on invalid ID."""
    with pytest.raises(ValueError, match="Invalid PDB ID"):
        build_entry_path("invalid_id")


# ── extract_pdb_id_from_s3_key ───────────────────────────────────────────


@pytest.mark.parametrize(
    ("key", "expected"),
    [
        pytest.param(
            "tenant-general-warehouse/kbase/datasets/pdb/raw_data/ab/pdb_00001abc/structures/file.cif.gz",
            "pdb_00001abc",
            id="standard_key",
        ),
        pytest.param("raw_data/ab/pdb_00001abc/pdb_00002def", "pdb_00001abc", id="returns_first_match"),
        pytest.param("some/random/key/without_pdb_id.txt", None, id="no_match"),
        pytest.param("prefix/PDB_00001ABC/file.cif", "pdb_00001abc", id="uppercase_normalised"),
    ],
)
def test_extract_pdb_id_from_s3_key(key: str, expected: str | None) -> None:
    """Verify PDB ID extraction from S3 object keys."""
    assert extract_pdb_id_from_s3_key(key) == expected


# ── FILE_TYPE_DIRS / ALL_FILE_TYPES ──────────────────────────────────────


def test_all_file_types_matches_file_type_dirs() -> None:
    """Verify ALL_FILE_TYPES contains exactly the same keys as FILE_TYPE_DIRS."""
    assert set(ALL_FILE_TYPES) == set(FILE_TYPE_DIRS.keys())
