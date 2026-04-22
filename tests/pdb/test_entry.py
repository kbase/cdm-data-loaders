"""Tests for pdb.entry module — path helpers, ID parsing, constants."""

import pytest

from cdm_data_loaders.pdb.entry import (
    ALL_FILE_TYPES,
    FILE_TYPE_DIRS,
    build_entry_path,
    extract_pdb_id_from_s3_key,
    pdb_id_hash,
)

_EXPECTED_FILE_TYPE_COUNT = 4


# ── pdb_id_hash ──────────────────────────────────────────────────────────


class TestPdbIdHash:
    """Test 2-character directory hash derivation."""

    def test_basic(self) -> None:
        """Verify hash for a standard extended PDB ID."""
        assert pdb_id_hash("pdb_00001abc") == "ab"

    def test_all_hex(self) -> None:
        """Verify hash with numeric-only suffix."""
        assert pdb_id_hash("pdb_00000012") == "01"

    def test_uppercase_input_normalised(self) -> None:
        """Verify uppercase input is normalised to lowercase."""
        assert pdb_id_hash("PDB_00001ABC") == "ab"

    def test_another_id(self) -> None:
        """Verify hash for a second representative ID."""
        assert pdb_id_hash("pdb_0000abcd") == "bc"

    def test_invalid_too_short_raises(self) -> None:
        """Verify ValueError on ID that is too short."""
        with pytest.raises(ValueError, match="Invalid PDB ID"):
            pdb_id_hash("pdb_001")

    def test_invalid_non_alphanumeric_raises(self) -> None:
        """Verify ValueError when suffix contains non-alphanumeric characters."""
        with pytest.raises(ValueError, match="Invalid PDB ID"):
            pdb_id_hash("pdb_0000-xyz")

    def test_invalid_missing_prefix_raises(self) -> None:
        """Verify ValueError when the 'pdb_' prefix is absent."""
        with pytest.raises(ValueError, match="Invalid PDB ID"):
            pdb_id_hash("00001abc")


# ── build_entry_path ─────────────────────────────────────────────────────


class TestBuildEntryPath:
    """Test relative S3 output path construction."""

    def test_basic(self) -> None:
        """Verify standard path construction."""
        assert build_entry_path("pdb_00001abc") == "raw_data/ab/pdb_00001abc/"

    def test_uppercase_normalised(self) -> None:
        """Verify uppercase IDs are normalised in the output path."""
        assert build_entry_path("PDB_00001ABC") == "raw_data/ab/pdb_00001abc/"

    def test_hash_matches_expected(self) -> None:
        """Verify path uses the same hash as pdb_id_hash."""
        pdb_id = "pdb_00002def"
        path = build_entry_path(pdb_id)
        assert path.startswith(f"raw_data/{pdb_id_hash(pdb_id)}/")

    def test_trailing_slash(self) -> None:
        """Verify the path ends with a trailing slash."""
        assert build_entry_path("pdb_00001abc").endswith("/")

    def test_invalid_raises(self) -> None:
        """Verify ValueError on invalid ID."""
        with pytest.raises(ValueError, match="Invalid PDB ID"):
            build_entry_path("invalid_id")


# ── extract_pdb_id_from_s3_key ───────────────────────────────────────────


class TestExtractPdbIdFromS3Key:
    """Test PDB ID extraction from S3 object keys."""

    def test_basic(self) -> None:
        """Verify extraction from a typical S3 key."""
        key = "tenant-general-warehouse/kbase/datasets/pdb/raw_data/ab/pdb_00001abc/structures/file.cif.gz"
        assert extract_pdb_id_from_s3_key(key) == "pdb_00001abc"

    def test_returns_first_match(self) -> None:
        """Verify only the first PDB ID is returned."""
        key = "raw_data/ab/pdb_00001abc/pdb_00002def"
        result = extract_pdb_id_from_s3_key(key)
        assert result == "pdb_00001abc"

    def test_no_match_returns_none(self) -> None:
        """Verify None is returned when no PDB ID is present."""
        assert extract_pdb_id_from_s3_key("some/random/key/without_pdb_id.txt") is None

    def test_uppercase_in_key_normalised(self) -> None:
        """Verify upper-case PDB ID in key is normalised."""
        assert extract_pdb_id_from_s3_key("prefix/PDB_00001ABC/file.cif") == "pdb_00001abc"


# ── FILE_TYPE_DIRS / ALL_FILE_TYPES ──────────────────────────────────────


class TestFileTypeConstants:
    """Sanity checks for file type constants."""

    def test_file_type_dirs_count(self) -> None:
        """Verify expected number of file type directories."""
        assert len(FILE_TYPE_DIRS) == _EXPECTED_FILE_TYPE_COUNT

    def test_all_file_types_matches_keys(self) -> None:
        """Verify ALL_FILE_TYPES contains exactly the keys of FILE_TYPE_DIRS."""
        assert set(ALL_FILE_TYPES) == set(FILE_TYPE_DIRS.keys())

    def test_structures_present(self) -> None:
        """Verify 'structures' is a known file type."""
        assert "structures" in FILE_TYPE_DIRS

    def test_dir_names_non_empty(self) -> None:
        """Verify all directory names are non-empty strings."""
        for name in FILE_TYPE_DIRS.values():
            assert isinstance(name, str)
            assert name
