"""Tests for pdb.manifest module — holdings parsing, diff, scan, manifest writing."""

import gzip
import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from cdm_data_loaders.pdb.entry import PDBRecord
from cdm_data_loaders.pdb.manifest import (
    PDBDiffResult,
    build_current_records,
    compute_diff,
    filter_by_hash_range,
    load_holdings_snapshot,
    merge_holdings_dates,
    parse_current_holdings,
    parse_last_modified_dates,
    parse_removed_entries,
    save_holdings_snapshot,
    scan_store_to_previous_ids,
    write_diff_summary,
    write_removed_manifest,
    write_transfer_manifest,
    write_updated_manifest,
)

from .conftest import SAMPLE_CURRENT_HOLDINGS, SAMPLE_DATES, SAMPLE_REMOVED

_EXPECTED_ENTRY_COUNT = 4


# ── parse_current_holdings ───────────────────────────────────────────────


class TestParseCurrentHoldings:
    """Test parsing of current_file_holdings data."""

    def test_dict_format(self) -> None:
        """Verify dict-format holdings are parsed correctly."""
        records = parse_current_holdings(SAMPLE_CURRENT_HOLDINGS)
        assert len(records) == _EXPECTED_ENTRY_COUNT
        assert "pdb_00001abc" in records

    def test_file_types_populated(self) -> None:
        """Verify file_types are extracted from the content_type key."""
        records = parse_current_holdings(SAMPLE_CURRENT_HOLDINGS)
        assert "coordinates_pdbx" in records["pdb_00001abc"].file_types

    def test_list_format(self) -> None:
        """Verify list-format holdings are also parsed."""
        data = [
            {"entry_id": "pdb_00001abc", "content_type": ["coordinates_pdbx"]},
            {"entry_id": "pdb_00002def", "content_type": []},
        ]
        records = parse_current_holdings(data)
        assert "pdb_00001abc" in records
        assert "pdb_00002def" in records

    def test_ids_lowercased(self) -> None:
        """Verify PDB IDs are lower-cased."""
        records = parse_current_holdings({"PDB_00001ABC": {"content_type": []}})
        assert "pdb_00001abc" in records
        assert "PDB_00001ABC" not in records

    def test_empty_input(self) -> None:
        """Verify empty dict returns empty result."""
        assert parse_current_holdings({}) == {}

    def test_unexpected_type_returns_empty(self) -> None:
        """Verify unexpected top-level type returns empty dict."""
        assert parse_current_holdings("bad_data") == {}  # type: ignore[arg-type]


# ── parse_last_modified_dates ────────────────────────────────────────────


class TestParseLastModifiedDates:
    """Test parsing of last_modified dates."""

    def test_dict_format(self) -> None:
        """Verify dict-format dates are parsed."""
        dates = parse_last_modified_dates(SAMPLE_DATES)
        assert dates["pdb_00001abc"] == "2024-01-10"

    def test_ids_lowercased(self) -> None:
        """Verify keys are lower-cased."""
        dates = parse_last_modified_dates({"PDB_00001ABC": "2024-01-01"})
        assert "pdb_00001abc" in dates

    def test_list_format(self) -> None:
        """Verify list-format dates are also parsed."""
        data = [
            {"entry_id": "pdb_00001abc", "last_modified": "2024-01-10"},
        ]
        dates = parse_last_modified_dates(data)
        assert dates["pdb_00001abc"] == "2024-01-10"


# ── parse_removed_entries ────────────────────────────────────────────────


class TestParseRemovedEntries:
    """Test parsing of removed entries."""

    def test_dict_format(self) -> None:
        """Verify dict-format removed entries are parsed as a set."""
        removed = parse_removed_entries(SAMPLE_REMOVED)
        assert "pdb_00009zzz" in removed

    def test_list_format(self) -> None:
        """Verify list-of-dicts format is also handled."""
        data = [{"entry_id": "pdb_00001abc"}]
        assert "pdb_00001abc" in parse_removed_entries(data)

    def test_list_of_strings(self) -> None:
        """Verify list-of-strings format is handled."""
        assert "pdb_00001abc" in parse_removed_entries(["pdb_00001abc"])

    def test_ids_lowercased(self) -> None:
        """Verify IDs are lower-cased."""
        assert "pdb_00001abc" in parse_removed_entries({"PDB_00001ABC": {}})


# ── merge_holdings_dates ─────────────────────────────────────────────────


class TestMergeHoldingsDates:
    """Test merging of date information into PDB records."""

    def test_dates_merged(self) -> None:
        """Verify last_modified is populated from the dates dict."""
        records = parse_current_holdings(SAMPLE_CURRENT_HOLDINGS)
        merged = merge_holdings_dates(records, SAMPLE_DATES)
        assert merged["pdb_00001abc"].last_modified == "2024-01-10"

    def test_missing_date_stays_empty(self) -> None:
        """Verify entries missing from dates keep empty last_modified."""
        records = {"pdb_new_xx": PDBRecord(pdb_id="pdb_new_xx", last_modified="")}
        merged = merge_holdings_dates(records, {})
        assert merged["pdb_new_xx"].last_modified == ""

    def test_returns_same_dict(self) -> None:
        """Verify the same dict object is returned (in-place mutation)."""
        records = parse_current_holdings(SAMPLE_CURRENT_HOLDINGS)
        result = merge_holdings_dates(records, SAMPLE_DATES)
        assert result is records


# ── build_current_records ────────────────────────────────────────────────


class TestBuildCurrentRecords:
    """Test the convenience wrapper that combines holdings + dates."""

    def test_basic(self) -> None:
        """Verify records have last_modified populated."""
        holdings_data = {
            "current": SAMPLE_CURRENT_HOLDINGS,
            "dates": SAMPLE_DATES,
            "removed": SAMPLE_REMOVED,
        }
        records = build_current_records(holdings_data)
        assert records["pdb_00001abc"].last_modified == "2024-01-10"
        assert len(records) == _EXPECTED_ENTRY_COUNT


# ── filter_by_hash_range ─────────────────────────────────────────────────


class TestFilterByHashRange:
    """Test hash-range filtering of PDB records."""

    @pytest.fixture
    def sample_records(self) -> dict[str, PDBRecord]:
        """Return parsed records with dates merged."""
        records = parse_current_holdings(SAMPLE_CURRENT_HOLDINGS)
        return merge_holdings_dates(records, SAMPLE_DATES)

    def test_no_filter_returns_all(self, sample_records: dict[str, PDBRecord]) -> None:
        """Verify no bounds returns the full set."""
        assert filter_by_hash_range(sample_records) is sample_records

    def test_hash_from_only(self, sample_records: dict[str, PDBRecord]) -> None:
        """Verify lower-bound filtering removes entries below threshold."""
        # pdb_00001abc → hash "ab", pdb_00002def → "de", pdb_00003ghi → "gh"
        result = filter_by_hash_range(sample_records, hash_from="de")
        assert "pdb_00001abc" not in result
        assert "pdb_00002def" in result

    def test_hash_to_only(self, sample_records: dict[str, PDBRecord]) -> None:
        """Verify upper-bound filtering removes entries above threshold."""
        result = filter_by_hash_range(sample_records, hash_to="ab")
        assert "pdb_00001abc" in result
        assert "pdb_00002def" not in result

    def test_both_bounds(self, sample_records: dict[str, PDBRecord]) -> None:
        """Verify both bounds together work correctly."""
        result = filter_by_hash_range(sample_records, hash_from="ab", hash_to="de")
        assert "pdb_00001abc" in result
        assert "pdb_00002def" in result
        assert "pdb_00003ghi" not in result


# ── compute_diff ─────────────────────────────────────────────────────────


class TestComputeDiff:
    """Test diff computation between current and previous holdings."""

    @pytest.fixture
    def current(self) -> dict[str, PDBRecord]:
        """Return sample current records."""
        records = parse_current_holdings(SAMPLE_CURRENT_HOLDINGS)
        return merge_holdings_dates(records, SAMPLE_DATES)

    def test_all_new_when_no_previous(self, current: dict[str, PDBRecord]) -> None:
        """Verify all entries are new when previous is empty."""
        diff = compute_diff(current)
        assert sorted(diff.new) == sorted(current.keys())
        assert diff.updated == []
        assert diff.removed == []

    def test_updated_when_date_increased(self, current: dict[str, PDBRecord]) -> None:
        """Verify an entry is marked updated when last_modified increases."""
        previous = {
            "pdb_00001abc": PDBRecord("pdb_00001abc", last_modified="2023-12-01"),
        }
        diff = compute_diff({"pdb_00001abc": current["pdb_00001abc"]}, previous=previous)
        assert "pdb_00001abc" in diff.updated
        assert "pdb_00001abc" not in diff.new

    def test_not_updated_when_date_unchanged(self, current: dict[str, PDBRecord]) -> None:
        """Verify an entry is not marked updated when date is the same."""
        previous = {
            "pdb_00001abc": PDBRecord("pdb_00001abc", last_modified="2024-01-10"),
        }
        diff = compute_diff({"pdb_00001abc": current["pdb_00001abc"]}, previous=previous)
        assert "pdb_00001abc" not in diff.updated
        assert "pdb_00001abc" not in diff.new

    def test_removed_from_removed_ids(self, current: dict[str, PDBRecord]) -> None:
        """Verify entries in removed_ids that were previously known are marked removed."""
        previous = {"pdb_00009zzz": PDBRecord("pdb_00009zzz", last_modified="2023-01-01")}
        diff = compute_diff(current, previous=previous, removed_ids={"pdb_00009zzz"})
        assert "pdb_00009zzz" in diff.removed

    def test_removed_when_gone_from_current(self) -> None:
        """Verify an entry absent from current is marked removed if it was known."""
        previous = {"pdb_00001abc": PDBRecord("pdb_00001abc", last_modified="2024-01-10")}
        diff = compute_diff({}, previous=previous)
        assert "pdb_00001abc" in diff.removed

    def test_previous_ids_fallback(self, current: dict[str, PDBRecord]) -> None:
        """Verify previous_ids fallback marks known entries as non-new."""
        known = {"pdb_00001abc"}
        diff = compute_diff(current, previous_ids=known)
        assert "pdb_00001abc" not in diff.new
        # others are new
        assert "pdb_00002def" in diff.new


# ── scan_store_to_previous_ids ───────────────────────────────────────────


class TestScanStoreToPreviousIds:
    """Test S3-store scanning for existing PDB IDs."""

    def test_returns_set_of_ids(self, mock_s3_client: object) -> None:
        """Verify IDs are extracted from S3 object keys."""
        import cdm_data_loaders.pdb.manifest as manifest_mod  # noqa: PLC0415

        from tests.pdb.conftest import TEST_BUCKET  # noqa: PLC0415

        client = mock_s3_client
        prefix = "tenant-general-warehouse/kbase/datasets/pdb/"
        key = f"{prefix}raw_data/ab/pdb_00001abc/structures/pdb_00001abc.cif.gz"
        client.put_object(Bucket=TEST_BUCKET, Key=key, Body=b"data")

        with patch.object(manifest_mod, "get_s3_client", return_value=client):
            ids = scan_store_to_previous_ids(TEST_BUCKET, prefix)

        assert "pdb_00001abc" in ids

    def test_empty_store_returns_empty_set(self, mock_s3_client: object) -> None:
        """Verify empty S3 prefix returns empty set."""
        import cdm_data_loaders.pdb.manifest as manifest_mod  # noqa: PLC0415

        from tests.pdb.conftest import TEST_BUCKET  # noqa: PLC0415

        client = mock_s3_client
        with patch.object(manifest_mod, "get_s3_client", return_value=client):
            ids = scan_store_to_previous_ids(TEST_BUCKET, "missing/prefix/")

        assert ids == set()


# ── Manifest writing ─────────────────────────────────────────────────────


class TestWriteTransferManifest:
    """Test transfer manifest writing."""

    def test_writes_new_and_updated(self, tmp_path: Path) -> None:
        """Verify transfer manifest contains new + updated IDs."""
        diff = PDBDiffResult(new=["pdb_00002def"], updated=["pdb_00001abc"])
        out = tmp_path / "transfer_manifest.txt"
        result = write_transfer_manifest(diff, out)
        lines = out.read_text().splitlines()
        assert "pdb_00001abc" in lines
        assert "pdb_00002def" in lines
        assert sorted(result) == sorted(["pdb_00001abc", "pdb_00002def"])

    def test_empty_diff_writes_empty_file(self, tmp_path: Path) -> None:
        """Verify empty diff produces an empty manifest."""
        diff = PDBDiffResult()
        out = tmp_path / "transfer_manifest.txt"
        result = write_transfer_manifest(diff, out)
        assert result == []
        assert out.read_text() == ""


class TestWriteRemovedManifest:
    """Test removed manifest writing."""

    def test_writes_removed_ids(self, tmp_path: Path) -> None:
        """Verify removed manifest contains removed IDs."""
        diff = PDBDiffResult(removed=["pdb_00009zzz"])
        out = tmp_path / "removed_manifest.txt"
        result = write_removed_manifest(diff, out)
        assert "pdb_00009zzz" in out.read_text().splitlines()
        assert result == ["pdb_00009zzz"]


class TestWriteUpdatedManifest:
    """Test updated manifest writing."""

    def test_writes_updated_ids(self, tmp_path: Path) -> None:
        """Verify updated manifest contains updated IDs."""
        diff = PDBDiffResult(updated=["pdb_00001abc"])
        out = tmp_path / "updated_manifest.txt"
        result = write_updated_manifest(diff, out)
        assert result == ["pdb_00001abc"]


class TestWriteDiffSummary:
    """Test diff summary JSON writing."""

    def test_writes_valid_json(self, tmp_path: Path) -> None:
        """Verify diff summary is valid JSON."""
        diff = PDBDiffResult(new=["pdb_00001abc"], updated=["pdb_00002def"], removed=["pdb_00003ghi"])
        out = tmp_path / "diff_summary.json"
        summary = write_diff_summary(diff, out, hash_from="00", hash_to="ff")
        data = json.loads(out.read_text())
        assert data["new"] == 1
        assert data["updated"] == 1
        assert data["removed"] == 1
        assert data["total_to_transfer"] == 2  # noqa: PLR2004
        assert summary == data


# ── Holdings snapshot I/O ────────────────────────────────────────────────


class TestHoldingsSnapshot:
    """Test local holdings snapshot save/load round-trip."""

    def test_round_trip(self, tmp_path: Path) -> None:
        """Verify records survive a save/load round-trip."""
        records = {
            "pdb_00001abc": PDBRecord("pdb_00001abc", "2024-01-10", ["coordinates_pdbx"]),
            "pdb_00002def": PDBRecord("pdb_00002def", "2024-02-15", []),
        }
        snap_path = tmp_path / "snapshot.json.gz"
        save_holdings_snapshot(records, snap_path)
        loaded = load_holdings_snapshot(snap_path)

        assert set(loaded.keys()) == set(records.keys())
        assert loaded["pdb_00001abc"].last_modified == "2024-01-10"
        assert loaded["pdb_00001abc"].file_types == ["coordinates_pdbx"]
