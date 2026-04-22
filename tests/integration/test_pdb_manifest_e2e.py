"""End-to-end tests for PDB Phase 1 — manifest generation and diffing.

These tests exercise the holdings parsing, diff computation, store scanning,
and manifest writing logic against a MinIO-backed S3 store.  They do NOT hit
the live wwPDB holdings server (all holdings data is injected as fixtures).

Marked ``integration`` and ``slow_test``; auto-skipped when MinIO is
unreachable.  Each test method gets its own bucket.
"""

from __future__ import annotations

import gzip
import json
from typing import TYPE_CHECKING

import pytest

from cdm_data_loaders.pdb.entry import DEFAULT_LAKEHOUSE_KEY_PREFIX, build_entry_path
from cdm_data_loaders.pdb.manifest import (
    PDBDiffResult,
    build_current_records,
    compute_diff,
    download_holdings_snapshot,
    filter_by_hash_range,
    parse_removed_entries,
    save_holdings_snapshot,
    scan_store_to_previous_ids,
    upload_holdings_snapshot,
    write_diff_summary,
    write_removed_manifest,
    write_transfer_manifest,
    write_updated_manifest,
)
from cdm_data_loaders.pdb.entry import PDBRecord

from .conftest import list_all_keys, seed_pdb_entry

if TYPE_CHECKING:
    from pathlib import Path

PATH_PREFIX = DEFAULT_LAKEHOUSE_KEY_PREFIX

# Synthetic holdings data (mirrors what wwPDB holdings files provide)
HOLDINGS_CURRENT = {
    "pdb_00001abc": {"content_type": ["coordinates_pdbx", "validation_report"]},
    "pdb_00002def": {"content_type": ["coordinates_pdbx", "structure_factors"]},
    "pdb_00003456": {"content_type": ["coordinates_pdbx"]},
}

HOLDINGS_DATES = {
    "pdb_00001abc": "2024-01-10",
    "pdb_00002def": "2024-02-15",
    "pdb_00003456": "2024-03-01",
}

HOLDINGS_REMOVED: dict = {}


def _make_holdings_data(
    current: dict | None = None,
    dates: dict | None = None,
    removed: dict | None = None,
) -> dict:
    return {
        "current": current if current is not None else HOLDINGS_CURRENT,
        "dates": dates if dates is not None else HOLDINGS_DATES,
        "removed": removed if removed is not None else HOLDINGS_REMOVED,
    }


# ── Tests: manifest generation (no previous snapshot) ───────────────────


@pytest.mark.integration
@pytest.mark.slow_test
class TestPdbFreshSyncNoPrevious:
    """Phase 1 with no previous snapshot — everything is 'new'."""

    def test_pdb_all_entries_new(self, minio_s3_client: object, test_bucket: str, tmp_path: Path) -> None:
        """When there is no previous snapshot all entries are marked new."""
        records = build_current_records(_make_holdings_data())
        diff = compute_diff(records)

        assert sorted(diff.new) == sorted(records.keys())
        assert diff.updated == []
        assert diff.removed == []

        manifest_path = tmp_path / "transfer_manifest.txt"
        written = write_transfer_manifest(diff, manifest_path)
        assert sorted(written) == sorted(records.keys())
        assert manifest_path.exists()
        lines = [line.strip() for line in manifest_path.read_text().splitlines() if line.strip()]
        assert sorted(lines) == sorted(records.keys())

    def test_pdb_diff_summary_written(self, minio_s3_client: object, test_bucket: str, tmp_path: Path) -> None:
        """Diff summary JSON reflects the correct counts."""
        records = build_current_records(_make_holdings_data())
        diff = compute_diff(records)

        out = tmp_path / "diff_summary.json"
        summary = write_diff_summary(diff, out)

        assert summary["new"] == len(records)
        assert summary["updated"] == 0
        assert summary["removed"] == 0
        assert summary["total_to_transfer"] == len(records)

        loaded = json.loads(out.read_text())
        assert loaded == summary


# ── Tests: diff with previous snapshot ──────────────────────────────────


@pytest.mark.integration
@pytest.mark.slow_test
class TestPdbDiffWithPreviousSnapshot:
    """Phase 1 diff when a previous snapshot is available."""

    def test_pdb_updated_entry_detected(self, minio_s3_client: object, test_bucket: str, tmp_path: Path) -> None:
        """An entry with a newer last_modified date is flagged as updated."""
        previous = {
            "pdb_00001abc": PDBRecord("pdb_00001abc", last_modified="2023-12-01"),
            "pdb_00002def": PDBRecord("pdb_00002def", last_modified="2024-02-15"),
            "pdb_00003456": PDBRecord("pdb_00003456", last_modified="2024-03-01"),
        }
        current = build_current_records(_make_holdings_data())
        diff = compute_diff(current, previous=previous)

        # pdb_00001abc date went from 2023-12-01 to 2024-01-10 → updated
        assert "pdb_00001abc" in diff.updated
        # pdb_00002def and pdb_00003456 unchanged
        assert "pdb_00002def" not in diff.updated
        assert diff.new == []

    def test_pdb_removed_entry_from_removed_ids(
        self, minio_s3_client: object, test_bucket: str, tmp_path: Path
    ) -> None:
        """Entries in the removed-IDs set that were previously known are marked removed."""
        previous = {
            "pdb_00009zzz": PDBRecord("pdb_00009zzz", last_modified="2023-01-01"),
            **{k: PDBRecord(k, last_modified=HOLDINGS_DATES[k]) for k in HOLDINGS_CURRENT},
        }
        current = build_current_records(_make_holdings_data())
        removed_ids = {"pdb_00009zzz"}

        diff = compute_diff(current, previous=previous, removed_ids=removed_ids)

        assert "pdb_00009zzz" in diff.removed
        assert diff.new == []
        assert diff.updated == []

    def test_pdb_gone_entry_marked_removed(self, minio_s3_client: object, test_bucket: str) -> None:
        """An entry present in previous but absent from current is marked removed."""
        previous = {
            "pdb_00001abc": PDBRecord("pdb_00001abc", last_modified="2024-01-10"),
            "pdb_00099xyz": PDBRecord("pdb_00099xyz", last_modified="2024-01-01"),  # vanished
        }
        current = {"pdb_00001abc": PDBRecord("pdb_00001abc", last_modified="2024-01-10")}
        diff = compute_diff(current, previous=previous)
        assert "pdb_00099xyz" in diff.removed

    def test_pdb_no_changes_produces_empty_transfer(self, minio_s3_client: object, test_bucket: str) -> None:
        """Identical current and previous → empty transfer manifest."""
        current = build_current_records(_make_holdings_data())
        previous = {k: PDBRecord(k, last_modified=HOLDINGS_DATES[k]) for k in HOLDINGS_CURRENT}
        diff = compute_diff(current, previous=previous)

        assert diff.new == []
        assert diff.updated == []


# ── Tests: hash-range filtering ──────────────────────────────────────────


@pytest.mark.integration
@pytest.mark.slow_test
class TestPdbHashRangeFilter:
    """Verify hash-range filtering restricts the diff to the requested window."""

    def test_pdb_filter_restricts_entries(self, minio_s3_client: object, test_bucket: str) -> None:
        """Only entries whose 2-char hash is within the requested range appear."""
        records = build_current_records(_make_holdings_data())
        # pdb_00001abc → hash "ab"; pdb_00002def → "de"; pdb_00003456 → "45"
        filtered = filter_by_hash_range(records, hash_from="ab", hash_to="ab")

        assert "pdb_00001abc" in filtered
        assert "pdb_00002def" not in filtered
        assert "pdb_00003456" not in filtered

    def test_pdb_full_range_returns_all(self, minio_s3_client: object, test_bucket: str) -> None:
        """No bounds → all entries returned."""
        records = build_current_records(_make_holdings_data())
        filtered = filter_by_hash_range(records)
        assert filtered is records


# ── Tests: store scan ────────────────────────────────────────────────────


@pytest.mark.integration
@pytest.mark.slow_test
class TestPdbScanStore:
    """Scanning an existing S3 store discovers PDB IDs from object keys."""

    def test_pdb_scan_finds_seeded_entries(self, minio_s3_client: object, test_bucket: str) -> None:
        """IDs seeded in the Lakehouse are returned by the store scan."""
        s3 = minio_s3_client
        seed_pdb_entry(s3, test_bucket, "pdb_00001abc", {"structures/file.cif.gz": b"data"}, PATH_PREFIX)
        seed_pdb_entry(s3, test_bucket, "pdb_00002def", {"structures/file.cif.gz": b"data"}, PATH_PREFIX)

        ids = scan_store_to_previous_ids(test_bucket, PATH_PREFIX)

        assert "pdb_00001abc" in ids
        assert "pdb_00002def" in ids

    def test_pdb_scan_empty_store(self, minio_s3_client: object, test_bucket: str) -> None:
        """Empty store returns empty set."""
        ids = scan_store_to_previous_ids(test_bucket, PATH_PREFIX)
        assert ids == set()

    def test_pdb_scan_used_as_previous_ids_for_diff(self, minio_s3_client: object, test_bucket: str) -> None:
        """Store-scan result used as previous_ids marks known IDs as non-new."""
        s3 = minio_s3_client
        seed_pdb_entry(s3, test_bucket, "pdb_00001abc", {"structures/f.cif.gz": b"data"}, PATH_PREFIX)

        ids = scan_store_to_previous_ids(test_bucket, PATH_PREFIX)
        current = build_current_records(_make_holdings_data())
        diff = compute_diff(current, previous_ids=ids)

        # pdb_00001abc was already in the store → not new
        assert "pdb_00001abc" not in diff.new
        # others were not in the store → new
        assert "pdb_00002def" in diff.new


# ── Tests: holdings snapshot round-trip via MinIO ────────────────────────


@pytest.mark.integration
@pytest.mark.slow_test
class TestPdbHoldingsSnapshotS3:
    """Holdings snapshot upload/download round-trip via MinIO."""

    def test_pdb_upload_download_round_trip(self, minio_s3_client: object, test_bucket: str, tmp_path: Path) -> None:
        """Records survive an upload → download round-trip through MinIO."""
        s3 = minio_s3_client
        records = build_current_records(_make_holdings_data())

        snapshot_key = "snapshots/holdings_snapshot.json.gz"
        upload_holdings_snapshot(records, test_bucket, snapshot_key)

        loaded = download_holdings_snapshot(test_bucket, snapshot_key)

        assert set(loaded.keys()) == set(records.keys())
        for pdb_id, rec in records.items():
            assert loaded[pdb_id].last_modified == rec.last_modified
            assert loaded[pdb_id].file_types == rec.file_types

    def test_pdb_snapshot_object_exists_in_minio(self, minio_s3_client: object, test_bucket: str) -> None:
        """After upload the snapshot key is present in MinIO."""
        s3 = minio_s3_client
        records = {"pdb_00001abc": PDBRecord("pdb_00001abc", "2024-01-10", ["coordinates_pdbx"])}
        snapshot_key = "snapshots/test_presence.json.gz"
        upload_holdings_snapshot(records, test_bucket, snapshot_key)

        keys = list_all_keys(s3, test_bucket, "snapshots/")
        assert snapshot_key in keys


# ── Tests: manifest writing ──────────────────────────────────────────────


@pytest.mark.integration
@pytest.mark.slow_test
class TestPdbManifestFiles:
    """Verify all four manifest files are correctly written."""

    def test_pdb_all_manifest_files_written(self, minio_s3_client: object, test_bucket: str, tmp_path: Path) -> None:
        """Transfer, removed, updated manifests and diff summary are all produced."""
        records = build_current_records(_make_holdings_data())
        diff = compute_diff(records)  # all-new scenario

        transfer_path = tmp_path / "transfer_manifest.txt"
        removed_path = tmp_path / "removed_manifest.txt"
        updated_path = tmp_path / "updated_manifest.txt"
        summary_path = tmp_path / "diff_summary.json"

        written = write_transfer_manifest(diff, transfer_path)
        removed = write_removed_manifest(diff, removed_path)
        updated = write_updated_manifest(diff, updated_path)
        summary = write_diff_summary(diff, summary_path)

        assert sorted(written) == sorted(records.keys())
        assert removed == []
        assert updated == []

        assert summary_path.exists()
        data = json.loads(summary_path.read_text())
        assert data["new"] == len(records)

    def test_pdb_transfer_manifest_contains_no_duplicates(
        self, minio_s3_client: object, test_bucket: str, tmp_path: Path
    ) -> None:
        """Transfer manifest has no duplicate entries even when new and updated overlap."""
        # Construct a diff where same ID appears in both new and updated
        diff = PDBDiffResult(new=["pdb_00001abc"], updated=["pdb_00001abc"])
        path = tmp_path / "transfer_manifest.txt"
        written = write_transfer_manifest(diff, path)

        # write_transfer_manifest uses set → no duplicates
        assert len(written) == 1
        lines = [l.strip() for l in path.read_text().splitlines() if l.strip()]
        assert len(lines) == 1
