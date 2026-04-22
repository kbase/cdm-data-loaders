"""End-to-end tests for PDB Phase 2 — live rsync download from wwPDB Beta.

These tests perform real rsync transfers from ``rsync-beta.rcsb.org``.
They are marked ``integration`` and ``slow_test``; auto-skipped when MinIO
is unreachable (which also implies no outbound network access in the test
environment).

A single, tiny well-known PDB entry (crambin, ``pdb_00001crn``) is used to
minimise transfer time.  Crambin is one of the smallest structures in the
PDB (~46 residues) and has been present since the early archive.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from cdm_data_loaders.pdb.entry import ALL_FILE_TYPES, build_entry_path
from cdm_data_loaders.pdb.manifest import (
    build_current_records,
    compute_diff,
    write_transfer_manifest,
)
from cdm_data_loaders.pipelines.pdb_rsync import download_batch, download_entry

if TYPE_CHECKING:
    pass

# A small, stable PDB entry guaranteed to exist in the wwPDB Beta archive.
# Crambin (1CRN in classic PDB; pdb_00001crn in extended-ID format) is one
# of the smallest protein structures (~46 residues, minimal file sizes).
STABLE_PDB_ID = "pdb_00001crn"


def _write_single_entry_manifest(tmp_path: Path, pdb_id: str = STABLE_PDB_ID) -> Path:
    """Write a transfer manifest containing exactly one PDB ID."""
    manifest_path = tmp_path / "transfer_manifest.txt"
    manifest_path.write_text(pdb_id + "\n")
    return manifest_path


# ── Tests ───────────────────────────────────────────────────────────────


@pytest.mark.integration
@pytest.mark.slow_test
class TestPdbDownloadSingleEntry:
    """Download a single PDB entry via rsync and verify local output."""

    def test_pdb_download_single_entry(self, tmp_path: Path) -> None:
        """Download one entry and verify directory structure and sidecar files."""
        manifest_path = _write_single_entry_manifest(tmp_path)
        output_dir = tmp_path / "output"
        output_dir.mkdir()

        report = download_batch(
            manifest_path=str(manifest_path),
            output_dir=str(output_dir),
            workers=1,
            file_types=["structures"],  # coordinates only — fastest possible download
            limit=1,
        )

        assert report["succeeded"] >= 1
        assert report["failed"] == 0

        # Verify the entry directory was created at the expected path
        entry_rel = build_entry_path(STABLE_PDB_ID)
        entry_dir = output_dir / entry_rel
        assert entry_dir.exists(), f"Expected entry dir at {entry_dir}"

        # Verify coordinate files landed under structures/
        structures_dir = entry_dir / "structures"
        assert structures_dir.exists(), "Expected structures/ subdir"
        structure_files = [p for p in structures_dir.rglob("*") if p.is_file() and not p.name.endswith(".crc64nvme")]
        assert len(structure_files) > 0, "Expected at least one coordinate file in structures/"

        # Verify .crc64nvme sidecar files were written alongside data files
        sidecar_files = list(entry_dir.rglob("*.crc64nvme"))
        assert len(sidecar_files) > 0, "Expected .crc64nvme sidecar files"
        assert len(sidecar_files) == len(structure_files), (
            f"Expected one sidecar per data file: {len(structure_files)} data, {len(sidecar_files)} sidecars"
        )

        # Verify sidecar content is a non-empty checksum string
        for sidecar in sidecar_files:
            content = sidecar.read_text().strip()
            assert content, f"Empty sidecar: {sidecar}"

        # Verify download report JSON
        report_file = output_dir / "download_report.json"
        assert report_file.exists()
        saved_report = json.loads(report_file.read_text())
        assert saved_report["succeeded"] >= 1


@pytest.mark.integration
@pytest.mark.slow_test
class TestPdbDownloadAllFileTypes:
    """Download all four file-type categories for a small entry."""

    def test_pdb_download_all_file_types(self, tmp_path: Path) -> None:
        """All requested file-type subdirs are populated after download."""
        manifest_path = _write_single_entry_manifest(tmp_path)
        output_dir = tmp_path / "output"
        output_dir.mkdir()

        report = download_batch(
            manifest_path=str(manifest_path),
            output_dir=str(output_dir),
            workers=1,
            file_types=list(ALL_FILE_TYPES),
            limit=1,
        )

        assert report["succeeded"] >= 1
        assert report["failed"] == 0

        entry_dir = output_dir / build_entry_path(STABLE_PDB_ID)
        assert entry_dir.exists()

        # structures/ (coordinates) must always be present for a released entry
        structures_dir = entry_dir / "structures"
        assert structures_dir.exists(), "structures/ subdir missing"
        structure_files = [p for p in structures_dir.rglob("*") if p.is_file() and not p.name.endswith(".crc64nvme")]
        assert len(structure_files) > 0, "No coordinate files downloaded"

        # All downloaded data files should have matching .crc64nvme sidecars
        all_data = [p for p in entry_dir.rglob("*") if p.is_file() and not p.name.endswith(".crc64nvme")]
        all_sidecars = [p for p in entry_dir.rglob("*.crc64nvme")]
        assert len(all_sidecars) == len(all_data), (
            f"Sidecar count mismatch: {len(all_data)} data files, {len(all_sidecars)} sidecars"
        )


@pytest.mark.integration
@pytest.mark.slow_test
class TestPdbDownloadResume:
    """Re-running download on the same manifest succeeds without errors."""

    def test_pdb_download_resume(self, tmp_path: Path) -> None:
        """Second download of the same entry succeeds and all files are present."""
        manifest_path = _write_single_entry_manifest(tmp_path)
        output_dir = tmp_path / "output"
        output_dir.mkdir()

        report1 = download_batch(
            manifest_path=str(manifest_path),
            output_dir=str(output_dir),
            workers=1,
            file_types=["structures"],
            limit=1,
        )
        assert report1["succeeded"] >= 1

        files_after_first = {p for p in output_dir.rglob("*") if p.is_file()}

        report2 = download_batch(
            manifest_path=str(manifest_path),
            output_dir=str(output_dir),
            workers=1,
            file_types=["structures"],
            limit=1,
        )

        assert report2["succeeded"] >= 1
        assert report2["failed"] == 0

        files_after_second = {p for p in output_dir.rglob("*") if p.is_file()}
        assert files_after_first.issubset(files_after_second)


@pytest.mark.integration
@pytest.mark.slow_test
class TestPdbDownloadSingleEntryDirect:
    """Exercise the lower-level ``download_entry`` function directly."""

    def test_pdb_download_entry_direct(self, tmp_path: Path) -> None:
        """``download_entry`` downloads coordinates and writes sidecars."""
        stats = download_entry(
            pdb_id=STABLE_PDB_ID,
            output_dir=str(tmp_path),
            file_types=["structures"],
        )

        assert stats["pdb_id"] == STABLE_PDB_ID
        assert stats["files_downloaded"] > 0
        assert stats["sidecars_written"] > 0
        assert stats["files_downloaded"] == stats["sidecars_written"]

        entry_dir = tmp_path / build_entry_path(STABLE_PDB_ID)
        data_files = [p for p in entry_dir.rglob("*") if p.is_file() and not p.name.endswith(".crc64nvme")]
        assert len(data_files) > 0
