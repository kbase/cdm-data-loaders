"""End-to-end tests for Phase 2 — FTP download of assemblies.

These tests download real (small) assemblies from the NCBI FTP server.
Marked ``integration`` and ``slow_test``; auto-skipped when MinIO is
unreachable.
"""

import json

import pytest

from pathlib import Path

from cdm_data_loaders.ncbi_ftp.manifest import (
    compute_diff,
    download_assembly_summary,
    filter_by_prefix_range,
    parse_assembly_summary,
    write_transfer_manifest,
)
from cdm_data_loaders.pipelines.ncbi_ftp_download import download_batch

# Use same stable prefix as manifest tests
STABLE_PREFIX = "900"


def _manifest_for_one_assembly(tmp_path: Path) -> tuple[Path, str]:
    """Create a transfer manifest containing exactly one FTP path.

    Returns ``(manifest_path, accession)`` for the first latest assembly
    in the stable prefix range.
    """
    raw = download_assembly_summary(database="refseq")
    full = parse_assembly_summary(raw)
    filtered = filter_by_prefix_range(full, prefix_from=STABLE_PREFIX, prefix_to=STABLE_PREFIX)
    diff = compute_diff(filtered, previous_assemblies=None)

    assert len(diff.new) > 0, f"No new assemblies in prefix {STABLE_PREFIX}"

    manifest_path = tmp_path / "transfer_manifest.txt"
    write_transfer_manifest(diff, filtered, manifest_path)

    return manifest_path, diff.new[0]


@pytest.mark.integration
@pytest.mark.slow_test
@pytest.mark.external_request
class TestDownloadSmallBatch:
    """Download a single assembly from NCBI FTP and verify local output."""

    def test_download_small_batch(self, tmp_path: Path) -> None:
        """Download one assembly and verify directory structure and report."""
        manifest_path, _acc = _manifest_for_one_assembly(tmp_path)

        output_dir = tmp_path / "output"
        output_dir.mkdir()

        report = download_batch(
            manifest_path=str(manifest_path),
            output_dir=str(output_dir),
            threads=1,
            limit=1,
        )

        assert report["succeeded"] >= 1
        assert report["failed"] == 0

        # Verify directory structure exists
        raw_data = output_dir / "raw_data"
        assert raw_data.exists(), "Expected raw_data/ directory in output"

        # Should have at least one assembly directory with files
        assembly_dirs = list(raw_data.rglob("GCF_*"))
        assert len(assembly_dirs) > 0, "Expected at least one assembly directory"

        # Check for .md5 sidecar files
        md5_files = list(raw_data.rglob("*.md5"))
        assert len(md5_files) > 0, "Expected .md5 sidecar files"

        # Check download report
        report_file = output_dir / "download_report.json"
        assert report_file.exists()
        with report_file.open() as f:
            saved_report = json.load(f)
        assert saved_report["succeeded"] >= 1


@pytest.mark.integration
@pytest.mark.slow_test
@pytest.mark.external_request
class TestDownloadResumeIncomplete:
    """Verify download handles re-runs when some files are already present."""

    def test_download_resume(self, tmp_path: Path) -> None:
        """Re-running download on the same manifest succeeds without errors."""
        manifest_path, _acc = _manifest_for_one_assembly(tmp_path)

        output_dir = tmp_path / "output"
        output_dir.mkdir()

        # First download
        report1 = download_batch(
            manifest_path=str(manifest_path),
            output_dir=str(output_dir),
            threads=1,
            limit=1,
        )
        assert report1["succeeded"] >= 1

        files_after_first = set(output_dir.rglob("*"))

        # Second download — same manifest
        report2 = download_batch(
            manifest_path=str(manifest_path),
            output_dir=str(output_dir),
            threads=1,
            limit=1,
        )

        # Should succeed without errors (files overwritten or skipped)
        assert report2["succeeded"] >= 1
        assert report2["failed"] == 0

        # All original files should still exist
        files_after_second = set(output_dir.rglob("*"))
        assert files_after_first.issubset(files_after_second)
