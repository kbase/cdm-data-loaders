"""End-to-end tests for Phase 1 — manifest generation and diffing.

These tests hit the real NCBI FTP server (with tight prefix filters) and
optionally use MinIO for checksum verification.  Marked ``integration``
and ``slow_test``; auto-skipped when MinIO is unreachable.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from cdm_data_loaders.ncbi_ftp.assembly import FILE_FILTERS, FTP_HOST, build_accession_path, parse_md5_checksums_file
from cdm_data_loaders.ncbi_ftp.manifest import (
    AssemblyRecord,
    compute_diff,
    download_assembly_summary,
    filter_by_prefix_range,
    parse_assembly_summary,
    scan_store_to_synthetic_summary,
    verify_transfer_candidates,
    write_diff_summary,
    write_removed_manifest,
    write_transfer_manifest,
    write_updated_manifest,
)
from cdm_data_loaders.ncbi_ftp.promote import DEFAULT_LAKEHOUSE_KEY_PREFIX
from cdm_data_loaders.utils.ftp_client import connect_ftp, ftp_retrieve_text

if TYPE_CHECKING:
    from pathlib import Path

# Use a high-numbered prefix range that typically has only a handful of
# assemblies, keeping FTP traffic minimal.
STABLE_PREFIX = "900"


# ── Helpers ─────────────────────────────────────────────────────────────


def _download_and_filter() -> tuple[dict[str, AssemblyRecord], dict[str, AssemblyRecord]]:
    """Download the current refseq summary and filter to the stable prefix range.

    Returns ``(full_parsed, filtered)``.
    """
    raw = download_assembly_summary(database="refseq")
    full = parse_assembly_summary(raw)
    filtered = filter_by_prefix_range(full, prefix_from=STABLE_PREFIX, prefix_to=STABLE_PREFIX)
    return full, filtered


# ── Tests ───────────────────────────────────────────────────────────────


@pytest.mark.integration
@pytest.mark.slow_test
class TestFreshSyncNoPrevious:
    """Phase 1 with no previous snapshot — everything is 'new'."""

    def test_fresh_sync_no_previous(self, tmp_path: Path) -> None:
        """All assemblies in range appear as new when there is no previous snapshot."""
        _full, filtered = _download_and_filter()
        assert len(filtered) > 0, f"Expected assemblies in prefix {STABLE_PREFIX}"

        diff = compute_diff(filtered, previous_assemblies=None)

        # With no previous, every *latest* assembly is new
        latest_count = sum(1 for r in filtered.values() if r.status == "latest")
        assert len(diff.new) == latest_count
        assert len(diff.updated) == 0
        assert len(diff.replaced) == 0
        assert len(diff.suppressed) == 0

        # Write manifests
        transfer_path = tmp_path / "transfer_manifest.txt"
        removed_path = tmp_path / "removed_manifest.txt"
        updated_path = tmp_path / "updated_manifest.txt"
        summary_path = tmp_path / "diff_summary.json"

        paths = write_transfer_manifest(diff, filtered, transfer_path)
        removed = write_removed_manifest(diff, removed_path)
        updated = write_updated_manifest(diff, updated_path)
        write_diff_summary(diff, summary_path, "refseq", STABLE_PREFIX, STABLE_PREFIX)

        assert len(paths) == latest_count
        assert len(removed) == 0
        assert len(updated) == 0
        assert transfer_path.exists()
        assert summary_path.exists()


@pytest.mark.integration
@pytest.mark.slow_test
class TestIncrementalDiffSyntheticPrevious:
    """Phase 1 incremental diff with a manufactured 'previous' snapshot."""

    def test_incremental_diff(self, tmp_path: Path) -> None:
        """Detects new, updated, replaced, and suppressed assemblies correctly."""
        _full, filtered = _download_and_filter()
        latest = {a: r for a, r in filtered.items() if r.status == "latest"}
        assert len(latest) >= 2, f"Need >=2 latest assemblies in prefix {STABLE_PREFIX}"  # noqa: PLR2004

        accs = sorted(latest.keys())

        # Build synthetic previous: copy current, then mutate
        previous: dict[str, AssemblyRecord] = {}
        for acc, rec in filtered.items():
            previous[acc] = AssemblyRecord(
                accession=rec.accession,
                status=rec.status,
                seq_rel_date=rec.seq_rel_date,
                ftp_path=rec.ftp_path,
                assembly_dir=rec.assembly_dir,
            )

        # Remove the first latest → should appear as "new" in diff
        new_acc = accs[0]
        del previous[new_acc]

        # Modify seq_rel_date of the second latest → should appear as "updated"
        updated_acc = accs[1]
        previous[updated_acc].seq_rel_date = "1999/01/01"

        # Add a fake accession to previous that is not in current → "suppressed"
        fake_suppressed = "GCF_900999999.1"
        previous[fake_suppressed] = AssemblyRecord(
            accession=fake_suppressed,
            status="latest",
            seq_rel_date="2020/01/01",
            ftp_path="https://ftp.ncbi.nlm.nih.gov/genomes/all/GCF/900/999/999/GCF_900999999.1_FakeAsm",
            assembly_dir="GCF_900999999.1_FakeAsm",
        )

        diff = compute_diff(filtered, previous_assemblies=previous)

        assert new_acc in diff.new
        assert updated_acc in diff.updated
        assert fake_suppressed in diff.suppressed

        # Write and verify manifests
        transfer_path = tmp_path / "transfer_manifest.txt"
        removed_path = tmp_path / "removed_manifest.txt"
        updated_path = tmp_path / "updated_manifest.txt"

        paths = write_transfer_manifest(diff, filtered, transfer_path)
        removed = write_removed_manifest(diff, removed_path)
        updated_list = write_updated_manifest(diff, updated_path)

        assert len(paths) >= 2  # noqa: PLR2004  # at least the new + updated
        assert fake_suppressed in removed
        assert updated_acc in updated_list


@pytest.mark.integration
@pytest.mark.slow_test
class TestVerifyTransferCandidatesPrunes:
    """verify_transfer_candidates should prune assemblies already in the store."""

    def test_prunes_existing_matching_md5(
        self,
        minio_s3_client: object,
        test_bucket: str,
    ) -> None:
        """Assemblies with matching MD5 metadata in MinIO are pruned from the transfer list."""
        _full, filtered = _download_and_filter()
        latest = {a: r for a, r in filtered.items() if r.status == "latest"}
        if not latest:
            pytest.skip(f"No latest assemblies in prefix {STABLE_PREFIX}")

        # Pick one assembly to pre-seed in MinIO with correct checksums
        acc = next(iter(sorted(latest)))
        rec = latest[acc]
        ftp_dir = rec.ftp_path.replace("https://ftp.ncbi.nlm.nih.gov", "")

        # Fetch the real md5checksums.txt from FTP
        ftp = connect_ftp(FTP_HOST)
        try:
            md5_text = ftp_retrieve_text(ftp, ftp_dir.rstrip("/") + "/md5checksums.txt")
        finally:
            ftp.quit()

        checksums = parse_md5_checksums_file(md5_text)

        # Seed MinIO with dummy files that have the right MD5 metadata
        rel = build_accession_path(rec.assembly_dir)
        s3 = minio_s3_client
        path_prefix = DEFAULT_LAKEHOUSE_KEY_PREFIX
        for fname, md5 in checksums.items():
            if any(fname.endswith(suffix) for suffix in FILE_FILTERS):
                key = f"{path_prefix}{rel}{fname}"
                s3.put_object(
                    Bucket=test_bucket,
                    Key=key,
                    Body=b"placeholder",
                    Metadata={"md5": md5},
                )

        # verify_transfer_candidates should prune the seeded assembly
        candidates = sorted(latest.keys())
        result = verify_transfer_candidates(
            candidates,
            filtered,
            bucket=test_bucket,
            key_prefix=path_prefix,
        )

        assert acc not in result, f"Expected {acc} to be pruned (MD5 matches)"
        # Other candidates without seeded data should remain
        remaining_candidates = [c for c in candidates if c != acc]
        for c in remaining_candidates:
            assert c in result, f"Expected {c} to remain (not seeded)"


@pytest.mark.integration
@pytest.mark.slow_test
class TestScanStoreToSyntheticSummary:
    """Test synthetic assembly summary generation from MinIO store."""

    def test_builds_summary_from_minio_store(
        self,
        minio_s3_client: object,
        test_bucket: str,
    ) -> None:
        """Verify synthetic summary captures assemblies from MinIO."""
        s3 = minio_s3_client
        path_prefix = DEFAULT_LAKEHOUSE_KEY_PREFIX

        # Seed MinIO with a couple of assemblies
        assemblies = {
            "GCF_000001215.4_v1": ["_genomic.fna.gz", "_protein.faa.gz"],
            "GCF_000005845.2_v2": ["_genomic.fna.gz"],
        }

        for assembly_dir, files in assemblies.items():
            for fname in files:
                key = f"{path_prefix}refseq/{assembly_dir}/{assembly_dir}{fname}"
                s3.put_object(
                    Bucket=test_bucket,
                    Key=key,
                    Body=b"placeholder",
                )

        # Scan the store
        result = scan_store_to_synthetic_summary(test_bucket, path_prefix)

        # Should have found both assemblies
        assert "GCF_000001215.4" in result
        assert "GCF_000005845.2" in result

        # Verify basic record structure
        rec1 = result["GCF_000001215.4"]
        assert rec1.accession == "GCF_000001215.4"
        assert rec1.status == "latest"
        assert rec1.assembly_dir == "GCF_000001215.4_v1"

    def test_synthetic_summary_diff_against_current(
        self,
        minio_s3_client: object,
        test_bucket: str,
    ) -> None:
        """Verify synthetic summary can be used as baseline for diffing."""
        s3 = minio_s3_client
        path_prefix = DEFAULT_LAKEHOUSE_KEY_PREFIX

        # Seed MinIO with one assembly
        key1 = f"{path_prefix}refseq/GCF_000001215.4_old/GCF_000001215.4_old_genomic.fna.gz"
        s3.put_object(Bucket=test_bucket, Key=key1, Body=b"data")

        # Build synthetic summary from store
        synthetic = scan_store_to_synthetic_summary(test_bucket, path_prefix)
        assert "GCF_000001215.4" in synthetic

        # Simulate current NCBI summary with one new and one existing
        current = {
            "GCF_000001215.4": AssemblyRecord(
                accession="GCF_000001215.4",
                status="latest",
                seq_rel_date=synthetic["GCF_000001215.4"].seq_rel_date,
                ftp_path="",
                assembly_dir="GCF_000001215.4_old",
            ),
            "GCF_000005845.2": AssemblyRecord(
                accession="GCF_000005845.2",
                status="latest",
                seq_rel_date="2024/04/20",
                ftp_path="",
                assembly_dir="GCF_000005845.2_new",
            ),
        }

        # Compute diff
        diff = compute_diff(current, previous_assemblies=synthetic)

        # Should find one new and zero updated
        assert "GCF_000005845.2" in diff.new
        assert "GCF_000001215.4" not in diff.new  # Already in store
        assert len(diff.updated) == 0  # Same date, same dir
