"""End-to-end tests for Phase 3 — promote and archive in MinIO.

Pre-stages fake assembly files in MinIO and exercises ``promote_from_s3``
with various combinations of manifests, archive operations, dry-run mode,
manifest trimming, and incomplete staging.

Marked ``integration`` and ``slow_test``; auto-skipped when MinIO is
unreachable.  Each test method gets its own bucket.
"""

from __future__ import annotations

import hashlib
from typing import TYPE_CHECKING

import pytest

from cdm_data_loaders.ncbi_ftp.assembly import build_accession_path
from cdm_data_loaders.ncbi_ftp.promote import DEFAULT_LAKEHOUSE_KEY_PREFIX, promote_from_s3

from .conftest import get_object_metadata, list_all_keys, seed_lakehouse

if TYPE_CHECKING:
    from pathlib import Path

# Fake assembly details used across tests
ACCESSION_A = "GCF_900000001.1"
ASSEMBLY_DIR_A = "GCF_900000001.1_FakeAssemblyA"
ACCESSION_B = "GCF_900000002.1"
ASSEMBLY_DIR_B = "GCF_900000002.1_FakeAssemblyB"
ACCESSION_C = "GCF_900000003.1"
ASSEMBLY_DIR_C = "GCF_900000003.1_FakeAssemblyC"

STAGING_PREFIX = "staging/run1/"
PATH_PREFIX = DEFAULT_LAKEHOUSE_KEY_PREFIX

# Fake file contents for staging
FAKE_GENOMIC = b">seq1\nATCGATCG\n"
FAKE_PROTEIN = b">prot1\nMKKL\n"


def _md5(data: bytes) -> str:
    return hashlib.md5(data).hexdigest()  # noqa: S324


def _stage_assembly(
    s3: object,
    bucket: str,
    assembly_dir: str,
) -> None:
    """Stage a fake assembly with data files and .md5 sidecars under the staging prefix."""
    rel = build_accession_path(assembly_dir)
    base = f"{STAGING_PREFIX}{rel}"

    files = {
        f"{assembly_dir}_genomic.fna.gz": FAKE_GENOMIC,
        f"{assembly_dir}_protein.faa.gz": FAKE_PROTEIN,
    }

    for fname, content in files.items():
        key = f"{base}{fname}"
        s3.put_object(Bucket=bucket, Key=key, Body=content)
        # Write .md5 sidecar
        md5_key = f"{key}.md5"
        s3.put_object(Bucket=bucket, Key=md5_key, Body=_md5(content).encode())


def _write_manifest(tmp_path: Path, accessions: list[str], name: str) -> Path:
    """Write a manifest file (one accession per line)."""
    path = tmp_path / name
    path.write_text("\n".join(accessions) + "\n")
    return path


# ── Tests ───────────────────────────────────────────────────────────────


@pytest.mark.integration
@pytest.mark.slow_test
class TestPromoteFromStaging:
    """Promote staged files to final Lakehouse paths."""

    def test_promote_from_staging(self, minio_s3_client: object, test_bucket: str) -> None:
        """Staged files appear at the final Lakehouse path with MD5 metadata."""
        s3 = minio_s3_client
        _stage_assembly(s3, test_bucket, ASSEMBLY_DIR_A)

        report = promote_from_s3(
            staging_key_prefix=STAGING_PREFIX,
            bucket=test_bucket,
            lakehouse_key_prefix=PATH_PREFIX,
        )

        assert report["promoted"] >= 2  # noqa: PLR2004  # genomic + protein
        assert report["failed"] == 0
        assert report["dry_run"] is False

        # Verify files at final path
        final_keys = list_all_keys(s3, test_bucket, PATH_PREFIX + "raw_data/")
        assert len(final_keys) >= 2  # noqa: PLR2004

        # Verify MD5 metadata is set
        for key in final_keys:
            meta = get_object_metadata(s3, test_bucket, key)
            assert "md5" in meta, f"Missing md5 metadata on {key}"


@pytest.mark.integration
@pytest.mark.slow_test
class TestPromoteIdempotent:
    """Promoting the same staging data twice should succeed without errors."""

    def test_promote_idempotent(self, minio_s3_client: object, test_bucket: str) -> None:
        """Second promote succeeds and produces the same final state."""
        s3 = minio_s3_client
        _stage_assembly(s3, test_bucket, ASSEMBLY_DIR_A)

        report1 = promote_from_s3(
            staging_key_prefix=STAGING_PREFIX,
            bucket=test_bucket,
            lakehouse_key_prefix=PATH_PREFIX,
        )
        keys_after_first = list_all_keys(s3, test_bucket, PATH_PREFIX + "raw_data/")

        report2 = promote_from_s3(
            staging_key_prefix=STAGING_PREFIX,
            bucket=test_bucket,
            lakehouse_key_prefix=PATH_PREFIX,
        )
        keys_after_second = list_all_keys(s3, test_bucket, PATH_PREFIX + "raw_data/")

        assert report1["failed"] == 0
        assert report2["failed"] == 0
        assert report2["promoted"] >= 1
        assert keys_after_first == keys_after_second


@pytest.mark.integration
@pytest.mark.slow_test
class TestPromoteArchiveUpdated:
    """Archive existing assemblies before overwriting with updated versions."""

    def test_archive_updated(self, minio_s3_client: object, test_bucket: str, tmp_path: Path) -> None:
        """Updated assemblies are archived before being overwritten."""
        s3 = minio_s3_client

        # Seed "old" version at the final Lakehouse path
        old_files = {
            f"{ASSEMBLY_DIR_A}_genomic.fna.gz": "old genomic content",
            f"{ASSEMBLY_DIR_A}_protein.faa.gz": "old protein content",
        }
        seed_lakehouse(s3, test_bucket, ACCESSION_A, old_files, PATH_PREFIX, ASSEMBLY_DIR_A)

        # Stage "new" version
        _stage_assembly(s3, test_bucket, ASSEMBLY_DIR_A)

        updated_manifest = _write_manifest(tmp_path, [ACCESSION_A], "updated_manifest.txt")

        report = promote_from_s3(
            staging_key_prefix=STAGING_PREFIX,
            bucket=test_bucket,
            updated_manifest_path=str(updated_manifest),
            ncbi_release="2024-01",
            lakehouse_key_prefix=PATH_PREFIX,
        )

        assert report["archived"] >= 2  # noqa: PLR2004
        assert report["promoted"] >= 2  # noqa: PLR2004
        assert report["failed"] == 0

        # Verify archive exists
        archive_keys = list_all_keys(s3, test_bucket, PATH_PREFIX + "archive/2024-01/")
        assert len(archive_keys) >= 2  # noqa: PLR2004

        # Verify archive metadata
        for key in archive_keys:
            meta = get_object_metadata(s3, test_bucket, key)
            assert meta.get("archive_reason") == "updated"
            assert meta.get("ncbi_last_release") == "2024-01"


@pytest.mark.integration
@pytest.mark.slow_test
class TestPromoteArchiveRemoved:
    """Archive and delete replaced/suppressed assemblies."""

    def test_archive_removed(self, minio_s3_client: object, test_bucket: str, tmp_path: Path) -> None:
        """Removed assemblies are archived and source objects are deleted."""
        s3 = minio_s3_client

        # Seed assemblies at final path
        files = {
            f"{ASSEMBLY_DIR_A}_genomic.fna.gz": "content to archive",
        }
        seed_lakehouse(s3, test_bucket, ACCESSION_A, files, PATH_PREFIX, ASSEMBLY_DIR_A)

        removed_manifest = _write_manifest(tmp_path, [ACCESSION_A], "removed_manifest.txt")

        # Stage something (even empty staging is fine — promote won't find data files for this accession)
        report = promote_from_s3(
            staging_key_prefix=STAGING_PREFIX,
            bucket=test_bucket,
            removed_manifest_path=str(removed_manifest),
            ncbi_release="2024-01",
            lakehouse_key_prefix=PATH_PREFIX,
        )

        assert report["archived"] >= 1
        assert report["failed"] == 0

        # Verify archive exists
        archive_keys = list_all_keys(s3, test_bucket, PATH_PREFIX + "archive/2024-01/")
        assert len(archive_keys) >= 1

        # Verify archive metadata
        for key in archive_keys:
            meta = get_object_metadata(s3, test_bucket, key)
            assert meta.get("archive_reason") == "replaced_or_suppressed"

        # Verify source objects are deleted
        rel = build_accession_path(ASSEMBLY_DIR_A)
        source_keys = list_all_keys(s3, test_bucket, PATH_PREFIX + rel)
        assert len(source_keys) == 0, f"Expected source objects deleted, found: {source_keys}"


@pytest.mark.integration
@pytest.mark.slow_test
class TestPromoteDryRun:
    """Dry-run mode should not create any objects."""

    def test_promote_dry_run(self, minio_s3_client: object, test_bucket: str) -> None:
        """Dry-run logs actions but creates no objects at the final path."""
        s3 = minio_s3_client
        _stage_assembly(s3, test_bucket, ASSEMBLY_DIR_A)

        report = promote_from_s3(
            staging_key_prefix=STAGING_PREFIX,
            bucket=test_bucket,
            lakehouse_key_prefix=PATH_PREFIX,
            dry_run=True,
        )

        assert report["dry_run"] is True
        assert report["promoted"] >= 1

        # No objects should exist at the final path
        final_keys = list_all_keys(s3, test_bucket, PATH_PREFIX + "raw_data/")
        assert len(final_keys) == 0, f"Dry-run should not create objects, found: {final_keys}"


@pytest.mark.integration
@pytest.mark.slow_test
class TestPromoteTrimsManifest:
    """Manifest trimming removes promoted accessions."""

    def test_trims_manifest(self, minio_s3_client: object, test_bucket: str, tmp_path: Path) -> None:
        """Transfer manifest in MinIO is trimmed to exclude promoted accessions."""
        s3 = minio_s3_client

        # Upload a transfer manifest with 3 entries to MinIO
        manifest_key = "ncbi/transfer_manifest.txt"
        manifest_lines = [
            "/genomes/all/GCF/900/000/001/GCF_900000001.1_FakeAssemblyA/\n",
            "/genomes/all/GCF/900/000/002/GCF_900000002.1_FakeAssemblyB/\n",
            "/genomes/all/GCF/900/000/003/GCF_900000003.1_FakeAssemblyC/\n",
        ]
        s3.put_object(Bucket=test_bucket, Key=manifest_key, Body="".join(manifest_lines).encode())

        # Stage only assemblies A and B (not C)
        _stage_assembly(s3, test_bucket, ASSEMBLY_DIR_A)
        _stage_assembly(s3, test_bucket, ASSEMBLY_DIR_B)

        report = promote_from_s3(
            staging_key_prefix=STAGING_PREFIX,
            bucket=test_bucket,
            manifest_s3_key=manifest_key,
            lakehouse_key_prefix=PATH_PREFIX,
        )

        assert report["failed"] == 0

        # Read back the manifest from MinIO
        resp = s3.get_object(Bucket=test_bucket, Key=manifest_key)
        remaining = resp["Body"].read().decode()
        remaining_lines = [line.strip() for line in remaining.strip().splitlines() if line.strip()]

        # Only C should remain (A and B were promoted)
        assert len(remaining_lines) == 1, f"Expected 1 remaining entry, got {len(remaining_lines)}: {remaining_lines}"
        assert "GCF_900000003" in remaining_lines[0]


@pytest.mark.integration
@pytest.mark.slow_test
class TestPromoteIncompleteStaging:
    """Incomplete staging (sidecar only, no data) should not promote anything."""

    def test_incomplete_staging(self, minio_s3_client: object, test_bucket: str) -> None:
        """Only .md5 sidecars staged → nothing promoted."""
        s3 = minio_s3_client

        # Stage only .md5 sidecars (no data files)
        rel = build_accession_path(ASSEMBLY_DIR_A)
        base = f"{STAGING_PREFIX}{rel}"
        fname = f"{ASSEMBLY_DIR_A}_genomic.fna.gz"
        md5_key = f"{base}{fname}.md5"
        s3.put_object(Bucket=test_bucket, Key=md5_key, Body=_md5(FAKE_GENOMIC).encode())

        report = promote_from_s3(
            staging_key_prefix=STAGING_PREFIX,
            bucket=test_bucket,
            lakehouse_key_prefix=PATH_PREFIX,
        )

        # .md5 files are sidecars and should not be promoted as data
        assert report["promoted"] == 0
        assert report["failed"] == 0

        # No objects at final path
        final_keys = list_all_keys(s3, test_bucket, PATH_PREFIX + "raw_data/")
        assert len(final_keys) == 0
