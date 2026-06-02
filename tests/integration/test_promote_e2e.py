"""End-to-end tests for Phase 3 — promote and archive in MinIO.

Pre-stages fake assembly files in MinIO and exercises ``promote_from_s3``
with various combinations of manifests, archive operations, dry-run mode,
manifest trimming, and incomplete staging.

Marked ``integration`` and ``slow_test``; auto-skipped when MinIO is
unreachable.  Each test method gets its own bucket.
"""

import hashlib
import json
from pathlib import Path

import pytest

from cdm_data_loaders.ncbi_ftp.assembly import build_accession_path
from cdm_data_loaders.ncbi_ftp.metadata import (
    build_archive_descriptor_key,
    build_descriptor_key,
    create_descriptor,
)
from cdm_data_loaders.ncbi_ftp.promote import DEFAULT_LAKEHOUSE_KEY_PREFIX, promote_from_s3

from .conftest import get_object_metadata, list_all_keys, seed_lakehouse, staging_test_bucket  # noqa: F401

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


# Tests


@pytest.mark.integration
@pytest.mark.slow_test
class TestPromoteFromStaging:
    """Promote staged files to final Lakehouse paths."""

    def test_promote_from_staging(self, minio_s3_client: object, test_bucket: str, staging_test_bucket: str) -> None:
        """Staged files appear at the final Lakehouse path with MD5 metadata."""
        s3 = minio_s3_client
        _stage_assembly(s3, staging_test_bucket, ASSEMBLY_DIR_A)

        report = promote_from_s3(
            staging_key_prefix=STAGING_PREFIX,
            staging_bucket=staging_test_bucket,
            lakehouse_bucket=test_bucket,
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

    def test_promote_idempotent(self, minio_s3_client: object, test_bucket: str, staging_test_bucket: str) -> None:
        """Second promote on empty staging succeeds and leaves the lakehouse unchanged.

        After the first promote, staged files are deleted.  A second run therefore
        finds nothing to promote — which is correct and expected.  The lakehouse
        contents must be identical after both runs.
        """
        s3 = minio_s3_client
        _stage_assembly(s3, staging_test_bucket, ASSEMBLY_DIR_A)

        report1 = promote_from_s3(
            staging_key_prefix=STAGING_PREFIX,
            staging_bucket=staging_test_bucket,
            lakehouse_bucket=test_bucket,
            lakehouse_key_prefix=PATH_PREFIX,
        )
        keys_after_first = list_all_keys(s3, test_bucket, PATH_PREFIX + "raw_data/")

        report2 = promote_from_s3(
            staging_key_prefix=STAGING_PREFIX,
            staging_bucket=staging_test_bucket,
            lakehouse_bucket=test_bucket,
            lakehouse_key_prefix=PATH_PREFIX,
        )
        keys_after_second = list_all_keys(s3, test_bucket, PATH_PREFIX + "raw_data/")

        assert report1["failed"] == 0
        assert report1["promoted"] >= 1
        assert report2["failed"] == 0
        assert report2["promoted"] == 0  # staging was cleared by the first run
        assert keys_after_first == keys_after_second


@pytest.mark.integration
@pytest.mark.slow_test
class TestPromoteArchiveUpdated:
    """Archive existing assemblies before overwriting with updated versions."""

    def test_archive_updated(
        self, minio_s3_client: object, test_bucket: str, staging_test_bucket: str, tmp_path: Path
    ) -> None:
        """Updated assemblies are archived before being overwritten."""
        s3 = minio_s3_client

        # Seed "old" version at the final Lakehouse path
        old_files = {
            f"{ASSEMBLY_DIR_A}_genomic.fna.gz": "old genomic content",
            f"{ASSEMBLY_DIR_A}_protein.faa.gz": "old protein content",
        }
        seed_lakehouse(s3, test_bucket, ACCESSION_A, old_files, PATH_PREFIX, ASSEMBLY_DIR_A)

        # Stage "new" version
        _stage_assembly(s3, staging_test_bucket, ASSEMBLY_DIR_A)

        updated_manifest = _write_manifest(tmp_path, [ACCESSION_A], "updated_manifest.txt")

        report = promote_from_s3(
            staging_key_prefix=STAGING_PREFIX,
            staging_bucket=staging_test_bucket,
            lakehouse_bucket=test_bucket,
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
            assert "/updated/" in key
            assert "/2024-01/" in key


@pytest.mark.integration
@pytest.mark.slow_test
class TestPromoteArchiveRemoved:
    """Archive and delete replaced/suppressed assemblies."""

    def test_archive_removed(
        self, minio_s3_client: object, test_bucket: str, staging_test_bucket: str, tmp_path: Path
    ) -> None:
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
            staging_bucket=staging_test_bucket,
            lakehouse_bucket=test_bucket,
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
            assert "/replaced_or_suppressed/" in key

        # Verify source objects are deleted
        rel = build_accession_path(ASSEMBLY_DIR_A)
        source_keys = list_all_keys(s3, test_bucket, PATH_PREFIX + rel)
        assert len(source_keys) == 0, f"Expected source objects deleted, found: {source_keys}"


@pytest.mark.integration
@pytest.mark.slow_test
class TestPromoteDryRun:
    """Dry-run mode should not create any objects."""

    def test_promote_dry_run(self, minio_s3_client: object, test_bucket: str, staging_test_bucket: str) -> None:
        """Dry-run logs actions but creates no objects at the final path."""
        s3 = minio_s3_client
        _stage_assembly(s3, staging_test_bucket, ASSEMBLY_DIR_A)

        report = promote_from_s3(
            staging_key_prefix=STAGING_PREFIX,
            staging_bucket=staging_test_bucket,
            lakehouse_bucket=test_bucket,
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

    def test_trims_manifest(
        self, minio_s3_client: object, test_bucket: str, staging_test_bucket: str, tmp_path: Path
    ) -> None:
        """Transfer manifest in MinIO is trimmed to exclude promoted accessions."""
        s3 = minio_s3_client

        # Upload a transfer manifest with 3 entries to MinIO (manifest lives in staging)
        manifest_key = "ncbi/transfer_manifest.txt"
        manifest_lines = [
            "/genomes/all/GCF/900/000/001/GCF_900000001.1_FakeAssemblyA/\n",
            "/genomes/all/GCF/900/000/002/GCF_900000002.1_FakeAssemblyB/\n",
            "/genomes/all/GCF/900/000/003/GCF_900000003.1_FakeAssemblyC/\n",
        ]
        s3.put_object(Bucket=staging_test_bucket, Key=manifest_key, Body="".join(manifest_lines).encode())

        # Stage only assemblies A and B (not C)
        _stage_assembly(s3, staging_test_bucket, ASSEMBLY_DIR_A)
        _stage_assembly(s3, staging_test_bucket, ASSEMBLY_DIR_B)

        report = promote_from_s3(
            staging_key_prefix=STAGING_PREFIX,
            staging_bucket=staging_test_bucket,
            lakehouse_bucket=test_bucket,
            manifest_s3_key=manifest_key,
            lakehouse_key_prefix=PATH_PREFIX,
        )

        assert report["failed"] == 0

        # Read back the manifest from MinIO (it lives in staging)
        resp = s3.get_object(Bucket=staging_test_bucket, Key=manifest_key)
        remaining = resp["Body"].read().decode()
        remaining_lines = [line.strip() for line in remaining.strip().splitlines() if line.strip()]

        # Only C should remain (A and B were promoted)
        assert len(remaining_lines) == 1, f"Expected 1 remaining entry, got {len(remaining_lines)}: {remaining_lines}"
        assert "GCF_900000003" in remaining_lines[0]


@pytest.mark.integration
@pytest.mark.slow_test
class TestPromoteIncompleteStaging:
    """Incomplete staging (sidecar only, no data) should not promote anything."""

    def test_incomplete_staging(self, minio_s3_client: object, test_bucket: str, staging_test_bucket: str) -> None:
        """Only .md5 sidecars staged → nothing promoted."""
        s3 = minio_s3_client

        # Stage only .md5 sidecars (no data files)
        rel = build_accession_path(ASSEMBLY_DIR_A)
        base = f"{STAGING_PREFIX}{rel}"
        fname = f"{ASSEMBLY_DIR_A}_genomic.fna.gz"
        md5_key = f"{base}{fname}.md5"
        s3.put_object(Bucket=staging_test_bucket, Key=md5_key, Body=_md5(FAKE_GENOMIC).encode())

        report = promote_from_s3(
            staging_key_prefix=STAGING_PREFIX,
            staging_bucket=staging_test_bucket,
            lakehouse_bucket=test_bucket,
            lakehouse_key_prefix=PATH_PREFIX,
        )

        # .md5 files are sidecars and should not be promoted as data
        assert report["promoted"] == 0
        assert report["failed"] == 0

        # No objects at final path
        final_keys = list_all_keys(s3, test_bucket, PATH_PREFIX + "raw_data/")
        assert len(final_keys) == 0


@pytest.mark.integration
@pytest.mark.slow_test
class TestPromoteCreatesDescriptor:
    """Promote step writes a frictionless descriptor for each promoted assembly."""

    def test_descriptor_created(self, minio_s3_client: object, test_bucket: str, staging_test_bucket: str) -> None:
        """After promote, a JSON descriptor exists under ``metadata/``."""
        s3 = minio_s3_client
        _stage_assembly(s3, staging_test_bucket, ASSEMBLY_DIR_A)

        promote_from_s3(
            staging_key_prefix=STAGING_PREFIX,
            staging_bucket=staging_test_bucket,
            lakehouse_bucket=test_bucket,
            lakehouse_key_prefix=PATH_PREFIX,
        )

        descriptor_key = build_descriptor_key(ASSEMBLY_DIR_A, PATH_PREFIX)
        obj = s3.get_object(Bucket=test_bucket, Key=descriptor_key)
        body = json.loads(obj["Body"].read())

        assert body["identifier"] == f"NCBI:{ACCESSION_A}"
        assert body["resource_type"] == "dataset"

    def test_descriptor_resources_include_promoted_files(
        self, minio_s3_client: object, test_bucket: str, staging_test_bucket: str
    ) -> None:
        """Descriptor's ``resources`` list references the final Lakehouse key."""
        s3 = minio_s3_client
        _stage_assembly(s3, staging_test_bucket, ASSEMBLY_DIR_A)

        promote_from_s3(
            staging_key_prefix=STAGING_PREFIX,
            staging_bucket=staging_test_bucket,
            lakehouse_bucket=test_bucket,
            lakehouse_key_prefix=PATH_PREFIX,
        )

        descriptor_key = build_descriptor_key(ASSEMBLY_DIR_A, PATH_PREFIX)
        obj = s3.get_object(Bucket=test_bucket, Key=descriptor_key)
        body = json.loads(obj["Body"].read())

        resource_paths = [r["path"] for r in body["resources"]]
        assert any(PATH_PREFIX + "raw_data/" in p for p in resource_paths)

    def test_descriptor_resources_have_md5(
        self, minio_s3_client: object, test_bucket: str, staging_test_bucket: str
    ) -> None:
        """Resources with .md5 sidecars include the hash value."""
        s3 = minio_s3_client
        _stage_assembly(s3, staging_test_bucket, ASSEMBLY_DIR_A)

        promote_from_s3(
            staging_key_prefix=STAGING_PREFIX,
            staging_bucket=staging_test_bucket,
            lakehouse_bucket=test_bucket,
            lakehouse_key_prefix=PATH_PREFIX,
        )

        descriptor_key = build_descriptor_key(ASSEMBLY_DIR_A, PATH_PREFIX)
        obj = s3.get_object(Bucket=test_bucket, Key=descriptor_key)
        body = json.loads(obj["Body"].read())

        # Both staged files have .md5 sidecars
        for resource in body["resources"]:
            assert "hash" in resource, f"Expected hash in resource: {resource}"

    def test_multiple_assemblies_get_separate_descriptors(
        self, minio_s3_client: object, test_bucket: str, staging_test_bucket: str
    ) -> None:
        """Each assembly gets its own descriptor file."""
        s3 = minio_s3_client
        _stage_assembly(s3, staging_test_bucket, ASSEMBLY_DIR_A)
        _stage_assembly(s3, staging_test_bucket, ASSEMBLY_DIR_B)

        promote_from_s3(
            staging_key_prefix=STAGING_PREFIX,
            staging_bucket=staging_test_bucket,
            lakehouse_bucket=test_bucket,
            lakehouse_key_prefix=PATH_PREFIX,
        )

        for assembly_dir, accession in [(ASSEMBLY_DIR_A, ACCESSION_A), (ASSEMBLY_DIR_B, ACCESSION_B)]:
            key = build_descriptor_key(assembly_dir, PATH_PREFIX)
            obj = s3.get_object(Bucket=test_bucket, Key=key)
            body = json.loads(obj["Body"].read())
            assert body["identifier"] == f"NCBI:{accession}"


@pytest.mark.integration
@pytest.mark.slow_test
class TestPromoteArchiveUpdatedIncludesDescriptor:
    """Archiving updated assemblies also archives the descriptor."""

    def test_archive_copies_descriptor(
        self, minio_s3_client: object, test_bucket: str, staging_test_bucket: str, tmp_path: Path
    ) -> None:
        """After archiving an updated assembly, the descriptor appears under archive/."""
        s3 = minio_s3_client

        # Seed old version at Lakehouse path *including* a live descriptor
        old_files = {f"{ASSEMBLY_DIR_A}_genomic.fna.gz": "old content"}
        seed_lakehouse(s3, test_bucket, ACCESSION_A, old_files, PATH_PREFIX, ASSEMBLY_DIR_A)
        # Pre-upload a descriptor so archive_descriptor can find it
        descriptor = create_descriptor(ASSEMBLY_DIR_A, ACCESSION_A, [])
        # Upload directly to MinIO (not via promote)
        descriptor_key = build_descriptor_key(ASSEMBLY_DIR_A, PATH_PREFIX)
        s3.put_object(Bucket=test_bucket, Key=descriptor_key, Body=json.dumps(descriptor).encode())

        _stage_assembly(s3, staging_test_bucket, ASSEMBLY_DIR_A)
        updated_manifest = _write_manifest(tmp_path, [ACCESSION_A], "updated_manifest.txt")

        promote_from_s3(
            staging_key_prefix=STAGING_PREFIX,
            staging_bucket=staging_test_bucket,
            lakehouse_bucket=test_bucket,
            updated_manifest_path=str(updated_manifest),
            ncbi_release="2024-01",
            lakehouse_key_prefix=PATH_PREFIX,
        )

        archive_key = build_archive_descriptor_key(ASSEMBLY_DIR_A, "2024-01", PATH_PREFIX, "updated")
        # Confirm the archive descriptor object exists
        resp = s3.head_object(Bucket=test_bucket, Key=archive_key)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200  # noqa: PLR2004


@pytest.mark.integration
@pytest.mark.slow_test
class TestPromoteArchiveRemovedIncludesDescriptor:
    """Archiving removed assemblies also archives the descriptor."""

    def test_archive_removed_copies_descriptor(
        self, minio_s3_client: object, test_bucket: str, staging_test_bucket: str, tmp_path: Path
    ) -> None:
        """After archiving a removed assembly, the descriptor is under archive/."""
        s3 = minio_s3_client

        # Seed the assembly at final Lakehouse path
        files = {f"{ASSEMBLY_DIR_A}_genomic.fna.gz": "content"}
        seed_lakehouse(s3, test_bucket, ACCESSION_A, files, PATH_PREFIX, ASSEMBLY_DIR_A)
        # Pre-upload a descriptor
        descriptor = create_descriptor(ASSEMBLY_DIR_A, ACCESSION_A, [])
        descriptor_key = build_descriptor_key(ASSEMBLY_DIR_A, PATH_PREFIX)
        s3.put_object(Bucket=test_bucket, Key=descriptor_key, Body=json.dumps(descriptor).encode())

        removed_manifest = _write_manifest(tmp_path, [ACCESSION_A], "removed_manifest.txt")

        promote_from_s3(
            staging_key_prefix=STAGING_PREFIX,
            staging_bucket=staging_test_bucket,
            lakehouse_bucket=test_bucket,
            removed_manifest_path=str(removed_manifest),
            ncbi_release="2024-01",
            lakehouse_key_prefix=PATH_PREFIX,
        )

        archive_key = build_archive_descriptor_key(ASSEMBLY_DIR_A, "2024-01", PATH_PREFIX, "replaced_or_suppressed")
        resp = s3.head_object(Bucket=test_bucket, Key=archive_key)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200  # noqa: PLR2004


@pytest.mark.integration
@pytest.mark.slow_test
class TestPromoteDryRunNoDescriptor:
    """Dry-run must not write any descriptor files."""

    def test_dry_run_no_descriptor(self, minio_s3_client: object, test_bucket: str, staging_test_bucket: str) -> None:
        """Dry-run does not upload a descriptor to the metadata/ prefix."""
        s3 = minio_s3_client
        _stage_assembly(s3, staging_test_bucket, ASSEMBLY_DIR_A)

        promote_from_s3(
            staging_key_prefix=STAGING_PREFIX,
            staging_bucket=staging_test_bucket,
            lakehouse_bucket=test_bucket,
            lakehouse_key_prefix=PATH_PREFIX,
            dry_run=True,
        )

        metadata_keys = list_all_keys(s3, test_bucket, PATH_PREFIX + "metadata/")
        assert len(metadata_keys) == 0, f"Dry-run should not create descriptor files, found: {metadata_keys}"


# Parallel archiving tests


@pytest.mark.integration
@pytest.mark.slow_test
class TestArchiveMultiFileConcurrent:
    """Verify parallel copy archives all files correctly with correct content."""

    def test_all_files_archived_with_correct_content(
        self, minio_s3_client: object, test_bucket: str, staging_test_bucket: str, tmp_path: Path
    ) -> None:
        """Every file is archived with byte-identical content when copied concurrently."""
        from cdm_data_loaders.ncbi_ftp.promote import _archive_assemblies

        s3 = minio_s3_client

        # Seed many files for assembly A at final Lakehouse path
        many_files = {
            f"{ASSEMBLY_DIR_A}_genomic.fna.gz": b"GENOMIC_CONTENT",
            f"{ASSEMBLY_DIR_A}_protein.faa.gz": b"PROTEIN_CONTENT",
            f"{ASSEMBLY_DIR_A}_rna.fna.gz": b"RNA_CONTENT",
            f"{ASSEMBLY_DIR_A}_assembly_report.txt": b"ASSEMBLY_REPORT",
            f"{ASSEMBLY_DIR_A}_assembly_stats.txt": b"ASSEMBLY_STATS",
            f"{ASSEMBLY_DIR_A}_cds_from_genomic.fna.gz": b"CDS_CONTENT",
        }
        seed_lakehouse(s3, test_bucket, ACCESSION_A, many_files, PATH_PREFIX, ASSEMBLY_DIR_A)

        updated_manifest = _write_manifest(tmp_path, [ACCESSION_A], "updated_manifest.txt")

        archived = _archive_assemblies(
            str(updated_manifest),
            lakehouse_bucket=test_bucket,
            ncbi_release="2024-01",
            archive_reason="updated",
            lakehouse_key_prefix=PATH_PREFIX,
            delete_source=False,
        )

        assert archived == len(many_files)

        # Verify every archived file has correct content
        rel = build_accession_path(ASSEMBLY_DIR_A)
        for fname, expected_body in many_files.items():
            archive_key = f"{PATH_PREFIX}archive/2024-01/updated/{rel}{fname}"
            obj = s3.get_object(Bucket=test_bucket, Key=archive_key)
            actual_body = obj["Body"].read()
            assert actual_body == expected_body, f"Content mismatch for {fname}"

    def test_archive_key_paths_are_correct(
        self, minio_s3_client: object, test_bucket: str, staging_test_bucket: str, tmp_path: Path
    ) -> None:
        """Archived keys follow the exact ``archive/{release}/{reason}/{rel_path}`` pattern."""
        from cdm_data_loaders.ncbi_ftp.promote import _archive_assemblies

        s3 = minio_s3_client
        files = {f"{ASSEMBLY_DIR_B}_genomic.fna.gz": b"content"}
        seed_lakehouse(s3, test_bucket, ACCESSION_B, files, PATH_PREFIX, ASSEMBLY_DIR_B)

        removed_manifest = _write_manifest(tmp_path, [ACCESSION_B], "removed_manifest.txt")
        _archive_assemblies(
            str(removed_manifest),
            lakehouse_bucket=test_bucket,
            ncbi_release="2024-02",
            archive_reason="replaced_or_suppressed",
            lakehouse_key_prefix=PATH_PREFIX,
            delete_source=False,
        )

        rel = build_accession_path(ASSEMBLY_DIR_B)
        expected_key = f"{PATH_PREFIX}archive/2024-02/replaced_or_suppressed/{rel}{ASSEMBLY_DIR_B}_genomic.fna.gz"
        resp = s3.head_object(Bucket=test_bucket, Key=expected_key)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200  # noqa: PLR2004


@pytest.mark.integration
@pytest.mark.slow_test
class TestArchiveDeleteSourceBatch:
    """Verify batch delete removes all source objects after concurrent copy."""

    def test_all_sources_deleted_after_archive(
        self, minio_s3_client: object, test_bucket: str, staging_test_bucket: str, tmp_path: Path
    ) -> None:
        """After archive with delete_source=True, no source objects remain."""
        from cdm_data_loaders.ncbi_ftp.promote import _archive_assemblies

        s3 = minio_s3_client
        many_files = {
            f"{ASSEMBLY_DIR_A}_genomic.fna.gz": b"genomic",
            f"{ASSEMBLY_DIR_A}_protein.faa.gz": b"protein",
            f"{ASSEMBLY_DIR_A}_rna.fna.gz": b"rna",
            f"{ASSEMBLY_DIR_A}_assembly_report.txt": b"report",
        }
        source_keys = seed_lakehouse(s3, test_bucket, ACCESSION_A, many_files, PATH_PREFIX, ASSEMBLY_DIR_A)

        removed_manifest = _write_manifest(tmp_path, [ACCESSION_A], "removed_manifest.txt")
        archived = _archive_assemblies(
            str(removed_manifest),
            lakehouse_bucket=test_bucket,
            ncbi_release="2024-01",
            archive_reason="replaced_or_suppressed",
            lakehouse_key_prefix=PATH_PREFIX,
            delete_source=True,
        )

        assert archived == len(many_files)
        # Source keys must all be gone
        for key in source_keys:
            remaining = list_all_keys(s3, test_bucket, key)
            assert len(remaining) == 0, f"Source not deleted: {key}"

    def test_archive_present_source_gone(
        self, minio_s3_client: object, test_bucket: str, staging_test_bucket: str, tmp_path: Path
    ) -> None:
        """Archive destinations exist AND sources are gone after replaced_or_suppressed archive."""
        from cdm_data_loaders.ncbi_ftp.promote import _archive_assemblies

        s3 = minio_s3_client
        files = {
            f"{ASSEMBLY_DIR_A}_genomic.fna.gz": b"genomic",
            f"{ASSEMBLY_DIR_A}_protein.faa.gz": b"protein",
        }
        seed_lakehouse(s3, test_bucket, ACCESSION_A, files, PATH_PREFIX, ASSEMBLY_DIR_A)

        removed_manifest = _write_manifest(tmp_path, [ACCESSION_A], "removed_manifest.txt")
        _archive_assemblies(
            str(removed_manifest),
            lakehouse_bucket=test_bucket,
            ncbi_release="2024-01",
            archive_reason="replaced_or_suppressed",
            lakehouse_key_prefix=PATH_PREFIX,
            delete_source=True,
        )

        rel = build_accession_path(ASSEMBLY_DIR_A)
        # Archive keys present
        archive_keys = list_all_keys(s3, test_bucket, f"{PATH_PREFIX}archive/2024-01/replaced_or_suppressed/")
        assert len(archive_keys) == len(files), f"Expected {len(files)} archive keys, got: {archive_keys}"
        # Source keys absent
        source_keys = list_all_keys(s3, test_bucket, f"{PATH_PREFIX}{rel}")
        assert len(source_keys) == 0, f"Source objects remain: {source_keys}"


@pytest.mark.integration
@pytest.mark.slow_test
class TestPartialArchiveResume:
    """Corner case: a prior archive run was interrupted mid-way.

    Re-running must complete cleanly without errors, leave all archive keys
    present with current content, and (when delete_source=True) remove all
    source keys regardless of which files were processed in the prior run.
    """

    def test_partial_updated_archive_resumes(
        self, minio_s3_client: object, test_bucket: str, staging_test_bucket: str, tmp_path: Path
    ) -> None:
        """Re-running after a partial updated archive overwrites stale copies and archives missing files.

        Scenario: 3 files, file_a was archived in a prior run (stale content),
        file_b and file_c were not. Re-run should overwrite file_a with current
        content and archive file_b, file_c.
        """
        from cdm_data_loaders.ncbi_ftp.promote import _archive_assemblies

        s3 = minio_s3_client
        rel = build_accession_path(ASSEMBLY_DIR_A)

        file_a = f"{ASSEMBLY_DIR_A}_genomic.fna.gz"
        file_b = f"{ASSEMBLY_DIR_A}_protein.faa.gz"
        file_c = f"{ASSEMBLY_DIR_A}_rna.fna.gz"

        current_content = {file_a: b"current-genomic", file_b: b"current-protein", file_c: b"current-rna"}
        seed_lakehouse(s3, test_bucket, ACCESSION_A, current_content, PATH_PREFIX, ASSEMBLY_DIR_A)

        # Pre-seed a stale archive copy for file_a (simulating prior partial run)
        archive_prefix = f"{PATH_PREFIX}archive/2024-01/updated/{rel}"
        s3.put_object(Bucket=test_bucket, Key=f"{archive_prefix}{file_a}", Body=b"stale-genomic")

        updated_manifest = _write_manifest(tmp_path, [ACCESSION_A], "updated_manifest.txt")
        archived = _archive_assemblies(
            str(updated_manifest),
            lakehouse_bucket=test_bucket,
            ncbi_release="2024-01",
            archive_reason="updated",
            lakehouse_key_prefix=PATH_PREFIX,
            delete_source=False,
        )

        # All 3 files counted
        assert archived == 3  # noqa: PLR2004
        # file_a overwritten with current content
        obj_a = s3.get_object(Bucket=test_bucket, Key=f"{archive_prefix}{file_a}")
        assert obj_a["Body"].read() == b"current-genomic", "file_a archive should be overwritten"
        # file_b and file_c now archived
        for fname in (file_b, file_c):
            resp = s3.head_object(Bucket=test_bucket, Key=f"{archive_prefix}{fname}")
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200  # noqa: PLR2004
        # Sources untouched (delete_source=False)
        source_keys = list_all_keys(s3, test_bucket, f"{PATH_PREFIX}{rel}")
        assert len(source_keys) == len(current_content)

    def test_partial_replaced_archive_resumes_and_deletes(
        self, minio_s3_client: object, test_bucket: str, staging_test_bucket: str, tmp_path: Path
    ) -> None:
        """Re-running replaced_or_suppressed archive after partial run completes and deletes all sources.

        Scenario: file_a was copied+deleted in prior run (no longer at source),
        file_b was copied but NOT deleted (still at source), file_c was untouched.
        Re-run processes file_b and file_c, deletes both. Result: no sources remain.
        """
        from cdm_data_loaders.ncbi_ftp.promote import _archive_assemblies

        s3 = minio_s3_client
        rel = build_accession_path(ASSEMBLY_DIR_A)

        file_a = f"{ASSEMBLY_DIR_A}_genomic.fna.gz"
        file_b = f"{ASSEMBLY_DIR_A}_protein.faa.gz"
        file_c = f"{ASSEMBLY_DIR_A}_rna.fna.gz"
        archive_prefix = f"{PATH_PREFIX}archive/2024-01/replaced_or_suppressed/{rel}"

        # Only file_b and file_c remain at source (file_a already gone)
        s3.put_object(
            Bucket=test_bucket,
            Key=f"{PATH_PREFIX}{rel}{file_b}",
            Body=b"protein",
            Metadata={"md5": hashlib.md5(b"protein").hexdigest()},  # noqa: S324
        )
        s3.put_object(
            Bucket=test_bucket,
            Key=f"{PATH_PREFIX}{rel}{file_c}",
            Body=b"rna",
            Metadata={"md5": hashlib.md5(b"rna").hexdigest()},  # noqa: S324
        )
        # file_a already at archive destination
        s3.put_object(Bucket=test_bucket, Key=f"{archive_prefix}{file_a}", Body=b"genomic")

        removed_manifest = _write_manifest(tmp_path, [ACCESSION_A], "removed_manifest.txt")
        archived = _archive_assemblies(
            str(removed_manifest),
            lakehouse_bucket=test_bucket,
            ncbi_release="2024-01",
            archive_reason="replaced_or_suppressed",
            lakehouse_key_prefix=PATH_PREFIX,
            delete_source=True,
        )

        # 2 newly archived (file_b and file_c)
        assert archived == 2  # noqa: PLR2004
        # file_b and file_c archive keys exist
        for fname in (file_b, file_c):
            resp = s3.head_object(Bucket=test_bucket, Key=f"{archive_prefix}{fname}")
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200  # noqa: PLR2004
        # No source keys remain (file_b and file_c were deleted)
        source_keys = list_all_keys(s3, test_bucket, f"{PATH_PREFIX}{rel}")
        assert len(source_keys) == 0, f"Source objects remain: {source_keys}"
        # file_a archive key is still intact
        resp_a = s3.head_object(Bucket=test_bucket, Key=f"{archive_prefix}{file_a}")
        assert resp_a["ResponseMetadata"]["HTTPStatusCode"] == 200  # noqa: PLR2004

    def test_full_rerun_after_complete_archive_is_idempotent(
        self, minio_s3_client: object, test_bucket: str, staging_test_bucket: str, tmp_path: Path
    ) -> None:
        """Running archive again when all files already exist at archive paths is safe (no errors)."""
        from cdm_data_loaders.ncbi_ftp.promote import _archive_assemblies

        s3 = minio_s3_client
        files = {
            f"{ASSEMBLY_DIR_A}_genomic.fna.gz": b"genomic",
            f"{ASSEMBLY_DIR_A}_protein.faa.gz": b"protein",
        }
        seed_lakehouse(s3, test_bucket, ACCESSION_A, files, PATH_PREFIX, ASSEMBLY_DIR_A)

        updated_manifest = _write_manifest(tmp_path, [ACCESSION_A], "updated_manifest.txt")

        # First run — archives all files
        archived_1 = _archive_assemblies(
            str(updated_manifest),
            lakehouse_bucket=test_bucket,
            ncbi_release="2024-01",
            archive_reason="updated",
            lakehouse_key_prefix=PATH_PREFIX,
            delete_source=False,
        )

        # Second run — same manifest, same source files still present
        archived_2 = _archive_assemblies(
            str(updated_manifest),
            lakehouse_bucket=test_bucket,
            ncbi_release="2024-01",
            archive_reason="updated",
            lakehouse_key_prefix=PATH_PREFIX,
            delete_source=False,
        )

        assert archived_1 == len(files)
        assert archived_2 == len(files)
        # Archive keys still present with correct content
        rel = build_accession_path(ASSEMBLY_DIR_A)
        for fname, expected_body in files.items():
            key = f"{PATH_PREFIX}archive/2024-01/updated/{rel}{fname}"
            obj = s3.get_object(Bucket=test_bucket, Key=key)
            assert obj["Body"].read() == expected_body


@pytest.mark.integration
@pytest.mark.slow_test
class TestArchiveMultiAccessionManifest:
    """Multiple accessions in a single manifest are all archived."""

    def test_two_accessions_both_archived(
        self, minio_s3_client: object, test_bucket: str, staging_test_bucket: str, tmp_path: Path
    ) -> None:
        """Both accessions are archived with correct keys when listed in one manifest."""
        from cdm_data_loaders.ncbi_ftp.promote import _archive_assemblies

        s3 = minio_s3_client

        files_a = {f"{ASSEMBLY_DIR_A}_genomic.fna.gz": b"genomic-A"}
        files_b = {f"{ASSEMBLY_DIR_B}_genomic.fna.gz": b"genomic-B"}
        seed_lakehouse(s3, test_bucket, ACCESSION_A, files_a, PATH_PREFIX, ASSEMBLY_DIR_A)
        seed_lakehouse(s3, test_bucket, ACCESSION_B, files_b, PATH_PREFIX, ASSEMBLY_DIR_B)

        manifest = _write_manifest(tmp_path, [ACCESSION_A, ACCESSION_B], "removed_manifest.txt")

        archived = _archive_assemblies(
            str(manifest),
            lakehouse_bucket=test_bucket,
            ncbi_release="2024-01",
            archive_reason="replaced_or_suppressed",
            lakehouse_key_prefix=PATH_PREFIX,
            delete_source=True,
        )

        assert archived == 2  # noqa: PLR2004
        rel_a = build_accession_path(ASSEMBLY_DIR_A)
        rel_b = build_accession_path(ASSEMBLY_DIR_B)
        key_a = f"{PATH_PREFIX}archive/2024-01/replaced_or_suppressed/{rel_a}{ASSEMBLY_DIR_A}_genomic.fna.gz"
        key_b = f"{PATH_PREFIX}archive/2024-01/replaced_or_suppressed/{rel_b}{ASSEMBLY_DIR_B}_genomic.fna.gz"
        assert s3.head_object(Bucket=test_bucket, Key=key_a)["ResponseMetadata"]["HTTPStatusCode"] == 200  # noqa: PLR2004
        assert s3.head_object(Bucket=test_bucket, Key=key_b)["ResponseMetadata"]["HTTPStatusCode"] == 200  # noqa: PLR2004
        # Sources deleted
        assert len(list_all_keys(s3, test_bucket, f"{PATH_PREFIX}{rel_a}")) == 0
        assert len(list_all_keys(s3, test_bucket, f"{PATH_PREFIX}{rel_b}")) == 0

    def test_three_accessions_correct_archive_reason_segment(
        self, minio_s3_client: object, test_bucket: str, staging_test_bucket: str, tmp_path: Path
    ) -> None:
        """Archive keys for all three accessions include the archive_reason segment."""
        from cdm_data_loaders.ncbi_ftp.promote import _archive_assemblies

        s3 = minio_s3_client
        accessions_and_dirs = [
            (ACCESSION_A, ASSEMBLY_DIR_A),
            (ACCESSION_B, ASSEMBLY_DIR_B),
            (ACCESSION_C, ASSEMBLY_DIR_C),
        ]
        for accession, assembly_dir in accessions_and_dirs:
            seed_lakehouse(
                s3,
                test_bucket,
                accession,
                {f"{assembly_dir}_genomic.fna.gz": b"data"},
                PATH_PREFIX,
                assembly_dir,
            )

        manifest = _write_manifest(tmp_path, [acc for acc, _ in accessions_and_dirs], "removed_manifest.txt")
        _archive_assemblies(
            str(manifest),
            lakehouse_bucket=test_bucket,
            ncbi_release="2024-03",
            archive_reason="replaced_or_suppressed",
            lakehouse_key_prefix=PATH_PREFIX,
            delete_source=False,
        )

        all_archive_keys = list_all_keys(s3, test_bucket, f"{PATH_PREFIX}archive/2024-03/")
        assert len(all_archive_keys) == 3  # noqa: PLR2004
        for key in all_archive_keys:
            assert "/replaced_or_suppressed/" in key, f"Archive key missing reason segment: {key}"
            assert "/2024-03/" in key, f"Archive key missing release segment: {key}"


@pytest.mark.integration
@pytest.mark.slow_test
class TestArchiveDryRunParallel:
    """Dry-run with many files leaves everything unchanged."""

    def test_dry_run_no_copies_no_deletes(
        self, minio_s3_client: object, test_bucket: str, staging_test_bucket: str, tmp_path: Path
    ) -> None:
        """Dry-run with multiple files per accession creates no archive keys and keeps sources."""
        from cdm_data_loaders.ncbi_ftp.promote import _archive_assemblies

        s3 = minio_s3_client
        many_files = {
            f"{ASSEMBLY_DIR_A}_genomic.fna.gz": b"genomic",
            f"{ASSEMBLY_DIR_A}_protein.faa.gz": b"protein",
            f"{ASSEMBLY_DIR_A}_rna.fna.gz": b"rna",
        }
        source_keys = seed_lakehouse(s3, test_bucket, ACCESSION_A, many_files, PATH_PREFIX, ASSEMBLY_DIR_A)

        removed_manifest = _write_manifest(tmp_path, [ACCESSION_A], "removed_manifest.txt")
        archived = _archive_assemblies(
            str(removed_manifest),
            lakehouse_bucket=test_bucket,
            ncbi_release="2024-01",
            archive_reason="replaced_or_suppressed",
            lakehouse_key_prefix=PATH_PREFIX,
            delete_source=True,
            dry_run=True,
        )

        assert archived == len(many_files)
        # No archive keys
        archive_keys = list_all_keys(s3, test_bucket, f"{PATH_PREFIX}archive/")
        assert len(archive_keys) == 0, f"Dry-run created archive keys: {archive_keys}"
        # All sources still present
        for key in source_keys:
            remaining = list_all_keys(s3, test_bucket, key)
            assert len(remaining) == 1, f"Source missing after dry-run: {key}"


# Concurrent promotion tests


def _stage_many(
    s3: object,
    bucket: str,
    assembly_dir: str,
    files: dict[str, bytes],
    *,
    with_md5: bool = True,
) -> None:
    """Stage *files* with optional .md5 sidecars under the standard staging prefix."""
    rel = build_accession_path(assembly_dir)
    base = f"{STAGING_PREFIX}{rel}"
    for fname, content in files.items():
        key = f"{base}{fname}"
        s3.put_object(Bucket=bucket, Key=key, Body=content)
        if with_md5:
            s3.put_object(Bucket=bucket, Key=f"{key}.md5", Body=_md5(content).encode())


@pytest.mark.integration
@pytest.mark.slow_test
class TestPromoteMultiFileConcurrent:
    """Verify concurrent promotion lands all files with correct content and MD5."""

    def test_six_files_all_promoted_with_correct_content(
        self, minio_s3_client: object, test_bucket: str, staging_test_bucket: str
    ) -> None:
        """Every staged file arrives at the correct final key with byte-identical content."""
        s3 = minio_s3_client
        many_files = {
            f"{ASSEMBLY_DIR_A}_genomic.fna.gz": b"GENOMIC",
            f"{ASSEMBLY_DIR_A}_protein.faa.gz": b"PROTEIN",
            f"{ASSEMBLY_DIR_A}_rna.fna.gz": b"RNA",
            f"{ASSEMBLY_DIR_A}_assembly_report.txt": b"REPORT",
            f"{ASSEMBLY_DIR_A}_assembly_stats.txt": b"STATS",
            f"{ASSEMBLY_DIR_A}_cds_from_genomic.fna.gz": b"CDS",
        }
        _stage_many(s3, staging_test_bucket, ASSEMBLY_DIR_A, many_files)

        report = promote_from_s3(
            staging_key_prefix=STAGING_PREFIX,
            staging_bucket=staging_test_bucket,
            lakehouse_bucket=test_bucket,
            lakehouse_key_prefix=PATH_PREFIX,
        )

        assert report["promoted"] == len(many_files)
        assert report["failed"] == 0

        rel = build_accession_path(ASSEMBLY_DIR_A)
        for fname, expected_body in many_files.items():
            key = f"{PATH_PREFIX}{rel}{fname}"
            obj = s3.get_object(Bucket=test_bucket, Key=key)
            assert obj["Body"].read() == expected_body, f"Content mismatch: {fname}"

    def test_md5_metadata_correct_per_file(
        self, minio_s3_client: object, test_bucket: str, staging_test_bucket: str
    ) -> None:
        """Each promoted file carries MD5 metadata matching its own content, not another file's."""
        s3 = minio_s3_client
        files = {
            f"{ASSEMBLY_DIR_A}_genomic.fna.gz": b"GENOMIC_UNIQUE",
            f"{ASSEMBLY_DIR_A}_protein.faa.gz": b"PROTEIN_UNIQUE",
            f"{ASSEMBLY_DIR_A}_rna.fna.gz": b"RNA_UNIQUE",
        }
        _stage_many(s3, staging_test_bucket, ASSEMBLY_DIR_A, files, with_md5=True)

        promote_from_s3(
            staging_key_prefix=STAGING_PREFIX,
            staging_bucket=staging_test_bucket,
            lakehouse_bucket=test_bucket,
            lakehouse_key_prefix=PATH_PREFIX,
        )

        rel = build_accession_path(ASSEMBLY_DIR_A)
        for fname, content in files.items():
            key = f"{PATH_PREFIX}{rel}{fname}"
            meta = get_object_metadata(s3, test_bucket, key)
            assert meta.get("md5") == _md5(content), f"Wrong MD5 metadata on {fname}"

    def test_file_without_sidecar_has_no_md5_metadata(
        self, minio_s3_client: object, test_bucket: str, staging_test_bucket: str
    ) -> None:
        """A file staged without a .md5 sidecar is promoted but has no md5 metadata key."""
        s3 = minio_s3_client
        fname = f"{ASSEMBLY_DIR_A}_genomic.fna.gz"
        _stage_many(s3, staging_test_bucket, ASSEMBLY_DIR_A, {fname: FAKE_GENOMIC}, with_md5=False)

        promote_from_s3(
            staging_key_prefix=STAGING_PREFIX,
            staging_bucket=staging_test_bucket,
            lakehouse_bucket=test_bucket,
            lakehouse_key_prefix=PATH_PREFIX,
        )

        rel = build_accession_path(ASSEMBLY_DIR_A)
        meta = get_object_metadata(s3, test_bucket, f"{PATH_PREFIX}{rel}{fname}")
        assert "md5" not in meta, f"Expected no md5 metadata, got: {meta}"


@pytest.mark.integration
@pytest.mark.slow_test
class TestPromoteStagingCleanup:
    """After a fully successful promote, all staged files and sidecars are deleted."""

    def test_staged_data_files_deleted(
        self, minio_s3_client: object, test_bucket: str, staging_test_bucket: str
    ) -> None:
        """Data files are removed from staging after a successful assembly promote."""
        s3 = minio_s3_client
        files = {
            f"{ASSEMBLY_DIR_A}_genomic.fna.gz": FAKE_GENOMIC,
            f"{ASSEMBLY_DIR_A}_protein.faa.gz": FAKE_PROTEIN,
        }
        _stage_many(s3, staging_test_bucket, ASSEMBLY_DIR_A, files, with_md5=False)

        promote_from_s3(
            staging_key_prefix=STAGING_PREFIX,
            staging_bucket=staging_test_bucket,
            lakehouse_bucket=test_bucket,
            lakehouse_key_prefix=PATH_PREFIX,
        )

        remaining_staging = list_all_keys(s3, staging_test_bucket, STAGING_PREFIX)
        assert len(remaining_staging) == 0, f"Staging not cleaned: {remaining_staging}"

    def test_md5_sidecars_deleted(self, minio_s3_client: object, test_bucket: str, staging_test_bucket: str) -> None:
        """Both data files and .md5 sidecars are removed from staging after promote."""
        s3 = minio_s3_client
        files = {
            f"{ASSEMBLY_DIR_A}_genomic.fna.gz": FAKE_GENOMIC,
            f"{ASSEMBLY_DIR_A}_protein.faa.gz": FAKE_PROTEIN,
        }
        _stage_many(s3, staging_test_bucket, ASSEMBLY_DIR_A, files, with_md5=True)

        # Verify sidecars exist before promote
        before_keys = list_all_keys(s3, staging_test_bucket, STAGING_PREFIX)
        assert any(k.endswith(".md5") for k in before_keys), "Test setup: expected .md5 sidecars"

        promote_from_s3(
            staging_key_prefix=STAGING_PREFIX,
            staging_bucket=staging_test_bucket,
            lakehouse_bucket=test_bucket,
            lakehouse_key_prefix=PATH_PREFIX,
        )

        after_keys = list_all_keys(s3, staging_test_bucket, STAGING_PREFIX)
        assert len(after_keys) == 0, f"Staging not fully cleaned (including sidecars): {after_keys}"

    def test_two_assemblies_staging_both_cleaned(
        self, minio_s3_client: object, test_bucket: str, staging_test_bucket: str
    ) -> None:
        """Staging for both assemblies is fully cleaned when both assemblies succeed."""
        s3 = minio_s3_client
        _stage_many(
            s3,
            staging_test_bucket,
            ASSEMBLY_DIR_A,
            {f"{ASSEMBLY_DIR_A}_genomic.fna.gz": FAKE_GENOMIC},
            with_md5=True,
        )
        _stage_many(
            s3,
            staging_test_bucket,
            ASSEMBLY_DIR_B,
            {f"{ASSEMBLY_DIR_B}_genomic.fna.gz": FAKE_GENOMIC},
            with_md5=True,
        )

        report = promote_from_s3(
            staging_key_prefix=STAGING_PREFIX,
            staging_bucket=staging_test_bucket,
            lakehouse_bucket=test_bucket,
            lakehouse_key_prefix=PATH_PREFIX,
        )

        assert report["promoted"] == 2  # noqa: PLR2004
        assert report["failed"] == 0
        remaining = list_all_keys(s3, staging_test_bucket, STAGING_PREFIX)
        assert len(remaining) == 0, f"Staging not fully cleaned after two-assembly promote: {remaining}"


@pytest.mark.integration
@pytest.mark.slow_test
class TestPromoteTwoAssembliesBothLand:
    """Both assemblies staged together are both promoted to correct Lakehouse paths."""

    def test_both_assemblies_at_correct_final_paths(
        self, minio_s3_client: object, test_bucket: str, staging_test_bucket: str
    ) -> None:
        """Each assembly's files appear at distinct, correctly-routed final Lakehouse paths."""
        s3 = minio_s3_client
        files_a = {f"{ASSEMBLY_DIR_A}_genomic.fna.gz": b"genomic-A"}
        files_b = {f"{ASSEMBLY_DIR_B}_genomic.fna.gz": b"genomic-B"}
        _stage_many(s3, staging_test_bucket, ASSEMBLY_DIR_A, files_a)
        _stage_many(s3, staging_test_bucket, ASSEMBLY_DIR_B, files_b)

        report = promote_from_s3(
            staging_key_prefix=STAGING_PREFIX,
            staging_bucket=staging_test_bucket,
            lakehouse_bucket=test_bucket,
            lakehouse_key_prefix=PATH_PREFIX,
        )

        assert report["promoted"] == 2  # noqa: PLR2004
        assert report["failed"] == 0

        rel_a = build_accession_path(ASSEMBLY_DIR_A)
        rel_b = build_accession_path(ASSEMBLY_DIR_B)
        obj_a = s3.get_object(Bucket=test_bucket, Key=f"{PATH_PREFIX}{rel_a}{ASSEMBLY_DIR_A}_genomic.fna.gz")
        obj_b = s3.get_object(Bucket=test_bucket, Key=f"{PATH_PREFIX}{rel_b}{ASSEMBLY_DIR_B}_genomic.fna.gz")
        assert obj_a["Body"].read() == b"genomic-A"
        assert obj_b["Body"].read() == b"genomic-B"

    def test_final_path_keys_do_not_overlap(
        self, minio_s3_client: object, test_bucket: str, staging_test_bucket: str
    ) -> None:
        """Files for assembly A and assembly B land at distinct paths — no key collision."""
        s3 = minio_s3_client
        _stage_many(s3, staging_test_bucket, ASSEMBLY_DIR_A, {f"{ASSEMBLY_DIR_A}_genomic.fna.gz": b"a"})
        _stage_many(s3, staging_test_bucket, ASSEMBLY_DIR_B, {f"{ASSEMBLY_DIR_B}_genomic.fna.gz": b"b"})

        promote_from_s3(
            staging_key_prefix=STAGING_PREFIX,
            staging_bucket=staging_test_bucket,
            lakehouse_bucket=test_bucket,
            lakehouse_key_prefix=PATH_PREFIX,
        )

        rel_a = build_accession_path(ASSEMBLY_DIR_A)
        rel_b = build_accession_path(ASSEMBLY_DIR_B)
        keys_a = list_all_keys(s3, test_bucket, f"{PATH_PREFIX}{rel_a}")
        keys_b = list_all_keys(s3, test_bucket, f"{PATH_PREFIX}{rel_b}")
        assert len(keys_a) == 1
        assert len(keys_b) == 1
        assert keys_a[0] != keys_b[0]


@pytest.mark.integration
@pytest.mark.slow_test
class TestPromoteDryRunMultiFile:
    """dry_run leaves staging untouched and writes nothing to the Lakehouse."""

    def test_dry_run_many_files_staging_untouched(
        self, minio_s3_client: object, test_bucket: str, staging_test_bucket: str
    ) -> None:
        """All staged files (data + .md5) survive a dry-run promote unchanged."""
        s3 = minio_s3_client
        many_files = {
            f"{ASSEMBLY_DIR_A}_genomic.fna.gz": FAKE_GENOMIC,
            f"{ASSEMBLY_DIR_A}_protein.faa.gz": FAKE_PROTEIN,
            f"{ASSEMBLY_DIR_A}_rna.fna.gz": b"RNA",
        }
        _stage_many(s3, staging_test_bucket, ASSEMBLY_DIR_A, many_files, with_md5=True)
        staging_before = list_all_keys(s3, staging_test_bucket, STAGING_PREFIX)

        report = promote_from_s3(
            staging_key_prefix=STAGING_PREFIX,
            staging_bucket=staging_test_bucket,
            lakehouse_bucket=test_bucket,
            lakehouse_key_prefix=PATH_PREFIX,
            dry_run=True,
        )

        assert report["promoted"] == len(many_files)
        assert report["dry_run"] is True

        # Staging unchanged
        staging_after = list_all_keys(s3, staging_test_bucket, STAGING_PREFIX)
        assert staging_after == staging_before, "Dry-run should not alter staging"

        # Nothing at final path
        final_keys = list_all_keys(s3, test_bucket, PATH_PREFIX + "raw_data/")
        assert len(final_keys) == 0, f"Dry-run created Lakehouse objects: {final_keys}"

    def test_dry_run_two_assemblies_nothing_written(
        self, minio_s3_client: object, test_bucket: str, staging_test_bucket: str
    ) -> None:
        """Dry-run with two staged assemblies creates no Lakehouse objects."""
        s3 = minio_s3_client
        _stage_many(s3, staging_test_bucket, ASSEMBLY_DIR_A, {f"{ASSEMBLY_DIR_A}_genomic.fna.gz": FAKE_GENOMIC})
        _stage_many(s3, staging_test_bucket, ASSEMBLY_DIR_B, {f"{ASSEMBLY_DIR_B}_genomic.fna.gz": FAKE_GENOMIC})

        promote_from_s3(
            staging_key_prefix=STAGING_PREFIX,
            staging_bucket=staging_test_bucket,
            lakehouse_bucket=test_bucket,
            lakehouse_key_prefix=PATH_PREFIX,
            dry_run=True,
        )

        final_keys = list_all_keys(s3, test_bucket, PATH_PREFIX + "raw_data/")
        assert len(final_keys) == 0, f"Dry-run created objects: {final_keys}"


@pytest.mark.integration
@pytest.mark.slow_test
class TestPromoteSecondRunOnEmptyStaging:
    """After staging is cleaned, a second promote run promotes 0 files without error."""

    def test_second_run_promoted_zero(
        self, minio_s3_client: object, test_bucket: str, staging_test_bucket: str
    ) -> None:
        """Re-running promote on already-cleaned staging succeeds with promoted=0."""
        s3 = minio_s3_client
        _stage_many(
            s3,
            staging_test_bucket,
            ASSEMBLY_DIR_A,
            {f"{ASSEMBLY_DIR_A}_genomic.fna.gz": FAKE_GENOMIC},
            with_md5=True,
        )

        report1 = promote_from_s3(
            staging_key_prefix=STAGING_PREFIX,
            staging_bucket=staging_test_bucket,
            lakehouse_bucket=test_bucket,
            lakehouse_key_prefix=PATH_PREFIX,
        )
        report2 = promote_from_s3(
            staging_key_prefix=STAGING_PREFIX,
            staging_bucket=staging_test_bucket,
            lakehouse_bucket=test_bucket,
            lakehouse_key_prefix=PATH_PREFIX,
        )

        assert report1["promoted"] == 1
        assert report2["promoted"] == 0
        assert report2["failed"] == 0

        # Final key still present after second run
        rel = build_accession_path(ASSEMBLY_DIR_A)
        final_keys = list_all_keys(s3, test_bucket, f"{PATH_PREFIX}{rel}")
        assert len(final_keys) == 1

    def test_lakehouse_unchanged_on_second_run(
        self, minio_s3_client: object, test_bucket: str, staging_test_bucket: str
    ) -> None:
        """Lakehouse contents are identical before and after a second (no-op) promote run."""
        s3 = minio_s3_client
        _stage_many(
            s3,
            staging_test_bucket,
            ASSEMBLY_DIR_A,
            {f"{ASSEMBLY_DIR_A}_genomic.fna.gz": FAKE_GENOMIC, f"{ASSEMBLY_DIR_A}_protein.faa.gz": FAKE_PROTEIN},
            with_md5=True,
        )

        promote_from_s3(
            staging_key_prefix=STAGING_PREFIX,
            staging_bucket=staging_test_bucket,
            lakehouse_bucket=test_bucket,
            lakehouse_key_prefix=PATH_PREFIX,
        )
        keys_after_first = list_all_keys(s3, test_bucket, PATH_PREFIX + "raw_data/")

        promote_from_s3(
            staging_key_prefix=STAGING_PREFIX,
            staging_bucket=staging_test_bucket,
            lakehouse_bucket=test_bucket,
            lakehouse_key_prefix=PATH_PREFIX,
        )
        keys_after_second = list_all_keys(s3, test_bucket, PATH_PREFIX + "raw_data/")

        assert keys_after_first == keys_after_second
