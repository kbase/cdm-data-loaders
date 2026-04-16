"""Tests for ncbi_ftp.promote module — S3 promote, archive, manifest trimming."""

from pathlib import Path

import botocore.client
import pytest

from cdm_data_loaders.ncbi_ftp.promote import (
    DEFAULT_PATH_PREFIX,
    _archive_assemblies,
    _trim_manifest,
    promote_from_s3,
)
from tests.ncbi_ftp.conftest import TEST_BUCKET


@pytest.mark.s3
class TestPromoteFromS3:
    """Test promote_from_s3 with moto-mocked S3."""

    def _stage_files(self, s3_client: botocore.client.BaseClient, prefix: str) -> None:
        """Upload sample staged files to mock S3."""
        for key in [
            f"{prefix}raw_data/GCF/000/001/215/GCF_000001215.4_Release_6/GCF_000001215.4_genomic.fna.gz",
            f"{prefix}raw_data/GCF/000/001/215/GCF_000001215.4_Release_6/GCF_000001215.4_genomic.fna.gz.md5",
            f"{prefix}download_report.json",
        ]:
            body = b"md5hash123" if key.endswith(".md5") else b"data"
            s3_client.put_object(Bucket=TEST_BUCKET, Key=key, Body=body)

    def test_dry_run_no_writes(self, mock_s3_client_no_checksum: botocore.client.BaseClient) -> None:
        """Verify dry_run does not write any objects."""
        prefix = "staging/run1/"
        self._stage_files(mock_s3_client_no_checksum, prefix)

        report = promote_from_s3(
            staging_prefix=prefix,
            bucket=TEST_BUCKET,
            dry_run=True,
        )
        assert report["promoted"] == 1
        assert report["dry_run"] is True

        # Final path should NOT exist
        final_key = (
            f"{DEFAULT_PATH_PREFIX}raw_data/GCF/000/001/215/GCF_000001215.4_Release_6/GCF_000001215.4_genomic.fna.gz"
        )
        resp = mock_s3_client_no_checksum.list_objects_v2(Bucket=TEST_BUCKET, Prefix=final_key)
        assert resp.get("KeyCount", 0) == 0

    def test_promotes_with_metadata(self, mock_s3_client_no_checksum: botocore.client.BaseClient) -> None:
        """Verify objects are promoted with MD5 metadata attached."""
        prefix = "staging/run1/"
        self._stage_files(mock_s3_client_no_checksum, prefix)

        report = promote_from_s3(
            staging_prefix=prefix,
            bucket=TEST_BUCKET,
        )
        assert report["promoted"] == 1
        assert report["failed"] == 0

        # Check final object exists with metadata
        final_key = (
            f"{DEFAULT_PATH_PREFIX}raw_data/GCF/000/001/215/GCF_000001215.4_Release_6/GCF_000001215.4_genomic.fna.gz"
        )
        resp = mock_s3_client_no_checksum.head_object(Bucket=TEST_BUCKET, Key=final_key)
        assert resp["Metadata"].get("md5") == "md5hash123"

    def test_skips_download_report(self, mock_s3_client_no_checksum: botocore.client.BaseClient) -> None:
        """Verify download_report.json is not promoted."""
        prefix = "staging/run1/"
        self._stage_files(mock_s3_client_no_checksum, prefix)

        report = promote_from_s3(staging_prefix=prefix, bucket=TEST_BUCKET)
        # Only the .fna.gz data file, not download_report.json
        assert report["promoted"] == 1


@pytest.mark.s3
class TestTrimManifest:
    """Test _trim_manifest removes promoted accessions from S3 manifest."""

    def test_trims_promoted(self, mock_s3_client_no_checksum: botocore.client.BaseClient) -> None:
        """Verify promoted accessions are removed from manifest."""
        manifest_key = "manifests/transfer_manifest.txt"
        manifest_body = (
            "/genomes/all/GCF/000/001/215/GCF_000001215.4_Release_6/\n"
            "/genomes/all/GCF/000/001/405/GCF_000001405.40_GRCh38.p14/\n"
        )
        mock_s3_client_no_checksum.put_object(Bucket=TEST_BUCKET, Key=manifest_key, Body=manifest_body.encode())

        _trim_manifest(manifest_key, TEST_BUCKET, {"GCF_000001215.4"})

        resp = mock_s3_client_no_checksum.get_object(Bucket=TEST_BUCKET, Key=manifest_key)
        remaining = resp["Body"].read().decode()
        assert "GCF_000001215.4" not in remaining
        assert "GCF_000001405.40" in remaining

    def test_trims_all(self, mock_s3_client_no_checksum: botocore.client.BaseClient) -> None:
        """Verify all entries can be trimmed leaving an empty manifest."""
        manifest_key = "manifests/transfer_manifest.txt"
        manifest_body = "/genomes/all/GCF/000/001/215/GCF_000001215.4_Release_6/\n"
        mock_s3_client_no_checksum.put_object(Bucket=TEST_BUCKET, Key=manifest_key, Body=manifest_body.encode())

        _trim_manifest(manifest_key, TEST_BUCKET, {"GCF_000001215.4"})

        resp = mock_s3_client_no_checksum.get_object(Bucket=TEST_BUCKET, Key=manifest_key)
        remaining = resp["Body"].read().decode().strip()
        assert remaining == ""


@pytest.mark.s3
class TestArchiveAssemblies:
    """Test _archive_assemblies with moto-mocked S3."""

    def test_archives_and_deletes_removed(
        self, mock_s3_client_no_checksum: botocore.client.BaseClient, tmp_path: Path
    ) -> None:
        """Verify removed accessions are archived and originals deleted."""
        accession = "GCF_000005845.2"
        key = f"{DEFAULT_PATH_PREFIX}raw_data/GCF/000/005/845/{accession}_ASM584v2/{accession}_genomic.fna.gz"
        mock_s3_client_no_checksum.put_object(Bucket=TEST_BUCKET, Key=key, Body=b"data")

        manifest = tmp_path / "removed.txt"
        manifest.write_text(f"{accession}\n")

        count = _archive_assemblies(
            str(manifest),
            bucket=TEST_BUCKET,
            ncbi_release="2024-01",
            archive_reason="replaced_or_suppressed",
            delete_source=True,
        )
        assert count == 1

        # Original should be deleted
        resp = mock_s3_client_no_checksum.list_objects_v2(Bucket=TEST_BUCKET, Prefix=key)
        assert resp.get("KeyCount", 0) == 0

        # Archived copy should exist
        archive_key = (
            f"{DEFAULT_PATH_PREFIX}archive/2024-01/"
            f"raw_data/GCF/000/005/845/{accession}_ASM584v2/{accession}_genomic.fna.gz"
        )
        resp = mock_s3_client_no_checksum.list_objects_v2(Bucket=TEST_BUCKET, Prefix=archive_key)
        assert resp.get("KeyCount", 0) == 1

    def test_archives_updated_without_deleting(
        self, mock_s3_client_no_checksum: botocore.client.BaseClient, tmp_path: Path
    ) -> None:
        """Verify updated accessions are archived but originals remain."""
        accession = "GCF_000001215.4"
        asm_dir = f"{accession}_Release_6"
        key = f"{DEFAULT_PATH_PREFIX}raw_data/GCF/000/001/215/{asm_dir}/{accession}_genomic.fna.gz"
        mock_s3_client_no_checksum.put_object(Bucket=TEST_BUCKET, Key=key, Body=b"original-data")

        manifest = tmp_path / "updated.txt"
        manifest.write_text(f"{accession}\n")

        count = _archive_assemblies(
            str(manifest),
            bucket=TEST_BUCKET,
            ncbi_release="2024-06",
            archive_reason="updated",
            delete_source=False,
        )
        assert count == 1

        # Original still exists (promote will overwrite it)
        resp = mock_s3_client_no_checksum.list_objects_v2(Bucket=TEST_BUCKET, Prefix=key)
        assert resp.get("KeyCount", 0) == 1

        # Archived copy exists with correct metadata
        archive_key = (
            f"{DEFAULT_PATH_PREFIX}archive/2024-06/"
            f"raw_data/GCF/000/001/215/{asm_dir}/{accession}_genomic.fna.gz"
        )
        resp = mock_s3_client_no_checksum.head_object(Bucket=TEST_BUCKET, Key=archive_key)
        assert resp["Metadata"]["archive_reason"] == "updated"
        assert resp["Metadata"]["ncbi_last_release"] == "2024-06"

    def test_multiple_releases_no_collision(
        self, mock_s3_client_no_checksum: botocore.client.BaseClient, tmp_path: Path
    ) -> None:
        """Verify archiving the same accession in different releases creates distinct folders."""
        accession = "GCF_000001215.4"
        asm_dir = f"{accession}_Release_6"
        key = f"{DEFAULT_PATH_PREFIX}raw_data/GCF/000/001/215/{asm_dir}/{accession}_genomic.fna.gz"
        mock_s3_client_no_checksum.put_object(Bucket=TEST_BUCKET, Key=key, Body=b"v1-data")

        manifest = tmp_path / "updated.txt"
        manifest.write_text(f"{accession}\n")

        # First archive: release 2024-01
        _archive_assemblies(str(manifest), bucket=TEST_BUCKET, ncbi_release="2024-01", archive_reason="updated")

        # Simulate promote overwriting source
        mock_s3_client_no_checksum.put_object(Bucket=TEST_BUCKET, Key=key, Body=b"v2-data")

        # Second archive: release 2024-06
        _archive_assemblies(str(manifest), bucket=TEST_BUCKET, ncbi_release="2024-06", archive_reason="updated")

        archive_key_1 = (
            f"{DEFAULT_PATH_PREFIX}archive/2024-01/"
            f"raw_data/GCF/000/001/215/{asm_dir}/{accession}_genomic.fna.gz"
        )
        archive_key_2 = (
            f"{DEFAULT_PATH_PREFIX}archive/2024-06/"
            f"raw_data/GCF/000/001/215/{asm_dir}/{accession}_genomic.fna.gz"
        )
        resp1 = mock_s3_client_no_checksum.get_object(Bucket=TEST_BUCKET, Key=archive_key_1)
        assert resp1["Body"].read() == b"v1-data"

        resp2 = mock_s3_client_no_checksum.get_object(Bucket=TEST_BUCKET, Key=archive_key_2)
        assert resp2["Body"].read() == b"v2-data"

    def test_dry_run_no_side_effects(
        self, mock_s3_client_no_checksum: botocore.client.BaseClient, tmp_path: Path
    ) -> None:
        """Verify dry_run does not copy or delete anything."""
        accession = "GCF_000005845.2"
        key = f"{DEFAULT_PATH_PREFIX}raw_data/GCF/000/005/845/{accession}_ASM584v2/{accession}_genomic.fna.gz"
        mock_s3_client_no_checksum.put_object(Bucket=TEST_BUCKET, Key=key, Body=b"data")

        manifest = tmp_path / "removed.txt"
        manifest.write_text(f"{accession}\n")

        count = _archive_assemblies(
            str(manifest),
            bucket=TEST_BUCKET,
            ncbi_release="2024-01",
            archive_reason="replaced_or_suppressed",
            delete_source=True,
            dry_run=True,
        )
        assert count == 1

        # Original still exists
        resp = mock_s3_client_no_checksum.list_objects_v2(Bucket=TEST_BUCKET, Prefix=key)
        assert resp.get("KeyCount", 0) == 1

        # No archive created
        archive_prefix = f"{DEFAULT_PATH_PREFIX}archive/2024-01/"
        resp = mock_s3_client_no_checksum.list_objects_v2(Bucket=TEST_BUCKET, Prefix=archive_prefix)
        assert resp.get("KeyCount", 0) == 0

    def test_no_existing_objects_skips(
        self, mock_s3_client_no_checksum: botocore.client.BaseClient, tmp_path: Path
    ) -> None:
        """Verify accessions with no existing S3 objects are silently skipped."""
        manifest = tmp_path / "updated.txt"
        manifest.write_text("GCF_000001215.4\n")

        count = _archive_assemblies(str(manifest), bucket=TEST_BUCKET, ncbi_release="2024-01")
        assert count == 0

    def test_unknown_release_fallback(
        self, mock_s3_client_no_checksum: botocore.client.BaseClient, tmp_path: Path
    ) -> None:
        """Verify ncbi_release=None falls back to 'unknown'."""
        accession = "GCF_000001215.4"
        asm_dir = f"{accession}_Release_6"
        key = f"{DEFAULT_PATH_PREFIX}raw_data/GCF/000/001/215/{asm_dir}/{accession}_genomic.fna.gz"
        mock_s3_client_no_checksum.put_object(Bucket=TEST_BUCKET, Key=key, Body=b"data")

        manifest = tmp_path / "updated.txt"
        manifest.write_text(f"{accession}\n")

        count = _archive_assemblies(str(manifest), bucket=TEST_BUCKET, ncbi_release=None)
        assert count == 1

        archive_key = (
            f"{DEFAULT_PATH_PREFIX}archive/unknown/"
            f"raw_data/GCF/000/001/215/{asm_dir}/{accession}_genomic.fna.gz"
        )
        resp = mock_s3_client_no_checksum.list_objects_v2(Bucket=TEST_BUCKET, Prefix=archive_key)
        assert resp.get("KeyCount", 0) == 1
