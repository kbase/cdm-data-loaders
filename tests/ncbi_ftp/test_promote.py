"""Tests for ncbi_ftp.promote module — S3 promote, archive, manifest trimming."""

from pathlib import Path

import botocore.client
import pytest

from cdm_data_loaders.ncbi_ftp.promote import (
    DEFAULT_LAKEHOUSE_KEY_PREFIX,
    _archive_assemblies,
    _trim_manifest,
    promote_from_s3,
)
from tests.ncbi_ftp.conftest import TEST_BUCKET


def _stage_files(s3_client: botocore.client.BaseClient, prefix: str) -> None:
    """Upload sample staged files to mock S3."""
    for key in [
        f"{prefix}raw_data/GCF/000/001/215/GCF_000001215.4_Release_6/GCF_000001215.4_genomic.fna.gz",
        f"{prefix}raw_data/GCF/000/001/215/GCF_000001215.4_Release_6/GCF_000001215.4_genomic.fna.gz.md5",
        f"{prefix}download_report.json",
    ]:
        body = b"md5hash123" if key.endswith(".md5") else b"data"
        s3_client.put_object(Bucket=TEST_BUCKET, Key=key, Body=body)


@pytest.mark.s3
def test_promote_dry_run_no_writes(mock_s3_client_no_checksum: botocore.client.BaseClient) -> None:
    """Verify dry_run does not write any objects."""
    prefix = "staging/run1/"
    _stage_files(mock_s3_client_no_checksum, prefix)

    report = promote_from_s3(staging_key_prefix=prefix, bucket=TEST_BUCKET, dry_run=True)
    assert report["promoted"] == 1
    assert report["dry_run"] is True

    final_key = f"{DEFAULT_LAKEHOUSE_KEY_PREFIX}raw_data/GCF/000/001/215/GCF_000001215.4_Release_6/GCF_000001215.4_genomic.fna.gz"
    assert mock_s3_client_no_checksum.list_objects_v2(Bucket=TEST_BUCKET, Prefix=final_key).get("KeyCount", 0) == 0


@pytest.mark.s3
def test_promote_with_metadata(mock_s3_client_no_checksum: botocore.client.BaseClient) -> None:
    """Objects are promoted with MD5 metadata; download_report.json is skipped."""
    prefix = "staging/run1/"
    _stage_files(mock_s3_client_no_checksum, prefix)

    report = promote_from_s3(staging_key_prefix=prefix, bucket=TEST_BUCKET)
    assert report["promoted"] == 1  # only .fna.gz, not download_report.json
    assert report["failed"] == 0

    final_key = f"{DEFAULT_LAKEHOUSE_KEY_PREFIX}raw_data/GCF/000/001/215/GCF_000001215.4_Release_6/GCF_000001215.4_genomic.fna.gz"
    resp = mock_s3_client_no_checksum.head_object(Bucket=TEST_BUCKET, Key=final_key)
    assert resp["Metadata"].get("md5") == "md5hash123"


@pytest.mark.s3
@pytest.mark.parametrize(
    ("manifest_body", "promoted_set", "expected_present", "expected_absent"),
    [
        pytest.param(
            "/genomes/all/GCF/000/001/215/GCF_000001215.4_Release_6/\n"
            "/genomes/all/GCF/000/001/405/GCF_000001405.40_GRCh38.p14/\n",
            {"GCF_000001215.4"},
            ["GCF_000001405.40"],
            ["GCF_000001215.4"],
            id="partial",
        ),
        pytest.param(
            "/genomes/all/GCF/000/001/215/GCF_000001215.4_Release_6/\n",
            {"GCF_000001215.4"},
            [],
            ["GCF_000001215.4"],
            id="all",
        ),
    ],
)
def test_trim_manifest(
    mock_s3_client_no_checksum: botocore.client.BaseClient,
    manifest_body: str,
    promoted_set: set[str],
    expected_present: list[str],
    expected_absent: list[str],
) -> None:
    """Promoted accessions are removed; others remain (partial) or the manifest empties (all)."""
    manifest_key = "manifests/transfer_manifest.txt"
    mock_s3_client_no_checksum.put_object(Bucket=TEST_BUCKET, Key=manifest_key, Body=manifest_body.encode())
    _trim_manifest(manifest_key, TEST_BUCKET, promoted_set)
    remaining = mock_s3_client_no_checksum.get_object(Bucket=TEST_BUCKET, Key=manifest_key)["Body"].read().decode()
    for acc in expected_present:
        assert acc in remaining
    for acc in expected_absent:
        assert acc not in remaining


@pytest.mark.s3
def test_archive_assemblies_removed(mock_s3_client_no_checksum: botocore.client.BaseClient, tmp_path: Path) -> None:
    """Removed accessions are archived and originals deleted."""
    accession = "GCF_000005845.2"
    key = f"{DEFAULT_LAKEHOUSE_KEY_PREFIX}raw_data/GCF/000/005/845/{accession}_ASM584v2/{accession}_genomic.fna.gz"
    mock_s3_client_no_checksum.put_object(Bucket=TEST_BUCKET, Key=key, Body=b"data")

    manifest = tmp_path / "removed.txt"
    manifest.write_text(f"{accession}\n")

    assert (
        _archive_assemblies(
            str(manifest),
            bucket=TEST_BUCKET,
            ncbi_release="2024-01",
            archive_reason="replaced_or_suppressed",
            delete_source=True,
        )
        == 1
    )
    assert mock_s3_client_no_checksum.list_objects_v2(Bucket=TEST_BUCKET, Prefix=key).get("KeyCount", 0) == 0

    archive_key = (
        f"{DEFAULT_LAKEHOUSE_KEY_PREFIX}archive/2024-01/"
        f"raw_data/GCF/000/005/845/{accession}_ASM584v2/{accession}_genomic.fna.gz"
    )
    assert mock_s3_client_no_checksum.list_objects_v2(Bucket=TEST_BUCKET, Prefix=archive_key).get("KeyCount", 0) == 1


@pytest.mark.s3
def test_archive_assemblies_updated_no_delete(
    mock_s3_client_no_checksum: botocore.client.BaseClient, tmp_path: Path
) -> None:
    """Updated accessions are archived but originals remain."""
    accession = "GCF_000001215.4"
    asm_dir = f"{accession}_Release_6"
    key = f"{DEFAULT_LAKEHOUSE_KEY_PREFIX}raw_data/GCF/000/001/215/{asm_dir}/{accession}_genomic.fna.gz"
    mock_s3_client_no_checksum.put_object(Bucket=TEST_BUCKET, Key=key, Body=b"original-data")

    manifest = tmp_path / "updated.txt"
    manifest.write_text(f"{accession}\n")

    assert (
        _archive_assemblies(
            str(manifest), bucket=TEST_BUCKET, ncbi_release="2024-06", archive_reason="updated", delete_source=False
        )
        == 1
    )
    assert mock_s3_client_no_checksum.list_objects_v2(Bucket=TEST_BUCKET, Prefix=key).get("KeyCount", 0) == 1

    archive_key = (
        f"{DEFAULT_LAKEHOUSE_KEY_PREFIX}archive/2024-06/raw_data/GCF/000/001/215/{asm_dir}/{accession}_genomic.fna.gz"
    )
    resp = mock_s3_client_no_checksum.head_object(Bucket=TEST_BUCKET, Key=archive_key)
    assert resp["Metadata"]["archive_reason"] == "updated"
    assert resp["Metadata"]["ncbi_last_release"] == "2024-06"


@pytest.mark.s3
def test_archive_assemblies_multiple_releases_no_collision(
    mock_s3_client_no_checksum: botocore.client.BaseClient, tmp_path: Path
) -> None:
    """Archiving the same accession in different releases creates distinct folders."""
    accession = "GCF_000001215.4"
    asm_dir = f"{accession}_Release_6"
    key = f"{DEFAULT_LAKEHOUSE_KEY_PREFIX}raw_data/GCF/000/001/215/{asm_dir}/{accession}_genomic.fna.gz"
    mock_s3_client_no_checksum.put_object(Bucket=TEST_BUCKET, Key=key, Body=b"v1-data")

    manifest = tmp_path / "updated.txt"
    manifest.write_text(f"{accession}\n")

    _archive_assemblies(str(manifest), bucket=TEST_BUCKET, ncbi_release="2024-01", archive_reason="updated")
    mock_s3_client_no_checksum.put_object(Bucket=TEST_BUCKET, Key=key, Body=b"v2-data")
    _archive_assemblies(str(manifest), bucket=TEST_BUCKET, ncbi_release="2024-06", archive_reason="updated")

    archive_key_1 = (
        f"{DEFAULT_LAKEHOUSE_KEY_PREFIX}archive/2024-01/raw_data/GCF/000/001/215/{asm_dir}/{accession}_genomic.fna.gz"
    )
    archive_key_2 = (
        f"{DEFAULT_LAKEHOUSE_KEY_PREFIX}archive/2024-06/raw_data/GCF/000/001/215/{asm_dir}/{accession}_genomic.fna.gz"
    )
    assert mock_s3_client_no_checksum.get_object(Bucket=TEST_BUCKET, Key=archive_key_1)["Body"].read() == b"v1-data"
    assert mock_s3_client_no_checksum.get_object(Bucket=TEST_BUCKET, Key=archive_key_2)["Body"].read() == b"v2-data"


@pytest.mark.s3
def test_archive_assemblies_dry_run(mock_s3_client_no_checksum: botocore.client.BaseClient, tmp_path: Path) -> None:
    """dry_run does not copy or delete anything."""
    accession = "GCF_000005845.2"
    key = f"{DEFAULT_LAKEHOUSE_KEY_PREFIX}raw_data/GCF/000/005/845/{accession}_ASM584v2/{accession}_genomic.fna.gz"
    mock_s3_client_no_checksum.put_object(Bucket=TEST_BUCKET, Key=key, Body=b"data")

    manifest = tmp_path / "removed.txt"
    manifest.write_text(f"{accession}\n")

    assert (
        _archive_assemblies(
            str(manifest),
            bucket=TEST_BUCKET,
            ncbi_release="2024-01",
            archive_reason="replaced_or_suppressed",
            delete_source=True,
            dry_run=True,
        )
        == 1
    )
    assert mock_s3_client_no_checksum.list_objects_v2(Bucket=TEST_BUCKET, Prefix=key).get("KeyCount", 0) == 1

    archive_prefix = f"{DEFAULT_LAKEHOUSE_KEY_PREFIX}archive/2024-01/"
    assert mock_s3_client_no_checksum.list_objects_v2(Bucket=TEST_BUCKET, Prefix=archive_prefix).get("KeyCount", 0) == 0


@pytest.mark.s3
def test_archive_assemblies_no_objects_skips(
    mock_s3_client_no_checksum: botocore.client.BaseClient, tmp_path: Path
) -> None:
    """Accessions with no existing S3 objects are silently skipped."""
    manifest = tmp_path / "updated.txt"
    manifest.write_text("GCF_000001215.4\n")
    assert _archive_assemblies(str(manifest), bucket=TEST_BUCKET, ncbi_release="2024-01") == 0


@pytest.mark.s3
def test_archive_assemblies_unknown_release_fallback(
    mock_s3_client_no_checksum: botocore.client.BaseClient, tmp_path: Path
) -> None:
    """ncbi_release=None falls back to 'unknown' in the archive path."""
    accession = "GCF_000001215.4"
    asm_dir = f"{accession}_Release_6"
    key = f"{DEFAULT_LAKEHOUSE_KEY_PREFIX}raw_data/GCF/000/001/215/{asm_dir}/{accession}_genomic.fna.gz"
    mock_s3_client_no_checksum.put_object(Bucket=TEST_BUCKET, Key=key, Body=b"data")

    manifest = tmp_path / "updated.txt"
    manifest.write_text(f"{accession}\n")

    assert _archive_assemblies(str(manifest), bucket=TEST_BUCKET, ncbi_release=None) == 1

    archive_key = (
        f"{DEFAULT_LAKEHOUSE_KEY_PREFIX}archive/unknown/raw_data/GCF/000/001/215/{asm_dir}/{accession}_genomic.fna.gz"
    )
    assert mock_s3_client_no_checksum.list_objects_v2(Bucket=TEST_BUCKET, Prefix=archive_key).get("KeyCount", 0) == 1
