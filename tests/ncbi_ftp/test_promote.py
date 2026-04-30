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

    report = promote_from_s3(
        staging_key_prefix=prefix, staging_bucket=TEST_BUCKET, lakehouse_bucket=TEST_BUCKET, dry_run=True
    )
    assert report["promoted"] == 1
    assert report["dry_run"] is True

    final_key = f"{DEFAULT_LAKEHOUSE_KEY_PREFIX}raw_data/GCF/000/001/215/GCF_000001215.4_Release_6/GCF_000001215.4_genomic.fna.gz"
    assert mock_s3_client_no_checksum.list_objects_v2(Bucket=TEST_BUCKET, Prefix=final_key).get("KeyCount", 0) == 0


@pytest.mark.s3
def test_promote_with_metadata(mock_s3_client_no_checksum: botocore.client.BaseClient) -> None:
    """Objects are promoted with MD5 metadata; download_report.json is skipped."""
    prefix = "staging/run1/"
    _stage_files(mock_s3_client_no_checksum, prefix)

    report = promote_from_s3(staging_key_prefix=prefix, staging_bucket=TEST_BUCKET, lakehouse_bucket=TEST_BUCKET)
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
            lakehouse_bucket=TEST_BUCKET,
            ncbi_release="2024-01",
            archive_reason="replaced_or_suppressed",
            delete_source=True,
        )
        == 1
    )
    assert mock_s3_client_no_checksum.list_objects_v2(Bucket=TEST_BUCKET, Prefix=key).get("KeyCount", 0) == 0

    archive_key = (
        f"{DEFAULT_LAKEHOUSE_KEY_PREFIX}archive/2024-01/replaced_or_suppressed/"
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
            str(manifest),
            lakehouse_bucket=TEST_BUCKET,
            ncbi_release="2024-06",
            archive_reason="updated",
            delete_source=False,
        )
        == 1
    )
    assert mock_s3_client_no_checksum.list_objects_v2(Bucket=TEST_BUCKET, Prefix=key).get("KeyCount", 0) == 1

    archive_key = f"{DEFAULT_LAKEHOUSE_KEY_PREFIX}archive/2024-06/updated/raw_data/GCF/000/001/215/{asm_dir}/{accession}_genomic.fna.gz"
    resp = mock_s3_client_no_checksum.head_object(Bucket=TEST_BUCKET, Key=archive_key)
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200  # noqa: PLR2004


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

    _archive_assemblies(str(manifest), lakehouse_bucket=TEST_BUCKET, ncbi_release="2024-01", archive_reason="updated")
    mock_s3_client_no_checksum.put_object(Bucket=TEST_BUCKET, Key=key, Body=b"v2-data")
    _archive_assemblies(str(manifest), lakehouse_bucket=TEST_BUCKET, ncbi_release="2024-06", archive_reason="updated")

    archive_key_1 = f"{DEFAULT_LAKEHOUSE_KEY_PREFIX}archive/2024-01/updated/raw_data/GCF/000/001/215/{asm_dir}/{accession}_genomic.fna.gz"
    archive_key_2 = f"{DEFAULT_LAKEHOUSE_KEY_PREFIX}archive/2024-06/updated/raw_data/GCF/000/001/215/{asm_dir}/{accession}_genomic.fna.gz"
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
            lakehouse_bucket=TEST_BUCKET,
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
    assert _archive_assemblies(str(manifest), lakehouse_bucket=TEST_BUCKET, ncbi_release="2024-01") == 0


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

    assert _archive_assemblies(str(manifest), lakehouse_bucket=TEST_BUCKET, ncbi_release=None) == 1

    archive_key = f"{DEFAULT_LAKEHOUSE_KEY_PREFIX}archive/unknown/unknown/raw_data/GCF/000/001/215/{asm_dir}/{accession}_genomic.fna.gz"
    assert mock_s3_client_no_checksum.list_objects_v2(Bucket=TEST_BUCKET, Prefix=archive_key).get("KeyCount", 0) == 1


# ── Concurrent / multi-file archive (new behaviour) ─────────────────────


@pytest.mark.s3
def test_archive_assemblies_multi_file_all_copied(
    mock_s3_client_no_checksum: botocore.client.BaseClient, tmp_path: Path
) -> None:
    """All files for an accession are copied concurrently — none missed."""
    accession = "GCF_000001215.4"
    asm_dir = f"{accession}_Release_6"
    base = f"{DEFAULT_LAKEHOUSE_KEY_PREFIX}raw_data/GCF/000/001/215/{asm_dir}/"
    file_names = [
        f"{accession}_genomic.fna.gz",
        f"{accession}_protein.faa.gz",
        f"{accession}_rna.fna.gz",
        f"{accession}_assembly_report.txt",
        f"{accession}_assembly_stats.txt",
    ]
    for fname in file_names:
        mock_s3_client_no_checksum.put_object(Bucket=TEST_BUCKET, Key=f"{base}{fname}", Body=fname.encode())

    manifest = tmp_path / "updated.txt"
    manifest.write_text(f"{accession}\n")

    archived = _archive_assemblies(
        str(manifest),
        lakehouse_bucket=TEST_BUCKET,
        ncbi_release="2024-01",
        archive_reason="updated",
        delete_source=False,
    )

    assert archived == len(file_names)
    archive_base = f"{DEFAULT_LAKEHOUSE_KEY_PREFIX}archive/2024-01/updated/raw_data/GCF/000/001/215/{asm_dir}/"
    for fname in file_names:
        resp = mock_s3_client_no_checksum.head_object(Bucket=TEST_BUCKET, Key=f"{archive_base}{fname}")
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200  # noqa: PLR2004


@pytest.mark.s3
def test_archive_assemblies_multi_file_content_preserved(
    mock_s3_client_no_checksum: botocore.client.BaseClient, tmp_path: Path
) -> None:
    """Archive copies preserve byte-for-byte content of each file."""
    accession = "GCF_000001215.4"
    asm_dir = f"{accession}_Release_6"
    base = f"{DEFAULT_LAKEHOUSE_KEY_PREFIX}raw_data/GCF/000/001/215/{asm_dir}/"
    files = {
        f"{accession}_genomic.fna.gz": b"\x1f\x8bGENOMIC",
        f"{accession}_protein.faa.gz": b"\x1f\x8bPROTEIN",
    }
    for fname, body in files.items():
        mock_s3_client_no_checksum.put_object(Bucket=TEST_BUCKET, Key=f"{base}{fname}", Body=body)

    manifest = tmp_path / "updated.txt"
    manifest.write_text(f"{accession}\n")

    _archive_assemblies(
        str(manifest),
        lakehouse_bucket=TEST_BUCKET,
        ncbi_release="2024-01",
        archive_reason="updated",
        delete_source=False,
    )

    archive_base = f"{DEFAULT_LAKEHOUSE_KEY_PREFIX}archive/2024-01/updated/raw_data/GCF/000/001/215/{asm_dir}/"
    for fname, original_body in files.items():
        obj = mock_s3_client_no_checksum.get_object(Bucket=TEST_BUCKET, Key=f"{archive_base}{fname}")
        assert obj["Body"].read() == original_body, f"Content mismatch for {fname}"


@pytest.mark.s3
def test_archive_assemblies_multi_file_delete_all(
    mock_s3_client_no_checksum: botocore.client.BaseClient, tmp_path: Path
) -> None:
    """Batch delete removes ALL source files when delete_source=True."""
    accession = "GCF_000005845.2"
    asm_dir = f"{accession}_ASM584v2"
    base = f"{DEFAULT_LAKEHOUSE_KEY_PREFIX}raw_data/GCF/000/005/845/{asm_dir}/"
    file_names = [
        f"{accession}_genomic.fna.gz",
        f"{accession}_protein.faa.gz",
        f"{accession}_assembly_report.txt",
    ]
    for fname in file_names:
        mock_s3_client_no_checksum.put_object(Bucket=TEST_BUCKET, Key=f"{base}{fname}", Body=b"data")

    manifest = tmp_path / "removed.txt"
    manifest.write_text(f"{accession}\n")

    archived = _archive_assemblies(
        str(manifest),
        lakehouse_bucket=TEST_BUCKET,
        ncbi_release="2024-03",
        archive_reason="replaced_or_suppressed",
        delete_source=True,
    )

    assert archived == len(file_names)
    # All sources deleted
    for fname in file_names:
        result = mock_s3_client_no_checksum.list_objects_v2(Bucket=TEST_BUCKET, Prefix=f"{base}{fname}")
        assert result.get("KeyCount", 0) == 0, f"Source not deleted: {fname}"
    # All archives present
    archive_base = (
        f"{DEFAULT_LAKEHOUSE_KEY_PREFIX}archive/2024-03/replaced_or_suppressed/raw_data/GCF/000/005/845/{asm_dir}/"
    )
    for fname in file_names:
        resp = mock_s3_client_no_checksum.head_object(Bucket=TEST_BUCKET, Key=f"{archive_base}{fname}")
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200  # noqa: PLR2004


# ── Partial-archive idempotency ──────────────────────────────────────────


@pytest.mark.s3
def test_archive_assemblies_partial_already_archived_overwritten(
    mock_s3_client_no_checksum: botocore.client.BaseClient, tmp_path: Path
) -> None:
    """Re-running archive after a partial run overwrites the already-archived files.

    Simulates a partial failure: file_a was archived, file_b was not.
    The second run should archive both file_a (overwrite) and file_b.
    """
    accession = "GCF_000001215.4"
    asm_dir = f"{accession}_Release_6"
    base = f"{DEFAULT_LAKEHOUSE_KEY_PREFIX}raw_data/GCF/000/001/215/{asm_dir}/"
    archive_base = f"{DEFAULT_LAKEHOUSE_KEY_PREFIX}archive/2024-01/updated/raw_data/GCF/000/001/215/{asm_dir}/"

    file_a = f"{accession}_genomic.fna.gz"
    file_b = f"{accession}_protein.faa.gz"

    mock_s3_client_no_checksum.put_object(Bucket=TEST_BUCKET, Key=f"{base}{file_a}", Body=b"new-genomic")
    mock_s3_client_no_checksum.put_object(Bucket=TEST_BUCKET, Key=f"{base}{file_b}", Body=b"new-protein")
    # Simulate partial prior run: file_a already archived with stale content
    mock_s3_client_no_checksum.put_object(Bucket=TEST_BUCKET, Key=f"{archive_base}{file_a}", Body=b"stale-genomic")

    manifest = tmp_path / "updated.txt"
    manifest.write_text(f"{accession}\n")

    archived = _archive_assemblies(
        str(manifest),
        lakehouse_bucket=TEST_BUCKET,
        ncbi_release="2024-01",
        archive_reason="updated",
        delete_source=False,
    )

    assert archived == 2  # noqa: PLR2004
    # file_a should now have the current content (overwritten)
    obj_a = mock_s3_client_no_checksum.get_object(Bucket=TEST_BUCKET, Key=f"{archive_base}{file_a}")
    assert obj_a["Body"].read() == b"new-genomic", "Re-run should overwrite stale archive"
    # file_b should now be archived
    obj_b = mock_s3_client_no_checksum.get_object(Bucket=TEST_BUCKET, Key=f"{archive_base}{file_b}")
    assert obj_b["Body"].read() == b"new-protein"


@pytest.mark.s3
def test_archive_assemblies_partial_delete_resumes(
    mock_s3_client_no_checksum: botocore.client.BaseClient, tmp_path: Path
) -> None:
    """Re-running replaced_or_suppressed archive after partial copy+delete is safe.

    Simulates: file_a was copied+deleted, file_b was copied but NOT deleted,
    file_c was not touched. The re-run finds only file_b and file_c present
    (file_a is gone), archives both, and deletes both.
    """
    accession = "GCF_000005845.2"
    asm_dir = f"{accession}_ASM584v2"
    base = f"{DEFAULT_LAKEHOUSE_KEY_PREFIX}raw_data/GCF/000/005/845/{asm_dir}/"
    archive_base = (
        f"{DEFAULT_LAKEHOUSE_KEY_PREFIX}archive/2024-03/replaced_or_suppressed/raw_data/GCF/000/005/845/{asm_dir}/"
    )

    file_b = f"{accession}_protein.faa.gz"
    file_c = f"{accession}_assembly_report.txt"

    # file_a already gone (deleted in first partial run)
    # file_b present at source (not yet deleted from first partial run)
    mock_s3_client_no_checksum.put_object(Bucket=TEST_BUCKET, Key=f"{base}{file_b}", Body=b"protein")
    # file_c present at source (not touched at all)
    mock_s3_client_no_checksum.put_object(Bucket=TEST_BUCKET, Key=f"{base}{file_c}", Body=b"report")
    # file_a already at archive destination
    mock_s3_client_no_checksum.put_object(
        Bucket=TEST_BUCKET, Key=f"{archive_base}{accession}_genomic.fna.gz", Body=b"genomic"
    )

    manifest = tmp_path / "removed.txt"
    manifest.write_text(f"{accession}\n")

    archived = _archive_assemblies(
        str(manifest),
        lakehouse_bucket=TEST_BUCKET,
        ncbi_release="2024-03",
        archive_reason="replaced_or_suppressed",
        delete_source=True,
    )

    # Only the 2 remaining source files were archived
    assert archived == 2  # noqa: PLR2004
    # Both now gone from source
    for fname in (file_b, file_c):
        result = mock_s3_client_no_checksum.list_objects_v2(Bucket=TEST_BUCKET, Prefix=f"{base}{fname}")
        assert result.get("KeyCount", 0) == 0, f"Expected {fname} deleted"
    # file_a archive still intact (not touched by re-run)
    resp = mock_s3_client_no_checksum.head_object(Bucket=TEST_BUCKET, Key=f"{archive_base}{accession}_genomic.fna.gz")
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200  # noqa: PLR2004


@pytest.mark.s3
def test_archive_assemblies_idempotent_updated_reruns_cleanly(
    mock_s3_client_no_checksum: botocore.client.BaseClient, tmp_path: Path
) -> None:
    """Running updated archive twice on the same data produces the same result."""
    accession = "GCF_000001215.4"
    asm_dir = f"{accession}_Release_6"
    base = f"{DEFAULT_LAKEHOUSE_KEY_PREFIX}raw_data/GCF/000/001/215/{asm_dir}/"
    file_names = [f"{accession}_genomic.fna.gz", f"{accession}_protein.faa.gz"]
    for fname in file_names:
        mock_s3_client_no_checksum.put_object(Bucket=TEST_BUCKET, Key=f"{base}{fname}", Body=b"content")

    manifest = tmp_path / "updated.txt"
    manifest.write_text(f"{accession}\n")

    archived_1 = _archive_assemblies(
        str(manifest), lakehouse_bucket=TEST_BUCKET, ncbi_release="2024-01", archive_reason="updated"
    )
    archived_2 = _archive_assemblies(
        str(manifest), lakehouse_bucket=TEST_BUCKET, ncbi_release="2024-01", archive_reason="updated"
    )

    assert archived_1 == len(file_names)
    assert archived_2 == len(file_names)
    # Sources still present after both runs (delete_source=False)
    for fname in file_names:
        resp = mock_s3_client_no_checksum.head_object(Bucket=TEST_BUCKET, Key=f"{base}{fname}")
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200  # noqa: PLR2004


@pytest.mark.s3
def test_archive_assemblies_multi_accession_manifest(
    mock_s3_client_no_checksum: botocore.client.BaseClient, tmp_path: Path
) -> None:
    """Multiple accessions in a single manifest are all archived."""
    accessions = [
        ("GCF_000001215.4", "GCF_000001215.4_Release_6", "GCF/000/001/215"),
        ("GCF_000005845.2", "GCF_000005845.2_ASM584v2", "GCF/000/005/845"),
    ]
    for accession, asm_dir, path in accessions:
        key = f"{DEFAULT_LAKEHOUSE_KEY_PREFIX}raw_data/{path}/{asm_dir}/{accession}_genomic.fna.gz"
        mock_s3_client_no_checksum.put_object(Bucket=TEST_BUCKET, Key=key, Body=b"data")

    manifest = tmp_path / "updated.txt"
    manifest.write_text("\n".join(acc for acc, _, _ in accessions) + "\n")

    archived = _archive_assemblies(
        str(manifest), lakehouse_bucket=TEST_BUCKET, ncbi_release="2024-01", archive_reason="updated"
    )

    assert archived == len(accessions)
    for accession, asm_dir, path in accessions:
        archive_key = f"{DEFAULT_LAKEHOUSE_KEY_PREFIX}archive/2024-01/updated/raw_data/{path}/{asm_dir}/{accession}_genomic.fna.gz"
        result = mock_s3_client_no_checksum.list_objects_v2(Bucket=TEST_BUCKET, Prefix=archive_key)
        assert result.get("KeyCount", 0) == 1, f"Archive missing for {accession}"


@pytest.mark.s3
def test_archive_assemblies_dry_run_multi_file(
    mock_s3_client_no_checksum: botocore.client.BaseClient, tmp_path: Path
) -> None:
    """dry_run with multiple files per accession makes no copies and no deletes."""
    accession = "GCF_000005845.2"
    asm_dir = f"{accession}_ASM584v2"
    base = f"{DEFAULT_LAKEHOUSE_KEY_PREFIX}raw_data/GCF/000/005/845/{asm_dir}/"
    file_names = [f"{accession}_genomic.fna.gz", f"{accession}_protein.faa.gz", f"{accession}_rna.fna.gz"]
    for fname in file_names:
        mock_s3_client_no_checksum.put_object(Bucket=TEST_BUCKET, Key=f"{base}{fname}", Body=b"data")

    manifest = tmp_path / "removed.txt"
    manifest.write_text(f"{accession}\n")

    archived = _archive_assemblies(
        str(manifest),
        lakehouse_bucket=TEST_BUCKET,
        ncbi_release="2024-01",
        archive_reason="replaced_or_suppressed",
        delete_source=True,
        dry_run=True,
    )

    # Reported count matches
    assert archived == len(file_names)
    # No actual archive keys created
    archive_prefix = f"{DEFAULT_LAKEHOUSE_KEY_PREFIX}archive/"
    result = mock_s3_client_no_checksum.list_objects_v2(Bucket=TEST_BUCKET, Prefix=archive_prefix)
    assert result.get("KeyCount", 0) == 0
    # Sources untouched
    for fname in file_names:
        resp = mock_s3_client_no_checksum.head_object(Bucket=TEST_BUCKET, Key=f"{base}{fname}")
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200  # noqa: PLR2004


@pytest.mark.s3
def test_archive_assemblies_invalid_accession_skipped(
    mock_s3_client_no_checksum: botocore.client.BaseClient, tmp_path: Path
) -> None:
    """Malformed accession lines are skipped; valid ones still archived."""
    accession = "GCF_000001215.4"
    asm_dir = f"{accession}_Release_6"
    key = f"{DEFAULT_LAKEHOUSE_KEY_PREFIX}raw_data/GCF/000/001/215/{asm_dir}/{accession}_genomic.fna.gz"
    mock_s3_client_no_checksum.put_object(Bucket=TEST_BUCKET, Key=key, Body=b"data")

    manifest = tmp_path / "mixed.txt"
    manifest.write_text("NOT_AN_ACCESSION\n\n   \n" + f"{accession}\n")

    archived = _archive_assemblies(
        str(manifest), lakehouse_bucket=TEST_BUCKET, ncbi_release="2024-01", archive_reason="updated"
    )
    assert archived == 1
