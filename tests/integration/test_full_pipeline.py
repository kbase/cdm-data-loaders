"""End-to-end tests for the full NCBI assembly pipeline (Phase 1 → 2 → 3).

Exercises the entire flow: download summary from real NCBI FTP, compute diff,
download a single assembly, stage in MinIO, promote to final Lakehouse path.

Marked ``integration`` and ``slow_test``; auto-skipped when MinIO is
unreachable.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from pathlib import Path

from cdm_data_loaders.ncbi_ftp.manifest import (
    AssemblyRecord,
    compute_diff,
    download_assembly_summary,
    filter_by_prefix_range,
    parse_assembly_summary,
    write_removed_manifest,
    write_transfer_manifest,
    write_updated_manifest,
)
from cdm_data_loaders.ncbi_ftp.promote import DEFAULT_LAKEHOUSE_KEY_PREFIX, promote_from_s3
from cdm_data_loaders.pipelines.ncbi_ftp_download import download_batch

from .conftest import get_object_metadata, list_all_keys, stage_files_to_minio

STABLE_PREFIX = "900"
STAGING_PREFIX = "staging/run1/"
PATH_PREFIX = DEFAULT_LAKEHOUSE_KEY_PREFIX


@pytest.mark.integration
@pytest.mark.slow_test
@pytest.mark.external_request
class TestFullPipelineSmallBatch:
    """Run the complete pipeline for a single assembly: diff → download → promote."""

    def test_full_pipeline_small_batch(
        self,
        minio_s3_client: object,
        test_bucket: str,
        tmp_path: Path,
    ) -> None:
        """Single assembly flows through all three phases into MinIO."""
        s3 = minio_s3_client

        # ── Phase 1: Manifest generation ────────────────────────────────
        raw = download_assembly_summary(database="refseq")
        full = parse_assembly_summary(raw)
        filtered = filter_by_prefix_range(full, prefix_from=STABLE_PREFIX, prefix_to=STABLE_PREFIX)

        diff = compute_diff(filtered, previous_assemblies=None)
        assert len(diff.new) > 0, f"No new assemblies in prefix {STABLE_PREFIX}"

        manifest_path = tmp_path / "transfer_manifest.txt"
        write_transfer_manifest(diff, filtered, manifest_path)

        # ── Phase 2: Download one assembly from real FTP ────────────────
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

        # ── Upload local output to MinIO staging ────────────────────────
        keys = stage_files_to_minio(s3, test_bucket, output_dir, STAGING_PREFIX)
        assert len(keys) > 0, "Expected files staged to MinIO"

        # ── Phase 3: Promote from staging to final path ─────────────────
        promote_report = promote_from_s3(
            staging_key_prefix=STAGING_PREFIX,
            bucket=test_bucket,
            lakehouse_key_prefix=PATH_PREFIX,
        )
        assert promote_report["promoted"] >= 1
        assert promote_report["failed"] == 0

        # ── Verify final state ──────────────────────────────────────────
        final_keys = list_all_keys(s3, test_bucket, PATH_PREFIX + "raw_data/")
        assert len(final_keys) >= 1, "Expected files at final Lakehouse path"

        # At least one file should have MD5 metadata
        has_md5 = False
        for key in final_keys:
            meta = get_object_metadata(s3, test_bucket, key)
            if meta.get("md5"):
                has_md5 = True
                break
        assert has_md5, "Expected at least one file with MD5 metadata"


@pytest.mark.integration
@pytest.mark.slow_test
@pytest.mark.external_request
class TestFullPipelineIncrementalSync:
    """Run the pipeline twice to test incremental sync with archival."""

    def test_full_pipeline_incremental(
        self,
        minio_s3_client: object,
        test_bucket: str,
        tmp_path: Path,
    ) -> None:
        """Second sync archives the old version and promotes the new one."""
        s3 = minio_s3_client

        # ── First sync: Phase 1 → 2 → 3 ────────────────────────────────
        raw = download_assembly_summary(database="refseq")
        full = parse_assembly_summary(raw)
        filtered = filter_by_prefix_range(full, prefix_from=STABLE_PREFIX, prefix_to=STABLE_PREFIX)

        diff1 = compute_diff(filtered, previous_assemblies=None)
        assert len(diff1.new) > 0, f"No new assemblies in prefix {STABLE_PREFIX}"

        manifest1 = tmp_path / "transfer_manifest_1.txt"
        write_transfer_manifest(diff1, filtered, manifest1)

        output1 = tmp_path / "output1"
        output1.mkdir()
        report1 = download_batch(str(manifest1), str(output1), threads=1, limit=1)
        assert report1["succeeded"] >= 1

        stage_files_to_minio(s3, test_bucket, output1, STAGING_PREFIX)

        # Upload manifest to MinIO for trimming
        manifest_key = "ncbi/transfer_manifest.txt"
        s3.upload_file(Filename=str(manifest1), Bucket=test_bucket, Key=manifest_key)

        promote1 = promote_from_s3(
            staging_key_prefix=STAGING_PREFIX,
            bucket=test_bucket,
            manifest_s3_key=manifest_key,
            lakehouse_key_prefix=PATH_PREFIX,
        )
        assert promote1["promoted"] >= 1

        first_sync_keys = list_all_keys(s3, test_bucket, PATH_PREFIX + "raw_data/")
        assert len(first_sync_keys) >= 1

        # ── Second sync: Manufacture "previous" with a tweak ────────────
        # Treat first-sync state as "previous", but modify one assembly's
        # seq_rel_date so it shows up as "updated".
        previous: dict[str, AssemblyRecord] = {}
        for acc, rec in filtered.items():
            previous[acc] = AssemblyRecord(
                accession=rec.accession,
                status=rec.status,
                seq_rel_date=rec.seq_rel_date,
                ftp_path=rec.ftp_path,
                assembly_dir=rec.assembly_dir,
            )

        # Pick the first accession that was actually downloaded
        downloaded_acc = diff1.new[0]
        if downloaded_acc in previous:
            previous[downloaded_acc].seq_rel_date = "1999/01/01"

        diff2 = compute_diff(filtered, previous_assemblies=previous)

        # The modified assembly should appear as "updated"
        if downloaded_acc in previous:
            assert downloaded_acc in diff2.updated, f"Expected {downloaded_acc} in updated list"

        manifest2 = tmp_path / "transfer_manifest_2.txt"
        write_transfer_manifest(diff2, filtered, manifest2)

        updated_manifest = tmp_path / "updated_manifest.txt"
        write_updated_manifest(diff2, updated_manifest)

        removed_manifest = tmp_path / "removed_manifest.txt"
        write_removed_manifest(diff2, removed_manifest)

        # Phase 2 — re-download the updated assembly
        output2 = tmp_path / "output2"
        output2.mkdir()
        report2 = download_batch(str(manifest2), str(output2), threads=1, limit=1)
        assert report2["succeeded"] >= 1

        # Clean staging and re-stage
        staging_keys = list_all_keys(s3, test_bucket, STAGING_PREFIX)
        for key in staging_keys:
            s3.delete_object(Bucket=test_bucket, Key=key)
        stage_files_to_minio(s3, test_bucket, output2, STAGING_PREFIX)

        # Phase 3 — promote with archival
        promote2 = promote_from_s3(
            staging_key_prefix=STAGING_PREFIX,
            bucket=test_bucket,
            updated_manifest_path=str(updated_manifest),
            ncbi_release="test-incremental",
            lakehouse_key_prefix=PATH_PREFIX,
        )
        assert promote2["failed"] == 0

        # Verify archive exists
        archive_keys = list_all_keys(s3, test_bucket, PATH_PREFIX + "archive/test-incremental/")
        if promote2["archived"] > 0:
            assert len(archive_keys) >= 1
            for key in archive_keys:
                meta = get_object_metadata(s3, test_bucket, key)
                assert meta.get("archive_reason") == "updated"

        # Final Lakehouse path should still have files
        final_keys = list_all_keys(s3, test_bucket, PATH_PREFIX + "raw_data/")
        assert len(final_keys) >= 1
