"""End-to-end integration tests for the full PDB pipeline (Phase 1 → 2 → 3).

Two test classes cover the two supported Phase 2 approaches:

- ``TestPdbFullPipelineNotebook`` — Phase 2 via ``download_and_stage()``, which
  is the function used by ``notebooks/pdb_download.ipynb``.  Downloads via
  rsync and uploads staged files directly to MinIO in one pipelined pass.

- ``TestPdbFullPipelineContainer`` — Phase 2 via ``download_batch()``, which is
  the code that runs inside the ``pdb_rsync_sync`` CTS container.  Downloads to
  local disk; staged files are then uploaded to MinIO manually (mirroring what
  CTS does automatically in production).

Both tests use a single, tiny well-known PDB entry (crambin, ``pdb_00001crn``)
to minimise transfer time and are restricted to the ``structures`` file-type
subdirectory only (the smallest download category).

Prerequisites:
- Local MinIO container reachable at ``MINIO_ENDPOINT_URL`` (auto-skipped if not)
- ``rsync`` installed on the test host
- Outbound access to ``rsync-beta.rcsb.org`` port 32382

Marked ``integration``, ``slow_test``, and ``external_request``; auto-skipped
when MinIO is unreachable.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from cdm_data_loaders.pdb.entry import DEFAULT_LAKEHOUSE_KEY_PREFIX, build_entry_path
from cdm_data_loaders.pdb.promote import promote_from_s3
from cdm_data_loaders.pipelines.pdb_rsync import download_and_stage, download_batch

from .conftest import list_all_keys, stage_files_to_minio, staging_test_bucket  # noqa: F401

if TYPE_CHECKING:
    from pathlib import Path

    import botocore.client

# A small, stable PDB entry guaranteed to exist in the wwPDB Beta archive.
# Crambin (1CRN / pdb_00001crn) is one of the smallest protein structures
# (~46 residues), giving the smallest possible download.
STABLE_PDB_ID = "pdb_00001crn"

# Staging prefix used in both tests — must end with "output/" to match the
# convention used by the CTS container and pdb_download.ipynb.
STAGING_PREFIX = "staging/pdb-run1/output/"

# Lakehouse key prefix under which Phase 3 places promoted files.
LAKEHOUSE_PREFIX = DEFAULT_LAKEHOUSE_KEY_PREFIX


def _write_manifest(tmp_path: Path, pdb_id: str = STABLE_PDB_ID) -> Path:
    """Write a transfer manifest containing exactly one PDB ID.

    :param tmp_path: temporary directory provided by pytest
    :param pdb_id: extended PDB ID to include in the manifest
    :return: path to the written manifest file
    """
    manifest_path = tmp_path / "transfer_manifest.txt"
    manifest_path.write_text(pdb_id + "\n")
    return manifest_path


# ── Option A: notebook (download_and_stage) ─────────────────────────────


@pytest.mark.integration
@pytest.mark.slow_test
@pytest.mark.external_request
class TestPdbFullPipelineNotebook:
    """Full pipeline using the notebook approach (Phase 2 = download_and_stage).

    Phase 1: manifest written directly to disk.
    Phase 2: ``download_and_stage()`` — rsync downloads and S3 uploads are
             pipelined per entry; no local staging directory persists.
    Phase 3: ``promote_from_s3()`` — promotes from staging bucket to Lakehouse.
    """

    def test_full_pipeline_notebook(
        self,
        minio_s3_client: botocore.client.BaseClient,
        staging_test_bucket: str,
        test_bucket: str,
        tmp_path: Path,
    ) -> None:
        """Single entry flows through all three phases (notebook approach)."""
        # ── Phase 1: write manifest ──────────────────────────────────────
        manifest_path = _write_manifest(tmp_path)

        # ── Phase 2: download_and_stage (notebook function) ──────────────
        # Downloads via rsync and uploads directly to MinIO staging bucket.
        dl_report = download_and_stage(
            staging_bucket=staging_test_bucket,
            staging_key_prefix=STAGING_PREFIX,
            manifest_local_path=manifest_path,
            workers=1,
            file_types=["structures"],  # smallest file-type category
            limit=1,
            dry_run=False,
        )

        assert dl_report["succeeded"] >= 1, f"Phase 2 download failed: {dl_report}"
        assert dl_report["failed"] == 0, f"Phase 2 had failures: {dl_report['failures']}"
        assert dl_report["staged_objects"] > 0, "No objects staged to MinIO"

        # Verify staging layout before promoting
        staged_keys = list_all_keys(minio_s3_client, staging_test_bucket, STAGING_PREFIX)
        assert any("raw_data/" in k for k in staged_keys), (
            f"No raw_data/ files found in staging bucket under {STAGING_PREFIX}"
        )
        assert any(STABLE_PDB_ID in k for k in staged_keys), (
            f"Expected {STABLE_PDB_ID} in staged keys"
        )
        assert any(k.endswith(".crc64nvme") for k in staged_keys), (
            "Expected .crc64nvme sidecar files in staging"
        )

        # ── Phase 3: promote ─────────────────────────────────────────────
        promote_report = promote_from_s3(
            staging_key_prefix=STAGING_PREFIX,
            staging_bucket=staging_test_bucket,
            lakehouse_bucket=test_bucket,
            lakehouse_key_prefix=LAKEHOUSE_PREFIX,
            dry_run=False,
        )

        assert promote_report["promoted"] > 0, f"No files promoted: {promote_report}"
        assert promote_report["failed"] == 0, f"Promote had failures: {promote_report}"

        # Verify files are in the Lakehouse bucket at the expected path
        entry_rel = build_entry_path(STABLE_PDB_ID)
        lakehouse_prefix = f"{LAKEHOUSE_PREFIX}raw_data/"
        lakehouse_keys = list_all_keys(minio_s3_client, test_bucket, lakehouse_prefix)

        assert len(lakehouse_keys) > 0, f"No files in Lakehouse under {lakehouse_prefix}"
        assert any(STABLE_PDB_ID in k for k in lakehouse_keys), (
            f"Expected {STABLE_PDB_ID} in Lakehouse keys: {lakehouse_keys}"
        )
        assert any(entry_rel in k for k in lakehouse_keys), (
            f"Expected entry path {entry_rel} in Lakehouse"
        )

        # Verify frictionless descriptor was written
        descriptor_keys = list_all_keys(minio_s3_client, test_bucket, f"{LAKEHOUSE_PREFIX}metadata/")
        assert any(STABLE_PDB_ID in k for k in descriptor_keys), (
            f"Expected frictionless descriptor for {STABLE_PDB_ID} in metadata/"
        )

        # Verify staging bucket is cleaned up (promote deletes staged files)
        remaining = list_all_keys(minio_s3_client, staging_test_bucket, STAGING_PREFIX + "raw_data/")
        assert len(remaining) == 0, (
            f"Staging files not cleaned up after promote: {remaining}"
        )


# ── Option B: container/CTS (download_batch + manual upload) ────────────


@pytest.mark.integration
@pytest.mark.slow_test
@pytest.mark.external_request
class TestPdbFullPipelineContainer:
    """Full pipeline using the container approach (Phase 2 = download_batch + upload).

    Phase 1: manifest written directly to disk.
    Phase 2a: ``download_batch()`` — the same code that runs inside the
              ``pdb_rsync_sync`` CTS container.  Output lands on local disk.
    Phase 2b: ``stage_files_to_minio()`` — upload local output to MinIO staging
              (CTS does this automatically in production).
    Phase 3: ``promote_from_s3()`` — promotes from staging bucket to Lakehouse.
    """

    def test_full_pipeline_container(
        self,
        minio_s3_client: botocore.client.BaseClient,
        staging_test_bucket: str,
        test_bucket: str,
        tmp_path: Path,
    ) -> None:
        """Single entry flows through all three phases (container approach)."""
        # ── Phase 1: write manifest ──────────────────────────────────────
        manifest_path = _write_manifest(tmp_path)

        # ── Phase 2a: download_batch (CTS container equivalent) ──────────
        # Downloads to local disk exactly as the pdb_rsync_sync container does.
        output_dir = tmp_path / "output"
        output_dir.mkdir()

        dl_report = download_batch(
            manifest_path=str(manifest_path),
            output_dir=str(output_dir),
            workers=1,
            file_types=["structures"],  # smallest file-type category
            limit=1,
        )

        assert dl_report["succeeded"] >= 1, f"Phase 2 download failed: {dl_report}"
        assert dl_report["failed"] == 0, f"Phase 2 had failures: {dl_report['failures']}"

        entry_rel = build_entry_path(STABLE_PDB_ID)
        entry_dir = output_dir / entry_rel
        assert entry_dir.exists(), f"Expected entry dir at {entry_dir}"

        data_files = [p for p in entry_dir.rglob("*") if p.is_file() and not p.name.endswith(".crc64nvme")]
        sidecar_files = [p for p in entry_dir.rglob("*.crc64nvme")]
        assert len(data_files) > 0, "No data files downloaded"
        assert len(sidecar_files) == len(data_files), (
            f"Sidecar count mismatch: {len(data_files)} data, {len(sidecar_files)} sidecars"
        )

        # ── Phase 2b: upload to MinIO staging ────────────────────────────
        # In production CTS handles this; here we do it manually.
        staged_keys = stage_files_to_minio(
            minio_s3_client,
            staging_test_bucket,
            output_dir,
            STAGING_PREFIX,
        )
        assert len(staged_keys) > 0, "No files uploaded to MinIO staging"
        assert any("raw_data/" in k for k in staged_keys), (
            "Expected raw_data/ files in staging"
        )
        assert any(k.endswith(".crc64nvme") for k in staged_keys), (
            "Expected .crc64nvme sidecar files in staging"
        )

        # ── Phase 3: promote ─────────────────────────────────────────────
        promote_report = promote_from_s3(
            staging_key_prefix=STAGING_PREFIX,
            staging_bucket=staging_test_bucket,
            lakehouse_bucket=test_bucket,
            lakehouse_key_prefix=LAKEHOUSE_PREFIX,
            dry_run=False,
        )

        assert promote_report["promoted"] > 0, f"No files promoted: {promote_report}"
        assert promote_report["failed"] == 0, f"Promote had failures: {promote_report}"

        # Verify files are in the Lakehouse bucket at the expected path
        lakehouse_prefix = f"{LAKEHOUSE_PREFIX}raw_data/"
        lakehouse_keys = list_all_keys(minio_s3_client, test_bucket, lakehouse_prefix)

        assert len(lakehouse_keys) > 0, f"No files in Lakehouse under {lakehouse_prefix}"
        assert any(STABLE_PDB_ID in k for k in lakehouse_keys), (
            f"Expected {STABLE_PDB_ID} in Lakehouse keys: {lakehouse_keys}"
        )
        assert any(entry_rel in k for k in lakehouse_keys), (
            f"Expected entry path {entry_rel} in Lakehouse"
        )

        # Verify frictionless descriptor was written
        descriptor_keys = list_all_keys(minio_s3_client, test_bucket, f"{LAKEHOUSE_PREFIX}metadata/")
        assert any(STABLE_PDB_ID in k for k in descriptor_keys), (
            f"Expected frictionless descriptor for {STABLE_PDB_ID} in metadata/"
        )

        # Verify staging bucket is cleaned up (promote deletes staged files)
        remaining = list_all_keys(minio_s3_client, staging_test_bucket, STAGING_PREFIX + "raw_data/")
        assert len(remaining) == 0, (
            f"Staging files not cleaned up after promote: {remaining}"
        )
