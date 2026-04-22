"""End-to-end tests for PDB Phase 3 — promote and archive in MinIO.

Pre-stages fake PDB entry files in MinIO and exercises ``promote_from_s3``
with various combinations of manifests, archive operations, dry-run mode,
and manifest trimming.

Marked ``integration`` and ``slow_test``; auto-skipped when MinIO is
unreachable.  Each test method gets its own bucket.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from cdm_data_loaders.pdb.entry import DEFAULT_LAKEHOUSE_KEY_PREFIX, build_entry_path
from cdm_data_loaders.pdb.promote import promote_from_s3

from .conftest import get_object_metadata, list_all_keys, seed_pdb_entry

if TYPE_CHECKING:
    from pathlib import Path

# Fake PDB entry details used across tests
PDB_ID_A = "pdb_00001abc"
PDB_ID_B = "pdb_00002def"
PDB_ID_C = "pdb_00003456"

STAGING_PREFIX = "staging/pdb-run1/"
PATH_PREFIX = DEFAULT_LAKEHOUSE_KEY_PREFIX

# Fake file contents staged per file-type subdirectory.
# structures/ (coordinates) is the primary type; the others are included so
# the promote logic sees a realistic entry layout.
FAKE_CIF          = b"data_\n_entry.id pdb_00001abc\n"
FAKE_SF           = b"data_r1abcsf\n_diffrn.id D1\n"
FAKE_VALIDATION   = b"%PDF-1.4 fake validation report\n"
FAKE_ASSEMBLY     = b"data_assembly1\n_pdbx_struct_assembly.id 1\n"


def _stage_entry(
    s3: object,
    bucket: str,
    pdb_id: str,
    extra_files: dict[str, bytes] | None = None,
) -> None:
    """Stage fake PDB entry files with .crc64nvme sidecars under the staging prefix.

    One representative file is staged per file-type subdirectory so that
    promote tests exercise all four content categories (coordinates,
    experimental data, validation reports, assemblies).
    """
    rel = build_entry_path(pdb_id)
    base = f"{STAGING_PREFIX}{rel}"

    files: dict[str, bytes] = {
        f"structures/{pdb_id}.cif.gz":                            FAKE_CIF,
        f"experimental_data/{pdb_id}-sf.cif.gz":                 FAKE_SF,
        f"validation_reports/{pdb_id}_validation.pdf.gz":         FAKE_VALIDATION,
        f"assemblies/{pdb_id}-assembly1.cif.gz":                  FAKE_ASSEMBLY,
        **(extra_files or {}),
    }

    for relname, content in files.items():
        key = f"{base}{relname}"
        s3.put_object(Bucket=bucket, Key=key, Body=content)
        # Write .crc64nvme sidecar (fake checksum value — just needs to be present)
        sidecar_key = f"{key}.crc64nvme"
        s3.put_object(Bucket=bucket, Key=sidecar_key, Body=b"AAAAAAAAAAAAAAAA==")


def _write_manifest(tmp_path: Path, pdb_ids: list[str], name: str) -> Path:
    """Write a manifest file (one PDB ID per line)."""
    path = tmp_path / name
    path.write_text("\n".join(pdb_ids) + "\n")
    return path


# ── Tests ───────────────────────────────────────────────────────────────


@pytest.mark.integration
@pytest.mark.slow_test
class TestPdbPromoteFromStaging:
    """Promote staged PDB files to final Lakehouse paths."""

    def test_pdb_promote_from_staging(self, minio_s3_client: object, test_bucket: str) -> None:
        """Staged files appear at the final Lakehouse path."""
        s3 = minio_s3_client
        _stage_entry(s3, test_bucket, PDB_ID_A)

        report = promote_from_s3(
            staging_key_prefix=STAGING_PREFIX,
            bucket=test_bucket,
            lakehouse_key_prefix=PATH_PREFIX,
        )

        assert report["promoted"] >= 4  # noqa: PLR2004  # one file per file-type subdir
        assert report["failed"] == 0
        assert report["dry_run"] is False

        # Verify files landed at the final Lakehouse path
        final_keys = list_all_keys(s3, test_bucket, PATH_PREFIX + "raw_data/")
        assert any(PDB_ID_A in k for k in final_keys), f"PDB_ID_A not found in final keys: {final_keys}"
        # Sidecars should NOT be promoted
        assert not any(k.endswith(".crc64nvme") for k in final_keys), "Sidecar files should not be promoted"


@pytest.mark.integration
@pytest.mark.slow_test
class TestPdbPromoteIdempotent:
    """Promoting the same staging data twice should succeed without errors."""

    def test_pdb_promote_idempotent(self, minio_s3_client: object, test_bucket: str) -> None:
        """Second promote succeeds and produces the same final state."""
        s3 = minio_s3_client
        _stage_entry(s3, test_bucket, PDB_ID_A)

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
class TestPdbPromoteArchiveUpdated:
    """Archive existing entries before overwriting with updated versions."""

    def test_pdb_archive_updated(self, minio_s3_client: object, test_bucket: str, tmp_path: Path) -> None:
        """Updated entries are archived before being overwritten."""
        s3 = minio_s3_client

        # Seed "old" version at the final Lakehouse path
        old_files = {
            "structures/old.cif.gz": b"old structure data",
        }
        seed_pdb_entry(s3, test_bucket, PDB_ID_A, old_files, PATH_PREFIX)

        # Stage "new" version
        _stage_entry(s3, test_bucket, PDB_ID_A)

        updated_manifest = _write_manifest(tmp_path, [PDB_ID_A], "updated_manifest.txt")

        report = promote_from_s3(
            staging_key_prefix=STAGING_PREFIX,
            bucket=test_bucket,
            updated_manifest_path=str(updated_manifest),
            pdb_release="2024-04-01",
            lakehouse_key_prefix=PATH_PREFIX,
        )

        assert report["archived"] >= 1
        assert report["promoted"] >= 4  # noqa: PLR2004
        assert report["failed"] == 0

        # Verify archive exists
        archive_keys = list_all_keys(s3, test_bucket, PATH_PREFIX + "archive/2024-04-01/")
        assert len(archive_keys) >= 1, f"Expected archive objects, found: {archive_keys}"

        # Verify archive metadata
        for key in archive_keys:
            meta = get_object_metadata(s3, test_bucket, key)
            assert meta.get("archive_reason") == "updated"
            assert meta.get("pdb_last_release") == "2024-04-01"


@pytest.mark.integration
@pytest.mark.slow_test
class TestPdbPromoteArchiveRemoved:
    """Archive and delete obsoleted entries."""

    def test_pdb_archive_removed(self, minio_s3_client: object, test_bucket: str, tmp_path: Path) -> None:
        """Obsoleted entries are archived and source objects are deleted."""
        s3 = minio_s3_client

        # Seed entry at final path
        seed_pdb_entry(s3, test_bucket, PDB_ID_A, {"structures/old.cif.gz": b"old data"}, PATH_PREFIX)

        removed_manifest = _write_manifest(tmp_path, [PDB_ID_A], "removed_manifest.txt")

        report = promote_from_s3(
            staging_key_prefix=STAGING_PREFIX,
            bucket=test_bucket,
            removed_manifest_path=str(removed_manifest),
            pdb_release="2024-04-01",
            lakehouse_key_prefix=PATH_PREFIX,
        )

        assert report["archived"] >= 1
        assert report["failed"] == 0

        # Verify archive exists
        archive_keys = list_all_keys(s3, test_bucket, PATH_PREFIX + "archive/2024-04-01/")
        assert len(archive_keys) >= 1

        # Verify archive metadata
        for key in archive_keys:
            meta = get_object_metadata(s3, test_bucket, key)
            assert meta.get("archive_reason") == "obsoleted"

        # Verify source objects are deleted
        source_keys = list_all_keys(s3, test_bucket, PATH_PREFIX + build_entry_path(PDB_ID_A))
        assert len(source_keys) == 0, f"Expected source objects deleted, found: {source_keys}"


@pytest.mark.integration
@pytest.mark.slow_test
class TestPdbPromoteDryRun:
    """Dry-run mode should not create any objects."""

    def test_pdb_promote_dry_run(self, minio_s3_client: object, test_bucket: str) -> None:
        """Dry-run logs actions but creates no objects at the final path."""
        s3 = minio_s3_client
        _stage_entry(s3, test_bucket, PDB_ID_A)

        report = promote_from_s3(
            staging_key_prefix=STAGING_PREFIX,
            bucket=test_bucket,
            lakehouse_key_prefix=PATH_PREFIX,
            dry_run=True,
        )

        assert report["dry_run"] is True
        assert report["promoted"] >= 1

        final_keys = list_all_keys(s3, test_bucket, PATH_PREFIX + "raw_data/")
        assert len(final_keys) == 0, f"Dry-run should not create objects, found: {final_keys}"


@pytest.mark.integration
@pytest.mark.slow_test
class TestPdbPromoteTrimsManifest:
    """Manifest trimming removes promoted PDB IDs."""

    def test_pdb_trims_manifest(self, minio_s3_client: object, test_bucket: str, tmp_path: Path) -> None:
        """Transfer manifest in MinIO is trimmed to exclude promoted PDB IDs."""
        s3 = minio_s3_client

        manifest_key = "pdb/transfer_manifest.txt"
        manifest_content = f"{PDB_ID_A}\n{PDB_ID_B}\n{PDB_ID_C}\n"
        s3.put_object(Bucket=test_bucket, Key=manifest_key, Body=manifest_content.encode())

        # Stage only A and B (not C)
        _stage_entry(s3, test_bucket, PDB_ID_A)
        _stage_entry(s3, test_bucket, PDB_ID_B)

        report = promote_from_s3(
            staging_key_prefix=STAGING_PREFIX,
            bucket=test_bucket,
            manifest_s3_key=manifest_key,
            lakehouse_key_prefix=PATH_PREFIX,
        )

        assert report["failed"] == 0

        resp = s3.get_object(Bucket=test_bucket, Key=manifest_key)
        remaining = [line.strip() for line in resp["Body"].read().decode().splitlines() if line.strip()]

        # Only C should remain
        assert len(remaining) == 1, f"Expected 1 remaining entry, got {len(remaining)}: {remaining}"
        assert remaining[0] == PDB_ID_C


@pytest.mark.integration
@pytest.mark.slow_test
class TestPdbPromoteOnlySidecars:
    """Staging with only .crc64nvme sidecars should not promote data files."""

    def test_pdb_only_sidecars_staged(self, minio_s3_client: object, test_bucket: str) -> None:
        """Only .crc64nvme sidecars staged → nothing promoted."""
        s3 = minio_s3_client

        rel = build_entry_path(PDB_ID_A)
        base = f"{STAGING_PREFIX}{rel}"
        sidecar_key = f"{base}structures/{PDB_ID_A}.cif.gz.crc64nvme"
        s3.put_object(Bucket=test_bucket, Key=sidecar_key, Body=b"AAAAAAAAAAAAAAAA==")

        report = promote_from_s3(
            staging_key_prefix=STAGING_PREFIX,
            bucket=test_bucket,
            lakehouse_key_prefix=PATH_PREFIX,
        )

        assert report["promoted"] == 0
        assert report["failed"] == 0

        final_keys = list_all_keys(s3, test_bucket, PATH_PREFIX + "raw_data/")
        assert len(final_keys) == 0


@pytest.mark.integration
@pytest.mark.slow_test
class TestPdbPromoteMultipleEntries:
    """Multiple entries staged together all get promoted."""

    def test_pdb_multiple_entries(self, minio_s3_client: object, test_bucket: str) -> None:
        """All staged entries are promoted to the correct hash-partitioned paths."""
        s3 = minio_s3_client
        _stage_entry(s3, test_bucket, PDB_ID_A)
        _stage_entry(s3, test_bucket, PDB_ID_B)
        _stage_entry(s3, test_bucket, PDB_ID_C)

        report = promote_from_s3(
            staging_key_prefix=STAGING_PREFIX,
            bucket=test_bucket,
            lakehouse_key_prefix=PATH_PREFIX,
        )

        assert report["failed"] == 0
        assert report["promoted"] >= 12  # noqa: PLR2004  # 4 files per entry × 3 entries

        final_keys = list_all_keys(s3, test_bucket, PATH_PREFIX + "raw_data/")
        for pdb_id in [PDB_ID_A, PDB_ID_B, PDB_ID_C]:
            assert any(pdb_id in k for k in final_keys), f"{pdb_id} missing from Lakehouse"
