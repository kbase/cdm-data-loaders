"""End-to-end tests for PDB Phase 3 — promote and archive in MinIO.

Pre-stages fake PDB entry files in MinIO and exercises ``promote_from_s3``
with various combinations of manifests, archive operations, dry-run mode,
and manifest trimming.

Marked ``integration`` and ``slow_test``; auto-skipped when MinIO is
unreachable.  Each test method gets its own bucket.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pytest

from cdm_data_loaders.pdb.entry import DEFAULT_LAKEHOUSE_KEY_PREFIX, build_entry_path
from cdm_data_loaders.pdb.metadata import (
    build_archive_descriptor_key,
    build_descriptor_key,
    create_descriptor,
)
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
        assert report["promoted"] >= 12  # noqa: PLR2004  # 4 files per entry x 3 entries

        final_keys = list_all_keys(s3, test_bucket, PATH_PREFIX + "raw_data/")
        for pdb_id in [PDB_ID_A, PDB_ID_B, PDB_ID_C]:
            assert any(pdb_id in k for k in final_keys), f"{pdb_id} missing from Lakehouse"


# ── Descriptor integration tests ────────────────────────────────────────


@pytest.mark.integration
@pytest.mark.slow_test
class TestPdbPromoteCreatesDescriptor:
    """Promote step writes a frictionless descriptor for each promoted PDB entry."""

    def test_descriptor_created(self, minio_s3_client: object, test_bucket: str) -> None:
        """After promote, a JSON descriptor exists under ``metadata/``."""
        s3 = minio_s3_client
        _stage_entry(s3, test_bucket, PDB_ID_A)

        promote_from_s3(
            staging_key_prefix=STAGING_PREFIX,
            bucket=test_bucket,
            lakehouse_key_prefix=PATH_PREFIX,
        )

        descriptor_key = build_descriptor_key(PDB_ID_A, PATH_PREFIX)
        obj = s3.get_object(Bucket=test_bucket, Key=descriptor_key)
        body = json.loads(obj["Body"].read())

        assert body["identifier"] == f"PDB:{PDB_ID_A}"
        assert body["resource_type"] == "dataset"

    def test_descriptor_at_correct_key(self, minio_s3_client: object, test_bucket: str) -> None:
        """Descriptor is stored exactly at ``{prefix}metadata/{pdb_id}_datapackage.json``."""
        s3 = minio_s3_client
        _stage_entry(s3, test_bucket, PDB_ID_A)

        promote_from_s3(
            staging_key_prefix=STAGING_PREFIX,
            bucket=test_bucket,
            lakehouse_key_prefix=PATH_PREFIX,
        )

        expected_key = build_descriptor_key(PDB_ID_A, PATH_PREFIX)
        metadata_keys = list_all_keys(s3, test_bucket, PATH_PREFIX + "metadata/")
        assert expected_key in metadata_keys, f"Expected {expected_key!r} in {metadata_keys}"

    def test_descriptor_resources_reference_lakehouse_paths(self, minio_s3_client: object, test_bucket: str) -> None:
        """Each resource ``path`` in the descriptor is a final Lakehouse key."""
        s3 = minio_s3_client
        _stage_entry(s3, test_bucket, PDB_ID_A)

        promote_from_s3(
            staging_key_prefix=STAGING_PREFIX,
            bucket=test_bucket,
            lakehouse_key_prefix=PATH_PREFIX,
        )

        descriptor_key = build_descriptor_key(PDB_ID_A, PATH_PREFIX)
        body = json.loads(s3.get_object(Bucket=test_bucket, Key=descriptor_key)["Body"].read())

        resource_paths = [r["path"] for r in body["resources"]]
        assert len(resource_paths) >= 4  # noqa: PLR2004  # one per file-type dir
        for path in resource_paths:
            assert path.startswith(PATH_PREFIX + "raw_data/"), (
                f"Resource path {path!r} does not start with Lakehouse raw_data prefix"
            )
            assert PDB_ID_A in path

    def test_multiple_entries_get_separate_descriptors(self, minio_s3_client: object, test_bucket: str) -> None:
        """Each promoted entry gets its own descriptor file."""
        s3 = minio_s3_client
        _stage_entry(s3, test_bucket, PDB_ID_A)
        _stage_entry(s3, test_bucket, PDB_ID_B)

        promote_from_s3(
            staging_key_prefix=STAGING_PREFIX,
            bucket=test_bucket,
            lakehouse_key_prefix=PATH_PREFIX,
        )

        for pdb_id in [PDB_ID_A, PDB_ID_B]:
            key = build_descriptor_key(pdb_id, PATH_PREFIX)
            obj = s3.get_object(Bucket=test_bucket, Key=key)
            body = json.loads(obj["Body"].read())
            assert body["identifier"] == f"PDB:{pdb_id}"

    def test_descriptor_is_valid_json(self, minio_s3_client: object, test_bucket: str) -> None:
        """Descriptor is well-formed JSON with expected top-level fields."""
        s3 = minio_s3_client
        _stage_entry(s3, test_bucket, PDB_ID_A)

        promote_from_s3(
            staging_key_prefix=STAGING_PREFIX,
            bucket=test_bucket,
            lakehouse_key_prefix=PATH_PREFIX,
        )

        descriptor_key = build_descriptor_key(PDB_ID_A, PATH_PREFIX)
        body = json.loads(s3.get_object(Bucket=test_bucket, Key=descriptor_key)["Body"].read())

        for field in ("identifier", "resource_type", "version", "titles", "resources", "meta"):
            assert field in body, f"Expected field {field!r} missing from descriptor"


@pytest.mark.integration
@pytest.mark.slow_test
class TestPdbPromoteArchiveUpdatedIncludesDescriptor:
    """Archiving updated entries also archives the existing descriptor."""

    def test_archive_copies_descriptor(self, minio_s3_client: object, test_bucket: str, tmp_path: Path) -> None:
        """After archiving an updated entry, the descriptor appears under archive/."""
        s3 = minio_s3_client

        # Seed an old version of the entry at the Lakehouse path
        seed_pdb_entry(s3, test_bucket, PDB_ID_A, {"structures/old.cif.gz": b"old data"}, PATH_PREFIX)

        # Pre-upload a live descriptor so archive_descriptor can find and copy it
        descriptor = create_descriptor(PDB_ID_A, [])
        descriptor_key = build_descriptor_key(PDB_ID_A, PATH_PREFIX)
        s3.put_object(Bucket=test_bucket, Key=descriptor_key, Body=json.dumps(descriptor).encode())

        # Stage a new version and promote with updated manifest
        _stage_entry(s3, test_bucket, PDB_ID_A)
        updated_manifest = _write_manifest(tmp_path, [PDB_ID_A], "updated_manifest.txt")

        promote_from_s3(
            staging_key_prefix=STAGING_PREFIX,
            bucket=test_bucket,
            updated_manifest_path=str(updated_manifest),
            pdb_release="2024-04-01",
            lakehouse_key_prefix=PATH_PREFIX,
        )

        archive_key = build_archive_descriptor_key(PDB_ID_A, "2024-04-01", PATH_PREFIX)
        resp = s3.head_object(Bucket=test_bucket, Key=archive_key)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200  # noqa: PLR2004

    def test_archive_descriptor_metadata(self, minio_s3_client: object, test_bucket: str, tmp_path: Path) -> None:
        """Archived descriptor carries the expected archive_reason metadata."""
        s3 = minio_s3_client

        seed_pdb_entry(s3, test_bucket, PDB_ID_A, {"structures/old.cif.gz": b"old data"}, PATH_PREFIX)
        descriptor = create_descriptor(PDB_ID_A, [])
        s3.put_object(
            Bucket=test_bucket,
            Key=build_descriptor_key(PDB_ID_A, PATH_PREFIX),
            Body=json.dumps(descriptor).encode(),
        )

        _stage_entry(s3, test_bucket, PDB_ID_A)
        updated_manifest = _write_manifest(tmp_path, [PDB_ID_A], "updated_manifest.txt")

        promote_from_s3(
            staging_key_prefix=STAGING_PREFIX,
            bucket=test_bucket,
            updated_manifest_path=str(updated_manifest),
            pdb_release="2024-04-01",
            lakehouse_key_prefix=PATH_PREFIX,
        )

        archive_key = build_archive_descriptor_key(PDB_ID_A, "2024-04-01", PATH_PREFIX)
        meta = get_object_metadata(s3, test_bucket, archive_key)
        assert meta.get("archive_reason") == "updated"
        assert meta.get("pdb_last_release") == "2024-04-01"

    def test_promote_overwrites_descriptor_with_new_version(
        self, minio_s3_client: object, test_bucket: str, tmp_path: Path
    ) -> None:
        """After archiving, the live descriptor is replaced with a new one for the promoted files."""
        s3 = minio_s3_client

        # Seed old version with a live descriptor
        seed_pdb_entry(s3, test_bucket, PDB_ID_A, {"structures/old.cif.gz": b"old data"}, PATH_PREFIX)
        old_descriptor = create_descriptor(PDB_ID_A, [])
        s3.put_object(
            Bucket=test_bucket,
            Key=build_descriptor_key(PDB_ID_A, PATH_PREFIX),
            Body=json.dumps(old_descriptor).encode(),
        )

        # Stage new version
        _stage_entry(s3, test_bucket, PDB_ID_A)
        updated_manifest = _write_manifest(tmp_path, [PDB_ID_A], "updated_manifest.txt")

        promote_from_s3(
            staging_key_prefix=STAGING_PREFIX,
            bucket=test_bucket,
            updated_manifest_path=str(updated_manifest),
            pdb_release="2024-04-01",
            lakehouse_key_prefix=PATH_PREFIX,
        )

        # Live descriptor is replaced with a new one listing the promoted resources
        live_key = build_descriptor_key(PDB_ID_A, PATH_PREFIX)
        body = json.loads(s3.get_object(Bucket=test_bucket, Key=live_key)["Body"].read())
        assert len(body["resources"]) >= 4  # noqa: PLR2004  # new promote wrote real resources


@pytest.mark.integration
@pytest.mark.slow_test
class TestPdbPromoteArchiveRemovedIncludesDescriptor:
    """Archiving obsoleted entries also archives the descriptor."""

    def test_archive_copies_descriptor(self, minio_s3_client: object, test_bucket: str, tmp_path: Path) -> None:
        """After obsoleting an entry, the descriptor appears under archive/."""
        s3 = minio_s3_client

        seed_pdb_entry(s3, test_bucket, PDB_ID_A, {"structures/old.cif.gz": b"old data"}, PATH_PREFIX)
        descriptor = create_descriptor(PDB_ID_A, [])
        s3.put_object(
            Bucket=test_bucket,
            Key=build_descriptor_key(PDB_ID_A, PATH_PREFIX),
            Body=json.dumps(descriptor).encode(),
        )

        removed_manifest = _write_manifest(tmp_path, [PDB_ID_A], "removed_manifest.txt")

        promote_from_s3(
            staging_key_prefix=STAGING_PREFIX,
            bucket=test_bucket,
            removed_manifest_path=str(removed_manifest),
            pdb_release="2024-04-01",
            lakehouse_key_prefix=PATH_PREFIX,
        )

        archive_key = build_archive_descriptor_key(PDB_ID_A, "2024-04-01", PATH_PREFIX)
        resp = s3.head_object(Bucket=test_bucket, Key=archive_key)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200  # noqa: PLR2004

    def test_archive_descriptor_has_obsoleted_reason(
        self, minio_s3_client: object, test_bucket: str, tmp_path: Path
    ) -> None:
        """Archived descriptor for an obsoleted entry carries archive_reason=obsoleted."""
        s3 = minio_s3_client

        seed_pdb_entry(s3, test_bucket, PDB_ID_A, {"structures/old.cif.gz": b"old data"}, PATH_PREFIX)
        descriptor = create_descriptor(PDB_ID_A, [])
        s3.put_object(
            Bucket=test_bucket,
            Key=build_descriptor_key(PDB_ID_A, PATH_PREFIX),
            Body=json.dumps(descriptor).encode(),
        )

        removed_manifest = _write_manifest(tmp_path, [PDB_ID_A], "removed_manifest.txt")

        promote_from_s3(
            staging_key_prefix=STAGING_PREFIX,
            bucket=test_bucket,
            removed_manifest_path=str(removed_manifest),
            pdb_release="2024-04-01",
            lakehouse_key_prefix=PATH_PREFIX,
        )

        archive_key = build_archive_descriptor_key(PDB_ID_A, "2024-04-01", PATH_PREFIX)
        meta = get_object_metadata(s3, test_bucket, archive_key)
        assert meta.get("archive_reason") == "obsoleted"
        assert meta.get("pdb_last_release") == "2024-04-01"

    def test_no_descriptor_to_archive_is_handled_gracefully(
        self, minio_s3_client: object, test_bucket: str, tmp_path: Path
    ) -> None:
        """Missing live descriptor does not cause the archive step to fail."""
        s3 = minio_s3_client

        # Seed raw files only — no descriptor
        seed_pdb_entry(s3, test_bucket, PDB_ID_A, {"structures/old.cif.gz": b"old data"}, PATH_PREFIX)

        removed_manifest = _write_manifest(tmp_path, [PDB_ID_A], "removed_manifest.txt")

        report = promote_from_s3(
            staging_key_prefix=STAGING_PREFIX,
            bucket=test_bucket,
            removed_manifest_path=str(removed_manifest),
            pdb_release="2024-04-01",
            lakehouse_key_prefix=PATH_PREFIX,
        )

        assert report["failed"] == 0
        # No descriptor archive key should exist (nothing to copy from)
        archive_key = build_archive_descriptor_key(PDB_ID_A, "2024-04-01", PATH_PREFIX)
        archive_keys = list_all_keys(s3, test_bucket, PATH_PREFIX + "archive/")
        assert archive_key not in archive_keys


@pytest.mark.integration
@pytest.mark.slow_test
class TestPdbPromoteDryRunNoDescriptor:
    """Dry-run must not write or archive any descriptor files."""

    def test_dry_run_no_descriptor(self, minio_s3_client: object, test_bucket: str) -> None:
        """Dry-run logs actions but does not upload a descriptor."""
        s3 = minio_s3_client
        _stage_entry(s3, test_bucket, PDB_ID_A)

        promote_from_s3(
            staging_key_prefix=STAGING_PREFIX,
            bucket=test_bucket,
            lakehouse_key_prefix=PATH_PREFIX,
            dry_run=True,
        )

        metadata_keys = list_all_keys(s3, test_bucket, PATH_PREFIX + "metadata/")
        assert len(metadata_keys) == 0, f"Dry-run should not create descriptor files, found: {metadata_keys}"

    def test_dry_run_no_archive_descriptor(
        self, minio_s3_client: object, test_bucket: str, tmp_path: Path
    ) -> None:
        """Dry-run archive step does not copy a descriptor even when one exists."""
        s3 = minio_s3_client

        seed_pdb_entry(s3, test_bucket, PDB_ID_A, {"structures/old.cif.gz": b"old data"}, PATH_PREFIX)
        descriptor = create_descriptor(PDB_ID_A, [])
        s3.put_object(
            Bucket=test_bucket,
            Key=build_descriptor_key(PDB_ID_A, PATH_PREFIX),
            Body=json.dumps(descriptor).encode(),
        )

        updated_manifest = _write_manifest(tmp_path, [PDB_ID_A], "updated_manifest.txt")

        promote_from_s3(
            staging_key_prefix=STAGING_PREFIX,
            bucket=test_bucket,
            updated_manifest_path=str(updated_manifest),
            pdb_release="2024-04-01",
            lakehouse_key_prefix=PATH_PREFIX,
            dry_run=True,
        )

        archive_keys = list_all_keys(s3, test_bucket, PATH_PREFIX + "archive/")
        assert len(archive_keys) == 0, f"Dry-run should not create archive objects, found: {archive_keys}"
