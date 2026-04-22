"""Tests for pdb.promote module — archive, trim, promote logic."""

from pathlib import Path
from unittest.mock import patch

import pytest

import cdm_data_loaders.pdb.promote as promote_mod
import cdm_data_loaders.utils.s3 as s3_utils
from cdm_data_loaders.pdb.promote import _archive_entries, _trim_manifest, promote_from_s3

from .conftest import TEST_BUCKET

_LAKEHOUSE_PREFIX = "tenant-general-warehouse/kbase/datasets/pdb/"


def _put(client: object, key: str, body: bytes = b"data") -> None:
    client.put_object(Bucket=TEST_BUCKET, Key=key, Body=body)  # type: ignore[attr-defined]


# ── _archive_entries ─────────────────────────────────────────────────────


class TestArchiveEntries:
    """Test archiving of existing Lakehouse objects."""

    def test_copies_to_archive_prefix(self, mock_s3_client: object, tmp_path: Path) -> None:
        """Verify objects are copied to archive/ with metadata."""
        client = mock_s3_client
        key = f"{_LAKEHOUSE_PREFIX}raw_data/ab/pdb_00001abc/structures/file.cif.gz"
        _put(client, key)

        manifest_path = tmp_path / "removed.txt"
        manifest_path.write_text("pdb_00001abc\n")

        with patch.object(promote_mod, "get_s3_client", return_value=client):
            with patch.object(promote_mod, "copy_object_with_metadata") as mock_copy:
                _archive_entries(
                    str(manifest_path),
                    bucket=TEST_BUCKET,
                    pdb_release="2024-04-01",
                    lakehouse_key_prefix=_LAKEHOUSE_PREFIX,
                    archive_reason="obsoleted",
                    delete_source=False,
                )
        mock_copy.assert_called_once()
        _, dest = mock_copy.call_args[0]
        assert "archive/2024-04-01" in dest

    def test_dry_run_does_not_copy(self, mock_s3_client: object, tmp_path: Path) -> None:
        """Verify dry_run skips actual copy calls."""
        client = mock_s3_client
        key = f"{_LAKEHOUSE_PREFIX}raw_data/ab/pdb_00001abc/structures/file.cif.gz"
        _put(client, key)

        manifest_path = tmp_path / "removed.txt"
        manifest_path.write_text("pdb_00001abc\n")

        with patch.object(promote_mod, "get_s3_client", return_value=client):
            with patch.object(promote_mod, "copy_object_with_metadata") as mock_copy:
                _archive_entries(str(manifest_path), bucket=TEST_BUCKET, dry_run=True)
        mock_copy.assert_not_called()

    def test_invalid_pdb_id_skipped(self, mock_s3_client: object, tmp_path: Path) -> None:
        """Verify entries with invalid PDB IDs are skipped gracefully."""
        client = mock_s3_client
        manifest_path = tmp_path / "removed.txt"
        manifest_path.write_text("not_a_pdb_id\n")

        with patch.object(promote_mod, "get_s3_client", return_value=client):
            count = _archive_entries(str(manifest_path), bucket=TEST_BUCKET)
        assert count == 0


# ── _trim_manifest ───────────────────────────────────────────────────────


class TestTrimManifest:
    """Test trimming promoted IDs from the manifest in S3."""

    def test_trims_promoted_ids(self, mock_s3_client: object) -> None:
        """Verify promoted IDs are removed from the manifest."""
        client = mock_s3_client
        manifest_key = "staging/run/transfer_manifest.txt"
        manifest_content = "pdb_00001abc\npdb_00002def\npdb_00003ghi\n"
        client.put_object(Bucket=TEST_BUCKET, Key=manifest_key, Body=manifest_content.encode())

        with patch.object(promote_mod, "get_s3_client", return_value=client):
            _trim_manifest(manifest_key, TEST_BUCKET, {"pdb_00001abc", "pdb_00002def"})

        obj = client.get_object(Bucket=TEST_BUCKET, Key=manifest_key)
        remaining = obj["Body"].read().decode()
        assert "pdb_00001abc" not in remaining
        assert "pdb_00002def" not in remaining
        assert "pdb_00003ghi" in remaining

    def test_missing_manifest_is_handled(self, mock_s3_client: object) -> None:
        """Verify missing manifest key does not raise."""
        client = mock_s3_client
        with patch.object(promote_mod, "get_s3_client", return_value=client):
            # Should not raise
            _trim_manifest("nonexistent/key.txt", TEST_BUCKET, {"pdb_00001abc"})


# ── promote_from_s3 ──────────────────────────────────────────────────────


class TestPromoteFromS3:
    """Test the main promote_from_s3 function."""

    def test_dry_run_returns_report(self, mock_s3_client: object) -> None:
        """Verify dry_run returns a valid report without moving files."""
        client = mock_s3_client
        staging_prefix = "staging/run123/"
        data_key = f"{staging_prefix}raw_data/ab/pdb_00001abc/structures/file.cif.gz"
        _put(client, data_key)

        with (
            patch.object(promote_mod, "get_s3_client", return_value=client),
            patch.object(s3_utils, "get_s3_client", return_value=client),
        ):
            report = promote_from_s3(
                staging_key_prefix=staging_prefix,
                bucket=TEST_BUCKET,
                dry_run=True,
            )

        assert report["dry_run"] is True
        assert report["promoted"] == 1
        assert report["failed"] == 0
