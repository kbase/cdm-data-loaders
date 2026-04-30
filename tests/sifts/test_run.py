"""Unit tests for sifts.run."""

from pathlib import Path
from unittest.mock import patch

import pytest

from cdm_data_loaders.sifts.run import SiftsResult, run_sifts
from cdm_data_loaders.sifts.settings import SiftsSettings
from cdm_data_loaders.utils.s3_versioned_upload import UploadResult

FAKE_CONTENT = b"pdb_chain\tuniprot_acc\nAAA_A\tP12345\n"


def _fake_settings(**kwargs) -> SiftsSettings:
    return SiftsSettings(lakehouse_bucket="test-lake", **kwargs)


def _make_fake_download(content: bytes = FAKE_CONTENT):
    """Return a side-effect function that writes fake content to the dest dir."""

    def fake_download(filename, dest_dir, ftp_host):
        path = dest_dir / filename
        path.write_bytes(content)
        return path

    return fake_download


class TestRunSiftsDryRun:
    def test_dry_run_returns_dry_run_status(self):
        settings = _fake_settings(dry_run=True)
        result = run_sifts(settings)
        assert result.dry_run is True
        assert result.upload_status == "dry_run"

    def test_dry_run_does_not_download(self):
        settings = _fake_settings(dry_run=True)
        with patch("cdm_data_loaders.sifts.run.download_sifts_file") as mock_dl:
            run_sifts(settings)
        mock_dl.assert_not_called()

    def test_dry_run_returns_correct_dest_path(self):
        settings = _fake_settings(dry_run=True)
        result = run_sifts(settings)
        assert "test-lake" in result.dest_path
        assert "pdb_chain_uniprot.tsv.gz" in result.dest_path


class TestRunSiftsFirstUpload:
    def test_first_upload_returns_new_status(self, mock_s3_client):
        settings = _fake_settings()
        with patch("cdm_data_loaders.sifts.run.download_sifts_file", side_effect=_make_fake_download()):
            result = run_sifts(settings)

        assert result.upload_status == "new"
        assert result.archive_key is None
        assert result.dry_run is False

    def test_first_upload_file_in_s3(self, mock_s3_client):
        settings = _fake_settings()
        with patch("cdm_data_loaders.sifts.run.download_sifts_file", side_effect=_make_fake_download()):
            result = run_sifts(settings)

        bucket, key = result.dest_path.split("/", 1)
        resp = mock_s3_client.get_object(Bucket=bucket, Key=key)
        assert resp["Body"].read() == FAKE_CONTENT


class TestRunSiftsUnchanged:
    def test_unchanged_content_returns_unchanged_status(self, mock_s3_client):
        settings = _fake_settings()
        with patch("cdm_data_loaders.sifts.run.download_sifts_file", side_effect=_make_fake_download()):
            run_sifts(settings)
            result = run_sifts(settings)

        assert result.upload_status == "unchanged"
        assert result.archive_key is None


class TestRunSiftsUpdate:
    def test_updated_content_returns_archived_and_replaced(self, mock_s3_client):
        settings = _fake_settings()
        with patch("cdm_data_loaders.sifts.run.download_sifts_file", side_effect=_make_fake_download(b"v1\n")):
            run_sifts(settings)
        with patch("cdm_data_loaders.sifts.run.download_sifts_file", side_effect=_make_fake_download(b"v2\n")):
            result = run_sifts(settings)

        assert result.upload_status == "archived_and_replaced"
        assert result.archive_key is not None

    def test_dest_path_contains_correct_key_parts(self, mock_s3_client):
        settings = _fake_settings()
        with patch("cdm_data_loaders.sifts.run.download_sifts_file", side_effect=_make_fake_download()):
            result = run_sifts(settings)

        assert "derived_data/sifts/pdb_chain_uniprot.tsv.gz" in result.dest_path
