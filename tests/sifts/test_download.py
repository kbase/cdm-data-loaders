"""Unit tests for sifts.download."""

from ftplib import FTP
from pathlib import Path
from unittest.mock import MagicMock, patch, call

import pytest

from cdm_data_loaders.sifts.download import (
    DEFAULT_SIFTS_HOST,
    SIFTS_BASE_PATH,
    SIFTS_UNIPROT_FILE,
    download_sifts_file,
)


class TestDownloadSiftsFile:
    def test_connects_to_correct_ftp_host(self, tmp_path: Path):
        mock_ftp = MagicMock(spec=FTP)
        with (
            patch("cdm_data_loaders.sifts.download.connect_ftp", return_value=mock_ftp) as mock_connect,
            patch("cdm_data_loaders.sifts.download.ftp_download_file") as mock_download,
        ):
            # Create a fake file so stat() doesn't fail
            dest = tmp_path / SIFTS_UNIPROT_FILE
            dest.write_bytes(b"fake content")
            with patch("cdm_data_loaders.sifts.download.ftp_download_file", side_effect=lambda ftp, remote, local: Path(local).write_bytes(b"fake")):
                download_sifts_file(SIFTS_UNIPROT_FILE, tmp_path)

        mock_connect.assert_called_once_with(DEFAULT_SIFTS_HOST)

    def test_downloads_to_dest_dir(self, tmp_path: Path):
        mock_ftp = MagicMock(spec=FTP)
        fake_content = b"pdb_chain\tuniprot_acc\nAAA_A\tP12345"

        def fake_download(ftp, remote, local):
            Path(local).write_bytes(fake_content)

        with (
            patch("cdm_data_loaders.sifts.download.connect_ftp", return_value=mock_ftp),
            patch("cdm_data_loaders.sifts.download.ftp_download_file", side_effect=fake_download),
        ):
            result = download_sifts_file(SIFTS_UNIPROT_FILE, tmp_path)

        assert result == tmp_path / SIFTS_UNIPROT_FILE
        assert result.read_bytes() == fake_content

    def test_uses_correct_remote_path(self, tmp_path: Path):
        mock_ftp = MagicMock(spec=FTP)
        captured_remote = []

        def fake_download(ftp, remote, local):
            captured_remote.append(remote)
            Path(local).write_bytes(b"data")

        with (
            patch("cdm_data_loaders.sifts.download.connect_ftp", return_value=mock_ftp),
            patch("cdm_data_loaders.sifts.download.ftp_download_file", side_effect=fake_download),
        ):
            download_sifts_file(SIFTS_UNIPROT_FILE, tmp_path)

        assert captured_remote == [f"{SIFTS_BASE_PATH}/{SIFTS_UNIPROT_FILE}"]

    def test_custom_ftp_host(self, tmp_path: Path):
        mock_ftp = MagicMock(spec=FTP)

        def fake_download(ftp, remote, local):
            Path(local).write_bytes(b"data")

        with (
            patch("cdm_data_loaders.sifts.download.connect_ftp", return_value=mock_ftp) as mock_connect,
            patch("cdm_data_loaders.sifts.download.ftp_download_file", side_effect=fake_download),
        ):
            download_sifts_file(SIFTS_UNIPROT_FILE, tmp_path, ftp_host="custom-ftp.example.com")

        mock_connect.assert_called_once_with("custom-ftp.example.com")

    def test_ftp_quit_called_on_success(self, tmp_path: Path):
        mock_ftp = MagicMock(spec=FTP)

        def fake_download(ftp, remote, local):
            Path(local).write_bytes(b"data")

        with (
            patch("cdm_data_loaders.sifts.download.connect_ftp", return_value=mock_ftp),
            patch("cdm_data_loaders.sifts.download.ftp_download_file", side_effect=fake_download),
        ):
            download_sifts_file(SIFTS_UNIPROT_FILE, tmp_path)

        mock_ftp.quit.assert_called_once()

    def test_ftp_quit_called_on_error(self, tmp_path: Path):
        mock_ftp = MagicMock(spec=FTP)

        with (
            patch("cdm_data_loaders.sifts.download.connect_ftp", return_value=mock_ftp),
            patch("cdm_data_loaders.sifts.download.ftp_download_file", side_effect=OSError("FTP error")),
        ):
            with pytest.raises(OSError):
                download_sifts_file(SIFTS_UNIPROT_FILE, tmp_path)

        mock_ftp.quit.assert_called_once()

    def test_creates_dest_dir(self, tmp_path: Path):
        new_dir = tmp_path / "sub" / "dir"
        mock_ftp = MagicMock(spec=FTP)

        def fake_download(ftp, remote, local):
            Path(local).write_bytes(b"data")

        with (
            patch("cdm_data_loaders.sifts.download.connect_ftp", return_value=mock_ftp),
            patch("cdm_data_loaders.sifts.download.ftp_download_file", side_effect=fake_download),
        ):
            download_sifts_file(SIFTS_UNIPROT_FILE, new_dir)

        assert new_dir.is_dir()
