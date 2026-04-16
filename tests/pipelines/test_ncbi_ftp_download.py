"""Tests for pipelines.ncbi_ftp_download — settings, batch orchestration, CLI."""

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from pydantic import ValidationError

from cdm_data_loaders.ncbi_ftp.assembly import FTP_HOST
from cdm_data_loaders.pipelines.cts_defaults import INPUT_MOUNT, OUTPUT_MOUNT
from cdm_data_loaders.pipelines.ncbi_ftp_download import DownloadSettings, download_batch

_DEFAULT_THREADS = 4
_CUSTOM_THREADS = 8
_ALIAS_THREADS = 16
_BOUNDARY_MIN = 1
_BOUNDARY_MAX = 32
_OVER_MAX = 64
_CUSTOM_LIMIT = 100
_ALIAS_LIMIT = 50
_EXPECTED_ATTEMPTED = 2


def make_settings(**kwargs: str | int) -> DownloadSettings:
    """Generate a validated DownloadSettings object."""
    return DownloadSettings(_cli_parse_args=[], **kwargs)


# ── Settings defaults ────────────────────────────────────────────────────


class TestDownloadSettingsDefaults:
    """Test default settings."""

    def test_manifest_default(self) -> None:
        """Verify default manifest path uses INPUT_MOUNT."""
        s = make_settings()
        assert s.manifest == f"{INPUT_MOUNT}/transfer_manifest.txt"

    def test_output_dir_default(self) -> None:
        """Verify default output_dir uses OUTPUT_MOUNT."""
        s = make_settings()
        assert s.output_dir == OUTPUT_MOUNT

    def test_threads_default(self) -> None:
        """Verify default threads is 4."""
        s = make_settings()
        assert s.threads == _DEFAULT_THREADS

    def test_ftp_host_default(self) -> None:
        """Verify default ftp_host matches FTP_HOST constant."""
        s = make_settings()
        assert s.ftp_host == FTP_HOST

    def test_limit_default_none(self) -> None:
        """Verify default limit is None."""
        s = make_settings()
        assert s.limit is None


# ── Settings all params ──────────────────────────────────────────────────


class TestDownloadSettingsAllParams:
    """Test with all params set."""

    def test_all_params(self) -> None:
        """Verify all parameters are correctly set when provided."""
        s = make_settings(
            manifest="/data/my_manifest.txt",
            output_dir="/data/output",
            threads=_CUSTOM_THREADS,
            ftp_host="ftp.example.com",
            limit=_CUSTOM_LIMIT,
        )
        assert s.manifest == "/data/my_manifest.txt"
        assert s.output_dir == "/data/output"
        assert s.threads == _CUSTOM_THREADS
        assert s.ftp_host == "ftp.example.com"
        assert s.limit == _CUSTOM_LIMIT


# ── Settings aliases ─────────────────────────────────────────────────────


class TestDownloadSettingsAliases:
    """Test CLI alias resolution."""

    def test_manifest_alias_m(self) -> None:
        """Verify 'm' alias resolves to manifest."""
        s = make_settings(m="/data/m.txt")
        assert s.manifest == "/data/m.txt"

    def test_output_dir_alias_o(self) -> None:
        """Verify 'o' alias resolves to output_dir."""
        s = make_settings(o="/data/o")
        assert s.output_dir == "/data/o"

    def test_threads_alias_t(self) -> None:
        """Verify 't' alias resolves to threads."""
        s = make_settings(t=_ALIAS_THREADS)
        assert s.threads == _ALIAS_THREADS

    def test_limit_alias_l(self) -> None:
        """Verify 'l' alias resolves to limit."""
        s = make_settings(l=_ALIAS_LIMIT)
        assert s.limit == _ALIAS_LIMIT


# ── Settings validation ──────────────────────────────────────────────────


class TestDownloadSettingsValidation:
    """Test validation constraints."""

    def test_threads_too_low(self) -> None:
        """Verify threads=0 raises ValidationError."""
        with pytest.raises(ValidationError):
            make_settings(threads=0)

    def test_threads_too_high(self) -> None:
        """Verify threads above 32 raises ValidationError."""
        with pytest.raises(ValidationError):
            make_settings(threads=_OVER_MAX)

    def test_threads_boundary_1(self) -> None:
        """Verify threads=1 is accepted."""
        s = make_settings(threads=_BOUNDARY_MIN)
        assert s.threads == _BOUNDARY_MIN

    def test_threads_boundary_32(self) -> None:
        """Verify threads=32 is accepted."""
        s = make_settings(threads=_BOUNDARY_MAX)
        assert s.threads == _BOUNDARY_MAX

    def test_limit_must_be_positive(self) -> None:
        """Verify limit=0 raises ValidationError."""
        with pytest.raises(ValidationError):
            make_settings(limit=0)


# ── download_batch ───────────────────────────────────────────────────────


class TestDownloadBatch:
    """Test download_batch with mocked internals."""

    @pytest.fixture(autouse=True)
    def _mock_ftp_pool(self) -> None:
        """Prevent real FTP connections from the ThreadLocalFTP pool."""
        mock_pool = MagicMock()
        with patch("cdm_data_loaders.pipelines.ncbi_ftp_download.ThreadLocalFTP", return_value=mock_pool):
            yield

    def test_reads_manifest_and_calls_download(self, tmp_path: Path) -> None:
        """Verify manifest is read and download is called for each entry."""
        manifest = tmp_path / "manifest.txt"
        manifest.write_text(
            "/genomes/all/GCF/000/001/215/GCF_000001215.4_Release_6_plus_ISO1_MT/\n"
            "/genomes/all/GCF/000/001/405/GCF_000001405.40_GRCh38.p14/\n"
        )
        output = tmp_path / "output"
        output.mkdir()

        mock_stats = {"accession": "test", "files_downloaded": 3}
        with patch("cdm_data_loaders.pipelines.ncbi_ftp_download.download_assembly_to_local", return_value=mock_stats):
            report = download_batch(
                manifest_path=str(manifest),
                output_dir=str(output),
                threads=1,
                ftp_host="ftp.example.com",
            )

        assert report["total_attempted"] == _EXPECTED_ATTEMPTED
        assert report["succeeded"] == _EXPECTED_ATTEMPTED
        assert report["failed"] == 0

    def test_limit_truncates(self, tmp_path: Path) -> None:
        """Verify limit parameter truncates the number of assemblies processed."""
        manifest = tmp_path / "manifest.txt"
        manifest.write_text(
            "/genomes/all/GCF/000/001/215/GCF_000001215.4_Release_6_plus_ISO1_MT/\n"
            "/genomes/all/GCF/000/001/405/GCF_000001405.40_GRCh38.p14/\n"
        )
        output = tmp_path / "output"
        output.mkdir()

        mock_stats = {"accession": "test", "files_downloaded": 1}
        with patch("cdm_data_loaders.pipelines.ncbi_ftp_download.download_assembly_to_local", return_value=mock_stats):
            report = download_batch(
                manifest_path=str(manifest),
                output_dir=str(output),
                threads=1,
                limit=1,
            )
        assert report["total_attempted"] == 1

    def test_writes_report_json(self, tmp_path: Path) -> None:
        """Verify download_report.json is written to the output directory."""
        manifest = tmp_path / "manifest.txt"
        manifest.write_text("/genomes/all/GCF/000/001/215/GCF_000001215.4_Release_6_plus_ISO1_MT/\n")
        output = tmp_path / "output"
        output.mkdir()

        mock_stats = {"accession": "GCF_000001215.4", "files_downloaded": 5}
        with patch("cdm_data_loaders.pipelines.ncbi_ftp_download.download_assembly_to_local", return_value=mock_stats):
            download_batch(manifest_path=str(manifest), output_dir=str(output), threads=1)

        report_file = output / "download_report.json"
        assert report_file.exists()
        report = json.loads(report_file.read_text())
        assert "timestamp" in report
        assert report["succeeded"] == 1

    def test_handles_download_failure(self, tmp_path: Path) -> None:
        """Verify failed downloads are counted and do not crash the batch."""
        manifest = tmp_path / "manifest.txt"
        manifest.write_text("/genomes/all/GCF/000/001/215/GCF_000001215.4_Release_6_plus_ISO1_MT/\n")
        output = tmp_path / "output"
        output.mkdir()

        with patch(
            "cdm_data_loaders.pipelines.ncbi_ftp_download.download_assembly_to_local",
            side_effect=RuntimeError("connection lost"),
        ):
            report = download_batch(manifest_path=str(manifest), output_dir=str(output), threads=1)

        assert report["failed"] == 1
        assert report["succeeded"] == 0
