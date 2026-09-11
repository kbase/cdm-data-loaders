"""Tests for pipelines.ncbi_ftp_download — settings, batch orchestration, CLI."""

import json
from collections.abc import Generator
from pathlib import Path, PurePosixPath
from typing import Any, cast
from unittest.mock import MagicMock, patch

import boto3
import pytest
from moto import mock_aws
from pydantic import ValidationError

from cdm_data_loaders.core.fields import INPUT_MOUNT, OUTPUT_MOUNT
from cdm_data_loaders.ncbi_ftp.assembly import FTP_HOST
from cdm_data_loaders.pipelines.ncbi_ftp_download import (
    DownloadSettings,
    download_and_stage,
    download_batch,
)
from cdm_data_loaders.utils.file_transfer.s3 import client
from cdm_data_loaders.utils.file_transfer.s3.client import reset_s3_client

_MOCK_STATS = {
    "accession": "GCF_000001215.4",
    "assembly_dir": "GCF_000001215.4_Release_6_plus_ISO1_MT",
    "files_downloaded": 0,
    "files_skipped_checksum_mismatch": 0,
    "files_without_checksum": 0,
}

_DEFAULT_THREADS = 4
_CUSTOM_THREADS = 8
_ALIAS_THREADS = 16
_BOUNDARY_MIN = 1
_BOUNDARY_MAX = 32
_OVER_MAX = 64
_CUSTOM_LIMIT = 100
_ALIAS_LIMIT = 50
_EXPECTED_ATTEMPTED = 2


def make_settings(**kwargs: str | int | bool | Path | PurePosixPath) -> DownloadSettings:
    """Generate a validated DownloadSettings object."""
    settings_ctor = cast("Any", DownloadSettings)
    return settings_ctor(_cli_parse_args=[], **kwargs)


# Settings defaults / all params / aliases

_DEFAULT_SETTINGS = {
    "manifest": Path(INPUT_MOUNT) / "transfer_manifest.txt",
    "output_dir": Path(OUTPUT_MOUNT),
    "threads": _DEFAULT_THREADS,
    "ftp_host": FTP_HOST,
    "limit": None,
}

_ALL_PARAMS_SETTINGS = {
    "manifest": Path("/") / "data" / "my_manifest.txt",
    "output_dir": Path("/") / "data" / "output",
    "threads": _CUSTOM_THREADS,
    "ftp_host": "ftp.example.com",
    "limit": _CUSTOM_LIMIT,
}

_ALIAS_SETTINGS = {
    "manifest": Path("/") / "data" / "m.txt",
    "output_dir": Path("/") / "data" / "o",
    "threads": _ALIAS_THREADS,
    "limit": _ALIAS_LIMIT,
}

# (kwargs, expected resolved values) — human-readable id per case
_SETTINGS_CASES = [
    pytest.param(({}, _DEFAULT_SETTINGS), id="defaults"),
    pytest.param((dict(_ALL_PARAMS_SETTINGS), _ALL_PARAMS_SETTINGS), id="all_params"),
    pytest.param(
        (
            {
                "m": _ALIAS_SETTINGS["manifest"],
                "output_dir": _ALIAS_SETTINGS["output_dir"],
                "t": _ALIAS_SETTINGS["threads"],
                "l": _ALIAS_SETTINGS["limit"],
            },
            _ALIAS_SETTINGS,
        ),
        id="aliases",
    ),
]

invalid_settings = [
    pytest.param({"threads": 0}, id="threads_too_low"),
    pytest.param({"threads": _OVER_MAX}, id="threads_too_high"),
    pytest.param({"limit": 0}, id="limit_must_be_positive"),
]


@pytest.mark.parametrize("case", _SETTINGS_CASES)
def test_settings_resolve_expected_values(case: tuple) -> None:
    """Defaults, explicit params, and CLI aliases all resolve to the expected settings values."""
    kwargs, expected = case
    s = make_settings(**kwargs)
    for field, expected_value in expected.items():
        assert getattr(s, field) == expected_value


@pytest.mark.parametrize("kwargs", invalid_settings)
def test_settings_invalid_constraints_raise(kwargs: dict) -> None:
    """Out-of-range threads/limit values raise ValidationError."""
    with pytest.raises(ValidationError):
        make_settings(**kwargs)


@pytest.mark.parametrize(
    "threads",
    [
        pytest.param(_BOUNDARY_MIN, id="threads_boundary_1"),
        pytest.param(_BOUNDARY_MAX, id="threads_boundary_32"),
    ],
)
def test_settings_threads_boundaries_accepted(threads: int) -> None:
    """Boundary thread counts (1 and 32) are accepted."""
    s = make_settings(threads=threads)
    assert s.threads == threads


# download_batch


class TestDownloadBatch:
    """Test download_batch with mocked internals."""

    @pytest.fixture(autouse=True)
    def _mock_ftp_pool(self) -> Generator[None]:
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
        with patch(
            "cdm_data_loaders.pipelines.ncbi_ftp_download.download_assembly_to_local",
            return_value=mock_stats,
        ):
            report = download_batch(
                manifest_path=manifest,
                output_dir=output,
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
        with patch(
            "cdm_data_loaders.pipelines.ncbi_ftp_download.download_assembly_to_local",
            return_value=mock_stats,
        ):
            report = download_batch(
                manifest_path=manifest,
                output_dir=output,
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
        with patch(
            "cdm_data_loaders.pipelines.ncbi_ftp_download.download_assembly_to_local",
            return_value=mock_stats,
        ):
            download_batch(manifest_path=manifest, output_dir=output, threads=1)

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
            report = download_batch(manifest_path=manifest, output_dir=output, threads=1)

        assert report["failed"] == 1
        assert report["succeeded"] == 0


# Helpers shared by download_and_stage tests

_MANIFEST_CONTENT = (
    "/genomes/all/GCF/000/001/215/GCF_000001215.4_Release_6_plus_ISO1_MT/\n"
    "/genomes/all/GCF/000/001/405/GCF_000001405.40_GRCh38.p14/\n"
)
_TEST_BUCKET = PurePosixPath("test-bucket")
_STAGING_PREFIX = PurePosixPath("staging") / "run1"


def _make_moto_s3(monkeypatch: pytest.MonkeyPatch):  # noqa: ANN202
    """Return a moto-backed S3 client with the test bucket created."""
    # Remove any real endpoint/credential env vars so moto intercepts all HTTP calls.
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.delenv("AWS_ENDPOINT_URL", raising=False)
    monkeypatch.delenv("AWS_ENDPOINT_URL_S3", raising=False)
    boto3.DEFAULT_SESSION = None
    s3_client = boto3.client("s3", region_name="us-east-1")
    s3_client.create_bucket(Bucket=f"{_TEST_BUCKET}")
    return s3_client


# download_and_stage — manifest source


@pytest.mark.parametrize(
    ("manifest_s3_key", "use_local"),
    [
        pytest.param(Path("staging") / "input" / "transfer_manifest.txt", False, id="s3_source"),
        pytest.param(None, True, id="local_source"),
    ],
)
@mock_aws
def test_download_and_stage_manifest_source(
    tmp_path: Path,
    manifest_s3_key: PurePosixPath | None,
    use_local: bool,  # noqa: ARG001
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Assembly paths from the manifest are processed regardless of source (S3 or local)."""
    reset_s3_client()
    s3_client = _make_moto_s3(monkeypatch)

    manifest_local: Path | None = None
    if manifest_s3_key is not None:
        s3_client.put_object(Bucket=str(_TEST_BUCKET), Key=str(manifest_s3_key), Body=_MANIFEST_CONTENT.encode())
    else:
        manifest_local = tmp_path / "manifest.txt"
        manifest_local.write_text(_MANIFEST_CONTENT)

    called_paths: list[Path] = []

    def _fake_download(path: Path, output_dir: Path, **kwargs: object) -> dict[str, str | int]:  # noqa: ARG001
        called_paths.append(path)
        return _MOCK_STATS

    with (
        patch.object(client, "get_s3_client", return_value=s3_client),
        patch.object(client, "_s3_client", s3_client),
        patch("cdm_data_loaders.pipelines.ncbi_ftp_download.ThreadLocalFTP"),
        patch(
            "cdm_data_loaders.pipelines.ncbi_ftp_download.download_assembly_to_local",
            side_effect=_fake_download,
        ),
    ):
        download_and_stage(
            bucket=_TEST_BUCKET,
            staging_key_prefix=_STAGING_PREFIX,
            manifest_s3_key=manifest_s3_key,
            manifest_local_path=manifest_local,
            dry_run=True,
            threads=1,
        )

    expected_paths: list[Path] = [Path(line) for line in _MANIFEST_CONTENT.splitlines() if line.strip()]
    assert sorted(called_paths) == sorted(expected_paths)

    reset_s3_client()


# download_and_stage — exactly one source required


@pytest.mark.parametrize(
    ("s3_key", "local_path", "should_raise"),
    [
        pytest.param(PurePosixPath("s3") / "key", Path("local") / "path", True, id="both_provided_raises"),
        pytest.param(None, None, True, id="neither_provided_raises"),
        pytest.param(PurePosixPath("s3") / "key", None, False, id="s3_only_ok"),
        pytest.param(None, Path("local") / "path", False, id="local_only_ok"),
    ],
)
@mock_aws
def test_download_and_stage_exactly_one_source_required(
    tmp_path: Path,
    s3_key: PurePosixPath | None,
    local_path: Path | None,
    should_raise: bool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """ValueError is raised when both or neither manifest sources are given."""
    reset_s3_client()

    if should_raise:
        with pytest.raises(ValueError, match="manifest"):
            download_and_stage(
                bucket=_TEST_BUCKET,
                staging_key_prefix=_STAGING_PREFIX,
                manifest_s3_key=s3_key,
                manifest_local_path=local_path,
            )
    else:
        s3_client = _make_moto_s3(monkeypatch)
        # For s3_only: seed the object; for local_only: create the file
        if s3_key is not None:
            s3_client.put_object(Bucket=str(_TEST_BUCKET), Key=str(s3_key), Body=_MANIFEST_CONTENT.encode())
        if local_path is not None:
            real_local = tmp_path / "manifest.txt"
            real_local.write_text(_MANIFEST_CONTENT)
            local_path = real_local

        with (
            patch.object(client, "get_s3_client", return_value=s3_client),
            patch.object(client, "_s3_client", s3_client),
            patch("cdm_data_loaders.pipelines.ncbi_ftp_download.ThreadLocalFTP"),
            patch(
                "cdm_data_loaders.pipelines.ncbi_ftp_download.download_assembly_to_local",
                return_value=_MOCK_STATS,
            ),
        ):
            result = download_and_stage(
                bucket=_TEST_BUCKET,
                staging_key_prefix=_STAGING_PREFIX,
                manifest_s3_key=s3_key,
                manifest_local_path=local_path,
                dry_run=True,
            )
        assert result["succeeded"] == _EXPECTED_ATTEMPTED

    reset_s3_client()


# download_and_stage — uploads to staging


@mock_aws
def test_download_and_stage_uploads_to_staging(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Files produced by download_assembly_to_local and download_report.json are all staged to S3."""
    reset_s3_client()
    s3_client = _make_moto_s3(monkeypatch)

    manifest_local = tmp_path / "manifest.txt"
    # Single assembly so the fake download writes exactly the files we expect
    manifest_local.write_text("/genomes/all/GCF/000/001/215/GCF_000001215.4_Release_6_plus_ISO1_MT/\n")

    assembly_rel = "raw_data/GCF/000/001/215/GCF_000001215.4_Release_6_plus_ISO1_MT"

    def _fake_download(path: Path, output_dir: Path, **kwargs: Path) -> dict[str, str | int]:  # noqa: ARG001
        asm_dir = Path(output_dir) / assembly_rel
        asm_dir.mkdir(parents=True)
        (asm_dir / "genomic.fna.gz").write_bytes(b"fasta_data")
        (asm_dir / "genomic.fna.gz.md5").write_bytes(b"abc123")
        return {**_MOCK_STATS, "files_downloaded": 2}

    with (
        patch.object(client, "get_s3_client", return_value=s3_client),
        patch.object(client, "_s3_client", s3_client),
        patch("cdm_data_loaders.pipelines.ncbi_ftp_download.ThreadLocalFTP"),
        patch(
            "cdm_data_loaders.pipelines.ncbi_ftp_download.download_assembly_to_local",
            side_effect=_fake_download,
        ),
    ):
        report = download_and_stage(
            bucket=_TEST_BUCKET,
            staging_key_prefix=_STAGING_PREFIX,
            manifest_local_path=manifest_local,
            dry_run=False,
            threads=1,
        )

    paginator = s3_client.get_paginator("list_objects_v2")
    uploaded_keys: set[PurePosixPath] = {
        PurePosixPath(obj["Key"])
        for page in paginator.paginate(Bucket=str(_TEST_BUCKET))
        for obj in page.get("Contents", [])
    }

    expected_keys = {
        _STAGING_PREFIX / assembly_rel / "genomic.fna.gz",
        _STAGING_PREFIX / assembly_rel / "genomic.fna.gz.md5",
        _STAGING_PREFIX / "download_report.json",
    }
    assert uploaded_keys == expected_keys
    assert report["staged_objects"] == len(expected_keys)

    reset_s3_client()


# download_and_stage — dry_run skips upload


@mock_aws
def test_download_and_stage_dry_run_skips_upload(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """dry_run=True leaves S3 empty and returns staged_objects=0."""
    reset_s3_client()
    s3_client = _make_moto_s3(monkeypatch)

    manifest_local = tmp_path / "manifest.txt"
    manifest_local.write_text(_MANIFEST_CONTENT)

    def _fake_download(path: Path, output_dir: Path, **kwargs: object) -> dict[str, str | int]:  # noqa: ARG001
        asm_dir = Path(output_dir) / "raw_data" / "GCF" / "000" / "001" / "215" / "GCF_000001215.4"
        asm_dir.mkdir(parents=True, exist_ok=True)
        (asm_dir / "genomic.fna.gz").write_bytes(b"fasta")
        return _MOCK_STATS

    with (
        patch.object(client, "get_s3_client", return_value=s3_client),
        patch.object(client, "_s3_client", s3_client),
        patch("cdm_data_loaders.pipelines.ncbi_ftp_download.ThreadLocalFTP"),
        patch(
            "cdm_data_loaders.pipelines.ncbi_ftp_download.download_assembly_to_local",
            side_effect=_fake_download,
        ),
    ):
        report = download_and_stage(
            bucket=_TEST_BUCKET,
            staging_key_prefix=_STAGING_PREFIX,
            manifest_local_path=manifest_local,
            dry_run=True,
            threads=1,
        )

    listed = s3_client.list_objects_v2(Bucket=str(_TEST_BUCKET))
    assert listed.get("KeyCount", 0) == 0
    assert report["staged_objects"] == 0
    assert report["dry_run"] is True

    reset_s3_client()


# download_and_stage — limit forwarded


@pytest.mark.parametrize(
    "limit",
    [
        pytest.param(1, id="limit_1"),
        pytest.param(10, id="limit_10"),
    ],
)
@mock_aws
def test_download_and_stage_limit_forwarded(tmp_path: Path, limit: int, monkeypatch: pytest.MonkeyPatch) -> None:
    """The limit parameter truncates the number of assemblies processed."""
    reset_s3_client()
    s3_client = _make_moto_s3(monkeypatch)

    manifest_local = tmp_path / "manifest.txt"
    manifest_local.write_text(_MANIFEST_CONTENT)

    with (
        patch.object(client, "get_s3_client", return_value=s3_client),
        patch.object(client, "_s3_client", s3_client),
        patch("cdm_data_loaders.pipelines.ncbi_ftp_download.ThreadLocalFTP"),
        patch(
            "cdm_data_loaders.pipelines.ncbi_ftp_download.download_assembly_to_local",
            return_value=_MOCK_STATS,
        ) as mock_dl,
    ):
        download_and_stage(
            bucket=_TEST_BUCKET,
            staging_key_prefix=_STAGING_PREFIX,
            manifest_local_path=manifest_local,
            limit=limit,
            dry_run=True,
        )

    # The manifest has 2 entries; limit caps how many were processed
    expected_calls = min(limit, _EXPECTED_ATTEMPTED)
    assert mock_dl.call_count == expected_calls

    reset_s3_client()


# download_and_stage — report shape


@mock_aws
def test_download_and_stage_report_shape(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Return value contains all expected keys including staged_objects, staging_key_prefix, dry_run."""
    reset_s3_client()
    s3_client = _make_moto_s3(monkeypatch)

    manifest_local = tmp_path / "manifest.txt"
    manifest_local.write_text(_MANIFEST_CONTENT)

    with (
        patch.object(client, "get_s3_client", return_value=s3_client),
        patch.object(client, "_s3_client", s3_client),
        patch("cdm_data_loaders.pipelines.ncbi_ftp_download.ThreadLocalFTP"),
        patch(
            "cdm_data_loaders.pipelines.ncbi_ftp_download.download_assembly_to_local",
            return_value=_MOCK_STATS,
        ),
    ):
        report = download_and_stage(
            bucket=_TEST_BUCKET,
            staging_key_prefix=_STAGING_PREFIX,
            manifest_local_path=manifest_local,
            dry_run=True,
        )

    for key in (
        "timestamp",
        "total_attempted",
        "succeeded",
        "failed",
        "failures",
        "assembly_stats",
    ):
        assert key in report
    assert report["staged_objects"] == 0
    assert report["staging_key_prefix"] == _STAGING_PREFIX
    assert report["dry_run"] is True
    assert report["total_attempted"] == _EXPECTED_ATTEMPTED
    assert report["succeeded"] == _EXPECTED_ATTEMPTED

    reset_s3_client()
