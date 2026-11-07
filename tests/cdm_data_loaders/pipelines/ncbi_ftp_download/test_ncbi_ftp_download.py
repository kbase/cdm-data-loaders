"""Tests for pipelines.ncbi_ftp_download — settings, batch orchestration, CLI."""

import json
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path, PurePosixPath
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from types_boto3_s3 import S3Client

from cdm_data_loaders.pipelines.ncbi_ftp_download import (
    download_and_stage,
    download_batch,
)
from tests.conftest import TEST_BUCKET

_MOCK_STATS = {
    "accession": "GCF_000001215.4",
    "assembly_dir": "GCF_000001215.4_Release_6_plus_ISO1_MT",
    "files_downloaded": 0,
    "files_skipped_checksum_mismatch": 0,
    "files_without_checksum": 0,
}
_EXPECTED_ATTEMPTED = 2


@contextmanager
def _patch_ftp_download(
    *,
    return_value: Any = None,  # noqa: ANN401
    side_effect: Any = None,  # noqa: ANN401
) -> Generator[MagicMock]:
    """Patch ``ThreadLocalFTP`` and ``download_assembly_to_local`` in ``ncbi_ftp_download``."""
    with (
        patch("cdm_data_loaders.pipelines.ncbi_ftp_download.ThreadLocalFTP"),
        patch(
            "cdm_data_loaders.pipelines.ncbi_ftp_download.download_assembly_to_local",
            return_value=return_value,
            side_effect=side_effect,
        ) as mock_dl,
    ):
        yield mock_dl


# download_batch
def test_download_batch_reads_manifest_and_calls_download(tmp_path: Path) -> None:
    """Manifest is read and download is called for each entry."""
    manifest = tmp_path / "manifest.txt"
    manifest.write_text(
        "/genomes/all/GCF/000/001/215/GCF_000001215.4_Release_6_plus_ISO1_MT/\n"
        "/genomes/all/GCF/000/001/405/GCF_000001405.40_GRCh38.p14/\n"
    )
    output = tmp_path / "output"
    output.mkdir()

    with _patch_ftp_download(return_value={"accession": "test", "files_downloaded": 3}):
        report = download_batch(
            manifest_path=manifest,
            output_dir=output,
            threads=1,
            ftp_host="ftp.example.com",
        )

    assert report["total_attempted"] == _EXPECTED_ATTEMPTED
    assert report["succeeded"] == _EXPECTED_ATTEMPTED
    assert report["failed"] == 0


def test_download_batch_limit_truncates(tmp_path: Path) -> None:
    """The limit parameter truncates the number of assemblies processed."""
    manifest = tmp_path / "manifest.txt"
    manifest.write_text(
        "/genomes/all/GCF/000/001/215/GCF_000001215.4_Release_6_plus_ISO1_MT/\n"
        "/genomes/all/GCF/000/001/405/GCF_000001405.40_GRCh38.p14/\n"
    )
    output = tmp_path / "output"
    output.mkdir()

    with _patch_ftp_download(return_value={"accession": "test", "files_downloaded": 1}):
        report = download_batch(
            manifest_path=manifest,
            output_dir=output,
            threads=1,
            limit=1,
        )
    assert report["total_attempted"] == 1


def test_download_batch_writes_report_json(tmp_path: Path) -> None:
    """download_report.json is written to the output directory."""
    manifest = tmp_path / "manifest.txt"
    manifest.write_text("/genomes/all/GCF/000/001/215/GCF_000001215.4_Release_6_plus_ISO1_MT/\n")
    output = tmp_path / "output"
    output.mkdir()

    with _patch_ftp_download(return_value={"accession": "GCF_000001215.4", "files_downloaded": 5}):
        download_batch(manifest_path=manifest, output_dir=output, threads=1)

    report_file = output / "download_report.json"
    assert report_file.exists()
    report = json.loads(report_file.read_text())
    assert "timestamp" in report
    assert report["succeeded"] == 1


def test_download_batch_handles_download_failure(tmp_path: Path) -> None:
    """Failed downloads are counted and do not crash the batch."""
    manifest = tmp_path / "manifest.txt"
    manifest.write_text("/genomes/all/GCF/000/001/215/GCF_000001215.4_Release_6_plus_ISO1_MT/\n")
    output = tmp_path / "output"
    output.mkdir()

    with _patch_ftp_download(side_effect=RuntimeError("connection lost")):
        report = download_batch(manifest_path=manifest, output_dir=output, threads=1)

    assert report["failed"] == 1
    assert report["succeeded"] == 0


def test_download_batch_pass_callback_and_comments(tmp_path: Path, output_dir: Path) -> None:
    """download_batch skips blank/comment lines and calls on_assembly_downloaded only on success."""
    manifest = tmp_path / "manifest.txt"
    good_path = "/genomes/all/GCF/000/001/215/GCF_000001215.4_Release_6_plus_ISO1_MT/"
    bad_path = "/genomes/all/GCF/000/001/405/GCF_000001405.40_GRCh38.p14/"
    manifest.write_text(f"# header comment\n\n{good_path}\n{bad_path}\n")

    callback_paths: list[PurePosixPath] = []

    def _fake_download(path: PurePosixPath, output_dir: Path, **kwargs: object) -> dict[str, str | int]:  # noqa: ARG001
        if str(path) == bad_path.rstrip("/"):
            msg = "simulated error"
            raise RuntimeError(msg)
        return _MOCK_STATS

    with _patch_ftp_download(side_effect=_fake_download):
        report = download_batch(
            manifest_path=manifest,
            output_dir=output_dir,
            threads=1,
            on_assembly_downloaded=callback_paths.append,
        )

    assert report["total_attempted"] == _EXPECTED_ATTEMPTED
    assert report["succeeded"] == 1
    assert report["failed"] == 1
    assert callback_paths == [PurePosixPath(good_path)]


# Helpers shared by download_and_stage tests

_MANIFEST_CONTENT = (
    "/genomes/all/GCF/000/001/215/GCF_000001215.4_Release_6_plus_ISO1_MT/\n"
    "/genomes/all/GCF/000/001/405/GCF_000001405.40_GRCh38.p14/\n"
)
_TEST_BUCKET = PurePosixPath(TEST_BUCKET)
_STAGING_PREFIX = PurePosixPath("staging") / "run1"


# download_and_stage — manifest source


@pytest.mark.parametrize(
    ("manifest_s3_key", "use_local"),
    [
        pytest.param(Path("staging") / "input" / "transfer_manifest.txt", False, id="s3_source"),
        pytest.param(None, True, id="local_source"),
    ],
)
def test_download_and_stage_manifest_source(
    tmp_path: Path,
    manifest_s3_key: PurePosixPath | None,
    use_local: bool,  # noqa: ARG001
    mock_s3_client: S3Client,
) -> None:
    """Assembly paths from the manifest are processed regardless of source (S3 or local)."""
    manifest_local: Path | None = None
    if manifest_s3_key is not None:
        mock_s3_client.put_object(Bucket=str(_TEST_BUCKET), Key=str(manifest_s3_key), Body=_MANIFEST_CONTENT.encode())
    else:
        manifest_local = tmp_path / "manifest.txt"
        manifest_local.write_text(_MANIFEST_CONTENT)

    called_paths: list[Path] = []

    def _fake_download(path: Path, output_dir: Path, **kwargs: object) -> dict[str, str | int]:  # noqa: ARG001
        called_paths.append(path)
        return _MOCK_STATS

    with _patch_ftp_download(side_effect=_fake_download):
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
def test_download_and_stage_exactly_one_source_required(
    tmp_path: Path,
    s3_key: PurePosixPath | None,
    local_path: Path | None,
    should_raise: bool,
    mock_s3_client: S3Client,
) -> None:
    """ValueError is raised when both or neither manifest sources are given."""
    if should_raise:
        with pytest.raises(ValueError, match="manifest"):
            download_and_stage(
                bucket=_TEST_BUCKET,
                staging_key_prefix=_STAGING_PREFIX,
                manifest_s3_key=s3_key,
                manifest_local_path=local_path,
            )
    else:
        if s3_key is not None:
            mock_s3_client.put_object(Bucket=str(_TEST_BUCKET), Key=str(s3_key), Body=_MANIFEST_CONTENT.encode())
        if local_path is not None:
            real_local = tmp_path / "manifest.txt"
            real_local.write_text(_MANIFEST_CONTENT)
            local_path = real_local

        with _patch_ftp_download(return_value=_MOCK_STATS):
            result = download_and_stage(
                bucket=_TEST_BUCKET,
                staging_key_prefix=_STAGING_PREFIX,
                manifest_s3_key=s3_key,
                manifest_local_path=local_path,
                dry_run=True,
            )
        assert result["succeeded"] == _EXPECTED_ATTEMPTED


# download_and_stage — uploads to staging


def test_download_and_stage_uploads_to_staging(tmp_path: Path, mock_s3_client: S3Client) -> None:
    """Files produced by download_assembly_to_local and download_report.json are all staged to S3."""
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

    with _patch_ftp_download(side_effect=_fake_download):
        report = download_and_stage(
            bucket=_TEST_BUCKET,
            staging_key_prefix=_STAGING_PREFIX,
            manifest_local_path=manifest_local,
            dry_run=False,
            threads=1,
        )

    paginator = mock_s3_client.get_paginator("list_objects_v2")
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


# download_and_stage — dry_run skips upload


def test_download_and_stage_dry_run_skips_upload(tmp_path: Path, mock_s3_client: S3Client) -> None:
    """dry_run=True leaves S3 empty and returns staged_objects=0."""
    manifest_local = tmp_path / "manifest.txt"
    manifest_local.write_text(_MANIFEST_CONTENT)

    def _fake_download(path: Path, output_dir: Path, **kwargs: object) -> dict[str, str | int]:  # noqa: ARG001
        asm_dir = Path(output_dir) / "raw_data" / "GCF" / "000" / "001" / "215" / "GCF_000001215.4"
        asm_dir.mkdir(parents=True, exist_ok=True)
        (asm_dir / "genomic.fna.gz").write_bytes(b"fasta")
        return _MOCK_STATS

    with _patch_ftp_download(side_effect=_fake_download):
        report = download_and_stage(
            bucket=_TEST_BUCKET,
            staging_key_prefix=_STAGING_PREFIX,
            manifest_local_path=manifest_local,
            dry_run=True,
            threads=1,
        )

    listed = mock_s3_client.list_objects_v2(Bucket=str(_TEST_BUCKET))
    assert listed.get("KeyCount", 0) == 0
    assert report["staged_objects"] == 0
    assert report["dry_run"] is True


# download_and_stage — limit forwarded


@pytest.mark.parametrize(
    "limit",
    [
        pytest.param(1, id="limit_1"),
        pytest.param(10, id="limit_10"),
    ],
)
@pytest.mark.usefixtures("mock_s3_client")
def test_download_and_stage_limit_forwarded(tmp_path: Path, limit: int) -> None:
    """The limit parameter truncates the number of assemblies processed."""
    manifest_local = tmp_path / "manifest.txt"
    manifest_local.write_text(_MANIFEST_CONTENT)

    with _patch_ftp_download(return_value=_MOCK_STATS) as mock_dl:
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


# download_and_stage — report shape


@pytest.mark.usefixtures("mock_s3_client")
def test_download_and_stage_report_shape(tmp_path: Path) -> None:
    """Return value contains all expected keys including staged_objects, staging_key_prefix, dry_run."""
    manifest_local = tmp_path / "manifest.txt"
    manifest_local.write_text(_MANIFEST_CONTENT)

    with _patch_ftp_download(return_value=_MOCK_STATS):
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


@pytest.mark.usefixtures("mock_s3_client")
def test_download_and_stage_pass_missing_dir_and_upload_failure(tmp_path: Path) -> None:
    """download_and_stage handles missing local assembly directory and failed S3 uploads gracefully."""
    manifest_local = tmp_path / "manifest.txt"
    manifest_local.write_text(_MANIFEST_CONTENT)

    assembly_rel = "raw_data/GCF/000/001/215/GCF_000001215.4_Release_6_plus_ISO1_MT"

    def _fake_download(path: PurePosixPath, output_dir: Path, **kwargs: object) -> dict[str, str | int]:  # noqa: ARG001
        if "000001215" in str(path):
            asm_dir = Path(output_dir) / assembly_rel
            asm_dir.mkdir(parents=True)
            (asm_dir / "genomic.fna.gz").write_bytes(b"fasta_data")
        # For the second assembly, do not create any directory to exercise the missing-dir path
        return _MOCK_STATS

    with (
        _patch_ftp_download(side_effect=_fake_download),
        patch(
            "cdm_data_loaders.pipelines.ncbi_ftp_download.upload_file",
            return_value=False,
        ),
    ):
        report = download_and_stage(
            bucket=_TEST_BUCKET,
            staging_key_prefix=_STAGING_PREFIX,
            manifest_local_path=manifest_local,
            dry_run=False,
            threads=1,
        )

    assert report["succeeded"] == _EXPECTED_ATTEMPTED
    assert report["staged_objects"] == 0
