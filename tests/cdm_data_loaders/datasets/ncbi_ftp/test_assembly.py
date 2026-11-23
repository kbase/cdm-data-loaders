"""Tests for ncbi_ftp.assembly module — path helpers, file filtering, and FTP assembly downloads."""

from collections.abc import Callable
from contextlib import nullcontext
from ftplib import FTP
from pathlib import Path, PurePosixPath
from unittest.mock import MagicMock, patch

import pytest

from cdm_data_loaders.datasets.ncbi_ftp.assembly import (
    MAX_DOWNLOAD_ATTEMPTS,
    build_accession_path,
    download_assembly_to_local,
    extract_accession,
    filter_assembly_checksums,
    matches_assembly_file_filter,
    parse_assembly_path,
)
from cdm_data_loaders.utils.checksums import new_hasher, parse_md5_checksums_file
from tests.cdm_data_loaders.datasets.ncbi_ftp._helpers import ACC_PATH_215

# Path helpers


@pytest.mark.parametrize(
    ("assembly_dir", "expected"),
    [
        pytest.param(
            "GCF_000001215.4_Release_6_plus_ISO1_MT",
            PurePosixPath("raw_data") / ACC_PATH_215,
            id="gcf",
        ),
        pytest.param(
            "GCA_012345678.1_ASM1234v1",
            PurePosixPath("raw_data") / "GCA" / "012" / "345" / "678" / "GCA_012345678.1_ASM1234v1",
            id="gca",
        ),
    ],
)
def test_build_accession_path(assembly_dir: PurePosixPath, expected: str) -> None:
    """Verify accession path construction for various inputs."""
    assert build_accession_path(assembly_dir) == expected


def test_build_accession_path_invalid() -> None:
    """Verify ValueError on invalid assembly name."""
    with pytest.raises(ValueError, match="Cannot parse"):
        build_accession_path(PurePosixPath("invalid_name"))


def test_parse_assembly_path() -> None:
    """Verify db, assembly_dir, and accession are parsed correctly."""
    assert parse_assembly_path(PurePosixPath("/") / "genomes" / "all" / ACC_PATH_215) == (
        "GCF",
        PurePosixPath("GCF_000001215.4_Release_6_plus_ISO1_MT"),
        "GCF_000001215.4",
    )


def test_parse_assembly_path_invalid() -> None:
    """Verify ValueError on invalid path."""
    with pytest.raises(ValueError, match="Cannot parse"):
        parse_assembly_path(PurePosixPath("/") / "random" / "path")


# parse_md5_checksums_file


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        pytest.param(
            "abc123  ./GCF_000001215.4_genomic.fna.gz\ndef456  ./GCF_000001215.4_genomic.gff.gz\n",
            {
                PurePosixPath("GCF_000001215.4_genomic.fna.gz"): "abc123",
                PurePosixPath("GCF_000001215.4_genomic.gff.gz"): "def456",
            },
            id="dot_slash_prefix",
        ),
        pytest.param(
            "abc123  GCF_000001215.4_genomic.fna.gz\n",
            {PurePosixPath("GCF_000001215.4_genomic.fna.gz"): "abc123"},
            id="no_dot_slash_prefix",
        ),
        pytest.param("", {}, id="empty_string"),
        pytest.param("   \n  \n", {}, id="whitespace_only"),
        pytest.param(
            "abc123  file1.txt\n\n\ndef456  file2.txt\n",
            {PurePosixPath("file1.txt"): "abc123", PurePosixPath("file2.txt"): "def456"},
            id="blank_lines_ignored",
        ),
    ],
)
def test_parse_md5_checksums_file(text: str, expected: dict[PurePosixPath, str]) -> None:
    """Verify parse_md5_checksums_file handles various input formats."""
    assert parse_md5_checksums_file(text) == expected


@pytest.mark.parametrize(
    ("entry", "expected"),
    [
        pytest.param(
            "/genomes/all/GCF/000/001/215/GCF_000001215.4_Release_6_plus_ISO1_MT/",
            "GCF_000001215.4",
            id="ftp-dir-path",
        ),
        pytest.param(
            PurePosixPath("cdm-lake/ncbi-refseq/raw_data/GCA/012/345/678/GCA_012345678.10_ASM123v10/file.fna.gz"),
            "GCA_012345678.10",
            id="s3-object-key",
        ),
        pytest.param("GCF_000005845.2_ASM584v2", "GCF_000005845.2", id="assembly-dir-name"),
        pytest.param("  GCF_000005845.20  ", "GCF_000005845.20", id="bare-accession-padded"),
    ],
)
def test_extract_accession_valid(entry: str | PurePosixPath, expected: str) -> None:
    """Extract exact versioned accessions from FTP paths, S3 keys, directory names, and bare IDs."""
    assert extract_accession(entry) == expected


@pytest.mark.parametrize(
    "entry",
    [
        pytest.param("", id="empty"),
        pytest.param("   ", id="whitespace"),
        pytest.param("GCF_000005845.2extra", id="trailing-alphanumeric-without-underscore"),
        pytest.param("XYZ_000005845.2", id="unknown-prefix"),
        pytest.param("/genomes/all/random/folder", id="unparseable-path"),
    ],
)
def test_extract_accession_invalid(entry: str) -> None:
    """Reject empty, malformed, or non-RefSeq/GenBank entries with ValueError."""
    with pytest.raises(ValueError, match="Cannot parse accession"):
        extract_accession(entry)


@pytest.mark.parametrize(
    ("filename", "expected"),
    [
        pytest.param(PurePosixPath("GCF_000001215.4_genomic.fna.gz"), True, id="genomic-fna"),
        pytest.param(PurePosixPath("sub/GCF_000001215.4_protein.faa.gz"), True, id="nested-protein-faa"),
        pytest.param(PurePosixPath("GCF_000001215.4_assembly_report.txt"), True, id="assembly-report"),
        pytest.param(PurePosixPath("md5checksums.txt"), False, id="md5checksums"),
        pytest.param(PurePosixPath("README.txt"), False, id="readme"),
    ],
)
def test_matches_assembly_file_filter(filename: PurePosixPath, expected: bool) -> None:
    """Match target assembly file suffixes and exclude unrelated files."""
    assert matches_assembly_file_filter(filename) is expected


def test_filter_assembly_checksums() -> None:
    """Retain checksum entries matching FILE_FILTERS and drop non-matching files."""
    raw = {
        PurePosixPath("GCF_000001215.4_genomic.fna.gz"): "aaa",
        PurePosixPath("GCF_000001215.4_assembly_stats.txt"): "bbb",
        PurePosixPath("annotation_hashes.txt"): "ccc",
    }
    assert filter_assembly_checksums(raw) == {
        PurePosixPath("GCF_000001215.4_genomic.fna.gz"): "aaa",
        PurePosixPath("GCF_000001215.4_assembly_stats.txt"): "bbb",
    }


def _md5_hex(data: bytes) -> str:
    """Compute lowercase MD5 hex digest for test payloads."""
    hasher = new_hasher("md5")
    hasher.update(data)
    return hasher.hexdigest()


def test_download_assembly_to_local_verifies_md5_and_handles_unlisted_file(tmp_path: Path) -> None:
    """Download filtered files, write .md5 sidecar for verified files, and track files lacking checksums."""
    fna_bytes = b">seq1\nACGT\n"
    fna_md5 = _md5_hex(fna_bytes)
    stats_bytes = b"# stats\n"
    md5_manifest = f"{fna_md5}  ./GCF_000001215.4_genomic.fna.gz\n"

    mock_ftp = MagicMock(spec=FTP)

    def fake_retrlines(cmd: str, callback: Callable[[str], None]) -> None:
        if cmd == "NLST":
            for name in [
                "md5checksums.txt",
                "GCF_000001215.4_genomic.fna.gz",
                "GCF_000001215.4_assembly_stats.txt",
                "README.txt",
            ]:
                callback(name)
        elif cmd == "RETR md5checksums.txt":
            for line in md5_manifest.splitlines():
                callback(line)

    def fake_retrbinary(cmd: str, callback: Callable[[bytes], None]) -> None:
        if cmd.endswith("_genomic.fna.gz"):
            callback(fna_bytes)
        elif cmd.endswith("_assembly_stats.txt"):
            callback(stats_bytes)

    mock_ftp.retrlines.side_effect = fake_retrlines
    mock_ftp.retrbinary.side_effect = fake_retrbinary

    assembly_ftp_path = PurePosixPath("/") / "genomes" / "all" / ACC_PATH_215
    result = download_assembly_to_local(assembly_ftp_path, tmp_path, ftp=mock_ftp)

    dest_dir = tmp_path / "raw_data" / ACC_PATH_215
    assert result == {
        "accession": "GCF_000001215.4",
        "assembly_dir": "GCF_000001215.4_Release_6_plus_ISO1_MT",
        "files_downloaded": 3,
        "files_skipped_checksum_mismatch": 0,
        "files_without_checksum": 1,
    }
    assert (dest_dir / "md5checksums.txt").read_text(encoding="utf-8") == md5_manifest.rstrip("\n")
    assert (dest_dir / "GCF_000001215.4_genomic.fna.gz").read_bytes() == fna_bytes
    assert (dest_dir / "GCF_000001215.4_genomic.fna.gz.md5").read_text(encoding="utf-8") == fna_md5
    assert (dest_dir / "GCF_000001215.4_assembly_stats.txt").read_bytes() == stats_bytes
    assert not (dest_dir / "GCF_000001215.4_assembly_stats.txt.md5").exists()
    assert not (dest_dir / "README.txt").exists()
    mock_ftp.quit.assert_not_called()


def test_download_assembly_to_local_skips_and_unlinks_after_checksum_mismatch(tmp_path: Path) -> None:
    """Retry mismatched MD5 downloads MAX_DOWNLOAD_ATTEMPTS times, then unlink the corrupt file."""
    expected_md5 = _md5_hex(b"expected")
    md5_manifest = f"{expected_md5}  ./GCF_000001215.4_genomic.fna.gz"
    mock_ftp = MagicMock(spec=FTP)
    retr_attempts = 0

    def fake_retrlines(cmd: str, callback: Callable[[str], None]) -> None:
        if cmd == "NLST":
            callback("md5checksums.txt")
            callback("GCF_000001215.4_genomic.fna.gz")
        elif cmd == "RETR md5checksums.txt":
            callback(md5_manifest)

    def fake_retrbinary(_cmd: str, callback: Callable[[bytes], None]) -> None:
        nonlocal retr_attempts
        retr_attempts += 1
        callback(b"corrupted-payload")

    mock_ftp.retrlines.side_effect = fake_retrlines
    mock_ftp.retrbinary.side_effect = fake_retrbinary

    assembly_ftp_path = PurePosixPath("/") / "genomes" / "all" / ACC_PATH_215
    result = download_assembly_to_local(assembly_ftp_path, tmp_path, ftp=mock_ftp)

    dest_dir = tmp_path / "raw_data" / ACC_PATH_215
    assert retr_attempts == MAX_DOWNLOAD_ATTEMPTS
    assert result["files_downloaded"] == 1
    assert result["files_skipped_checksum_mismatch"] == 1
    assert not (dest_dir / "GCF_000001215.4_genomic.fna.gz").exists()
    assert not (dest_dir / "GCF_000001215.4_genomic.fna.gz.md5").exists()


def test_download_assembly_to_local_no_md5_manifest_and_keepalive_args(
    tmp_path: Path,
) -> None:
    """download_assembly_to_local forwards ftp_host, retries=1, checksum_fn, and keepalive timestamps."""
    mock_ftp = MagicMock(spec=FTP)
    assembly_ftp_path = PurePosixPath("/") / "genomes" / "all" / ACC_PATH_215
    keepalive_calls: list[float] = []
    download_calls: list[tuple[PurePosixPath, int, str | None]] = []

    def fake_keepalive(_ftp: FTP, last_ts: float) -> float:
        keepalive_calls.append(last_ts)
        return last_ts + 10.0

    def fake_download(
        _ftp: FTP,
        remote: PurePosixPath,
        local: Path,
        *,
        retries: int = 3,
        checksum_fn: str | None = None,
    ) -> str | None:
        download_calls.append((remote, retries, checksum_fn))
        local.write_bytes(b"data")
        return None

    with (
        patch("cdm_data_loaders.datasets.ncbi_ftp.assembly.ftp_session", return_value=nullcontext(mock_ftp)) as m_sess,
        patch(
            "cdm_data_loaders.datasets.ncbi_ftp.assembly.ftp_list_dir",
            return_value=[
                PurePosixPath("GCF_000001215.4_genomic.fna.gz"),
                PurePosixPath("GCF_000001215.4_assembly_stats.txt"),
            ],
        ),
        patch("cdm_data_loaders.datasets.ncbi_ftp.assembly.ftp_noop_keepalive", side_effect=fake_keepalive),
        patch("cdm_data_loaders.datasets.ncbi_ftp.assembly.ftp_download_file", side_effect=fake_download),
        patch("cdm_data_loaders.datasets.ncbi_ftp.assembly.time.monotonic", side_effect=[100.0, 200.0, 300.0]),
    ):
        stats = download_assembly_to_local(assembly_ftp_path, tmp_path, ftp_host="ftp.custom.org")

    m_sess.assert_called_once_with("ftp.custom.org", None)
    assert stats == {
        "accession": "GCF_000001215.4",
        "assembly_dir": "GCF_000001215.4_Release_6_plus_ISO1_MT",
        "files_downloaded": 2,
        "files_skipped_checksum_mismatch": 0,
        "files_without_checksum": 2,
    }
    assert keepalive_calls == [100.0, 200.0]
    assert download_calls == [
        (PurePosixPath("GCF_000001215.4_genomic.fna.gz"), 1, None),
        (PurePosixPath("GCF_000001215.4_assembly_stats.txt"), 1, None),
    ]
    assert not (tmp_path / "raw_data" / ACC_PATH_215 / "md5checksums.txt").exists()
