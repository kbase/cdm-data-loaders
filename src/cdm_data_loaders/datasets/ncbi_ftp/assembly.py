"""NCBI FTP assembly path helpers, file filters, and single-assembly FTP download."""

import contextlib
import time
from dataclasses import dataclass
from ftplib import FTP
from logging import Logger, getLogger
from pathlib import Path, PurePosixPath
from typing import Any, Final

from cdm_data_loaders.datasets.ncbi_ftp.constants import ACCESSION_PARTS_REGEX, ASSEMBLY_PATH_REGEX, FTP_HOST
from cdm_data_loaders.io.ftp_client import (
    ftp_download_file,
    ftp_list_dir,
    ftp_noop_keepalive,
    ftp_retrieve_text,
    ftp_session,
)
from cdm_data_loaders.utils.checksums import parse_md5_checksums_file

logger: Logger = getLogger(__name__)

FILE_FILTERS: Final[tuple[str, ...]] = (
    "_gene_ontology.gaf.gz",
    "_genomic.fna.gz",
    "_genomic.gff.gz",
    "_protein.faa.gz",
    "_ani_contam_ranges.tsv",
    "_assembly_regions.txt",
    "_assembly_report.txt",
    "_assembly_stats.txt",
    "_gene_expression_counts.txt.gz",
    "_normalized_gene_expression_counts.txt.gz",
)

MAX_DOWNLOAD_ATTEMPTS: Final[int] = 3


@dataclass(frozen=True, slots=True)
class _AssemblyDownloadContext:
    """Per-assembly local destination, expected MD5 table, and mutable stats dictionary."""

    dest_dir: Path
    md5_checksums: dict[PurePosixPath, str]
    stats: dict[str, Any]


def matches_assembly_file_filter(path: PurePosixPath) -> bool:
    """Return ``True`` when ``path`` ends with one of the suffixes in ``FILE_FILTERS``.

    :param path: relative or remote file path
    :type path: PurePosixPath
    :return: ``True`` if ``path`` matches ``FILE_FILTERS``
    :rtype: bool
    """
    return any(path.match(f"**{suffix}") for suffix in FILE_FILTERS)


def filter_assembly_checksums(checksums: dict[PurePosixPath, str]) -> dict[PurePosixPath, str]:
    """Filter a parsed ``md5checksums.txt`` mapping to files matching ``FILE_FILTERS``.

    :param checksums: mapping of relative file paths to MD5 hex digests
    :type checksums: dict[PurePosixPath, str]
    :return: filtered mapping retaining entries whose suffixes match ``FILE_FILTERS``
    :rtype: dict[PurePosixPath, str]
    """
    return {fname: md5 for fname, md5 in checksums.items() if matches_assembly_file_filter(fname)}


def build_accession_path(assembly_dir: PurePosixPath) -> PurePosixPath:
    """Build the relative output path ``raw_data/{GCF|GCA}/{000}/{001}/{215}/{assembly_dir}``.

    :param assembly_dir: assembly directory name (for example, ``GCF_000001215.4_Release_6...``)
    :type assembly_dir: PurePosixPath
    :return: relative destination path
    :rtype: PurePosixPath
    :raises ValueError: if ``assembly_dir`` does not start with a valid RefSeq or GenBank accession
    """
    if m := ACCESSION_PARTS_REGEX.match(str(assembly_dir)):
        return PurePosixPath("raw_data", m.group("db"), m.group("p1"), m.group("p2"), m.group("p3"), assembly_dir)
    msg = f"Cannot parse accession: {assembly_dir}"
    raise ValueError(msg)


def parse_assembly_path(assembly_path: PurePosixPath) -> tuple[str, PurePosixPath, str]:
    """Extract ``(database, assembly_dir, accession)`` from an FTP or S3 assembly path.

    :param assembly_path: FTP or S3 path containing an assembly directory segment
    :type assembly_path: PurePosixPath
    :return: tuple of ``(database, assembly_dir, accession)``
    :rtype: tuple[str, PurePosixPath, str]
    :raises ValueError: if ``assembly_path`` cannot be parsed
    """
    if m := ASSEMBLY_PATH_REGEX.search(str(assembly_path)):
        return m.group("db"), PurePosixPath(m.group("assembly_dir")), m.group("accession")
    msg = f"Cannot parse assembly path: {assembly_path}"
    raise ValueError(msg)


def extract_accession(entry: str | PurePosixPath) -> str:
    """Extract the exact versioned accession ID from an FTP path, S3 key, directory name, or bare accession.

    :param entry: path or accession token (for example, ``"/genomes/all/.../GCF_000001215.4_Release_6/"`` or ``"GCF_000001215.4"``)
    :type entry: str | PurePosixPath
    :return: exact accession string (for example, ``"GCF_000001215.4"``)
    :rtype: str
    :raises ValueError: if ``entry`` does not contain a valid RefSeq or GenBank accession
    """
    raw = str(entry).strip()
    if not raw:
        msg = f"Cannot parse accession: {entry}"
        raise ValueError(msg)
    with contextlib.suppress(ValueError):
        _db, _assembly_dir, accession = parse_assembly_path(PurePosixPath(raw))
        return accession
    if m := ACCESSION_PARTS_REGEX.match(raw.strip("/")):
        return m.group("accession")
    msg = f"Cannot parse accession: {entry}"
    raise ValueError(msg)


def _download_and_verify(
    ftp: FTP,
    filename: PurePosixPath,
    ctx: _AssemblyDownloadContext,
    last_activity: float,
) -> float:
    """Download one assembly file, verify its MD5 checksum in-stream, and write an ``.md5`` sidecar if valid.

    :param ftp: active FTP connection
    :type ftp: FTP
    :param filename: remote file name within the active FTP working directory
    :type filename: PurePosixPath
    :param ctx: local destination directory, MD5 table, and download stats
    :type ctx: _AssemblyDownloadContext
    :param last_activity: monotonic timestamp of the prior FTP operation
    :type last_activity: float
    :return: updated monotonic last-activity timestamp
    :rtype: float
    """
    last_activity = ftp_noop_keepalive(ftp, last_activity)
    local_file = ctx.dest_dir / filename
    expected_md5 = ctx.md5_checksums.get(filename)

    for attempt in range(1, MAX_DOWNLOAD_ATTEMPTS + 1):
        logger.debug("  Downloading %s (attempt %d/%d)", filename, attempt, MAX_DOWNLOAD_ATTEMPTS)
        actual_md5 = ftp_download_file(
            ftp,
            filename,
            local_file,
            retries=1,
            checksum_fn="md5" if expected_md5 else None,
        )
        last_activity = time.monotonic()

        if not expected_md5:
            ctx.stats["files_without_checksum"] += 1
            ctx.stats["files_downloaded"] += 1
            return last_activity

        if actual_md5 == expected_md5:
            (ctx.dest_dir / f"{filename}.md5").write_text(expected_md5, encoding="utf-8")
            logger.debug("  MD5 verified: %s", filename)
            ctx.stats["files_downloaded"] += 1
            return last_activity

        logger.warning(
            "  MD5 mismatch for %s: expected %s",
            filename,
            expected_md5,
        )

    ctx.stats["files_skipped_checksum_mismatch"] += 1
    local_file.unlink(missing_ok=True)
    return last_activity


def download_assembly_to_local(
    assembly_path: PurePosixPath,
    output_dir: Path,
    ftp_host: str = FTP_HOST,
    ftp: FTP | None = None,
) -> dict[str, Any]:
    """Download one assembly from NCBI FTP to a local directory.

    Creates a directory layout under ``output_dir`` matching the Lakehouse S3 layout,
    downloads filtered files, verifies MD5 checksums, and writes ``.md5`` sidecar files.

    :param assembly_path: FTP directory path for the assembly
    :type assembly_path: PurePosixPath
    :param output_dir: base local output directory
    :type output_dir: Path
    :param ftp_host: FTP hostname
    :type ftp_host: str
    :param ftp: optional caller-managed FTP connection
    :type ftp: FTP | None
    :return: JSON-serializable dictionary of download statistics
    :rtype: dict[str, Any]
    """
    _database, assembly_dir, accession = parse_assembly_path(assembly_path)
    rel_path = build_accession_path(assembly_dir)
    dest_dir: Path = output_dir / rel_path
    dest_dir.mkdir(parents=True, exist_ok=True)

    logger.debug("Downloading %s -> %s", accession, dest_dir)

    stats: dict[str, Any] = {
        "accession": accession,
        "assembly_dir": str(assembly_dir),
        "files_downloaded": 0,
        "files_skipped_checksum_mismatch": 0,
        "files_without_checksum": 0,
    }

    with ftp_session(ftp_host, ftp) as active_ftp:
        files = ftp_list_dir(active_ftp, assembly_path)

        md5_checksums: dict[PurePosixPath, str] = {}
        if PurePosixPath("md5checksums.txt") in files:
            md5_text = ftp_retrieve_text(active_ftp, PurePosixPath("md5checksums.txt"))
            md5_checksums = parse_md5_checksums_file(md5_text)
            (dest_dir / "md5checksums.txt").write_text(md5_text, encoding="utf-8")
            stats["files_downloaded"] += 1

        target_files = [f for f in files if matches_assembly_file_filter(f)]
        last_activity = time.monotonic()
        ctx = _AssemblyDownloadContext(dest_dir=dest_dir, md5_checksums=md5_checksums, stats=stats)

        for filename in target_files:
            last_activity = _download_and_verify(active_ftp, filename, ctx, last_activity)

        logger.debug("  %s: %d files downloaded", accession, stats["files_downloaded"])

    return stats
