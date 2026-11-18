"""NCBI FTP assembly summary download, parsing, prefix filtering, and diffing."""

import csv
from collections.abc import Iterable
from dataclasses import dataclass, field
from logging import Logger, getLogger
from pathlib import Path, PurePosixPath
from types import MappingProxyType
from typing import Final
from urllib.parse import urlsplit

from cdm_data_loaders.datasets.ncbi_ftp.constants import ACCESSION_PARTS_REGEX, FTP_HOST, Source
from cdm_data_loaders.io.ftp_client import ftp_retrieve_text, ftp_session

logger: Logger = getLogger(__name__)

SUMMARY_FTP_PATHS: Final[MappingProxyType[str, PurePosixPath]] = MappingProxyType(
    {
        Source.REFSEQ: PurePosixPath("/") / "genomes" / "ASSEMBLY_REPORTS" / "assembly_summary_refseq.txt",
        Source.GENBANK: PurePosixPath("/") / "genomes" / "ASSEMBLY_REPORTS" / "assembly_summary_genbank.txt",
    }
)

ACCESSION_COL: Final[int] = 0
STATUS_COL: Final[int] = 10
SEQ_REL_DATE_COL: Final[int] = 14
FTP_URL_COL: Final[int] = 19
MIN_COL: Final[int] = 20


@dataclass
class AssemblyRecord:
    """Parsed row from an NCBI assembly summary file."""

    accession: str
    status: str
    seq_rel_date: str
    ftp_url: str
    assembly_dir: PurePosixPath


@dataclass
class DiffResult:
    """Result of comparing two NCBI assembly summary states."""

    new: list[str] = field(default_factory=list)
    updated: list[str] = field(default_factory=list)
    replaced: list[str] = field(default_factory=list)
    suppressed: list[str] = field(default_factory=list)


def ftp_dir_from_url(ftp_url: str, ftp_host: str = FTP_HOST) -> PurePosixPath:
    """Convert an FTP or HTTPS URL from the assembly summary to an FTP directory path.

    :param ftp_url: FTP or HTTPS URL from an assembly summary row
    :type ftp_url: str
    :param ftp_host: expected FTP hostname
    :type ftp_host: str
    :return: FTP directory path
    :rtype: PurePosixPath
    :raises ValueError: if ``ftp_url`` uses an unexpected scheme or host
    """
    parsed = urlsplit(ftp_url)
    if parsed.scheme in {"ftp", "https"}:
        if parsed.netloc and parsed.netloc != ftp_host:
            msg = f"Unexpected FTP host: {parsed.netloc}"
            raise ValueError(msg)
        return PurePosixPath(parsed.path)

    msg = f"Could not parse FTP URL: {ftp_url}"
    raise ValueError(msg)


def download_assembly_summary(database: str = Source.REFSEQ, ftp_host: str = FTP_HOST) -> str:
    """Download the assembly summary TSV from NCBI FTP.

    :param database: ``"refseq"`` or ``"genbank"``
    :type database: str
    :param ftp_host: FTP hostname
    :type ftp_host: str
    :return: raw text content of the summary file
    :rtype: str
    :raises ValueError: if ``database`` is not ``"refseq"`` or ``"genbank"``
    """
    ftp_path = SUMMARY_FTP_PATHS.get(database)
    if not ftp_path:
        msg = f"Unknown database: {database}"
        raise ValueError(msg)

    logger.info("Downloading assembly_summary_%s.txt from NCBI FTP ...", database)
    with ftp_session(ftp_host) as ftp:
        content = ftp_retrieve_text(ftp, ftp_path)

    logger.info("Downloaded assembly summary (%d bytes)", len(content))
    return content


def parse_assembly_summary(source: str | Path | list[str]) -> dict[str, AssemblyRecord]:
    """Parse an NCBI assembly summary from a file path, raw text string, or list of lines.

    :param source: file path, raw TSV text, or list of TSV lines
    :type source: str | Path | list[str]
    :return: mapping of accession to :class:`AssemblyRecord`
    :rtype: dict[str, AssemblyRecord]
    """
    assemblies: dict[str, AssemblyRecord] = {}

    def _parse_lines(lines: Iterable[str]) -> None:
        reader = csv.reader(
            (line.rstrip("\n") for line in lines if not line.startswith("#")),
            delimiter="\t",
        )
        for row in reader:
            if len(row) < MIN_COL:
                continue
            accession = row[ACCESSION_COL]
            ftp_url = row[FTP_URL_COL]
            if "https://ftp" not in ftp_url:
                for col in row:
                    if "https://ftp" in col:
                        ftp_url = col
                        break
            if "https://ftp" not in ftp_url:
                logger.warning("Missing ftp path for record %s.", accession)
                continue
            assembly_dir = PurePosixPath(ftp_dir_from_url(ftp_url).name)
            assemblies[accession] = AssemblyRecord(
                accession=accession,
                status=row[STATUS_COL],
                seq_rel_date=row[SEQ_REL_DATE_COL],
                ftp_url=ftp_url,
                assembly_dir=assembly_dir,
            )

    if isinstance(source, Path) or (isinstance(source, str) and "\n" not in source and Path(source).is_file()):
        with Path(source).open(encoding="utf-8") as fh:
            _parse_lines(fh)
    elif isinstance(source, list):
        _parse_lines(source)
    else:
        _parse_lines(source.splitlines(keepends=True))

    logger.info("Parsed %d assemblies from summary", len(assemblies))
    return assemblies


def get_latest_assembly_paths(
    assemblies: dict[str, AssemblyRecord],
    ftp_host: str = FTP_HOST,
) -> list[tuple[str, PurePosixPath]]:
    """Extract FTP directory paths for all assemblies with ``latest`` status.

    :param assemblies: parsed assembly records
    :type assemblies: dict[str, AssemblyRecord]
    :param ftp_host: FTP hostname for URL checks
    :type ftp_host: str
    :return: list of ``(accession, ftp_dir_path)`` pairs
    :rtype: list[tuple[str, PurePosixPath]]
    """
    paths: list[tuple[str, PurePosixPath]] = []
    for accession, rec in assemblies.items():
        if rec.status != "latest":
            continue
        paths.append((accession, ftp_dir_from_url(rec.ftp_url, ftp_host)))
    return paths


def accession_prefix(accession: str) -> str:
    """Extract the first 3-digit prefix segment from an accession (for example, ``"000"``).

    :param accession: RefSeq or GenBank assembly accession
    :type accession: str
    :return: 3-digit prefix string
    :rtype: str
    :raises ValueError: if ``accession`` cannot be parsed
    """
    if m := ACCESSION_PARTS_REGEX.match(accession):
        return m.group("p1")
    msg = f"Could not parse accession: {accession}"
    raise ValueError(msg)


def filter_by_prefix_range(
    assemblies: dict[str, AssemblyRecord],
    prefix_from: str | None = None,
    prefix_to: str | None = None,
) -> dict[str, AssemblyRecord]:
    """Filter assemblies to those whose 3-digit accession prefix lies within ``[prefix_from, prefix_to]``.

    :param assemblies: mapping of accession to :class:`AssemblyRecord`
    :type assemblies: dict[str, AssemblyRecord]
    :param prefix_from: inclusive lower bound (for example, ``"000"``)
    :type prefix_from: str | None
    :param prefix_to: inclusive upper bound (for example, ``"003"``)
    :type prefix_to: str | None
    :return: filtered assembly record dictionary
    :rtype: dict[str, AssemblyRecord]
    """
    if prefix_from is None and prefix_to is None:
        return assemblies
    filtered: dict[str, AssemblyRecord] = {}
    for acc, rec in assemblies.items():
        pfx = accession_prefix(acc)
        if (prefix_from is not None and pfx < prefix_from) or (prefix_to is not None and pfx > prefix_to):
            continue
        filtered[acc] = rec
    return filtered


def compute_diff(
    current: dict[str, AssemblyRecord],
    previous_assemblies: dict[str, AssemblyRecord] | None = None,
    previous_accessions: set[str] | None = None,
) -> DiffResult:
    """Compute new, updated, replaced, and suppressed assemblies between two states.

    :param current: parsed NCBI assembly summary
    :type current: dict[str, AssemblyRecord]
    :param previous_assemblies: parsed baseline summary, or ``None`` when using store-scan fallback
    :type previous_assemblies: dict[str, AssemblyRecord] | None
    :param previous_accessions: set of known accessions from store scan when ``previous_assemblies`` is ``None``
    :type previous_accessions: set[str] | None
    :return: sorted diff lists
    :rtype: DiffResult
    """
    diff = DiffResult()
    known = set(previous_assemblies) if previous_assemblies is not None else (previous_accessions or set())

    for acc, rec in current.items():
        if rec.status in ("replaced", "suppressed"):
            if acc in known:
                getattr(diff, rec.status).append(acc)
            continue
        if rec.status != "latest":
            continue

        if acc not in known:
            diff.new.append(acc)
        elif previous_assemblies is not None:
            prev = previous_assemblies.get(acc)
            if prev and (rec.seq_rel_date > prev.seq_rel_date or rec.assembly_dir != prev.assembly_dir):
                diff.updated.append(acc)

    diff.suppressed.extend(known - set(current))

    diff.new.sort()
    diff.updated.sort()
    diff.replaced.sort()
    diff.suppressed.sort()
    return diff
