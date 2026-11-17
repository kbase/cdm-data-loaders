"""S3 store scanning, FTP checksum verification, and manifest file writers for NCBI FTP."""

import contextlib
import json
import time
from collections.abc import Callable, Sequence
from datetime import UTC, datetime
from ftplib import FTP
from logging import Logger, getLogger
from pathlib import Path, PurePosixPath
from typing import Any, TypedDict, Unpack

from cdm_data_loaders.datasets.ncbi_ftp.assembly import (
    build_accession_path,
    filter_assembly_checksums,
    parse_assembly_path,
)
from cdm_data_loaders.datasets.ncbi_ftp.constants import FTP_HOST
from cdm_data_loaders.datasets.ncbi_ftp.summary import (
    DATABASE_ACC_PREFIX,
    AssemblyRecord,
    DiffResult,
    ftp_dir_from_url,
)
from cdm_data_loaders.io.ftp_client import connect_ftp, ftp_noop_keepalive, ftp_retrieve_text
from cdm_data_loaders.io.s3.object_utils import get_existing_object_info, list_objects
from cdm_data_loaders.utils.checksums import parse_md5_checksums_file

logger: Logger = getLogger(__name__)


class VerifyCandidatesOptions(TypedDict, total=False):
    """Optional parameters for :func:`verify_transfer_candidates`."""

    ftp_host: str
    progress_callback: Callable[[int, int, str], None] | None


def _extract_accession_dir_and_id_from_s3_key(key: PurePosixPath) -> tuple[str, str]:
    """Extract ``(assembly_dir, accession)`` from an S3 object key.

    :param key: S3 object key containing an assembly directory segment
    :type key: PurePosixPath
    :return: tuple of ``(assembly_dir_name, accession)``
    :rtype: tuple[str, str]
    :raises ValueError: if ``key`` does not contain a valid assembly directory
    """
    try:
        _db, assembly_dir, accession = parse_assembly_path(key)
    except ValueError as exc:
        msg = f"Could not parse S3 key for accession info: {key}"
        raise ValueError(msg) from exc
    return (str(assembly_dir), accession)


def scan_store_to_synthetic_summary(
    bucket: PurePosixPath,
    key_prefix: PurePosixPath,
    release_date: str,
    database: str = "refseq",
    progress_callback: Callable[[int, str], None] | None = None,
) -> dict[str, AssemblyRecord]:
    """Scan an S3 store and build a synthetic assembly summary from existing objects.

    :param bucket: S3 bucket name
    :type bucket: PurePosixPath
    :param key_prefix: S3 key prefix under which objects are scanned
    :type key_prefix: PurePosixPath
    :param release_date: synthetic release date string in ``YYYY/MM/DD`` format
    :type release_date: str
    :param database: ``"refseq"`` or ``"genbank"``
    :type database: str
    :param progress_callback: optional ``(count, accession)`` callback invoked per unique accession
    :type progress_callback: Callable[[int, str], None] | None
    :return: mapping of accession to synthetic :class:`AssemblyRecord`
    :rtype: dict[str, AssemblyRecord]
    :raises ValueError: if ``release_date`` or ``database`` is invalid
    """
    try:
        datetime.strptime(release_date, "%Y/%m/%d").replace(tzinfo=UTC)
    except ValueError as exc:
        msg = f"Invalid release_date '{release_date}'. Expected format YYYY/MM/DD."
        raise ValueError(msg) from exc

    acc_prefix = DATABASE_ACC_PREFIX.get(database)
    if acc_prefix is None:
        msg = f"Unknown database: {database!r}. Expected 'refseq' or 'genbank'."
        raise ValueError(msg)

    assemblies: dict[str, AssemblyRecord] = {}
    processed_count = 0

    try:
        objs = list_objects(str(bucket / key_prefix))
        for obj in objs:
            try:
                assembly_dir, acc = _extract_accession_dir_and_id_from_s3_key(obj["Key"])
            except ValueError:
                continue

            if not acc.startswith(acc_prefix):
                continue

            if acc not in assemblies:
                fake_ftp_path = f"https://ftp.ncbi.nlm.nih.gov/synthetic/{assembly_dir}"
                assemblies[acc] = AssemblyRecord(
                    accession=acc,
                    status="latest",
                    seq_rel_date=release_date,
                    ftp_url=fake_ftp_path,
                    assembly_dir=PurePosixPath(assembly_dir),
                )
                processed_count += 1
                if progress_callback is not None:
                    progress_callback(processed_count, acc)
    except Exception:
        logger.exception("Error scanning store")
        raise

    logger.info("Scanned S3 store: found %d unique assemblies", len(assemblies))
    return assemblies


def _fetch_accession_checksums_from_ftp(
    ftp: FTP | None,
    ftp_host: str,
    last_activity: float,
    current_ftp_url: str,
) -> tuple[dict[PurePosixPath, str], float, FTP]:
    """Fetch and filter ``md5checksums.txt`` for an assembly URL from NCBI FTP.

    :param ftp: active FTP client or ``None`` to connect lazily
    :type ftp: FTP | None
    :param ftp_host: NCBI FTP hostname
    :type ftp_host: str
    :param last_activity: monotonic timestamp of last FTP activity
    :type last_activity: float
    :param current_ftp_url: assembly FTP URL
    :type current_ftp_url: str
    :return: tuple of ``(filtered_checksums, last_activity, ftp)``
    :rtype: tuple[dict[PurePosixPath, str], float, FTP]
    """
    if ftp is None:
        ftp = connect_ftp(ftp_host)
    last_activity = ftp_noop_keepalive(ftp, last_activity)
    ftp_dir = ftp_dir_from_url(current_ftp_url, ftp_host)
    try:
        md5_text = ftp_retrieve_text(ftp, ftp_dir / "md5checksums.txt")
        last_activity = time.monotonic()
        ftp_checksums = parse_md5_checksums_file(md5_text)
    except Exception:
        logger.exception("Cannot fetch md5checksums.txt for %s, keeping in transfer list", current_ftp_url)
        return {}, last_activity, ftp
    return filter_assembly_checksums(ftp_checksums), last_activity, ftp


def _does_accession_need_update(
    target_checksums: dict[PurePosixPath, str],
    bucket: PurePosixPath,
    s3_prefix: PurePosixPath,
) -> bool:
    """Return ``True`` if any filtered file is missing from S3 or has a mismatched ``md5`` metadata tag.

    :param target_checksums: mapping of file names to expected MD5 hex digests
    :type target_checksums: dict[PurePosixPath, str]
    :param bucket: S3 bucket name
    :type bucket: PurePosixPath
    :param s3_prefix: S3 key prefix for the assembly
    :type s3_prefix: PurePosixPath
    :return: ``True`` when at least one file needs transfer, ``False`` when all match
    :rtype: bool
    """
    for fname, expected_md5 in target_checksums.items():
        s3_path = bucket / s3_prefix / fname
        obj_info = get_existing_object_info(str(s3_path))
        if obj_info is None:
            logger.debug("File missing from store: %s", s3_path)
            return True

        s3_md5 = obj_info.get("Metadata", {}).get("md5", "")
        if s3_md5 != expected_md5:
            logger.debug("MD5 mismatch for %s: S3=%s FTP=%s", s3_path, s3_md5, expected_md5)
            return True

    return False


def verify_transfer_candidates(
    accessions: list[str],
    current_assemblies: dict[str, AssemblyRecord],
    bucket: PurePosixPath,
    key_prefix: PurePosixPath,
    **options: Unpack[VerifyCandidatesOptions],
) -> list[str]:
    """Filter candidate accessions to those missing from S3 or differing from FTP MD5 checksums.

    :param accessions: candidate accessions (new + updated from diff)
    :type accessions: list[str]
    :param current_assemblies: parsed current assembly summary
    :type current_assemblies: dict[str, AssemblyRecord]
    :param bucket: S3 bucket name
    :type bucket: PurePosixPath
    :param key_prefix: S3 key prefix for the Lakehouse dataset root
    :type key_prefix: PurePosixPath
    :param options: optional ``ftp_host`` and ``progress_callback`` overrides
    :type options: Unpack[VerifyCandidatesOptions]
    :return: confirmed list of accessions requiring download
    :rtype: list[str]
    """
    if not accessions:
        return []

    ftp_host = options.get("ftp_host", FTP_HOST)
    progress_callback = options.get("progress_callback")

    ftp: FTP | None = None
    confirmed: list[str] = []
    pruned = 0
    skipped_missing = 0
    last_activity = time.monotonic()

    def _progress(done: int, total: int, acc: str) -> None:
        if progress_callback is not None:
            progress_callback(done, total, acc)

    try:
        for done, acc in enumerate(accessions, start=1):
            rec = current_assemblies.get(acc)
            if not rec:
                confirmed.append(acc)
                _progress(done, len(accessions), acc)
                continue

            s3_prefix = key_prefix / build_accession_path(rec.assembly_dir)
            resp = list_objects(str(bucket / s3_prefix), max_keys=1)
            if not resp:
                confirmed.append(acc)
                skipped_missing += 1
                _progress(done, len(accessions), acc)
                continue

            target_checksums, last_activity, ftp = _fetch_accession_checksums_from_ftp(
                ftp, ftp_host, last_activity, rec.ftp_url
            )
            if not target_checksums or _does_accession_need_update(target_checksums, bucket, s3_prefix):
                confirmed.append(acc)
            else:
                pruned += 1
                logger.debug("Pruned %s — all files match S3 checksums", acc)

            _progress(done, len(accessions), acc)
    finally:
        if ftp is not None:
            with contextlib.suppress(Exception):
                ftp.quit()

    logger.info(
        "Checksum verification: %d confirmed (%d missing from store), %d pruned (of %d candidates)",
        len(confirmed),
        skipped_missing,
        pruned,
        len(accessions),
    )
    return confirmed


def _write_manifest_lines(items: Sequence[str | PurePosixPath], output_path: Path, label: str) -> None:
    """Write one item per line to ``output_path`` and log the entry count."""
    with Path(output_path).open("w", encoding="utf-8") as fh:
        fh.writelines(f"{item}\n" for item in items)
    logger.info("Wrote %d entries to %s manifest: %s", len(items), label, output_path)


def write_transfer_manifest(
    diff: DiffResult,
    current_assemblies: dict[str, AssemblyRecord],
    output_path: Path,
    ftp_host: str = FTP_HOST,
) -> list[PurePosixPath]:
    """Write the transfer manifest (new + updated assemblies) as FTP directory paths.

    :param diff: computed diff result
    :type diff: DiffResult
    :param current_assemblies: parsed current assembly summary
    :type current_assemblies: dict[str, AssemblyRecord]
    :param output_path: destination manifest file path
    :type output_path: Path
    :param ftp_host: FTP hostname for URL validation
    :type ftp_host: str
    :return: list of FTP paths written
    :rtype: list[PurePosixPath]
    """
    paths: list[PurePosixPath] = []
    for acc in sorted(diff.new + diff.updated):
        rec = current_assemblies.get(acc)
        if not rec:
            continue
        paths.append(ftp_dir_from_url(rec.ftp_url, ftp_host))

    _write_manifest_lines(paths, output_path, "transfer")
    return paths


def write_removed_manifest(diff: DiffResult, output_path: Path) -> list[str]:
    """Write the removed manifest (replaced + suppressed accessions).

    :param diff: computed diff result
    :type diff: DiffResult
    :param output_path: destination manifest file path
    :type output_path: Path
    :return: sorted list of removed accessions written
    :rtype: list[str]
    """
    removed = sorted(diff.replaced + diff.suppressed)
    _write_manifest_lines(removed, output_path, "removed")
    return removed


def write_updated_manifest(diff: DiffResult, output_path: Path) -> list[str]:
    """Write the updated manifest (accessions whose content changed).

    :param diff: computed diff result
    :type diff: DiffResult
    :param output_path: destination manifest file path
    :type output_path: Path
    :return: sorted list of updated accessions written
    :rtype: list[str]
    """
    updated = sorted(diff.updated)
    _write_manifest_lines(updated, output_path, "updated")
    return updated


def write_diff_summary(
    diff: DiffResult,
    output_path: Path,
    database: str,
    prefix_from: str | None = None,
    prefix_to: str | None = None,
) -> dict[str, Any]:
    """Write a JSON diff summary report to ``output_path``.

    :param diff: computed diff result
    :type diff: DiffResult
    :param output_path: destination JSON file path
    :type output_path: Path
    :param database: database name (``"refseq"`` or ``"genbank"``)
    :type database: str
    :param prefix_from: lower bound of prefix filter, if set
    :type prefix_from: str | None
    :param prefix_to: upper bound of prefix filter, if set
    :type prefix_to: str | None
    :return: summary dictionary written to disk
    :rtype: dict[str, Any]
    """
    summary: dict[str, Any] = {
        "database": database,
        "timestamp": datetime.now(UTC).isoformat(),
        "prefix_range": {
            "from": prefix_from,
            "to": prefix_to,
        },
        "counts": {
            "new": len(diff.new),
            "updated": len(diff.updated),
            "replaced": len(diff.replaced),
            "suppressed": len(diff.suppressed),
            "total_to_transfer": len(diff.new) + len(diff.updated),
            "total_to_remove": len(diff.replaced) + len(diff.suppressed),
        },
        "accessions": {
            "new": diff.new,
            "updated": diff.updated,
            "replaced": diff.replaced,
            "suppressed": diff.suppressed,
        },
    }
    with output_path.open("w", encoding="utf-8") as fh:
        json.dump(summary, fh, indent=2)
    logger.info("Wrote diff summary to: %s", output_path)
    return summary
