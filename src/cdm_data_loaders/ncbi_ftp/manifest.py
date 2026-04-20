"""Phase 1: Assembly summary diffing and manifest generation.

Downloads the current NCBI assembly summary from FTP, compares it against a
previous snapshot, and produces ``transfer_manifest.txt`` (assemblies to
download), ``removed_manifest.txt`` (assemblies to archive), and a JSON diff
summary.  All filtering logic (prefix range, limit) lives here so that
downstream phases receive a final, pre-filtered manifest.
"""

import contextlib
import csv
import json
import re
import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from cdm_data_loaders.ncbi_ftp.assembly import (
    FILE_FILTERS,
    FTP_HOST,
    build_accession_path,
    parse_md5_checksums_file,
)
from cdm_data_loaders.utils.cdm_logger import get_cdm_logger
from cdm_data_loaders.utils.ftp_client import connect_ftp, ftp_noop_keepalive, ftp_retrieve_text
from cdm_data_loaders.utils.s3 import get_s3_client, head_object

logger = get_cdm_logger()

SUMMARY_FTP_PATHS: dict[str, str] = {
    "refseq": "/genomes/ASSEMBLY_REPORTS/assembly_summary_refseq.txt",
    "genbank": "/genomes/ASSEMBLY_REPORTS/assembly_summary_genbank.txt",
}


# ── Data structures ─────────────────────────────────────────────────────


@dataclass
class AssemblyRecord:
    """Parsed row from an NCBI assembly summary file."""

    accession: str
    status: str
    seq_rel_date: str
    ftp_path: str
    assembly_dir: str


@dataclass
class DiffResult:
    """Result of comparing current and previous assembly summaries."""

    new: list[str] = field(default_factory=list)
    updated: list[str] = field(default_factory=list)
    replaced: list[str] = field(default_factory=list)
    suppressed: list[str] = field(default_factory=list)


# ── Assembly summary download & parsing ──────────────────────────────────


def download_assembly_summary(database: str = "refseq", ftp_host: str = FTP_HOST) -> str:
    """Download the assembly summary file from NCBI FTP.

    :param database: ``"refseq"`` or ``"genbank"``
    :param ftp_host: FTP hostname
    :return: raw text content of the summary file
    """
    ftp_path = SUMMARY_FTP_PATHS.get(database)
    if not ftp_path:
        msg = f"Unknown database: {database}"
        raise ValueError(msg)

    logger.info("Downloading assembly_summary_%s.txt from NCBI FTP ...", database)
    ftp = connect_ftp(ftp_host)
    try:
        content = ftp_retrieve_text(ftp, ftp_path)
    finally:
        with contextlib.suppress(Exception):
            ftp.quit()

    logger.info("Downloaded assembly summary (%d bytes)", len(content))
    return content


def parse_assembly_summary(source: str | Path | list[str]) -> dict[str, AssemblyRecord]:
    """Parse an NCBI assembly summary into a dict of assembly records.

    Accepts a file path, raw text string, or list of lines.

    Columns of interest (0-indexed):
        0: assembly_accession (e.g. GCF_000001215.4)
        10: version_status ("latest", "replaced", "suppressed")
        14: seq_rel_date
        19: ftp_path (full FTP URL or "na")

    :param source: file path, raw text, or list of lines
    :return: dict mapping accession to :class:`AssemblyRecord`
    """
    assemblies: dict[str, AssemblyRecord] = {}

    def _parse_lines(lines: Iterable[str]) -> None:
        reader = csv.reader(
            (line.rstrip("\n") for line in lines if not line.startswith("#")),
            delimiter="\t",
        )
        for row in reader:
            if len(row) < 20:  # noqa: PLR2004
                continue
            accession = row[0]
            ftp_path = row[19]
            if ftp_path == "na":
                continue
            assemblies[accession] = AssemblyRecord(
                accession=accession,
                status=row[10],
                seq_rel_date=row[14],
                ftp_path=ftp_path,
                assembly_dir=ftp_path.rstrip("/").split("/")[-1],
            )

    if isinstance(source, Path) or (isinstance(source, str) and "\n" not in source and Path(source).is_file()):
        with Path(source).open() as f:
            _parse_lines(f)
    elif isinstance(source, list):
        _parse_lines(source)
    else:
        _parse_lines(source.splitlines(keepends=True))

    logger.info("Parsed %d assemblies from summary", len(assemblies))
    return assemblies


def get_latest_assembly_paths(assemblies: dict[str, AssemblyRecord], ftp_host: str = FTP_HOST) -> list[tuple[str, str]]:
    """Extract FTP directory paths for all assemblies with ``latest`` status.

    :param assemblies: parsed assembly records
    :param ftp_host: FTP hostname for URL stripping
    :return: list of ``(accession, ftp_dir_path)`` tuples
    """
    paths: list[tuple[str, str]] = []
    for accession, rec in assemblies.items():
        if rec.status != "latest":
            continue
        ftp_path = _ftp_dir_from_url(rec.ftp_path, ftp_host)
        paths.append((accession, ftp_path.rstrip("/") + "/"))
    return paths


# ── Prefix filtering ────────────────────────────────────────────────────


def accession_prefix(accession: str) -> str | None:
    """Extract the 3-digit prefix from an accession (e.g. ``GCF_000005845.2`` → ``"000"``)."""
    m = re.match(r"GC[AF]_(\d{3})\d{6}\.\d+", accession)
    return m.group(1) if m else None


def filter_by_prefix_range(
    assemblies: dict[str, AssemblyRecord],
    prefix_from: str | None = None,
    prefix_to: str | None = None,
) -> dict[str, AssemblyRecord]:
    """Filter assemblies to those whose 3-digit accession prefix is in range.

    Both bounds are inclusive. If neither is set, returns all assemblies.

    :param assemblies: dict of parsed assembly records
    :param prefix_from: lower bound (inclusive), e.g. ``"000"``
    :param prefix_to: upper bound (inclusive), e.g. ``"003"``
    :return: filtered dict
    """
    if prefix_from is None and prefix_to is None:
        return assemblies
    filtered: dict[str, AssemblyRecord] = {}
    for acc, rec in assemblies.items():
        pfx = accession_prefix(acc)
        if pfx is None:
            continue
        if prefix_from is not None and pfx < prefix_from:
            continue
        if prefix_to is not None and pfx > prefix_to:
            continue
        filtered[acc] = rec
    return filtered


# ── Diff computation ────────────────────────────────────────────────────


def compute_diff(  # noqa: PLR0912
    current: dict[str, AssemblyRecord],
    previous_assemblies: dict[str, AssemblyRecord] | None = None,
    previous_accessions: set[str] | None = None,
) -> DiffResult:
    """Compute the diff between current and previous assembly state.

    :param current: the new NCBI summary (parsed)
    :param previous_assemblies: full parsed previous summary, or None if using fallback
    :param previous_accessions: set of known accessions (store-scan fallback)
    :return: diff result with new/updated/replaced/suppressed lists
    """
    diff = DiffResult()

    if previous_assemblies is not None:
        known = set(previous_assemblies.keys())
    elif previous_accessions is not None:
        known = previous_accessions
    else:
        known = set()

    for acc, rec in current.items():
        if rec.status == "replaced":
            if acc in known:
                diff.replaced.append(acc)
            continue
        if rec.status == "suppressed":
            if acc in known:
                diff.suppressed.append(acc)
            continue
        if rec.status != "latest":
            continue

        if acc not in known:
            diff.new.append(acc)
        elif previous_assemblies is not None:
            prev = previous_assemblies.get(acc)
            if prev and (rec.seq_rel_date != prev.seq_rel_date or rec.assembly_dir != prev.assembly_dir):
                diff.updated.append(acc)

    # Accessions in previous but entirely absent from current (withdrawn)
    current_accs = set(current.keys())
    for acc in known:
        if acc not in current_accs and acc not in diff.suppressed:
            diff.suppressed.append(acc)

    diff.new.sort()
    diff.updated.sort()
    diff.replaced.sort()
    diff.suppressed.sort()
    return diff


# ── FTP URL helpers ──────────────────────────────────────────────────────


def _ftp_dir_from_url(ftp_url: str, ftp_host: str = FTP_HOST) -> str:
    """Convert an FTP URL from the assembly summary to an FTP directory path."""
    if ftp_url.startswith("https://"):
        return ftp_url.replace(f"https://{ftp_host}", "")
    if ftp_url.startswith("ftp://"):
        return ftp_url.replace(f"ftp://{ftp_host}", "")
    return ftp_url


# ── Synthetic summary from S3 store scan ────────────────────────────────


def _extract_accession_from_s3_key(key: str) -> str | None:
    """Extract the assembly accession from an S3 object key.

    Looks for the pattern GCF_######.# or GCA_######.# in the key path.

    :param key: S3 object key
    :return: accession (e.g. "GCF_000001215.4") or None if not found
    """
    m = re.search(r"(GC[AF]_\d{3}\d{6}\.\d+)", key)
    return m.group(1) if m else None


def _extract_assembly_dir_from_s3_key(key: str) -> str | None:
    """Extract the assembly directory name from an S3 object key.

    The assembly directory is the path component that follows the accession
    and contains assembly metadata (e.g. "GCF_000001215.4_Release_6_plus_ISO1_MT").

    :param key: S3 object key
    :return: assembly directory name or None if not found
    """
    # Match accession followed by underscore and then capture until next /
    m = re.search(r"(GC[AF]_\d{3}\d{6}\.\d+[^/]*)/", key)
    return m.group(1) if m else None


def scan_store_to_synthetic_summary(
    bucket: str,
    key_prefix: str,
    progress_callback: Callable[[int, str], None] | None = None,
) -> dict[str, AssemblyRecord]:
    """Scan S3 store and build a synthetic assembly summary from existing objects.

    This function is useful when bootstrapping a diffs against an existing,
    pre-populated S3 store that lacks a baseline assembly summary.

    For each assembly found in the store:
    - Extracts the accession and assembly directory name from S3 paths
    - Uses the earliest ``LastModified`` timestamp across all files in that
      assembly as the synthetic ``seq_rel_date`` (conservative estimate)
    - Creates an ``AssemblyRecord`` with ``status="latest"``

    The function paginates through S3 to handle large stores efficiently.

    :param bucket: S3 bucket name
    :param key_prefix: S3 key prefix (all objects under this prefix are scanned)
    :param progress_callback: optional callable invoked after each accession is
        processed with ``(count, accession)`` where count is the running total
        of unique accessions found
    :return: dict mapping accession to ``AssemblyRecord``
    """
    s3 = get_s3_client()
    assemblies: dict[str, AssemblyRecord] = {}
    processed_count = 0

    try:
        paginator = s3.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=bucket, Prefix=key_prefix)

        for page in pages:
            for obj in page.get("Contents", []):
                acc = _extract_accession_from_s3_key(obj["Key"])
                assembly_dir = _extract_assembly_dir_from_s3_key(obj["Key"])

                if not acc or not assembly_dir:
                    continue

                # Convert LastModified to NCBI date format (YYYY/MM/DD)
                last_modified = obj["LastModified"]
                # Handle both aware and naive datetimes
                if last_modified.tzinfo is None:
                    last_modified = last_modified.replace(tzinfo=UTC)
                obj_date_str = last_modified.strftime("%Y/%m/%d")

                if acc not in assemblies:
                    # First object for this accession; store it
                    assemblies[acc] = AssemblyRecord(
                        accession=acc,
                        status="latest",
                        seq_rel_date=obj_date_str,
                        ftp_path="",  # synthesized; empty as it's not from FTP
                        assembly_dir=assembly_dir,
                    )
                    processed_count += 1
                    if progress_callback is not None:
                        progress_callback(processed_count, acc)
                else:
                    # Update to earliest timestamp (conservative)
                    existing_record = assemblies[acc]
                    existing_date = datetime.strptime(existing_record.seq_rel_date, "%Y/%m/%d").replace(
                        tzinfo=UTC
                    )
                    if last_modified < existing_date:
                        existing_record.seq_rel_date = obj_date_str

    except Exception as e:  # noqa: BLE001
        logger.error("Error scanning store: %s", e)
        raise

    logger.info("Scanned S3 store: found %d unique assemblies", len(assemblies))
    return assemblies


# ── Checksum verification against S3 store ───────────────────────────────


def verify_transfer_candidates(  # noqa: PLR0912, PLR0915
    accessions: list[str],
    current_assemblies: dict[str, AssemblyRecord],
    bucket: str,
    key_prefix: str,
    ftp_host: str = FTP_HOST,
    progress_callback: Callable[[int, int, str], None] | None = None,
) -> list[str]:
    """Verify which transfer candidates actually need downloading.

    For each accession, downloads ``md5checksums.txt`` from NCBI FTP and
    compares the checksums of filtered files against the ``md5`` user metadata
    on corresponding S3 objects.  Only accessions where at least one file
    differs or is missing from S3 are returned.

    This acts as a final gate before Phase 2: even if the summary diff flags an
    assembly, we skip it if every file in the store already matches.

    :param accessions: list of candidate accessions (new + updated from diff)
    :param current_assemblies: parsed current assembly summary
    :param bucket: S3 bucket name
    :param key_prefix: S3 key prefix for the Lakehouse dataset root
    :param ftp_host: NCBI FTP hostname
    :param progress_callback: optional callable invoked after each accession is
        processed with ``(done, total, accession)`` so callers can display a
        progress bar.  ``done`` is the 1-based count of completed accessions.
    :return: filtered list of accessions that actually need downloading
    """
    if not accessions:
        return []

    s3 = get_s3_client()
    ftp: Any = None  # lazily connected only when needed
    confirmed: list[str] = []
    pruned = 0
    skipped_missing = 0
    last_activity = time.monotonic()

    try:
        for done, acc in enumerate(accessions, start=1):
            rec = current_assemblies.get(acc)
            if not rec:
                confirmed.append(acc)
                if progress_callback is not None:
                    progress_callback(done, len(accessions), acc)
                continue

            # Build S3 prefix for this assembly
            s3_rel = build_accession_path(rec.assembly_dir)
            s3_prefix = f"{key_prefix}{s3_rel}"

            # Quick check: does *anything* exist under this prefix?
            resp = s3.list_objects_v2(Bucket=bucket, Prefix=s3_prefix, MaxKeys=1)
            if resp.get("KeyCount", 0) == 0:
                # Nothing in the store — definitely needs downloading
                confirmed.append(acc)
                skipped_missing += 1
                if progress_callback is not None:
                    progress_callback(done, len(accessions), acc)
                continue

            # Objects exist — need FTP md5 checksums to decide
            if ftp is None:
                ftp = connect_ftp(ftp_host)

            last_activity = ftp_noop_keepalive(ftp, last_activity)

            ftp_dir = _ftp_dir_from_url(rec.ftp_path, ftp_host)
            try:
                md5_text = ftp_retrieve_text(ftp, ftp_dir.rstrip("/") + "/md5checksums.txt")
                last_activity = time.monotonic()
                ftp_checksums = parse_md5_checksums_file(md5_text)
            except Exception:  # noqa: BLE001
                logger.warning("Cannot fetch md5checksums.txt for %s, keeping in transfer list", acc)
                confirmed.append(acc)
                if progress_callback is not None:
                    progress_callback(done, len(accessions), acc)
                continue

            # Filter to files we'd actually download
            target_checksums = {
                fname: md5
                for fname, md5 in ftp_checksums.items()
                if any(fname.endswith(suffix) for suffix in FILE_FILTERS)
            }

            if not target_checksums:
                confirmed.append(acc)
                if progress_callback is not None:
                    progress_callback(done, len(accessions), acc)
                continue

            # Short-circuit: if any file differs or is missing, keep the assembly
            needs_update = False
            for fname, expected_md5 in target_checksums.items():
                s3_path = f"{bucket}/{s3_prefix}{fname}"
                obj_info = head_object(s3_path)

                if obj_info is None:
                    needs_update = True
                    break

                s3_md5 = obj_info["metadata"].get("md5", "")
                if s3_md5 != expected_md5:
                    logger.debug("MD5 mismatch for %s/%s: S3=%s FTP=%s", acc, fname, s3_md5, expected_md5)
                    needs_update = True
                    break

            if needs_update:
                confirmed.append(acc)
            else:
                pruned += 1
                logger.debug("Pruned %s — all files match S3 checksums", acc)

            if progress_callback is not None:
                progress_callback(done, len(accessions), acc)
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


# ── Manifest writing ────────────────────────────────────────────────────


def write_transfer_manifest(
    diff: DiffResult,
    current_assemblies: dict[str, AssemblyRecord],
    output_path: str | Path,
    ftp_host: str = FTP_HOST,
) -> list[str]:
    """Write the transfer manifest (new + updated assemblies).

    Each line is an FTP directory path suitable for Phase 2 download.

    :param diff: computed diff result
    :param current_assemblies: parsed current assembly summary
    :param output_path: path to write the manifest file
    :param ftp_host: FTP hostname for URL stripping
    :return: list of FTP paths written
    """
    to_transfer = diff.new + diff.updated
    paths: list[str] = []
    for acc in sorted(to_transfer):
        rec = current_assemblies.get(acc)
        if not rec:
            continue
        ftp_path = _ftp_dir_from_url(rec.ftp_path, ftp_host)
        paths.append(ftp_path.rstrip("/") + "/")

    with Path(output_path).open("w") as f:
        f.writelines(p + "\n" for p in paths)

    logger.info("Wrote %d entries to transfer manifest: %s", len(paths), output_path)
    return paths


def write_removed_manifest(diff: DiffResult, output_path: str | Path) -> list[str]:
    """Write the removed manifest (replaced + suppressed accessions).

    :param diff: computed diff result
    :param output_path: path to write the manifest file
    :return: list of accessions written
    """
    removed = sorted(diff.replaced + diff.suppressed)
    with Path(output_path).open("w") as f:
        f.writelines(acc + "\n" for acc in removed)
    logger.info("Wrote %d entries to removed manifest: %s", len(removed), output_path)
    return removed


def write_updated_manifest(diff: DiffResult, output_path: str | Path) -> list[str]:
    """Write the updated manifest (accessions whose content changed).

    This file is consumed by Phase 3 to archive existing S3 objects
    before they are overwritten by the new versions.

    :param diff: computed diff result
    :param output_path: path to write the manifest file
    :return: list of accessions written
    """
    updated = sorted(diff.updated)
    with Path(output_path).open("w") as f:
        f.writelines(acc + "\n" for acc in updated)
    logger.info("Wrote %d entries to updated manifest: %s", len(updated), output_path)
    return updated


def write_diff_summary(
    diff: DiffResult,
    output_path: str | Path,
    database: str,
    prefix_from: str | None = None,
    prefix_to: str | None = None,
) -> dict[str, Any]:
    """Write a JSON diff summary file.

    :param diff: computed diff result
    :param output_path: path to write the JSON file
    :param database: database name (``"refseq"`` or ``"genbank"``)
    :param prefix_from: lower bound of prefix filter (if any)
    :param prefix_to: upper bound of prefix filter (if any)
    :return: the summary dict that was written
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
    with Path(output_path).open("w") as f:
        json.dump(summary, f, indent=2)
    logger.info("Wrote diff summary to: %s", output_path)
    return summary
