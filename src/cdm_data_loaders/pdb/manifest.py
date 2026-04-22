"""Phase 1: PDB holdings download, diff, and manifest generation.

Downloads the current holdings inventory from the wwPDB Beta archive,
compares it against a previous snapshot, and produces:

* ``transfer_manifest.txt`` — PDB IDs to download in Phase 2
* ``removed_manifest.txt`` — PDB IDs to archive (obsoleted entries)
* ``updated_manifest.txt`` — PDB IDs being replaced (for pre-archive in Phase 3)
* ``diff_summary.json``     — human-readable diff statistics

Holdings files used (gzip JSON, fetched via HTTPS):

``current_file_holdings.json.gz``
    Mapping of extended PDB ID → holdings dict.  Expected structure::

        {
            "pdb_00001abc": {
                "content_type": ["coordinates_pdbx", "validation_report", ...]
            },
            ...
        }

``released_structures_last_modified_dates.json.gz``
    Mapping of extended PDB ID → ISO-8601 modification date string::

        {"pdb_00001abc": "2024-01-15", ...}

``all_removed_entries.json.gz``
    Mapping of extended PDB ID → removal metadata (or a list of dicts)::

        {"pdb_00001abc": {"obsolete_date": "...", "superseded_by": "..."}, ...}

.. note::

    The exact JSON structure of the holdings files should be verified against
    the live Beta archive before production use.  The parsers below handle the
    most likely formats but emit warnings when the structure is unexpected.
"""

import gzip
import json
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.request import urlopen

from cdm_data_loaders.pdb.entry import (
    HOLDINGS_BASE_URL,
    PDBRecord,
    extract_pdb_id_from_s3_key,
    pdb_id_hash,
)
from cdm_data_loaders.utils.cdm_logger import get_cdm_logger
from cdm_data_loaders.utils.s3 import get_s3_client

logger = get_cdm_logger()

_HOLDINGS_FILES = {
    "current": "current_file_holdings.json.gz",
    "dates": "released_structures_last_modified_dates.json.gz",
    "removed": "all_removed_entries.json.gz",
}


# ── Data structures ─────────────────────────────────────────────────────


@dataclass
class PDBDiffResult:
    """Result of comparing current and previous PDB holdings."""

    new: list[str] = field(default_factory=list)
    updated: list[str] = field(default_factory=list)
    removed: list[str] = field(default_factory=list)


# ── Holdings download ────────────────────────────────────────────────────


def download_holdings(base_url: str = HOLDINGS_BASE_URL) -> dict[str, Any]:
    """Download and decompress the three wwPDB holdings JSON files.

    Returns a dict with keys ``"current"``, ``"dates"``, and ``"removed"``,
    each containing the parsed JSON object from the corresponding file.

    :param base_url: base URL for the holdings directory
    :return: dict of parsed holdings JSON objects
    """
    result: dict[str, Any] = {}
    for key, filename in _HOLDINGS_FILES.items():
        url = f"{base_url}/{filename}"
        logger.info("Downloading holdings file: %s", url)
        with urlopen(url) as response:  # noqa: S310
            compressed = response.read()
        data = gzip.decompress(compressed)
        result[key] = json.loads(data)
        logger.info("Downloaded %s (%d bytes compressed)", filename, len(compressed))
    return result


# ── Holdings parsing ─────────────────────────────────────────────────────


def parse_current_holdings(data: object) -> dict[str, PDBRecord]:
    """Parse the ``current_file_holdings.json.gz`` payload into PDB records.

    Handles both dict-of-dicts and list-of-dicts top-level structures.

    :param data: parsed JSON from ``current_file_holdings.json.gz``
    :return: dict mapping lower-cased PDB ID to :class:`PDBRecord`
    """
    records: dict[str, PDBRecord] = {}

    if isinstance(data, dict):
        items: list[tuple[str, Any]] = list(data.items())
    elif isinstance(data, list):
        # list of {"entry_id": ..., "content_type": [...]} dicts
        items = [(entry.get("entry_id", ""), entry) for entry in data if isinstance(entry, dict)]
    else:
        logger.warning("Unexpected top-level type in current holdings: %s", type(data).__name__)
        return records

    for raw_id, entry in items:
        if not raw_id:
            continue
        pdb_id = raw_id.lower()

        # Accept both "content_type" and "content_types" keys
        file_types = entry.get("content_type") or entry.get("content_types") or [] if isinstance(entry, dict) else []

        if not isinstance(file_types, list):
            file_types = list(file_types)

        records[pdb_id] = PDBRecord(
            pdb_id=pdb_id,
            last_modified="",  # filled in by merge_holdings_dates
            file_types=[str(ft) for ft in file_types],
        )

    logger.info("Parsed %d entries from current holdings", len(records))
    return records


def parse_last_modified_dates(data: object) -> dict[str, str]:
    """Parse the ``released_structures_last_modified_dates.json.gz`` payload.

    Handles both dict ``{pdb_id: date_str}`` and list
    ``[{"entry_id": ..., "last_modified": ...}]`` structures.

    :param data: parsed JSON from ``released_structures_last_modified_dates.json.gz``
    :return: dict mapping lower-cased PDB ID to ISO-8601 date string
    """
    dates: dict[str, str] = {}

    if isinstance(data, dict):
        for pdb_id, value in data.items():
            if isinstance(value, str):
                dates[pdb_id.lower()] = value
            elif isinstance(value, dict):
                dates[pdb_id.lower()] = value.get("last_modified", "")
    elif isinstance(data, list):
        for entry in data:
            if not isinstance(entry, dict):
                continue
            pdb_id = entry.get("entry_id", "")
            date_val = entry.get("last_modified", "")
            if pdb_id and date_val:
                dates[pdb_id.lower()] = str(date_val)
    else:
        logger.warning("Unexpected top-level type in dates file: %s", type(data).__name__)

    logger.info("Parsed last-modified dates for %d entries", len(dates))
    return dates


def parse_removed_entries(data: object) -> set[str]:
    """Parse the ``all_removed_entries.json.gz`` payload.

    Handles both dict ``{pdb_id: ...}`` and list ``[{"entry_id": ...}, ...]``
    structures.

    :param data: parsed JSON from ``all_removed_entries.json.gz``
    :return: set of lower-cased removed PDB IDs
    """
    removed: set[str] = set()

    if isinstance(data, dict):
        removed.update(k.lower() for k in data)
    elif isinstance(data, list):
        for entry in data:
            if isinstance(entry, dict):
                pdb_id = entry.get("entry_id", "")
                if pdb_id:
                    removed.add(pdb_id.lower())
            elif isinstance(entry, str):
                removed.add(entry.lower())
    else:
        logger.warning("Unexpected top-level type in removed entries file: %s", type(data).__name__)

    logger.info("Parsed %d removed/obsolete PDB entries", len(removed))
    return removed


def merge_holdings_dates(
    records: dict[str, PDBRecord],
    dates: dict[str, str],
) -> dict[str, PDBRecord]:
    """Merge last-modified dates into PDB records in-place.

    Entries that appear in *records* but are absent from *dates* retain an
    empty ``last_modified`` string and a warning is emitted.

    :param records: PDB records from :func:`parse_current_holdings`
    :param dates: date map from :func:`parse_last_modified_dates`
    :return: the same *records* dict with ``last_modified`` fields populated
    """
    missing_dates = 0
    for pdb_id, rec in records.items():
        date = dates.get(pdb_id, "")
        if not date:
            missing_dates += 1
        rec.last_modified = date

    if missing_dates:
        logger.warning("%d entries have no last-modified date", missing_dates)
    return records


def build_current_records(
    holdings_data: dict[str, Any],
) -> dict[str, PDBRecord]:
    """Build a full :class:`PDBRecord` dict from downloaded holdings data.

    Convenience wrapper that calls :func:`parse_current_holdings`,
    :func:`parse_last_modified_dates`, and :func:`merge_holdings_dates`.

    :param holdings_data: output of :func:`download_holdings`
    :return: dict mapping PDB ID to :class:`PDBRecord`
    """
    records = parse_current_holdings(holdings_data["current"])
    dates = parse_last_modified_dates(holdings_data["dates"])
    return merge_holdings_dates(records, dates)


# ── Prefix / hash-range filtering ────────────────────────────────────────


def filter_by_hash_range(
    records: dict[str, PDBRecord],
    hash_from: str | None = None,
    hash_to: str | None = None,
) -> dict[str, PDBRecord]:
    """Filter PDB records to those whose 2-character hash is in range.

    Both bounds are inclusive. Pass ``None`` to skip a bound.

    :param records: dict of PDB records
    :param hash_from: lower bound (inclusive), e.g. ``"00"``
    :param hash_to: upper bound (inclusive), e.g. ``"3f"``
    :return: filtered dict
    """
    if hash_from is None and hash_to is None:
        return records

    filtered: dict[str, PDBRecord] = {}
    for pdb_id, rec in records.items():
        try:
            h = pdb_id_hash(pdb_id)
        except ValueError:
            continue
        if hash_from is not None and h < hash_from.lower():
            continue
        if hash_to is not None and h > hash_to.lower():
            continue
        filtered[pdb_id] = rec

    return filtered


# ── Diff computation ────────────────────────────────────────────────────


def compute_diff(
    current: dict[str, PDBRecord],
    previous: dict[str, PDBRecord] | None = None,
    previous_ids: set[str] | None = None,
    removed_ids: set[str] | None = None,
) -> PDBDiffResult:
    """Compute the diff between current and previous PDB holdings.

    Comparison logic:

    * **new** — ID present in *current* but not in *previous*
    * **updated** — ID in both, but ``last_modified`` date increased
    * **removed** — ID in *removed_ids* (from ``all_removed_entries.json.gz``)
      **or** present in *previous* but absent from *current*

    :param current: current PDB records from holdings
    :param previous: previous full records (from a prior holdings snapshot),
        or ``None`` to fall back to *previous_ids*
    :param previous_ids: set of previously known PDB IDs (store-scan fallback)
    :param removed_ids: set of IDs that appear in ``all_removed_entries.json.gz``
    :return: :class:`PDBDiffResult`
    """
    diff = PDBDiffResult()
    known_ids: set[str]

    if previous is not None:
        known_ids = set(previous.keys())
    elif previous_ids is not None:
        known_ids = previous_ids
    else:
        known_ids = set()

    current_ids = set(current.keys())

    for pdb_id, rec in current.items():
        if pdb_id not in known_ids:
            diff.new.append(pdb_id)
        elif previous is not None:
            prev = previous.get(pdb_id)
            if prev and rec.last_modified and prev.last_modified and rec.last_modified > prev.last_modified:
                diff.updated.append(pdb_id)

    # Removed: explicitly in removed_ids, OR was known but vanished from current
    explicitly_removed: set[str] = removed_ids or set()
    gone_from_current = known_ids - current_ids

    diff.removed = sorted((explicitly_removed & known_ids) | gone_from_current)
    diff.new.sort()
    diff.updated.sort()
    return diff


# ── Synthetic summary from S3 store scan ────────────────────────────────


def scan_store_to_previous_ids(
    bucket: str,
    key_prefix: str,
    progress_callback: Callable[[int, str], None] | None = None,
) -> set[str]:
    """Scan S3 store and return the set of PDB IDs that are already present.

    Used when there is no previous holdings snapshot file.

    :param bucket: S3 bucket name
    :param key_prefix: S3 key prefix (all objects under this prefix are scanned)
    :param progress_callback: optional ``(count, pdb_id)`` callback for progress
    :return: set of lower-cased PDB IDs found in the store
    """
    s3 = get_s3_client()
    paginator = s3.get_paginator("list_objects_v2")
    ids: set[str] = set()

    for page in paginator.paginate(Bucket=bucket, Prefix=key_prefix):
        for obj in page.get("Contents", []):
            pdb_id = extract_pdb_id_from_s3_key(obj["Key"])
            if pdb_id and pdb_id not in ids:
                ids.add(pdb_id)
                if progress_callback is not None:
                    progress_callback(len(ids), pdb_id)

    logger.info("Scanned S3 store: found %d unique PDB entries", len(ids))
    return ids


# ── Manifest writing ────────────────────────────────────────────────────


def write_transfer_manifest(
    diff: PDBDiffResult,
    output_path: str | Path,
) -> list[str]:
    """Write the transfer manifest (new + updated PDB IDs).

    Each line is a single extended PDB ID (e.g. ``pdb_00001abc``), consumed
    by the Phase 2 rsync pipeline.

    :param diff: computed diff result
    :param output_path: path to write the manifest file
    :return: list of PDB IDs written
    """
    to_transfer = sorted(set(diff.new + diff.updated))
    with Path(output_path).open("w") as f:
        f.writelines(pdb_id + "\n" for pdb_id in to_transfer)
    logger.info("Wrote %d entries to transfer manifest: %s", len(to_transfer), output_path)
    return to_transfer


def write_removed_manifest(
    diff: PDBDiffResult,
    output_path: str | Path,
) -> list[str]:
    """Write the removed manifest (obsoleted PDB IDs).

    :param diff: computed diff result
    :param output_path: path to write the manifest file
    :return: list of PDB IDs written
    """
    removed = sorted(diff.removed)
    with Path(output_path).open("w") as f:
        f.writelines(pdb_id + "\n" for pdb_id in removed)
    logger.info("Wrote %d entries to removed manifest: %s", len(removed), output_path)
    return removed


def write_updated_manifest(
    diff: PDBDiffResult,
    output_path: str | Path,
) -> list[str]:
    """Write the updated manifest (PDB IDs whose content changed).

    Consumed by Phase 3 to archive existing S3 objects before overwriting.

    :param diff: computed diff result
    :param output_path: path to write the manifest file
    :return: list of PDB IDs written
    """
    updated = sorted(diff.updated)
    with Path(output_path).open("w") as f:
        f.writelines(pdb_id + "\n" for pdb_id in updated)
    logger.info("Wrote %d entries to updated manifest: %s", len(updated), output_path)
    return updated


def write_diff_summary(
    diff: PDBDiffResult,
    output_path: str | Path,
    hash_from: str | None = None,
    hash_to: str | None = None,
) -> dict[str, Any]:
    """Write a JSON diff summary file.

    :param diff: computed diff result
    :param output_path: path to write the JSON file
    :param hash_from: lower bound of hash filter applied (if any)
    :param hash_to: upper bound of hash filter applied (if any)
    :return: the summary dict that was written
    """
    summary: dict[str, Any] = {
        "generated_at": datetime.now(UTC).isoformat(),
        "hash_from": hash_from,
        "hash_to": hash_to,
        "new": len(diff.new),
        "updated": len(diff.updated),
        "removed": len(diff.removed),
        "total_to_transfer": len(diff.new) + len(diff.updated),
    }
    with Path(output_path).open("w") as f:
        json.dump(summary, f, indent=2)
    logger.info(
        "Diff summary: %d new, %d updated, %d removed",
        len(diff.new),
        len(diff.updated),
        len(diff.removed),
    )
    return summary


# ── Holdings snapshot I/O ────────────────────────────────────────────────


def save_holdings_snapshot(
    records: dict[str, PDBRecord],
    output_path: str | Path,
) -> None:
    """Save a PDB holdings snapshot to a gzip JSON file.

    :param records: dict mapping PDB ID to :class:`PDBRecord`
    :param output_path: path to write (should end in ``.json.gz``)
    """
    payload = {
        rec.pdb_id: {
            "last_modified": rec.last_modified,
            "file_types": rec.file_types,
        }
        for rec in records.values()
    }
    data = json.dumps(payload).encode()
    with gzip.open(output_path, "wb") as f:
        f.write(data)
    logger.info("Saved holdings snapshot (%d entries) to %s", len(records), output_path)


def load_holdings_snapshot(path: str | Path) -> dict[str, PDBRecord]:
    """Load a PDB holdings snapshot previously saved by :func:`save_holdings_snapshot`.

    :param path: path to the ``.json.gz`` snapshot file
    :return: dict mapping PDB ID to :class:`PDBRecord`
    """
    with gzip.open(path, "rb") as f:
        payload: dict[str, Any] = json.loads(f.read())

    records: dict[str, PDBRecord] = {}
    for pdb_id, data in payload.items():
        records[pdb_id] = PDBRecord(
            pdb_id=pdb_id,
            last_modified=data.get("last_modified", ""),
            file_types=data.get("file_types", []),
        )
    logger.info("Loaded holdings snapshot: %d entries from %s", len(records), path)
    return records


def upload_holdings_snapshot(
    records: dict[str, PDBRecord],
    bucket: str,
    key: str,
) -> None:
    """Serialise and upload a holdings snapshot to S3.

    :param records: dict mapping PDB ID to :class:`PDBRecord`
    :param bucket: S3 bucket name
    :param key: S3 object key (should end in ``.json.gz``)
    """
    import tempfile  # noqa: PLC0415

    s3 = get_s3_client()
    with tempfile.NamedTemporaryFile(suffix=".json.gz", delete=False) as tmp:
        tmp_path = tmp.name
    try:
        save_holdings_snapshot(records, tmp_path)
        s3.upload_file(Filename=tmp_path, Bucket=bucket, Key=key)
        logger.info("Uploaded holdings snapshot to s3://%s/%s", bucket, key)
    finally:
        Path(tmp_path).unlink()


def download_holdings_snapshot(bucket: str, key: str) -> dict[str, PDBRecord]:
    """Download and deserialise a holdings snapshot from S3.

    :param bucket: S3 bucket name
    :param key: S3 object key
    :return: dict mapping PDB ID to :class:`PDBRecord`
    """
    import tempfile  # noqa: PLC0415

    s3 = get_s3_client()
    with tempfile.NamedTemporaryFile(suffix=".json.gz", delete=False) as tmp:
        tmp_path = tmp.name
    try:
        s3.download_file(Bucket=bucket, Key=key, Filename=tmp_path)
        return load_holdings_snapshot(tmp_path)
    finally:
        Path(tmp_path).unlink()
