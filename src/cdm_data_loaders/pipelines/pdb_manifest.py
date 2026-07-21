"""Generate a transfer manifest based on current PDB holdings files.

Project documentation: https://www.rcsb.org/

Batch downloading via rsync from: "rsync-beta.rcsb.org:32382

Steps:
- Downloads the current PDB holdings files
- (Optional) Loads the set of holdings files from the last transfer OR bootsraps a set of holdings files from the current S3 store state
- Produces a set of transfer manifest files:
    * ``transfer_manifest.txt`` - PDB IDs to download (new and updated)
    * ``removed_manifest.txt``  - PDB IDs to archive
    * ``updated_manifest.txt``  - PDB IDs being replaced (download new; archive old)
    * ``missing_dates.txt``     - PDB IDs with missing last-modified dates in the current holdings files
    * ``summary.json``          - manifest generation statistics
"""

import gzip
import json
import re
import tempfile
from datetime import UTC, datetime
from logging import Logger, getLogger
from pathlib import Path, PurePosixPath
from typing import Any
from urllib.request import urlopen

from pydantic import AliasChoices, Field
from pydantic_settings import SettingsConfigDict

from cdm_data_loaders.core.settings import DEFAULT_SETTINGS_CONFIG_DICT, CtsSettings
from cdm_data_loaders.pdb.constants import (
    DEFAULT_DESTINATION_BUCKET,
    DEFAULT_DESTINATION_PREFIX,
    DEFAULT_HOLDINGS_SNAPSHOT,
    HOLDINGS_BASE_URL,
    HOLDINGS_FILES,
    PDB_ID_SEARCH_RE,
    HoldingsFile,
    HoldingsFileSchemas,
    HoldingsFileTypes,
    ManifestData,
    PDBRecord,
)
from cdm_data_loaders.utils.s3 import get_s3_client

logger: Logger = getLogger(__name__)


class PdbManfestSettings(CtsSettings):
    """Configuration for running the PDB manifest-generation pipeline."""

    model_config = SettingsConfigDict(**DEFAULT_SETTINGS_CONFIG_DICT, cli_prog_name="pdb_manifest")

    destination_bucket: PurePosixPath = Field(
        default=DEFAULT_DESTINATION_BUCKET,
        description="Bucket holding current PDB records",
        validation_alias=AliasChoices("destination-bucket", "destination_bucket"),
    )
    destination_prefix: PurePosixPath = Field(
        default=DEFAULT_DESTINATION_PREFIX,
        description="Path to folder in the destination bucket where PDB records exist. Must contain `raw_data/` folder",
        validation_alias=AliasChoices("destination-prefix", "destination_prefix"),
    )
    holdings_snapshot_path: PurePosixPath = Field(
        default=DEFAULT_HOLDINGS_SNAPSHOT,
        description="File path relative to destination bucket/prefix for current holdings snapshot file",
        validation_alias=AliasChoices("snapshot"),
    )
    bootstrap_date: datetime | None = Field(
        default=None,
        description="If a date is provided, the current PDB Lakehouse records will be used to generate a synthetic snapshot file with last-modified date set to this value for each record",
        validation_alias=AliasChoices("bootstrap"),
    )
    skip_diff: bool = Field(
        default=False,
        description="If set, prevents reading of snapshot data. All current PDB records will be include in manifest",
        validation_alias=AliasChoices("skip-diff", "skip_diff"),
    )
    output_path: Path = Field(
        description="Local path to save generated manfiest files to",
        validation_alias=AliasChoices("o", "output-path", "output_path"),
    )
    regex_filter: str | None = Field(
        default=None,
        description="RegEx string to filter records. Only PDB ids that pass the RegEx search will be included in manifest files.",
        validation_alias=AliasChoices("r", "regex"),
    )


def run_manifest_generation(config: PdbManfestSettings) -> None:
    """Main CTS entry point for PDB manifest generation.

    "param config: validated pipeline settings
    """
    snapshot: dict[str, PDBRecord] = {}
    if config.bootstrap_date:
        snapshot = _generate_snapshot_from_s3_state(
            config.destination_bucket,
            config.destination_prefix,
            config.bootstrap_date,
        )
    elif not config.skip_diff:
        snapshot = _download_holdings_snapshot(
            bucket=config.destination_bucket,
            key=config.destination_prefix / config.holdings_snapshot_path,
        )
    raw_data = _download_holdings_files()
    if config.regex_filter:
        regex = re.compile(config.regex_filter)

        def apply_regex_filter(data: dict[str, PDBRecord]) -> dict[str, PDBRecord]:
            return {key: val for key, val in data.items() if regex.search(key)}

        raw_data[HoldingsFileTypes.CURRENT] = apply_regex_filter(raw_data[HoldingsFileTypes.CURRENT])
        raw_data[HoldingsFileTypes.LAST_MODIFIED] = apply_regex_filter(raw_data[HoldingsFileTypes.LAST_MODIFIED])
        raw_data[HoldingsFileTypes.REMOVED] = apply_regex_filter(raw_data[HoldingsFileTypes.REMOVED])
        snapshot = apply_regex_filter(snapshot)
    current_with_dates = {
        pdb_id: raw_data[HoldingsFileTypes.LAST_MODIFIED].get(pdb_id, rec)
        for pdb_id, rec in raw_data[HoldingsFileTypes.CURRENT].items()
    }
    manifest_data = _generate_manifest_data(
        current=current_with_dates,
        removed=set(raw_data[HoldingsFileTypes.REMOVED].keys()),
        previous=snapshot,
        missing_dates=list(
            raw_data[HoldingsFileTypes.CURRENT].keys() - raw_data[HoldingsFileTypes.LAST_MODIFIED].keys()
        ),
    )
    _save_manifest_files(manifest_data, config.output_path)
    _save_summary_file(manifest_data, config.regex_filter, config.output_path)


def _download_holdings_files(
    base_url: PurePosixPath = HOLDINGS_BASE_URL,
) -> dict[HoldingsFileTypes, dict[str, PDBRecord]]:
    """Download the set of holdings files.

    Returns a dict keyed on filename with the JSON contents of each holdings file.
    :param base_url: base URL for the folder containing the holdings files
    :return: dict of holdings JSON objects
    """
    result: dict[HoldingsFileTypes, Any] = {}
    for key, holdings in HOLDINGS_FILES.items():
        url = f"https://{base_url / holdings.filename}"
        msg = f"Downloading PDB holdings file: {url}"
        logger.info(msg)
        with urlopen(url) as response:  # noqa: S310
            compressed = response.read()
        data = json.loads(gzip.decompress(compressed))
        if not isinstance(data, dict):
            msg = "Holdings file expected to contain a JSON dict"
            raise TypeError(msg)
        result[key] = _parse_pdb_record(holdings, data)
        msg = f"Downloaded {holdings.filename} ({len(compressed)} bytes compressed)"
        logger.info(msg)
    return result


def _parse_pdb_record(holdings: HoldingsFile, records: dict[str, Any]) -> dict[str, PDBRecord]:
    """Parse a key-value pair from a holdings file entry into a PDBRecord."""
    if holdings.schema == HoldingsFileSchemas.ID_DATE:
        # extract data as a dict of id/updated-date pairs
        return {key.lower(): PDBRecord(id=key.lower(), last_modified=val) for key, val in records.items()}
    if holdings.schema == HoldingsFileSchemas.ID_ONLY:
        # extract keys (ids) from data
        return {key.lower(): PDBRecord(id=key.lower()) for key in records}
    msg = f"Invalid holdings file schema: {holdings.schema}"
    raise ValueError(msg)


def _generate_manifest_data(
    current: dict[str, PDBRecord],
    removed: set[str],
    *,
    previous: dict[str, PDBRecord] | None = None,
    missing_dates: list[str] | None = None,
) -> ManifestData:
    """Generates a set of manifest data."""
    previous = previous or {}
    missing_dates = missing_dates or []
    return ManifestData(
        new=list(current.keys() - previous.keys()),
        updated=[
            key
            for key, val in current.items()
            if (prev_rec := previous.get(key)) is not None
            and val.last_modified
            and ((not prev_rec.last_modified) or val.last_modified > prev_rec.last_modified)
        ],
        removed=sorted((removed & previous.keys()) | (previous.keys() - current.keys())),
        missing_dates=missing_dates,
    )


def _extract_id_from_s3_key(key: str) -> str | None:
    m = PDB_ID_SEARCH_RE.search(key)
    return m.group(0).lower() if m else None


def _generate_snapshot_from_s3_state(
    bucket: PurePosixPath,
    key_prefix: PurePosixPath,
    date: datetime,
) -> dict[str, PDBRecord]:
    """Bootstraps a holdings snapshot file based on the current store state."""
    s3 = get_s3_client()
    paginator = s3.get_paginator("list_objects_v2")
    results: dict[str, PDBRecord] = {}

    for page in paginator.paginate(Bucket=str(bucket), Prefix=str(key_prefix)):
        for obj in page.get("Contents", []):
            pdb_id = _extract_id_from_s3_key(obj["Key"])
            if pdb_id and pdb_id not in results:
                results[pdb_id] = PDBRecord(id=pdb_id, last_modified=date.isoformat())

    msg = f"Scanned S3 store: found {len(results)} unique PDB records"
    logger.info(msg)
    return results


# File I/O


def _save_holdings_snapshot(
    records: dict[str, PDBRecord],
    output_path: Path,
) -> None:
    """Save a PDB holdings snapshot as a gzipped JSON file.

    :param records: map of PDB ID to PDBRecord
    :param output_path: path to write to (should end in ``.json.gz``)
    """
    payload = {
        rec.id: {
            "last_modified": rec.last_modified,
        }
        for rec in records.values()
    }
    data = json.dumps(payload).encode()
    with gzip.open(output_path, "wb") as f:
        f.write(data)
    msg = f"Saved holdings snapshot ({len(records)} entries) to {output_path}"
    logger.info(msg)


def _load_holdings_snapshot(
    path: Path,
) -> dict[str, PDBRecord]:
    """Loads holdings snapshot data from a local file."""
    with gzip.open(path, "rb") as f:
        payload: dict[str, Any] = json.loads(f.read())
    records: dict[str, PDBRecord] = {
        key: PDBRecord(
            id=key,
            last_modified=val.get("last_modified", ""),
        )
        for key, val in payload.items()
    }
    msg = f"Loaded PDB holdings snapshot: {len(records)} entries from {path}"
    logger.info(msg)
    return records


def _download_holdings_snapshot(
    bucket: PurePosixPath,
    key: PurePosixPath,
) -> dict[str, PDBRecord]:
    """Download and parse a holdings snapshot file from S3."""
    s3 = get_s3_client()
    with tempfile.NamedTemporaryFile(suffix=".json.gz", delete=False) as tmp:
        tmp_path = Path(tmp.name)
    try:
        s3.download_file(Bucket=str(bucket), Key=str(key), Filename=str(tmp_path))
        return _load_holdings_snapshot(tmp_path)
    finally:
        Path(tmp_path).unlink()


def _save_manifest_files(data: ManifestData, output_path: Path) -> None:
    """Saves manifest data to files."""

    def save_file(filename: str, ids: list[str]) -> None:
        file = output_path / filename
        with file.open("w") as f:
            f.writelines(pdb_id + "\n" for pdb_id in ids)
        msg = f"Wrote {len(ids)} entries to manifest: {filename}"
        logger.info(msg)

    save_file("transfer_manifest.txt", data.new + data.updated)
    save_file("updated_manifest.txt", data.updated)
    save_file("removed_manifest.txt", data.removed)
    save_file("missing_dates.txt", data.missing_dates)


def _save_summary_file(data: ManifestData, regex_filter: str | None, output_path: Path) -> None:
    """Saves a summary of the manifest generation to a JSON file."""
    summary: dict[str, Any] = {
        "generated_at": datetime.now(UTC).isoformat(),
        **({"regex_filter": regex_filter} if regex_filter is not None else {}),
        "saved_to": str(output_path),
        "new": len(data.new),
        "updated": len(data.updated),
        "removed": len(data.removed),
        "missing_dates": len(data.missing_dates),
    }
    file = output_path / "summary.json"
    with file.open("w") as f:
        f.write(json.dumps(summary, indent=2))
    msg = (
        f"Manifest summary: {len(data.new)} new; {len(data.updated)} updated; "
        f"{len(data.removed)} removed; {len(data.missing_dates)} missing dates"
    )
    logger.info(msg)
