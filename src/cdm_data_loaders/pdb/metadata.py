"""Frictionless data package descriptor creation for PDB entries.

Creates KBase credit metadata descriptors for each promoted PDB entry,
following the same schema used by the NCBI FTP workflow.

Each descriptor is a frictionless ``Package``-compatible JSON document
describing the entry's data files, stored at::

    {key_prefix}metadata/{pdb_id}_datapackage.json

and archived alongside raw data at::

    {key_prefix}archive/{release_tag}/metadata/{pdb_id}_datapackage.json

The descriptor ``resources`` list records the final Lakehouse S3 key, byte
size, file format, and CRC64NVME / MD5 hash of each promoted data file.
"""

from __future__ import annotations

import json
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, TypedDict

from frictionless import Package

from cdm_data_loaders.utils.cdm_logger import get_cdm_logger
from cdm_data_loaders.utils.s3 import copy_object_with_metadata, get_s3_client

logger = get_cdm_logger()

_RCSB_CONTRIBUTOR = {
    "contributor_type": "Organization",
    "name": "Research Collaboratory for Structural Bioinformatics",
    "contributor_id": "ROR:02e8wq794",
    "contributor_roles": "DataCurator",
}
_RCSB_PUBLISHER = {
    "organization_name": "Research Collaboratory for Structural Bioinformatics",
    "organization_id": "ROR:02e8wq794",
}
_SAVED_BY = "cdm-data-loaders-pdb"
_SCHEMA_VERSION = "1.0"


class DescriptorResource(TypedDict, total=False):
    """A single resource entry in the frictionless descriptor ``resources`` list."""

    name: str
    path: str
    format: str
    bytes: int | None
    hash: str | None


# ── Public helpers ────────────────────────────────────────────────────────


def build_descriptor_key(pdb_id: str, key_prefix: str) -> str:
    """Return the S3 key for the live descriptor of *pdb_id*.

    :param pdb_id: extended PDB ID, e.g. ``"pdb_00001abc"``
    :param key_prefix: Lakehouse key prefix (trailing slash optional)
    :return: S3 key, e.g. ``tenant-general-warehouse/.../pdb/metadata/pdb_00001abc_datapackage.json``
    """
    prefix = key_prefix.rstrip("/") + "/"
    return f"{prefix}metadata/{pdb_id}_datapackage.json"


def build_archive_descriptor_key(pdb_id: str, release_tag: str, key_prefix: str) -> str:
    """Return the S3 key for the archived descriptor of *pdb_id*.

    :param pdb_id: extended PDB ID, e.g. ``"pdb_00001abc"``
    :param release_tag: PDB release date tag used in the archive path, e.g. ``"2024-04-01"``
    :param key_prefix: Lakehouse key prefix
    :return: S3 key under ``archive/{release_tag}/metadata/``
    """
    prefix = key_prefix.rstrip("/") + "/"
    return f"{prefix}archive/{release_tag}/metadata/{pdb_id}_datapackage.json"


def create_descriptor(
    pdb_id: str,
    resources: list[DescriptorResource],
    *,
    last_modified: str | None = None,
    timestamp: int | None = None,
) -> dict[str, Any]:
    """Build a KBase credit metadata descriptor for a PDB entry.

    Resource names are lowercased.  Resources whose ``hash`` or ``bytes``
    value is ``None`` have those keys removed entirely.

    :param pdb_id: extended PDB ID, e.g. ``"pdb_00001abc"``
    :param resources: list of :class:`DescriptorResource` dicts
    :param last_modified: ISO-8601 date of last modification (used as version);
        defaults to today's date
    :param timestamp: Unix timestamp to embed; defaults to ``datetime.now(UTC)``
    :return: descriptor dict ready for serialisation and frictionless validation
    """
    ts = timestamp if timestamp is not None else int(datetime.now(UTC).timestamp())
    version = last_modified or datetime.now(UTC).strftime("%Y-%m-%d")

    # Classic 4-char PDB ID for the RCSB URL (strip "pdb_" prefix and leading zeros)
    classic_id = pdb_id[4:].lstrip("0") or pdb_id[4:]

    normalised: list[dict[str, Any]] = []
    for res in resources:
        entry: dict[str, Any] = {
            "name": res["name"].lower(),
            "path": res["path"],
            "format": res.get("format", ""),
        }
        if res.get("bytes") is not None:
            entry["bytes"] = res["bytes"]
        if res.get("hash") is not None:
            entry["hash"] = res["hash"]
        normalised.append(entry)

    return {
        "identifier": f"PDB:{pdb_id}",
        "resource_type": "dataset",
        "version": version,
        "titles": [{"title": f"PDB Entry {pdb_id}"}],
        "descriptions": [
            {
                "description_text": (
                    f"Macromolecular structure data for PDB entry {pdb_id} downloaded from the wwPDB Beta archive"
                )
            }
        ],
        "url": f"https://www.rcsb.org/structure/{classic_id.upper()}",
        "contributors": [_RCSB_CONTRIBUTOR],
        "publisher": _RCSB_PUBLISHER,
        "license": {},
        "meta": {
            "credit_metadata_schema_version": _SCHEMA_VERSION,
            "credit_metadata_source": [
                {
                    "source_name": "wwPDB Beta Archive",
                    "source_url": "rsync-beta.rcsb.org",
                    "access_timestamp": ts,
                }
            ],
            "saved_by": _SAVED_BY,
            "timestamp": ts,
        },
        "resources": normalised,
    }


def validate_descriptor(descriptor: dict[str, Any], pdb_id: str) -> None:
    """Validate a descriptor with frictionless.

    :param descriptor: descriptor dict from :func:`create_descriptor`
    :param pdb_id: PDB ID (used only in error messages)
    :raises ValueError: if frictionless reports any metadata errors
    """
    errors = list(Package.metadata_validate(descriptor))
    if errors:
        error_details = "; ".join(str(e) for e in errors)
        msg = f"Frictionless validation failed for {pdb_id}: {error_details}"
        raise ValueError(msg)
    logger.debug("Frictionless descriptor valid for %s", pdb_id)


def upload_descriptor(
    descriptor: dict[str, Any],
    pdb_id: str,
    bucket: str,
    key_prefix: str,
    *,
    dry_run: bool = False,
) -> str:
    """Serialise and upload a descriptor to the live ``metadata/`` path.

    :param descriptor: descriptor dict from :func:`create_descriptor`
    :param pdb_id: extended PDB ID, e.g. ``"pdb_00001abc"``
    :param bucket: S3 bucket name
    :param key_prefix: Lakehouse key prefix
    :param dry_run: if True, log without uploading
    :return: S3 key the descriptor was (or would be) written to
    """
    key = build_descriptor_key(pdb_id, key_prefix)

    if dry_run:
        logger.info("[dry-run] would upload descriptor: s3://%s/%s", bucket, key)
        return key

    s3 = get_s3_client()
    body = json.dumps(descriptor, indent=2).encode()

    with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as tmp:
        tmp_path = tmp.name
        tmp.write(body)

    try:
        s3.upload_file(Filename=tmp_path, Bucket=bucket, Key=key)
        logger.info("Uploaded descriptor: s3://%s/%s", bucket, key)
    finally:
        Path(tmp_path).unlink()

    return key


def archive_descriptor(  # noqa: PLR0913
    pdb_id: str,
    bucket: str,
    key_prefix: str,
    release_tag: str,
    *,
    archive_reason: str = "unknown",
    dry_run: bool = False,
) -> bool:
    """Copy the live descriptor to the archive path.

    If the live descriptor does not yet exist (e.g. archival runs before the
    first promote), logs a warning and returns ``False``.

    :param pdb_id: extended PDB ID, e.g. ``"pdb_00001abc"``
    :param bucket: S3 bucket name
    :param key_prefix: Lakehouse key prefix
    :param release_tag: PDB release date tag for the archive path
    :param archive_reason: metadata value describing why archived
    :param dry_run: if True, log without copying
    :return: ``True`` if the descriptor was (or would be) archived; ``False`` if not found
    """
    source_key = build_descriptor_key(pdb_id, key_prefix)
    archive_key = build_archive_descriptor_key(pdb_id, release_tag, key_prefix)

    if dry_run:
        logger.info("[dry-run] would archive descriptor: s3://%s/%s -> %s", bucket, source_key, archive_key)
        return True

    s3 = get_s3_client()
    try:
        s3.head_object(Bucket=bucket, Key=source_key)
    except s3.exceptions.NoSuchKey:
        logger.warning("Descriptor not found, skipping archive: s3://%s/%s", bucket, source_key)
        return False
    except Exception as e:
        if hasattr(e, "response") and e.response.get("Error", {}).get("Code") in ("404", "NoSuchKey"):  # type: ignore[union-attr]
            logger.warning("Descriptor not found, skipping archive: s3://%s/%s", bucket, source_key)
            return False
        raise

    datestamp = datetime.now(UTC).strftime("%Y-%m-%d")
    copy_object_with_metadata(
        f"{bucket}/{source_key}",
        f"{bucket}/{archive_key}",
        metadata={
            "pdb_last_release": release_tag,
            "archive_reason": archive_reason,
            "archive_date": datestamp,
        },
    )
    logger.debug("Archived descriptor: s3://%s/%s -> %s", bucket, source_key, archive_key)
    return True
