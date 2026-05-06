"""Frictionless data package descriptor creation for the EBI SIFTS mapping snapshot.

Creates a single KBase credit metadata descriptor covering the full set of
TSV files downloaded in one pipeline run, following the same schema used by
the NCBI FTP and PDB pipelines.

The descriptor is stored at::

    {key_prefix}/metadata/sifts/metadata/sifts_datapackage.json

and archived at::

    {key_prefix}/metadata/archive/{date_tag}/sifts/metadata/sifts_datapackage.json

The descriptor ``resources`` list records the Lakehouse S3 path, byte size,
format, and MD5 hash for each TSV file produced by the run.
"""

from __future__ import annotations

import json
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, TypedDict

from frictionless import Package

from cdm_data_loaders.utils.cdm_logger import get_cdm_logger
from cdm_data_loaders.utils.s3 import copy_object, get_s3_client, head_object

logger = get_cdm_logger()

_EBI_CONTRIBUTOR = {
    "contributor_type": "Organization",
    "name": "European Bioinformatics Institute",
    "contributor_id": "ROR:02catss52",
    "contributor_roles": "DataCurator",
}
_EBI_PUBLISHER = {
    "organization_name": "European Bioinformatics Institute",
    "organization_id": "ROR:02catss52",
}
_SAVED_BY = "cdm-data-loaders-sifts"
_SCHEMA_VERSION = "1.0"

# S3 path relative to key_prefix where the live descriptor lives
_DESCRIPTOR_SUBPATH = "metadata/sifts/metadata/sifts_datapackage.json"


class DescriptorResource(TypedDict, total=False):
    """A single resource entry in the frictionless descriptor ``resources`` list."""

    name: str
    path: str
    format: str
    bytes: int | None
    hash: str | None


# ── Public helpers ────────────────────────────────────────────────────────


def build_descriptor_key(key_prefix: str) -> str:
    """Return the S3 key for the live SIFTS descriptor.

    :param key_prefix: Lakehouse key prefix (trailing slash optional)
    :return: S3 key ending with ``metadata/sifts/metadata/sifts_datapackage.json``
    """
    prefix = key_prefix.rstrip("/") + "/"
    return f"{prefix}{_DESCRIPTOR_SUBPATH}"


def build_archive_descriptor_key(key_prefix: str, date_tag: str) -> str:
    """Return the S3 key for the archived SIFTS descriptor.

    :param key_prefix: Lakehouse key prefix (trailing slash optional)
    :param date_tag: ISO date string used in the archive path, e.g. ``"2026-05-01"``
    :return: S3 key under ``metadata/archive/{date_tag}/sifts/metadata/``
    """
    prefix = key_prefix.rstrip("/") + "/"
    return f"{prefix}metadata/archive/{date_tag}/sifts/metadata/sifts_datapackage.json"


def create_descriptor(
    filenames: list[str],
    resources: list[DescriptorResource],
    *,
    timestamp: int | None = None,
) -> dict[str, Any]:
    """Build a KBase credit metadata descriptor for the SIFTS mapping snapshot.

    Produces one descriptor covering all TSV files from a single run.
    Resource names are lowercased; resources whose ``hash`` or ``bytes`` value
    is ``None`` have those keys removed entirely.

    :param filenames: ordered list of downloaded filenames included in this run
        (used to populate the description, e.g. ``["pdb_chain_uniprot.tsv.gz", ...]``)
    :param resources: list of :class:`DescriptorResource` dicts for each TSV file
    :param timestamp: Unix timestamp to embed; defaults to ``datetime.now(UTC)``
    :return: descriptor dict ready for serialisation and frictionless validation
    """
    ts = timestamp if timestamp is not None else int(datetime.now(UTC).timestamp())
    version = datetime.fromtimestamp(ts, UTC).strftime("%Y-%m-%d")
    file_list = ", ".join(filenames)

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
        "identifier": "EBI_SIFTS:mappings",
        "resource_type": "dataset",
        "version": version,
        "titles": [{"title": "EBI SIFTS PDB Cross-Reference Mappings"}],
        "descriptions": [
            {
                "description_text": (
                    f"Cross-reference mapping files from the EBI SIFTS project, "
                    f"providing mappings between PDB chains and UniProt, Pfam, GO, "
                    f"taxonomy, and other databases. "
                    f"Files: {file_list}."
                )
            }
        ],
        "url": "https://www.ebi.ac.uk/pdbe/docs/sifts/",
        "contributors": [_EBI_CONTRIBUTOR],
        "publisher": _EBI_PUBLISHER,
        "license": {},
        "meta": {
            "credit_metadata_schema_version": _SCHEMA_VERSION,
            "credit_metadata_source": [
                {
                    "source_name": "EBI SIFTS FTP",
                    "source_url": "ftp://ftp.ebi.ac.uk/pub/databases/msd/sifts/flatfiles/tsv/",
                    "access_timestamp": ts,
                }
            ],
            "saved_by": _SAVED_BY,
            "timestamp": ts,
        },
        "resources": normalised,
    }


def validate_descriptor(descriptor: dict[str, Any]) -> None:
    """Validate *descriptor* with frictionless.

    :param descriptor: descriptor dict from :func:`create_descriptor`
    :raises ValueError: if frictionless reports any metadata errors
    """
    errors = list(Package.metadata_validate(descriptor))
    if errors:
        error_details = "; ".join(str(e) for e in errors)
        msg = f"Frictionless validation failed for SIFTS descriptor: {error_details}"
        raise ValueError(msg)
    logger.debug("Frictionless descriptor valid for SIFTS")


def upload_descriptor(
    descriptor: dict[str, Any],
    bucket: str,
    key_prefix: str,
    *,
    dry_run: bool = False,
) -> str:
    """Serialise and upload the descriptor to the live ``metadata/`` path.

    :param descriptor: descriptor dict from :func:`create_descriptor`
    :param bucket: S3 bucket name
    :param key_prefix: Lakehouse key prefix
    :param dry_run: if True, log without uploading
    :return: S3 key the descriptor was (or would be) written to
    """
    key = build_descriptor_key(key_prefix)

    if dry_run:
        logger.debug("[dry-run] would upload SIFTS descriptor: s3://%s/%s", bucket, key)
        return key

    s3 = get_s3_client()
    body = json.dumps(descriptor, indent=2).encode()

    with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as tmp:
        tmp_path = tmp.name
        tmp.write(body)

    try:
        s3.upload_file(Filename=tmp_path, Bucket=bucket, Key=key)
        logger.debug("Uploaded SIFTS descriptor: s3://%s/%s", bucket, key)
    finally:
        Path(tmp_path).unlink(missing_ok=True)

    return key


def archive_descriptor(
    bucket: str,
    key_prefix: str,
    date_tag: str,
    *,
    dry_run: bool = False,
) -> bool:
    """Copy the live descriptor to the archive path.

    If the live descriptor does not yet exist (e.g. first run), logs a warning
    and returns ``False``.

    :param bucket: S3 bucket name
    :param key_prefix: Lakehouse key prefix
    :param date_tag: ISO date string for the archive path, e.g. ``"2026-05-01"``
    :param dry_run: if True, log without copying
    :return: ``True`` if the descriptor was (or would be) archived; ``False`` if not found
    """
    source_key = build_descriptor_key(key_prefix)
    archive_key = build_archive_descriptor_key(key_prefix, date_tag)
    source_path = f"{bucket}/{source_key}"
    archive_path = f"{bucket}/{archive_key}"

    if head_object(source_path) is None:
        logger.warning("No existing SIFTS descriptor to archive at s3://%s", source_path)
        return False

    if dry_run:
        logger.debug("[dry-run] would archive SIFTS descriptor: %s -> %s", source_path, archive_path)
        return True

    copy_object(source_path, archive_path)
    logger.debug("Archived SIFTS descriptor: %s -> %s", source_path, archive_path)
    return True
