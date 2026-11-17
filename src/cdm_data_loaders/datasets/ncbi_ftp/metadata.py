"""Frictionless data package descriptor creation, validation, upload, and archival for NCBI FTP."""

import json
from datetime import UTC, datetime
from logging import Logger, getLogger
from pathlib import PurePosixPath
from typing import Any, Final, TypedDict, Unpack

from frictionless import Package

from cdm_data_loaders.io.s3.object_utils import copy_object, object_exists, put_object_bytes

logger: Logger = getLogger(__name__)

NCBI_CONTRIBUTOR: Final[dict[str, str]] = {
    "contributor_type": "Organization",
    "name": "National Center for Biotechnology Information",
    "contributor_id": "ROR:02meqm098",
    "contributor_roles": "DataCurator",
}
NCBI_PUBLISHER: Final[dict[str, str]] = {
    "organization_name": "National Center for Biotechnology Information",
    "organization_id": "ROR:02meqm098",
}
SAVED_BY: Final[str] = "cdm-data-loaders-ncbi-ftp"
SCHEMA_VERSION: Final[str] = "1.0"


class DescriptorResource(TypedDict, total=False):
    """A single resource entry in the frictionless descriptor ``resources`` list."""

    name: str
    path: PurePosixPath
    format: str
    bytes: int | None
    hash: str | None


class ArchiveDescriptorOptions(TypedDict, total=False):
    """Optional keyword arguments for :func:`archive_descriptor`."""

    archive_reason: str
    dry_run: bool


def build_descriptor_key(assembly_dir: PurePosixPath, key_prefix: PurePosixPath) -> PurePosixPath:
    """Return the S3 key for the live descriptor of ``assembly_dir``.

    :param assembly_dir: assembly directory name
    :type assembly_dir: PurePosixPath
    :param key_prefix: Lakehouse S3 key prefix
    :type key_prefix: PurePosixPath
    :return: S3 key under ``{key_prefix}/metadata/``
    :rtype: PurePosixPath
    """
    return key_prefix / "metadata" / assembly_dir.with_name(f"{assembly_dir.name}_datapackage.json")


def build_archive_descriptor_key(
    assembly_dir: PurePosixPath,
    release_tag: str,
    key_prefix: PurePosixPath,
    archive_reason: str = "unknown",
) -> PurePosixPath:
    """Return the S3 key for the archived descriptor of ``assembly_dir``.

    :param assembly_dir: assembly directory name
    :type assembly_dir: PurePosixPath
    :param release_tag: NCBI release tag used in the archive path
    :type release_tag: str
    :param key_prefix: Lakehouse S3 key prefix
    :type key_prefix: PurePosixPath
    :param archive_reason: reason segment for the archive path
    :type archive_reason: str
    :return: S3 key under ``{key_prefix}/archive/{release_tag}/{archive_reason}/metadata/``
    :rtype: PurePosixPath
    """
    return (
        key_prefix
        / "archive"
        / release_tag
        / archive_reason
        / "metadata"
        / assembly_dir.with_name(f"{assembly_dir.name}_datapackage.json")
    )


def create_descriptor(
    assembly_dir: PurePosixPath,
    accession_full: str,
    resources: list[DescriptorResource],
    *,
    timestamp: int | None = None,
) -> dict[str, Any]:
    """Build a KBase credit metadata descriptor for an NCBI assembly.

    :param assembly_dir: assembly directory name
    :type assembly_dir: PurePosixPath
    :param accession_full: accession identifier (for example, ``"GCF_000001215.4"``)
    :type accession_full: str
    :param resources: list of :class:`DescriptorResource` entries
    :type resources: list[DescriptorResource]
    :param timestamp: optional Unix timestamp override (defaults to current UTC epoch seconds)
    :type timestamp: int | None
    :return: descriptor dictionary ready for JSON serialization
    :rtype: dict[str, Any]
    :raises ValueError: if any resource omits ``name`` or ``path``
    """
    ts = timestamp if timestamp is not None else int(datetime.now(UTC).timestamp())
    version = accession_full.rsplit(".", 1)[-1]

    normalised: list[dict[str, Any]] = []
    for res in resources:
        entry: dict[str, Any] = {
            "name": res["name"].lower() if "name" in res else None,
            "path": str(res["path"]) if "path" in res else None,
            "format": res.get("format", ""),
        }
        if entry["name"] is None:
            msg = f"Missing name for file descriptor in {accession_full}"
            raise ValueError(msg)
        if entry["path"] is None:
            msg = f"Missing path for file descriptor in {accession_full}"
            raise ValueError(msg)
        if res.get("bytes") is not None:
            entry["bytes"] = res.get("bytes")
        if res.get("hash") is not None:
            entry["hash"] = res.get("hash")
        normalised.append(entry)

    return {
        "identifier": f"NCBI:{accession_full}",
        "resource_type": "dataset",
        "version": version,
        "titles": [{"title": f"NCBI Genome Assembly {assembly_dir}"}],
        "descriptions": [
            {"description_text": f"Genome assembly files for {accession_full} downloaded from NCBI Datasets"}
        ],
        "url": f"https://www.ncbi.nlm.nih.gov/datasets/genome/{accession_full}/",
        "contributors": [NCBI_CONTRIBUTOR],
        "publisher": NCBI_PUBLISHER,
        "license": {},
        "meta": {
            "credit_metadata_schema_version": SCHEMA_VERSION,
            "credit_metadata_source": [
                {
                    "source_name": "NCBI Genomes FTP",
                    "source_url": "ftp.ncbi.nlm.nih.gov/genomes/all/",
                    "access_timestamp": ts,
                }
            ],
            "saved_by": SAVED_BY,
            "timestamp": ts,
        },
        "resources": normalised,
    }


def validate_descriptor(descriptor: dict[str, Any], accession_full: str) -> None:
    """Validate a descriptor dictionary with frictionless ``Package.metadata_validate``.

    :param descriptor: descriptor dictionary produced by :func:`create_descriptor`
    :type descriptor: dict[str, Any]
    :param accession_full: accession identifier used in error messages
    :type accession_full: str
    :raises ValueError: if frictionless reports metadata validation errors
    """
    errors = list(Package.metadata_validate(descriptor))
    if errors:
        error_details = "; ".join(str(e) for e in errors)
        msg = f"Frictionless validation failed for {accession_full}: {error_details}"
        raise ValueError(msg)
    logger.debug("Frictionless descriptor valid for %s", accession_full)


def upload_descriptor(
    descriptor: dict[str, Any],
    assembly_dir: PurePosixPath,
    bucket: PurePosixPath,
    key_prefix: PurePosixPath,
    *,
    dry_run: bool = False,
) -> PurePosixPath:
    """Serialize and upload a descriptor to the live ``metadata/`` S3 path.

    :param descriptor: descriptor dictionary from :func:`create_descriptor`
    :type descriptor: dict[str, Any]
    :param assembly_dir: assembly directory name
    :type assembly_dir: PurePosixPath
    :param bucket: S3 bucket name
    :type bucket: PurePosixPath
    :param key_prefix: Lakehouse S3 key prefix
    :type key_prefix: PurePosixPath
    :param dry_run: when ``True``, return the target key without uploading
    :type dry_run: bool
    :return: S3 key of the uploaded (or planned) descriptor
    :rtype: PurePosixPath
    """
    key = build_descriptor_key(assembly_dir, key_prefix)
    if dry_run:
        logger.debug("[dry-run] would upload descriptor: s3://%s/%s", bucket, key)
        return key

    body = json.dumps(descriptor, indent=2).encode("utf-8")
    put_object_bytes(body, str(bucket / key), content_type="application/json")
    logger.debug("Uploaded descriptor: s3://%s/%s", bucket, key)
    return key


def archive_descriptor(
    assembly_dir: PurePosixPath,
    bucket: PurePosixPath,
    key_prefix: PurePosixPath,
    release_tag: str,
    **options: Unpack[ArchiveDescriptorOptions],
) -> bool:
    """Copy a live assembly descriptor to the archive path in S3.

    :param assembly_dir: assembly directory name
    :type assembly_dir: PurePosixPath
    :param bucket: S3 bucket name
    :type bucket: PurePosixPath
    :param key_prefix: Lakehouse S3 key prefix
    :type key_prefix: PurePosixPath
    :param release_tag: NCBI release tag for the archive path
    :type release_tag: str
    :param options: optional ``archive_reason`` and ``dry_run`` settings
    :type options: Unpack[ArchiveDescriptorOptions]
    :return: ``True`` if archived (or dry-run), ``False`` if the live descriptor was absent
    :rtype: bool
    """
    archive_reason = options.get("archive_reason", "unknown")
    dry_run = options.get("dry_run", False)
    source_key = build_descriptor_key(assembly_dir, key_prefix)
    archive_key = build_archive_descriptor_key(assembly_dir, release_tag, key_prefix, archive_reason)

    if dry_run:
        logger.debug("[dry-run] would archive descriptor: s3://%s/%s -> %s", bucket, source_key, archive_key)
        return True

    if not object_exists(str(bucket / source_key)):
        logger.warning("Descriptor not found, skipping archive: s3://%s/%s", bucket, source_key)
        return False

    copy_object(str(bucket / source_key), str(bucket / archive_key))
    logger.debug("Archived descriptor: s3://%s/%s -> %s", bucket, source_key, archive_key)
    return True
