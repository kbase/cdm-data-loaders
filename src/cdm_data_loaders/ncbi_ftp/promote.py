"""Phase 3: Promote staged files to final Lakehouse paths in S3.

Walks staged files in an S3 staging prefix (written by CTS after Phase 2),
uploads each to the final Lakehouse path with MD5 metadata from sidecar files,
archives replaced/suppressed and updated assemblies, and trims the transfer
manifest so that a re-run of Phase 2 only downloads remaining entries.
"""

import re
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from cdm_data_loaders.utils.cdm_logger import get_cdm_logger
from cdm_data_loaders.utils.s3 import (
    copy_object_with_metadata,
    delete_object,
    get_s3_client,
    upload_file_with_metadata,
)

logger = get_cdm_logger()

DEFAULT_PATH_PREFIX = "tenant-general-warehouse/kbase/datasets/ncbi/"


# ── Promote from S3 staging prefix ──────────────────────────────────────


def promote_from_s3(  # noqa: PLR0913
    staging_prefix: str,
    bucket: str,
    removed_manifest: str | Path | None = None,
    updated_manifest: str | Path | None = None,
    ncbi_release: str | None = None,
    manifest_path: str | None = None,
    path_prefix: str = DEFAULT_PATH_PREFIX,
    *,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Promote files from an S3 staging prefix to the final Lakehouse path.

    Downloads each file to a temp location and re-uploads to the final path
    with MD5 metadata from ``.md5`` sidecar files.

    :param staging_prefix: S3 key prefix where CTS output was written
    :param bucket: S3 bucket name
    :param removed_manifest: local path to the removed_manifest file
    :param updated_manifest: local path to the updated_manifest file
    :param ncbi_release: NCBI release version tag for archiving
    :param manifest_path: S3 path to transfer_manifest.txt for trimming
    :param path_prefix: Lakehouse path prefix for final locations
    :param dry_run: if True, log actions without side effects
    :return: report dict with counts
    """
    s3 = get_s3_client()
    paginator = s3.get_paginator("list_objects_v2")

    promoted = 0
    failed = 0

    # Collect all objects under the staging prefix
    staged_objects: list[str] = []
    for page in paginator.paginate(Bucket=bucket, Prefix=staging_prefix):
        staged_objects.extend(obj["Key"] for obj in page.get("Contents", []))

    # Separate data files from sidecars
    sidecars = {k for k in staged_objects if k.endswith((".crc64nvme", ".md5"))}
    data_files = [k for k in staged_objects if k not in sidecars]

    logger.info("Found %d data files and %d sidecars in staging", len(data_files), len(sidecars))

    # Archive all affected assemblies BEFORE promoting or deleting
    archived = 0
    for manifest_file, reason, delete in [
        (updated_manifest, "updated", False),
        (removed_manifest, "replaced_or_suppressed", True),
    ]:
        if manifest_file and Path(str(manifest_file)).is_file():
            archived += _archive_assemblies(
                str(manifest_file),
                bucket=bucket,
                ncbi_release=ncbi_release,
                path_prefix=path_prefix,
                archive_reason=reason,
                delete_source=delete,
                dry_run=dry_run,
            )

    promoted_accessions: set[str] = set()

    for staged_key in data_files:
        if staged_key.endswith("download_report.json"):
            continue

        rel_path = staged_key[len(staging_prefix) :]
        if not rel_path.startswith("raw_data/"):
            continue
        final_key = path_prefix + rel_path

        if dry_run:
            logger.info("[dry-run] would promote: %s -> %s", staged_key, final_key)
            promoted += 1
            continue

        try:
            with tempfile.NamedTemporaryFile(delete=False) as tmp:
                tmp_path = tmp.name
            try:
                s3.download_file(Bucket=bucket, Key=staged_key, Filename=tmp_path)

                # Read MD5 from sidecar
                metadata: dict[str, str] = {}
                md5_key = staged_key + ".md5"
                if md5_key in sidecars:
                    md5_obj = s3.get_object(Bucket=bucket, Key=md5_key)
                    metadata["md5"] = md5_obj["Body"].read().decode().strip()

                upload_file_with_metadata(
                    tmp_path,
                    f"{bucket}/{Path(final_key).parent}",
                    metadata=metadata,
                    object_name=Path(final_key).name,
                )
                promoted += 1

                # Track promoted accession for manifest trimming
                acc_match = re.search(r"(GC[AF]_\d{9}\.\d+)", staged_key)
                if acc_match:
                    promoted_accessions.add(acc_match.group(1))

            finally:
                Path(tmp_path).unlink()
        except Exception:
            logger.exception("Failed to promote %s", staged_key)
            failed += 1

    # Trim manifest for resumability
    if manifest_path and promoted_accessions and not dry_run:
        _trim_manifest(manifest_path, bucket, promoted_accessions)

    report: dict[str, Any] = {
        "timestamp": datetime.now(UTC).isoformat(),
        "promoted": promoted,
        "archived": archived,
        "failed": failed,
        "dry_run": dry_run,
    }

    logger.info(
        "PROMOTE SUMMARY: %d promoted, %d archived, %d failed%s",
        promoted,
        archived,
        failed,
        " (dry-run)" if dry_run else "",
    )
    return report


# ── Archive assemblies ──────────────────────────────────────────────────


def _archive_assemblies(  # noqa: PLR0913
    manifest_path: str,
    bucket: str,
    ncbi_release: str | None = None,
    path_prefix: str = DEFAULT_PATH_PREFIX,
    archive_reason: str = "unknown",
    *,
    delete_source: bool = False,
    dry_run: bool = False,
) -> int:
    """Archive assembly objects to ``archive/{release_tag}/``.

    Copies S3 objects matching each accession to the archive prefix.
    When *delete_source* is True (replaced/suppressed), the original
    objects are deleted after copying.  When False (updated), the
    originals remain in place to be overwritten by the promote step.

    :param manifest_path: local path to a manifest file (one accession per line)
    :param bucket: S3 bucket name
    :param ncbi_release: release tag used in the archive path
    :param path_prefix: Lakehouse path prefix
    :param archive_reason: metadata value describing why the object was archived
    :param delete_source: if True, delete the source object after copying
    :param dry_run: if True, log without making changes
    :return: number of objects archived
    """
    s3 = get_s3_client()
    release_tag = ncbi_release or "unknown"
    datestamp = datetime.now(UTC).strftime("%Y-%m-%d")
    archived = 0

    with Path(manifest_path).open() as f:
        accessions = [line.strip() for line in f if line.strip()]

    for accession in accessions:
        m = re.match(r"(GC[AF])_(\d{3})(\d{3})(\d{3})\.\d+", accession)
        if not m:
            logger.warning("Cannot parse accession for archival: %s", accession)
            continue

        db = m.group(1)
        p1, p2, p3 = m.group(2), m.group(3), m.group(4)
        source_prefix = f"{path_prefix}raw_data/{db}/{p1}/{p2}/{p3}/"

        paginator = s3.get_paginator("list_objects_v2")
        matching_keys: list[str] = []
        for page in paginator.paginate(Bucket=bucket, Prefix=source_prefix):
            matching_keys.extend(obj["Key"] for obj in page.get("Contents", []) if accession in obj["Key"])

        if not matching_keys:
            logger.debug("No objects found for %s, skipping archive", accession)
            continue

        for source_key in matching_keys:
            rel = source_key[len(path_prefix) :]
            archive_key = f"{path_prefix}archive/{release_tag}/{rel}"

            if dry_run:
                logger.info("[dry-run] would archive: %s -> %s", source_key, archive_key)
                archived += 1
                continue

            try:
                copy_object_with_metadata(
                    f"{bucket}/{source_key}",
                    f"{bucket}/{archive_key}",
                    metadata={
                        "ncbi_last_release": release_tag,
                        "archive_reason": archive_reason,
                        "archive_date": datestamp,
                    },
                )
                if delete_source:
                    delete_object(f"{bucket}/{source_key}")
                archived += 1
                logger.debug("  Archived: %s -> %s", source_key, archive_key)
            except Exception:
                logger.exception("Failed to archive %s", source_key)

    logger.info("Archived %d objects for %d accessions (%s)", archived, len(accessions), archive_reason)
    return archived


# ── Manifest trimming ───────────────────────────────────────────────────


def _trim_manifest(manifest_s3_path: str, bucket: str, promoted_accessions: set[str]) -> None:
    """Remove promoted accessions from the transfer manifest in S3.

    :param manifest_s3_path: S3 key of the transfer_manifest.txt
    :param bucket: S3 bucket name
    :param promoted_accessions: set of accessions that were successfully promoted
    """
    s3 = get_s3_client()

    with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".txt") as tmp:
        tmp_path = tmp.name

    try:
        s3.download_file(Bucket=bucket, Key=manifest_s3_path, Filename=tmp_path)

        with Path(tmp_path).open() as f:
            lines = f.readlines()

        remaining = [line for line in lines if line.strip() and not any(acc in line for acc in promoted_accessions)]

        with Path(tmp_path).open("w") as f:
            f.writelines(remaining)

        s3.upload_file(Filename=tmp_path, Bucket=bucket, Key=manifest_s3_path)
        logger.info(
            "Trimmed manifest: %d -> %d entries (%d promoted)",
            len(lines),
            len(remaining),
            len(lines) - len(remaining),
        )
    finally:
        Path(tmp_path).unlink()
