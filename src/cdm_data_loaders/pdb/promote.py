"""Phase 3: Promote staged PDB files to final Lakehouse paths in S3.

Walks staged files in an S3 staging prefix (written by CTS after Phase 2),
uploads each to the final Lakehouse path, archives obsoleted/updated entries,
and trims the transfer manifest so that a re-run of Phase 2 only downloads
remaining entries.
"""

import re
import tempfile
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import Any

import botocore.exceptions

from cdm_data_loaders.pdb.entry import DEFAULT_LAKEHOUSE_KEY_PREFIX, build_entry_path
from cdm_data_loaders.utils.cdm_logger import get_cdm_logger
from cdm_data_loaders.utils.s3 import (
    copy_object_with_metadata,
    delete_object,
    get_s3_client,
    upload_file_with_metadata,
)

logger = get_cdm_logger()

# Pattern for matching PDB extended IDs in S3 object keys.
_PDB_ID_RE = re.compile(r"pdb_[0-9a-f]{8}", re.IGNORECASE)


# ── Promote from S3 staging prefix ──────────────────────────────────────


def promote_from_s3(  # noqa: PLR0913
    staging_key_prefix: str,
    bucket: str,
    removed_manifest_path: str | Path | None = None,
    updated_manifest_path: str | Path | None = None,
    pdb_release: str | None = None,
    manifest_s3_key: str | None = None,
    lakehouse_key_prefix: str = DEFAULT_LAKEHOUSE_KEY_PREFIX,
    *,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Promote PDB files from an S3 staging prefix to the final Lakehouse path.

    Downloads each staged file to a temp location and re-uploads to the final
    path.  The upload uses CRC64NVME as the native S3 checksum (handled
    transparently by :func:`upload_file_with_metadata`).

    :param staging_key_prefix: S3 key prefix where CTS output was written
    :param bucket: S3 bucket name
    :param removed_manifest_path: local path to the removed_manifest file
    :param updated_manifest_path: local path to the updated_manifest file
    :param pdb_release: PDB release date tag (YYYY-MM-DD) for archive metadata
    :param manifest_s3_key: S3 object key for transfer_manifest.txt (for trimming)
    :param lakehouse_key_prefix: S3 key prefix for the final Lakehouse locations
    :param dry_run: if True, log actions without making changes
    :return: report dict with counts
    """
    s3 = get_s3_client()
    paginator = s3.get_paginator("list_objects_v2")
    normalized_staging_key_prefix = staging_key_prefix.rstrip("/") + "/"

    promoted = 0
    failed = 0

    # Collect all objects under the staging prefix
    staged_objects: list[str] = []
    for page in paginator.paginate(Bucket=bucket, Prefix=normalized_staging_key_prefix):
        staged_objects.extend(obj["Key"] for obj in page.get("Contents", []))

    # Separate data files from sidecars
    sidecars = {k for k in staged_objects if k.endswith((".crc64nvme", ".md5"))}
    data_files = [k for k in staged_objects if k not in sidecars]

    logger.info("Found %d data files and %d sidecars in staging", len(data_files), len(sidecars))

    # Archive affected entries BEFORE promoting or deleting
    archived = 0
    for manifest_file, reason, delete in [
        (updated_manifest_path, "updated", False),
        (removed_manifest_path, "obsoleted", True),
    ]:
        if manifest_file and Path(str(manifest_file)).is_file():
            archived += _archive_entries(
                str(manifest_file),
                bucket=bucket,
                pdb_release=pdb_release,
                lakehouse_key_prefix=lakehouse_key_prefix,
                archive_reason=reason,
                delete_source=delete,
                dry_run=dry_run,
            )

    promoted_ids: set[str] = set()

    for staged_key in data_files:
        if staged_key.endswith("download_report.json"):
            continue

        rel_path = staged_key[len(normalized_staging_key_prefix):]
        if not rel_path.startswith("raw_data/"):
            continue
        final_key = lakehouse_key_prefix + rel_path

        if dry_run:
            logger.info("[dry-run] would promote: %s -> %s", staged_key, final_key)
            promoted += 1
            continue

        try:
            with tempfile.NamedTemporaryFile(delete=False) as tmp:
                tmp_path = tmp.name
            try:
                s3.download_file(Bucket=bucket, Key=staged_key, Filename=tmp_path)

                final_key_path = PurePosixPath(final_key)
                upload_succeeded = upload_file_with_metadata(
                    tmp_path,
                    f"{bucket}/{final_key_path.parent}",
                    metadata={},
                    object_name=final_key_path.name,
                )
                if not upload_succeeded:
                    logger.error("Failed to upload promoted file %s to %s", staged_key, final_key)
                    failed += 1
                    continue

                promoted += 1

                # Track promoted entry for manifest trimming
                m = _PDB_ID_RE.search(staged_key)
                if m:
                    promoted_ids.add(m.group(0).lower())

            finally:
                Path(tmp_path).unlink()
        except Exception:
            logger.exception("Failed to promote %s", staged_key)
            failed += 1

    # Trim manifest for resumability
    if manifest_s3_key and promoted_ids and not dry_run:
        _trim_manifest(manifest_s3_key, bucket, promoted_ids)

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


# ── Archive entries ─────────────────────────────────────────────────────


def _archive_entries(  # noqa: PLR0913
    manifest_local_path: str,
    bucket: str,
    pdb_release: str | None = None,
    lakehouse_key_prefix: str = DEFAULT_LAKEHOUSE_KEY_PREFIX,
    archive_reason: str = "unknown",
    *,
    delete_source: bool = False,
    dry_run: bool = False,
) -> int:
    """Archive PDB entry objects to ``archive/{release_tag}/``.

    Copies S3 objects matching each PDB ID to the archive prefix.  When
    *delete_source* is True (obsoleted entries), the originals are deleted
    after copying.  When False (updated entries), the originals remain to be
    overwritten by the promote step.

    :param manifest_local_path: local path to a manifest file (one PDB ID per line)
    :param bucket: S3 bucket name
    :param pdb_release: release date tag used in the archive path
    :param lakehouse_key_prefix: S3 key prefix for the Lakehouse dataset root
    :param archive_reason: metadata value describing why the object was archived
    :param delete_source: if True, delete the source object after copying
    :param dry_run: if True, log without making changes
    :return: number of objects archived
    """
    s3 = get_s3_client()
    release_tag = pdb_release or "unknown"
    datestamp = datetime.now(UTC).strftime("%Y-%m-%d")
    archived = 0

    with Path(manifest_local_path).open() as f:
        pdb_ids = [line.strip().lower() for line in f if line.strip()]

    for pdb_id in pdb_ids:
        try:
            entry_rel = build_entry_path(pdb_id)
        except ValueError:
            logger.warning("Cannot build entry path for archival: %s", pdb_id)
            continue

        source_prefix = f"{lakehouse_key_prefix}{entry_rel}"
        paginator = s3.get_paginator("list_objects_v2")
        matching_keys: list[str] = []
        for page in paginator.paginate(Bucket=bucket, Prefix=source_prefix):
            matching_keys.extend(obj["Key"] for obj in page.get("Contents", []))

        if not matching_keys:
            logger.debug("No objects found for %s, skipping archive", pdb_id)
            continue

        for source_key in matching_keys:
            rel = source_key[len(lakehouse_key_prefix):]
            archive_key = f"{lakehouse_key_prefix}archive/{release_tag}/{rel}"

            if dry_run:
                logger.info("[dry-run] would archive: %s -> %s", source_key, archive_key)
                archived += 1
                continue

            try:
                copy_object_with_metadata(
                    f"{bucket}/{source_key}",
                    f"{bucket}/{archive_key}",
                    metadata={
                        "pdb_last_release": release_tag,
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

    logger.info("Archived %d objects for %d entries (%s)", archived, len(pdb_ids), archive_reason)
    return archived


# ── Manifest trimming ───────────────────────────────────────────────────


def _trim_manifest(manifest_s3_key: str, bucket: str, promoted_ids: set[str]) -> None:
    """Remove promoted PDB IDs from the transfer manifest in S3.

    :param manifest_s3_key: S3 object key of the transfer_manifest.txt
    :param bucket: S3 bucket name
    :param promoted_ids: set of PDB IDs that were successfully promoted
    """
    s3 = get_s3_client()

    with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".txt") as tmp:
        tmp_path = tmp.name

    try:
        try:
            s3.download_file(Bucket=bucket, Key=manifest_s3_key, Filename=tmp_path)
        except s3.exceptions.NoSuchKey:
            logger.warning("Manifest not found in S3 (s3://%s/%s) — skipping trim", bucket, manifest_s3_key)
            return
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "404":
                logger.warning("Manifest not found in S3 (s3://%s/%s) — skipping trim", bucket, manifest_s3_key)
                return
            raise

        with Path(tmp_path).open() as f:
            lines = f.readlines()

        remaining = [
            line for line in lines
            if line.strip() and line.strip().lower() not in promoted_ids
        ]

        with Path(tmp_path).open("w") as f:
            f.writelines(remaining)

        s3.upload_file(Filename=tmp_path, Bucket=bucket, Key=manifest_s3_key)
        logger.info(
            "Trimmed manifest: %d -> %d entries (%d promoted)",
            len(lines),
            len(remaining),
            len(lines) - len(remaining),
        )
    finally:
        Path(tmp_path).unlink()
