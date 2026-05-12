"""Phase 3: Promote staged PDB files to final Lakehouse paths in S3.

Walks staged files in an S3 staging prefix (written by Phase 2 — either CTS
rsync or the standalone download notebook), uploads each to the final Lakehouse
path with checksum metadata from sidecar files, archives obsoleted/updated
entries, and trims the transfer manifest so that a re-run of Phase 2 only
downloads remaining entries.
"""

import re
import tempfile
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import Any

import botocore.exceptions
import httpx
import tqdm

from cdm_data_loaders.pdb.entry import DEFAULT_LAKEHOUSE_KEY_PREFIX, build_entry_path
from cdm_data_loaders.pdb.metadata import (
    DescriptorResource,
    archive_descriptor,
    build_archive_descriptor_key,
    build_descriptor_key,
    create_descriptor,
    upload_descriptor,
)
from cdm_data_loaders.pdb.rcsb_api import fetch_entry_core, fetch_entry_pubmed
from cdm_data_loaders.utils.cdm_logger import get_cdm_logger
from cdm_data_loaders.utils.s3 import (
    copy_object,
    delete_objects,
    get_s3_client,
    upload_file,
)

logger = get_cdm_logger()

# Pattern for matching PDB extended IDs in S3 object keys.
_PDB_ID_RE = re.compile(r"pdb_[0-9a-z]{8}", re.IGNORECASE)


# ── Promote from S3 staging prefix ──────────────────────────────────────


def promote_from_s3(  # noqa: PLR0913
    staging_key_prefix: str,
    staging_bucket: str,
    lakehouse_bucket: str,
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
    path with CRC64NVME checksum metadata from ``.crc64nvme`` sidecar files.
    Files are grouped by PDB entry; when all files for an entry succeed the
    frictionless descriptor is written immediately and staged files are deleted
    in a single batch API call.  This ensures a re-run picks up exactly where
    it left off.

    :param staging_key_prefix: S3 key prefix where Phase 2 output was written
    :param staging_bucket: S3 bucket containing the staged files (e.g. ``"cts"``)
    :param lakehouse_bucket: S3 bucket for the final Lakehouse destination (e.g. ``"cdm-lake"``)
    :param removed_manifest_path: local path to the removed_manifest file
    :param updated_manifest_path: local path to the updated_manifest file
    :param pdb_release: PDB release date tag (YYYY-MM-DD) for archive paths
    :param manifest_s3_key: S3 object key for transfer_manifest.txt (for trimming)
    :param lakehouse_key_prefix: S3 key prefix for the final Lakehouse locations
    :param dry_run: if True, log actions without making changes
    :return: report dict with counts
    """
    s3 = get_s3_client()
    paginator = s3.get_paginator("list_objects_v2")
    normalized_staging_key_prefix = staging_key_prefix.rstrip("/") + "/"

    # Collect all objects under the staging prefix
    staged_objects: list[str] = []
    for page in paginator.paginate(Bucket=staging_bucket, Prefix=normalized_staging_key_prefix):
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
                lakehouse_bucket=lakehouse_bucket,
                pdb_release=pdb_release,
                lakehouse_key_prefix=lakehouse_key_prefix,
                archive_reason=reason,
                delete_source=delete,
                dry_run=dry_run,
            )

    promoted, failed, descriptors_written, promoted_ids = _promote_data_files(
        data_files,
        sidecars,
        normalized_staging_key_prefix,
        lakehouse_key_prefix,
        staging_bucket,
        lakehouse_bucket,
        dry_run=dry_run,
    )

    # Trim manifest for resumability
    if manifest_s3_key and promoted_ids and not dry_run:
        _trim_manifest(manifest_s3_key, staging_bucket, promoted_ids)

    if descriptors_written:
        logger.info("Wrote %d frictionless descriptor(s)", descriptors_written)

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


# ── Promote data files (per-entry loop) ─────────────────────────────────


def _promote_data_files(  # noqa: PLR0913
    data_files: list[str],
    sidecars: set[str],
    normalized_staging_prefix: str,
    lakehouse_key_prefix: str,
    staging_bucket: str,
    lakehouse_bucket: str,
    *,
    dry_run: bool,
) -> tuple[int, int, int, set[str]]:
    """Promote each data file from staging to the final Lakehouse path.

    Files are grouped by PDB entry.  When all files for an entry are promoted
    successfully, the frictionless descriptor is written immediately and the
    staged files (including sidecars) are deleted in a single batch API call.
    This prevents staging accumulation and ensures partial runs leave descriptors
    for all completed entries.

    :return: (promoted_count, failed_count, descriptors_written, promoted_ids)
    """
    s3 = get_s3_client()
    promoted = 0
    failed = 0
    descriptors_written = 0
    promoted_ids: set[str] = set()

    # Group files by PDB entry; skip download_report.json and non-raw_data paths
    entry_files: defaultdict[str, list[str]] = defaultdict(list)
    for staged_key in data_files:
        if staged_key.endswith("download_report.json"):
            continue
        rel_path = staged_key[len(normalized_staging_prefix) :]
        if not rel_path.startswith("raw_data/"):
            continue
        m = _PDB_ID_RE.search(staged_key)
        if m:
            entry_files[m.group(0).lower()].append(staged_key)

    def _promote_one(staged_key: str) -> tuple[DescriptorResource, str]:
        """Download one staged file and re-upload to Lakehouse with checksum metadata.

        :return: ``(resource_dict, staged_key)`` on success; raises on failure.
        """
        rel_path = staged_key[len(normalized_staging_prefix) :]
        final_key = lakehouse_key_prefix + rel_path
        final_key_path = PurePosixPath(final_key)

        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            tmp_path = tmp.name
        try:
            s3.download_file(Bucket=staging_bucket, Key=staged_key, Filename=tmp_path)

            metadata: dict[str, str] = {}
            crc_key = staged_key + ".crc64nvme"
            if crc_key in sidecars:
                crc_obj = s3.get_object(Bucket=staging_bucket, Key=crc_key)
                metadata["crc64nvme"] = crc_obj["Body"].read().decode().strip()

            upload_succeeded = upload_file(
                tmp_path,
                f"{lakehouse_bucket}/{final_key_path.parent}",
                tags=metadata,
                object_name=final_key_path.name,
                show_progress=False,
            )
            if not upload_succeeded:
                msg = f"upload_file returned False for {staged_key}"
                raise RuntimeError(msg)

            fname = final_key_path.name
            ext = fname.rsplit(".", 1)[-1] if "." in fname else ""
            resource: DescriptorResource = {
                "name": fname.lower(),
                "path": final_key,
                "format": ext,
                "bytes": Path(tmp_path).stat().st_size,
                "hash": metadata.get("crc64nvme"),
            }
            return resource, staged_key
        finally:
            Path(tmp_path).unlink()

    total_files = sum(len(v) for v in entry_files.values())
    _dry_run_log_count = 0
    rcsb_client = httpx.Client(timeout=httpx.Timeout(30.0), follow_redirects=True)
    with tqdm.tqdm(total=total_files, unit="file", desc="Promoting") as pbar:
        for pdb_id, files in entry_files.items():
            entry_failed = 0
            resources: list[DescriptorResource] = []
            promoted_keys: list[str] = []

            if dry_run:
                for staged_key in files:
                    rel_path = staged_key[len(normalized_staging_prefix) :]
                    final_key = lakehouse_key_prefix + rel_path
                    if _dry_run_log_count < 10:  # noqa: PLR2004
                        logger.info("[dry-run] would promote: %s -> %s", staged_key, final_key)
                    else:
                        logger.debug("[dry-run] would promote: %s -> %s", staged_key, final_key)
                    _dry_run_log_count += 1
                    promoted += 1
                    pbar.update(1)
                continue

            # Download and re-upload all files for this entry concurrently
            n_workers = min(32, len(files))
            with ThreadPoolExecutor(max_workers=n_workers) as executor:
                futures = {executor.submit(_promote_one, key): key for key in files}
                for future in as_completed(futures):
                    staged_key = futures[future]
                    try:
                        resource, _ = future.result()
                        resources.append(resource)
                        promoted_keys.append(staged_key)
                        promoted += 1
                        promoted_ids.add(pdb_id)
                    except Exception:
                        logger.exception("Failed to promote %s", staged_key)
                        entry_failed += 1
                    pbar.update(1)

            failed += entry_failed

            # Write descriptor after a fully successful entry
            if entry_failed == 0 and promoted_keys:
                rcsb_entry = None
                rcsb_pubmed = None
                try:
                    rcsb_entry = fetch_entry_core(pdb_id, client=rcsb_client)
                    rcsb_pubmed = fetch_entry_pubmed(pdb_id, client=rcsb_client)
                except Exception:
                    logger.warning(
                        "Failed to fetch RCSB metadata for %s; descriptor will use fallback values",
                        pdb_id,
                    )
                try:
                    descriptor = create_descriptor(
                        pdb_id, resources, rcsb_entry=rcsb_entry, rcsb_pubmed=rcsb_pubmed
                    )
                    descriptor_key = upload_descriptor(
                        descriptor, pdb_id, lakehouse_bucket, lakehouse_key_prefix, dry_run=False
                    )
                    logger.debug("Uploaded descriptor: %s", descriptor_key)
                    descriptors_written += 1
                except Exception:
                    logger.exception("Failed to write descriptor for %s", pdb_id)
    rcsb_client.close()

    return promoted, failed, descriptors_written, promoted_ids


# ── Archive entries ─────────────────────────────────────────────────────


def _archive_entries(  # noqa: PLR0913
    manifest_local_path: str,
    lakehouse_bucket: str,
    pdb_release: str | None = None,
    lakehouse_key_prefix: str = DEFAULT_LAKEHOUSE_KEY_PREFIX,
    archive_reason: str = "unknown",
    *,
    delete_source: bool = False,
    dry_run: bool = False,
) -> int:
    """Archive PDB entry objects to ``archive/{release_tag}/{archive_reason}/``.

    Copies S3 objects matching each PDB ID to the archive prefix concurrently.
    When *delete_source* is True (obsoleted entries), the originals are deleted
    in a single batch API call after copying.  When False (updated entries), the
    originals remain to be overwritten by the promote step.

    :param manifest_local_path: local path to a manifest file (one PDB ID per line)
    :param lakehouse_bucket: S3 bucket for the Lakehouse (source and archive destination)
    :param pdb_release: release date tag used in the archive path
    :param lakehouse_key_prefix: S3 key prefix for the Lakehouse dataset root
    :param archive_reason: reason for archival, encoded as a path segment
    :param delete_source: if True, batch-delete source objects after copying
    :param dry_run: if True, log without making changes
    :return: number of objects archived
    """
    s3 = get_s3_client()
    release_tag = pdb_release or "unknown"
    archived = 0

    with Path(manifest_local_path).open() as f:
        pdb_ids = [line.strip().lower() for line in f if line.strip()]

    _dry_run_log_count = 0
    for pdb_id in tqdm.tqdm(pdb_ids, unit="entry", desc="Archiving"):
        try:
            entry_rel = build_entry_path(pdb_id)
        except ValueError:
            logger.warning("Cannot build entry path for archival: %s", pdb_id)
            continue

        source_prefix = f"{lakehouse_key_prefix}{entry_rel}"
        paginator = s3.get_paginator("list_objects_v2")
        matching_keys: list[str] = []
        for page in paginator.paginate(Bucket=lakehouse_bucket, Prefix=source_prefix):
            matching_keys.extend(obj["Key"] for obj in page.get("Contents", []))

        if not matching_keys:
            logger.debug("No objects found for %s, skipping archive", pdb_id)
            continue

        key_pairs = [
            (
                source_key,
                f"{lakehouse_key_prefix}archive/{release_tag}/{archive_reason}/{source_key[len(lakehouse_key_prefix) :]}",
            )
            for source_key in matching_keys
        ]

        if dry_run:
            for source_key, archive_key in key_pairs:
                if _dry_run_log_count < 10:  # noqa: PLR2004
                    logger.info("[dry-run] would archive: %s -> %s", source_key, archive_key)
                else:
                    logger.debug("[dry-run] would archive: %s -> %s", source_key, archive_key)
                _dry_run_log_count += 1
            archived += len(key_pairs)
            # Archive descriptor in dry-run too (inline to share _dry_run_log_count)
            desc_src = build_descriptor_key(pdb_id, lakehouse_key_prefix)
            desc_arch = build_archive_descriptor_key(pdb_id, release_tag, lakehouse_key_prefix, archive_reason)
            if _dry_run_log_count < 10:  # noqa: PLR2004
                logger.info(
                    "[dry-run] would archive descriptor: s3://%s/%s -> %s", lakehouse_bucket, desc_src, desc_arch
                )
            else:
                logger.debug(
                    "[dry-run] would archive descriptor: s3://%s/%s -> %s", lakehouse_bucket, desc_src, desc_arch
                )
            _dry_run_log_count += 1
            continue

        # Copy all files for this entry concurrently
        keys_to_delete: list[str] = []
        n_workers = min(32, len(key_pairs))
        with ThreadPoolExecutor(max_workers=n_workers) as executor:
            futures = {
                executor.submit(
                    copy_object,
                    f"{lakehouse_bucket}/{src}",
                    f"{lakehouse_bucket}/{arch}",
                ): src
                for src, arch in key_pairs
            }
            for future in as_completed(futures):
                src = futures[future]
                try:
                    future.result()
                    archived += 1
                    if delete_source:
                        keys_to_delete.append(src)
                    logger.debug("  Archived: %s", src)
                except Exception:
                    logger.exception("Failed to archive %s", src)

        # Batch-delete source keys in a single API call
        if keys_to_delete:
            del_errors = delete_objects(lakehouse_bucket, keys_to_delete)
            for err in del_errors:
                logger.warning("Failed to delete %s: %s", err.get("Key"), err.get("Message"))

        # Archive the frictionless descriptor alongside raw data
        try:
            archived_desc = archive_descriptor(
                pdb_id,
                lakehouse_bucket,
                lakehouse_key_prefix,
                release_tag,
                archive_reason=archive_reason,
                dry_run=False,
            )
            if not archived_desc:
                logger.debug("No descriptor found to archive for %s", pdb_id)
        except Exception:
            logger.exception("Failed to archive descriptor for %s", pdb_id)

    logger.info("Archived %d objects for %d entries (%s)", archived, len(pdb_ids), archive_reason)
    return archived


# ── Manifest trimming ───────────────────────────────────────────────────


def _trim_manifest(manifest_s3_key: str, staging_bucket: str, promoted_ids: set[str]) -> None:
    """Remove promoted PDB IDs from the transfer manifest in S3.

    :param manifest_s3_key: S3 object key of the transfer_manifest.txt
    :param staging_bucket: S3 bucket containing the manifest
    :param promoted_ids: set of PDB IDs that were successfully promoted
    """
    s3 = get_s3_client()

    with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".txt") as tmp:
        tmp_path = tmp.name

    try:
        try:
            s3.download_file(Bucket=staging_bucket, Key=manifest_s3_key, Filename=tmp_path)
        except s3.exceptions.NoSuchKey:
            logger.warning("Manifest not found in S3 (s3://%s/%s) — skipping trim", staging_bucket, manifest_s3_key)
            return
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "404":
                logger.warning("Manifest not found in S3 (s3://%s/%s) — skipping trim", staging_bucket, manifest_s3_key)
                return
            raise

        with Path(tmp_path).open() as f:
            lines = f.readlines()

        remaining = [line for line in lines if line.strip() and line.strip().lower() not in promoted_ids]

        with Path(tmp_path).open("w") as f:
            f.writelines(remaining)

        s3.upload_file(Filename=tmp_path, Bucket=staging_bucket, Key=manifest_s3_key)
        logger.info(
            "Trimmed manifest: %d -> %d entries (%d promoted)",
            len(lines),
            len(remaining),
            len(lines) - len(remaining),
        )
    finally:
        Path(tmp_path).unlink()
