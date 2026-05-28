"""Phase 3: Promote staged files to final Lakehouse paths in S3.

Walks staged files in an S3 staging prefix (written by CTS after Phase 2),
uploads each to the final Lakehouse path with MD5 metadata from sidecar files,
archives replaced/suppressed and updated assemblies, and trims the transfer
manifest so that a re-run of Phase 2 only downloads remaining entries.
"""

import re
import tempfile
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import Any

import botocore.exceptions
import tqdm

from cdm_data_loaders.ncbi_ftp.metadata import (
    DescriptorResource,
    archive_descriptor,
    build_descriptor_key,
    create_descriptor,
    upload_descriptor,
)
from cdm_data_loaders.utils.cdm_logger import get_cdm_logger
from cdm_data_loaders.utils.s3 import (
    copy_object,
    delete_objects,
    get_s3_client,
    list_matching_objects,
    object_exists,
    upload_file,
)

logger = get_cdm_logger()

DEFAULT_LAKEHOUSE_KEY_PREFIX = "tenant-general-warehouse/kbase/datasets/ncbi/"

_MAX_DRY_RUN_LOGS = 10


# Promote from S3 staging prefix


def promote_from_s3(  # noqa: PLR0913
    staging_key_prefix: str,
    staging_bucket: str,
    lakehouse_bucket: str,
    removed_manifest_path: str | Path | None = None,
    updated_manifest_path: str | Path | None = None,
    ncbi_release: str | None = None,
    manifest_s3_key: str | None = None,
    lakehouse_key_prefix: str = DEFAULT_LAKEHOUSE_KEY_PREFIX,
    *,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Promote files from an S3 staging prefix to the final Lakehouse path.

    Downloads each file to a temp location and re-uploads to the final path
    with MD5 metadata from ``.md5`` sidecar files.

    :param staging_key_prefix: S3 key prefix where CTS output was written
    :param staging_bucket: S3 bucket containing the staged files (e.g. ``"cts"``)
    :param lakehouse_bucket: S3 bucket for the final Lakehouse destination (e.g. ``"cdm-lake"``)
    :param removed_manifest_path: local path to the removed_manifest file
    :param updated_manifest_path: local path to the updated_manifest file
    :param ncbi_release: NCBI release version tag for archiving
    :param manifest_s3_key: S3 object key for transfer_manifest.txt (for trimming)
    :param lakehouse_key_prefix: S3 key prefix for final Lakehouse locations
    :param dry_run: if True, log actions without side effects
    :return: report dict with counts
    """
    # Get list of objects under the staging prefix
    normalized_staging_key_prefix = staging_key_prefix.rstrip("/") + "/"
    staged_objects: list[dict[str, Any]] = list_matching_objects(f"{staging_bucket}/{normalized_staging_key_prefix}")

    # Separate data files from sidecars
    sidecars = {k["Key"] for k in staged_objects if k["Key"].endswith((".crc64nvme", ".md5"))}
    data_files = [k["Key"] for k in staged_objects if k["Key"] not in sidecars]

    logger.info("Found %d data files and %d sidecars in staging", len(data_files), len(sidecars))

    # Archive all affected assemblies BEFORE promoting or deleting
    archived = 0
    for manifest_file, reason, delete in [
        (updated_manifest_path, "updated", False),
        (removed_manifest_path, "replaced_or_suppressed", True),
    ]:
        if manifest_file and Path(str(manifest_file)).is_file():
            archived += _archive_assemblies(
                str(manifest_file),
                lakehouse_bucket=lakehouse_bucket,
                ncbi_release=ncbi_release,
                lakehouse_key_prefix=lakehouse_key_prefix,
                archive_reason=reason,
                delete_source=delete,
                dry_run=dry_run,
            )

    promoted, failed, descriptors_written, promoted_accessions = _promote_data_files(
        data_files,
        sidecars,
        normalized_staging_key_prefix,
        lakehouse_key_prefix,
        staging_bucket,
        lakehouse_bucket,
        dry_run=dry_run,
    )

    # Trim manifest for resumability
    if manifest_s3_key and promoted_accessions and not dry_run:
        _trim_manifest(manifest_s3_key, staging_bucket, promoted_accessions)

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


# Promote data files (per-file loop)


def _promote_data_files(  # noqa: PLR0913, PLR0915
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

    Files are grouped by assembly.  When all files for an assembly are promoted
    successfully, the frictionless descriptor is written immediately and the staged
    files (including sidecars) are deleted from staging.  This prevents staging
    accumulation across runs and ensures partial runs leave descriptors for all
    completed assemblies.

    :return: (promoted_count, failed_count, descriptors_written, promoted_accessions)
    """
    s3 = get_s3_client()
    promoted = 0
    failed = 0
    descriptors_written = 0
    promoted_accessions: set[str] = set()

    # Group files by assembly; skip download_report.json and non-raw_data paths
    assembly_files: defaultdict[tuple[str, str], list[str]] = defaultdict(list)
    for staged_key in data_files:
        if staged_key.endswith("download_report.json"):
            continue
        rel_path = staged_key[len(normalized_staging_prefix) :]
        if not rel_path.startswith("raw_data/"):
            continue
        acc_match = re.search(r"(GC[AF]_\d{9}\.\d+)", staged_key)
        adir_match = re.search(r"raw_data/GC[AF]/\d+/\d+/\d+/([^/]+)/", staged_key)
        if acc_match and adir_match:
            assembly_files[(adir_match.group(1), acc_match.group(1))].append(staged_key)

    def _promote_one(staged_key: str) -> tuple[DescriptorResource, str]:
        """Download one staged file, re-upload to Lakehouse with MD5 metadata.

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
            md5_key = staged_key + ".md5"
            if md5_key in sidecars:
                md5_obj = s3.get_object(Bucket=staging_bucket, Key=md5_key)
                metadata["md5"] = md5_obj["Body"].read().decode().strip()

            upload_succeeded = upload_file(
                tmp_path,
                f"{lakehouse_bucket}/{final_key_path.parent}",
                user_metadata=metadata,
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
                "hash": metadata.get("md5"),
            }
            return resource, staged_key
        finally:
            Path(tmp_path).unlink()

    total_files = sum(len(v) for v in assembly_files.values())
    _dry_run_log_count = 0
    with tqdm.tqdm(total=total_files, unit="file", desc="Promoting") as pbar:
        for (adir, acc), files in assembly_files.items():
            assembly_failed = 0
            resources: list[DescriptorResource] = []
            promoted_keys: list[str] = []

            if dry_run:
                for staged_key in files:
                    rel_path = staged_key[len(normalized_staging_prefix) :]
                    final_key = lakehouse_key_prefix + rel_path
                    if _dry_run_log_count < _MAX_DRY_RUN_LOGS:
                        logger.info("[dry-run] would promote: %s -> %s", staged_key, final_key)
                    else:
                        logger.debug("[dry-run] would promote: %s -> %s", staged_key, final_key)
                    _dry_run_log_count += 1
                    promoted += 1
                    pbar.update(1)
                continue

            # Download and re-upload all files for this assembly concurrently
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
                        promoted_accessions.add(acc)
                    except Exception:
                        logger.exception("Failed to promote %s", staged_key)
                        assembly_failed += 1
                    pbar.update(1)

            failed += assembly_failed

            # Write descriptor and delete staged files immediately after a fully successful assembly
            if assembly_failed == 0 and promoted_keys:
                try:
                    descriptor_key = build_descriptor_key(adir, lakehouse_key_prefix)
                    if object_exists(f"{lakehouse_bucket}/{descriptor_key}"):
                        logger.debug("Descriptor already exists, skipping: %s", descriptor_key)
                    else:
                        descriptor = create_descriptor(adir, acc, resources)
                        descriptor_key = upload_descriptor(
                            descriptor, adir, lakehouse_bucket, lakehouse_key_prefix, dry_run=False
                        )
                        logger.debug("Uploaded descriptor: %s", descriptor_key)
                        descriptors_written += 1
                except Exception:
                    logger.exception("Failed to write descriptor for %s", adir)

                # Batch-delete all staged data files and their sidecars in one API call
                keys_to_delete = list(promoted_keys)
                keys_to_delete.extend(
                    key + sidecar_ext
                    for key in promoted_keys
                    for sidecar_ext in (".md5", ".crc64nvme")
                    if key + sidecar_ext in sidecars
                )
                del_errors = delete_objects(staging_bucket, keys_to_delete)
                for err in del_errors:
                    logger.warning("Failed to delete staged file %s: %s", err.get("Key"), err.get("Message"))

    return promoted, failed, descriptors_written, promoted_accessions


# Archive assemblies


def _archive_assemblies(  # noqa: PLR0913
    manifest_local_path: str,
    lakehouse_bucket: str,
    ncbi_release: str | None = None,
    lakehouse_key_prefix: str = DEFAULT_LAKEHOUSE_KEY_PREFIX,
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

    :param manifest_local_path: local path to a manifest file (one accession per line)
    :param lakehouse_bucket: S3 bucket for the Lakehouse (source and archive destination)
    :param ncbi_release: release tag used in the archive path
    :param lakehouse_key_prefix: S3 key prefix for the Lakehouse dataset root
    :param archive_reason: metadata value describing why the object was archived
    :param delete_source: if True, delete the source object after copying
    :param dry_run: if True, log without making changes
    :return: number of objects archived
    """
    s3 = get_s3_client()
    release_tag = ncbi_release or "unknown"
    archived = 0

    with Path(manifest_local_path).open() as f:
        accessions = [line.strip() for line in f if line.strip()]

    _dry_run_log_count = 0
    for accession in tqdm.tqdm(accessions, unit="accession", desc="Archiving"):
        m = re.match(r"(GC[AF])_(\d{3})(\d{3})(\d{3})\.\d+", accession)
        if not m:
            logger.warning("Cannot parse accession for archival: %s", accession)
            continue

        db = m.group(1)
        p1, p2, p3 = m.group(2), m.group(3), m.group(4)
        source_prefix = f"{lakehouse_key_prefix}raw_data/{db}/{p1}/{p2}/{p3}/"

        paginator = s3.get_paginator("list_objects_v2")
        matching_keys: list[str] = []
        for page in paginator.paginate(Bucket=lakehouse_bucket, Prefix=source_prefix):
            matching_keys.extend(obj["Key"] for obj in page.get("Contents", []) if accession in obj["Key"])

        if not matching_keys:
            logger.debug("No objects found for %s, skipping archive", accession)
            continue

        # Infer assembly_dir from key paths for descriptor archival
        assembly_dir: str | None = None
        for key in matching_keys:
            adir_match = re.search(r"raw_data/GC[AF]/\d+/\d+/\d+/([^/]+)/", key)
            if adir_match:
                assembly_dir = adir_match.group(1)
                break

        key_pairs = [
            (
                source_key,
                f"{lakehouse_key_prefix}archive/{release_tag}/{archive_reason}/{source_key[len(lakehouse_key_prefix) :]}",
            )
            for source_key in matching_keys
        ]

        if dry_run:
            for source_key, archive_key in key_pairs:
                if _dry_run_log_count < _MAX_DRY_RUN_LOGS:
                    logger.info("[dry-run] would archive: %s -> %s", source_key, archive_key)
                else:
                    logger.debug("[dry-run] would archive: %s -> %s", source_key, archive_key)
                _dry_run_log_count += 1
            archived += len(key_pairs)
            continue

        # Copy all files for this accession concurrently
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
        if assembly_dir:
            try:
                archived_desc = archive_descriptor(
                    assembly_dir,
                    lakehouse_bucket,
                    lakehouse_key_prefix,
                    release_tag,
                    archive_reason=archive_reason,
                    dry_run=dry_run,
                )
                if not archived_desc:
                    logger.debug("No descriptor found to archive for %s", assembly_dir)
            except Exception:
                logger.exception("Failed to archive descriptor for %s", assembly_dir)

    logger.info("Archived %d objects for %d accessions (%s)", archived, len(accessions), archive_reason)
    return archived


# Manifest trimming


def _trim_manifest(manifest_s3_key: str, staging_bucket: str, promoted_accessions: set[str]) -> None:
    """Remove promoted accessions from the transfer manifest in S3.

    :param manifest_s3_key: S3 object key of the transfer_manifest.txt
    :param staging_bucket: S3 bucket containing the transfer manifest
    :param promoted_accessions: set of accessions that were successfully promoted
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

        remaining = [line for line in lines if line.strip() and not any(acc in line for acc in promoted_accessions)]

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
