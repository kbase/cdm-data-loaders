"""Promote staged NCBI FTP files to Lakehouse S3 paths, archive superseded assemblies, and trim manifests."""

import contextlib
import tempfile
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import UTC, datetime
from logging import Logger, getLogger
from pathlib import Path, PurePosixPath
from typing import Any, Final, TypedDict, Unpack

import tqdm

from cdm_data_loaders.datasets.ncbi_ftp.assembly import (
    build_accession_path,
    extract_accession,
    parse_assembly_path,
)
from cdm_data_loaders.datasets.ncbi_ftp.manifest import (
    filter_unpromoted_manifest_lines,
    read_manifest_lines,
)
from cdm_data_loaders.datasets.ncbi_ftp.metadata import (
    DescriptorResource,
    archive_descriptor,
    build_descriptor_key,
    create_descriptor,
    upload_descriptor,
)
from cdm_data_loaders.io.s3.object_utils import (
    copy_object,
    delete_objects,
    download_file,
    get_object_bytes,
    get_object_bytes_if_exists,
    list_objects,
    object_exists,
    put_object_bytes,
    upload_file,
)

logger: Logger = getLogger(__name__)

ASSEMBLY_DIR_SEGMENT_INDEX: Final[int] = 5
MAX_DRY_RUN_LOGS: Final[int] = 10
MAX_TRANSFER_WORKERS: Final[int] = 32


@dataclass(frozen=True, slots=True)
class _PromoteTargets:
    """Source staging and destination Lakehouse S3 bucket/prefix configuration."""

    staging_bucket: PurePosixPath
    staging_prefix: PurePosixPath
    lakehouse_bucket: PurePosixPath
    lakehouse_key_prefix: PurePosixPath
    sidecars: set[PurePosixPath]


class PromoteOptions(TypedDict, total=False):
    """Optional parameters for :func:`promote_from_s3`."""

    removed_manifest_path: Path | None
    updated_manifest_path: Path | None
    manifest_s3_key: PurePosixPath | None
    ncbi_release: str | None
    dry_run: bool


class ArchiveAssembliesOptions(TypedDict, total=False):
    """Optional parameters for :func:`_archive_assemblies`."""

    ncbi_release: str | None
    archive_reason: str
    delete_source: bool
    dry_run: bool


def promote_from_s3(
    *,
    staging_bucket: PurePosixPath,
    staging_key_prefix: PurePosixPath,
    lakehouse_bucket: PurePosixPath,
    lakehouse_key_prefix: PurePosixPath,
    **options: Unpack[PromoteOptions],
) -> dict[str, Any]:
    """Promote staged S3 files to final Lakehouse paths, archive affected assemblies, and trim manifests.

    :param staging_bucket: S3 bucket containing staged files
    :type staging_bucket: PurePosixPath
    :param staging_key_prefix: S3 key prefix where staged output was written
    :type staging_key_prefix: PurePosixPath
    :param lakehouse_bucket: destination Lakehouse S3 bucket
    :type lakehouse_bucket: PurePosixPath
    :param lakehouse_key_prefix: destination Lakehouse S3 key prefix
    :type lakehouse_key_prefix: PurePosixPath
    :param options: optional manifest paths, release tag, and ``dry_run`` flag
    :type options: Unpack[PromoteOptions]
    :return: promotion report dictionary
    :rtype: dict[str, Any]
    """
    removed_manifest_path = options.get("removed_manifest_path")
    updated_manifest_path = options.get("updated_manifest_path")
    manifest_s3_key = options.get("manifest_s3_key")
    ncbi_release = options.get("ncbi_release")
    dry_run = options.get("dry_run", False)

    staged_objects: list[dict[str, Any]] = list_objects(f"{staging_bucket / staging_key_prefix}/")
    sidecars = {PurePosixPath(k["Key"]) for k in staged_objects if k["Key"].endswith((".crc64nvme", ".md5"))}
    data_files = [PurePosixPath(k["Key"]) for k in staged_objects if PurePosixPath(k["Key"]) not in sidecars]

    logger.info("Found %d data files and %d sidecars in staging", len(data_files), len(sidecars))

    archived = 0
    for manifest_file, reason, delete in (
        (updated_manifest_path, "updated", False),
        (removed_manifest_path, "replaced_or_suppressed", True),
    ):
        if manifest_file and Path(str(manifest_file)).is_file():
            archived += _archive_assemblies(
                manifest_file,
                lakehouse_bucket=lakehouse_bucket,
                lakehouse_key_prefix=lakehouse_key_prefix,
                ncbi_release=ncbi_release,
                archive_reason=reason,
                delete_source=delete,
                dry_run=dry_run,
            )

    targets = _PromoteTargets(
        staging_bucket=staging_bucket,
        staging_prefix=staging_key_prefix,
        lakehouse_bucket=lakehouse_bucket,
        lakehouse_key_prefix=lakehouse_key_prefix,
        sidecars=sidecars,
    )
    promoted, failed, descriptors_written, promoted_accessions = _promote_data_files(
        data_files,
        targets,
        dry_run=dry_run,
    )

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


def _group_files_by_assembly(
    data_files: list[PurePosixPath],
    staging_prefix: PurePosixPath,
) -> defaultdict[tuple[PurePosixPath, str], list[PurePosixPath]]:
    """Group staged data files by ``(assembly_dir, accession)``, skipping reports and non-``raw_data/`` keys."""
    assembly_files: defaultdict[tuple[PurePosixPath, str], list[PurePosixPath]] = defaultdict(list)
    for staged_key in data_files:
        if staged_key.match("**/download_report.json"):
            continue
        rel_path = staged_key.relative_to(staging_prefix)
        if not rel_path.is_relative_to("raw_data/"):
            continue
        with contextlib.suppress(ValueError):
            _db, assembly_dir, accession = parse_assembly_path(staged_key)
            assembly_files[(assembly_dir, accession)].append(staged_key)
    return assembly_files


def _promote_file(
    staged_key: PurePosixPath,
    targets: _PromoteTargets,
) -> tuple[DescriptorResource, PurePosixPath]:
    """Download one staged file and upload it to Lakehouse S3 with optional MD5 sidecar metadata."""
    rel_path = staged_key.relative_to(targets.staging_prefix)
    final_key = targets.lakehouse_key_prefix / rel_path
    final_key_path = PurePosixPath(final_key)

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_path = Path(tmp.name)
    try:
        download_file(
            str(targets.staging_bucket / staged_key),
            tmp_path,
            show_progress=False,
        )

        metadata: dict[str, str] = {}
        md5_key = staged_key.with_name(f"{staged_key.name}.md5")
        if md5_key in targets.sidecars:
            metadata["md5"] = get_object_bytes(str(targets.staging_bucket / md5_key)).decode("utf-8").strip()

        upload_succeeded = upload_file(
            tmp_path,
            str(targets.lakehouse_bucket / final_key_path.parent),
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
            "bytes": tmp_path.stat().st_size,
            "hash": metadata.get("md5"),
        }
        return resource, staged_key
    finally:
        tmp_path.unlink(missing_ok=True)


def _write_descriptor_for_assembly(
    assembly_dir: PurePosixPath,
    accession: str,
    resources: list[DescriptorResource],
    lakehouse_bucket: PurePosixPath,
    lakehouse_key_prefix: PurePosixPath,
) -> bool:
    """Create and upload a frictionless descriptor for an assembly if one does not exist."""
    try:
        descriptor_key = build_descriptor_key(assembly_dir, lakehouse_key_prefix)
        if object_exists(str(lakehouse_bucket / descriptor_key)):
            logger.debug("Descriptor exists, skipping: %s", descriptor_key)
        else:
            descriptor = create_descriptor(assembly_dir, accession, resources)
            descriptor_key = upload_descriptor(
                descriptor,
                assembly_dir,
                lakehouse_bucket,
                lakehouse_key_prefix,
                dry_run=False,
            )
            logger.debug("Uploaded descriptor: %s", descriptor_key)
            return True
    except Exception:
        logger.exception("Failed to write descriptor for %s", assembly_dir)
    return False


def _delete_with_warnings(bucket: PurePosixPath, keys: list[str], label: str = "") -> None:
    """Delete S3 keys from ``bucket``, logging a warning for any per-key deletion error."""
    prefix = f"Failed to delete {label} " if label else "Failed to delete "
    for err in delete_objects(str(bucket), keys):
        logger.warning("%s%s: %s", prefix, err.get("Key"), err.get("Message"))


def _dry_run_output(
    key_pairs: list[tuple[PurePosixPath, PurePosixPath]],
    log_count: int,
    action: str = "archive",
) -> int:
    """Log source and destination key pairs for a dry run, capped at ``INFO`` level."""
    dry_run_log_count = log_count
    for source_key, dest_key in key_pairs:
        if dry_run_log_count < MAX_DRY_RUN_LOGS:
            logger.info("[dry-run] would %s: %s -> %s", action, source_key, dest_key)
        else:
            logger.debug("[dry-run] would %s: %s -> %s", action, source_key, dest_key)
        dry_run_log_count += 1
    return dry_run_log_count


def _batch_delete(
    promoted_keys: list[PurePosixPath],
    sidecars: set[PurePosixPath],
    staging_bucket: PurePosixPath,
) -> None:
    """Batch-delete promoted staged data files and their sidecar objects."""
    keys_to_delete = list(promoted_keys)
    keys_to_delete.extend(
        key.with_name(f"{key.name}{sidecar_ext}")
        for key in promoted_keys
        for sidecar_ext in (".md5", ".crc64nvme")
        if key.with_name(f"{key.name}{sidecar_ext}") in sidecars
    )
    _delete_with_warnings(staging_bucket, [str(key) for key in keys_to_delete], label="staged file")


def _promote_data_files(
    data_files: list[PurePosixPath],
    targets: _PromoteTargets,
    *,
    dry_run: bool,
) -> tuple[int, int, int, set[str]]:
    """Promote staged data files per assembly, writing descriptors and cleaning staging on success."""
    promoted = 0
    failed = 0
    descriptors_written = 0
    promoted_accessions: set[str] = set()
    assembly_files = _group_files_by_assembly(data_files, targets.staging_prefix)

    total_files = sum(len(v) for v in assembly_files.values())
    dry_run_log_count = 0
    with tqdm.tqdm(total=total_files, unit="file", desc="Promoting") as pbar:
        for (adir, acc), files in assembly_files.items():
            assembly_failed = 0
            resources: list[DescriptorResource] = []
            promoted_keys: list[PurePosixPath] = []

            if dry_run:
                pairs = [
                    (staged_key, targets.lakehouse_key_prefix / staged_key.relative_to(targets.staging_prefix))
                    for staged_key in files
                ]
                dry_run_log_count = _dry_run_output(pairs, dry_run_log_count, action="promote")
                promoted += len(files)
                pbar.update(len(files))
                continue

            n_workers = min(MAX_TRANSFER_WORKERS, len(files))
            with ThreadPoolExecutor(max_workers=n_workers) as executor:
                futures = {executor.submit(_promote_file, key, targets): key for key in files}
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

            if assembly_failed == 0 and promoted_keys:
                if _write_descriptor_for_assembly(
                    adir,
                    acc,
                    resources,
                    targets.lakehouse_bucket,
                    targets.lakehouse_key_prefix,
                ):
                    descriptors_written += 1
                _batch_delete(promoted_keys, targets.sidecars, targets.staging_bucket)

    return promoted, failed, descriptors_written, promoted_accessions


def _get_accession_path_prefix(accession: str, lakehouse_key_prefix: PurePosixPath) -> PurePosixPath | None:
    """Return the S3 key prefix for all objects belonging to ``accession``, or ``None`` if invalid."""
    try:
        return lakehouse_key_prefix / build_accession_path(PurePosixPath(accession))
    except ValueError:
        logger.warning("Invalid accession format: %s", accession)
        return None


def _get_source_dest_pairs_for_accession(
    accession: str,
    lakehouse_bucket: PurePosixPath,
    lakehouse_key_prefix: PurePosixPath,
    release_tag: str,
    archive_reason: str,
) -> list[tuple[PurePosixPath, PurePosixPath]]:
    """Build ``(source_key, archive_key)`` pairs for all S3 objects belonging to ``accession``."""
    source_prefix = _get_accession_path_prefix(accession, lakehouse_key_prefix)
    if not source_prefix:
        return []
    target_accession = extract_accession(accession)
    matching_objs: list[dict[str, Any]] = list_objects(f"{lakehouse_bucket / source_prefix}")
    pairs: list[tuple[PurePosixPath, PurePosixPath]] = []
    for obj in matching_objs:
        src_key = PurePosixPath(obj["Key"])
        rel_key = src_key.relative_to(lakehouse_key_prefix)
        if len(rel_key.parts) > ASSEMBLY_DIR_SEGMENT_INDEX:
            with contextlib.suppress(ValueError):
                if extract_accession(rel_key.parts[ASSEMBLY_DIR_SEGMENT_INDEX]) != target_accession:
                    continue
        pairs.append(
            (
                src_key,
                lakehouse_key_prefix / "archive" / release_tag / archive_reason / rel_key,
            )
        )
    return pairs


def _archive_objects(
    key_pairs: list[tuple[PurePosixPath, PurePosixPath]],
    lakehouse_bucket: PurePosixPath,
    *,
    delete_source: bool,
) -> int:
    """Copy objects from source keys to archive keys, deleting source objects when ``delete_source`` is ``True``."""
    archived = 0
    if not key_pairs:
        return archived
    keys_to_delete: list[str] = []
    n_workers = min(MAX_TRANSFER_WORKERS, len(key_pairs))
    with ThreadPoolExecutor(max_workers=n_workers) as executor:
        futures = {
            executor.submit(
                copy_object,
                str(lakehouse_bucket / src),
                str(lakehouse_bucket / arch),
            ): src
            for src, arch in key_pairs
        }
        for future in as_completed(futures):
            src = futures[future]
            try:
                future.result()
                archived += 1
                if delete_source:
                    keys_to_delete.append(str(src))
                logger.debug("  Archived: %s", src)
            except Exception:
                logger.exception("Failed to archive %s", src)

    if delete_source and keys_to_delete:
        _delete_with_warnings(lakehouse_bucket, keys_to_delete)

    return archived


def _archive_assemblies(
    manifest_local_path: Path,
    lakehouse_bucket: PurePosixPath,
    lakehouse_key_prefix: PurePosixPath,
    **options: Unpack[ArchiveAssembliesOptions],
) -> int:
    """Archive Lakehouse assembly objects listed in ``manifest_local_path`` under ``archive/{release_tag}/``."""
    release_tag = options.get("ncbi_release") or "unknown"
    archive_reason = options.get("archive_reason", "unknown")
    delete_source = options.get("delete_source", False)
    dry_run = options.get("dry_run", False)
    archived = 0

    accessions = read_manifest_lines(Path(manifest_local_path))

    dry_run_log_count = 0
    for accession in tqdm.tqdm(accessions, unit="accession", desc="Archiving"):
        key_pairs = _get_source_dest_pairs_for_accession(
            accession,
            lakehouse_bucket,
            lakehouse_key_prefix,
            release_tag,
            archive_reason,
        )
        if not key_pairs:
            logger.debug("No objects found for %s, skipping archive", accession)
            continue

        if dry_run:
            dry_run_log_count = _dry_run_output(key_pairs, dry_run_log_count)
            archived += len(key_pairs)
            continue
        archived += _archive_objects(key_pairs, lakehouse_bucket, delete_source=delete_source)

        assembly_dir: PurePosixPath | None = None
        for src, _ in key_pairs:
            with contextlib.suppress(ValueError):
                _, assembly_dir, _ = parse_assembly_path(src)
                break

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


def _trim_manifest(
    manifest_s3_key: PurePosixPath,
    staging_bucket: PurePosixPath,
    promoted_accessions: set[str],
) -> None:
    """Remove promoted accessions from ``manifest_s3_key`` in ``staging_bucket``."""
    s3_path = str(staging_bucket / manifest_s3_key)
    raw_bytes = get_object_bytes_if_exists(s3_path)
    if raw_bytes is None:
        logger.warning("Manifest not found in S3 (s3://%s/%s) — skipping trim", staging_bucket, manifest_s3_key)
        return

    remaining, total_entries = filter_unpromoted_manifest_lines(raw_bytes.decode("utf-8"), promoted_accessions)
    payload = "".join(f"{entry}\n" for entry in remaining).encode("utf-8")
    put_object_bytes(payload, s3_path, content_type="text/plain")
    logger.info(
        "Trimmed manifest: %d -> %d entries (%d promoted)",
        total_entries,
        len(remaining),
        total_entries - len(remaining),
    )
