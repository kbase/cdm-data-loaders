"""PDB rsync download pipeline (Phase 2).

Orchestrates parallel rsync-based downloading of PDB entries listed in a
transfer manifest.  Settings, batching, CLI entry point, and CTS integration
live here; domain-specific path logic is in :mod:`cdm_data_loaders.pdb.entry`.

Each entry is synced from the wwPDB Beta archive using ``rsync`` as a
subprocess.  After each successful entry download, CRC64NVME checksum sidecar
files (``.crc64nvme``) are written alongside every downloaded file so that
Phase 3 (promote) can validate transfer integrity.
"""

import json
import shutil
import subprocess
import tempfile
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import tqdm

from pydantic import AliasChoices, Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from cdm_data_loaders.pdb.entry import (
    ALL_FILE_TYPES,
    RSYNC_ENTRIES_PATH,
    RSYNC_HOST,
    RSYNC_MODULE,
    RSYNC_PORT,
    build_entry_path,
    pdb_id_hash,
)
from cdm_data_loaders.pipelines.core import run_cli
from cdm_data_loaders.pipelines.cts_defaults import DEFAULT_SETTINGS_CONFIG_DICT, INPUT_MOUNT, OUTPUT_MOUNT
from cdm_data_loaders.utils.cdm_logger import get_cdm_logger
from cdm_data_loaders.utils.checksums import compute_crc64nvme
from cdm_data_loaders.utils.s3 import get_s3_client, upload_file

logger = get_cdm_logger()


# ── Settings ─────────────────────────────────────────────────────────────


class PdbRsyncSettings(BaseSettings):
    """Configuration for the PDB rsync download pipeline."""

    model_config = SettingsConfigDict(**DEFAULT_SETTINGS_CONFIG_DICT)

    manifest: str = Field(
        default=f"{INPUT_MOUNT}/transfer_manifest.txt",
        description="Path to the transfer manifest file listing PDB IDs to download",
        validation_alias=AliasChoices("m", "manifest"),
    )
    output_dir: str = Field(
        default=OUTPUT_MOUNT,
        description="Output directory for downloaded PDB entry files",
        validation_alias=AliasChoices("output-dir", "output_dir"),
    )
    workers: int = Field(
        default=4,
        ge=1,
        le=16,
        description="Number of parallel rsync workers",
        validation_alias=AliasChoices("w", "workers"),
    )
    rsync_host: str = Field(
        default=RSYNC_HOST,
        description="wwPDB Beta rsync hostname",
        validation_alias=AliasChoices("rsync-host", "rsync_host"),
    )
    rsync_port: int = Field(
        default=RSYNC_PORT,
        description="rsync port",
        validation_alias=AliasChoices("rsync-port", "rsync_port"),
    )
    file_types: list[str] = Field(
        default=list(ALL_FILE_TYPES),
        description="PDB file type subdirectories to download (default: all)",
        validation_alias=AliasChoices("file-types", "file_types"),
    )
    limit: int | None = Field(
        default=None,
        ge=1,
        description="Limit to first N entries (for testing)",
        validation_alias=AliasChoices("l", "limit"),
    )

    @field_validator("workers")
    @classmethod
    def validate_workers(cls, v: int) -> int:
        """Validate workers is within allowed range."""
        if v < 1 or v > 16:  # noqa: PLR2004
            msg = f"workers must be between 1 and 16, got {v}"
            raise ValueError(msg)
        return v


# ── Rsync command builder ────────────────────────────────────────────────


def _build_rsync_cmd(
    pdb_id: str,
    output_dir: str | Path,
    rsync_host: str = RSYNC_HOST,
    rsync_port: int = RSYNC_PORT,
    file_types: list[str] | None = None,
) -> list[str]:
    """Build the rsync command for a single PDB entry.

    :param pdb_id: extended PDB ID, e.g. ``"pdb_00001abc"``
    :param output_dir: base output directory
    :param rsync_host: rsync server hostname
    :param rsync_port: rsync server port
    :param file_types: list of file-type subdirs to include, or None for all
    :return: list of command tokens suitable for :func:`subprocess.run`
    """
    normed = pdb_id.lower()
    h = pdb_id_hash(normed)

    source = f"{rsync_host}::{RSYNC_MODULE}/{RSYNC_ENTRIES_PATH}/{h}/{normed}/"
    dest = str(Path(output_dir) / build_entry_path(normed))

    cmd: list[str] = [
        "rsync",
        f"--port={rsync_port}",
        "-rlpt",
        "--delete",
        "--prune-empty-dirs",
        "--timeout=60",
    ]

    all_types = set(ALL_FILE_TYPES)
    requested = set(file_types) if file_types else all_types

    if requested != all_types:
        # Add include/exclude filters for requested subdirectories only
        cmd += ["--include", "*/"]  # always include directories
        for ft in sorted(requested):
            cmd += ["--include", f"{ft}/**"]
        cmd += ["--exclude", "*"]

    cmd += [source, dest]
    return cmd


# ── Single-entry download ────────────────────────────────────────────────


def _write_crc64nvme_sidecars(entry_local_path: Path) -> int:
    """Compute and write ``.crc64nvme`` sidecar files for all non-sidecar files.

    :param entry_local_path: local directory containing downloaded entry files
    :return: number of sidecar files written
    """
    count = 0
    for file_path in entry_local_path.rglob("*"):
        if not file_path.is_file():
            continue
        if file_path.suffix in (".crc64nvme", ".md5"):
            continue
        sidecar_path = file_path.with_suffix(file_path.suffix + ".crc64nvme")
        checksum = compute_crc64nvme(file_path)
        sidecar_path.write_text(checksum)
        count += 1
    return count


def download_entry(
    pdb_id: str,
    output_dir: str | Path,
    rsync_host: str = RSYNC_HOST,
    rsync_port: int = RSYNC_PORT,
    file_types: list[str] | None = None,
) -> dict[str, Any]:
    """Download a single PDB entry via rsync and write CRC64NVME sidecars.

    :param pdb_id: extended PDB ID, e.g. ``"pdb_00001abc"``
    :param output_dir: base output directory (``raw_data/`` will be created inside)
    :param rsync_host: rsync server hostname
    :param rsync_port: rsync server port
    :param file_types: file-type subdirs to include, or None for all
    :return: stats dict with ``pdb_id``, ``files_downloaded``, ``sidecars_written``
    """
    normed = pdb_id.lower()
    entry_rel = build_entry_path(normed)
    entry_local = Path(output_dir) / entry_rel
    entry_local.mkdir(parents=True, exist_ok=True)

    cmd = _build_rsync_cmd(normed, output_dir, rsync_host, rsync_port, file_types)
    logger.debug("rsync command: %s", " ".join(cmd))

    result = subprocess.run(cmd, capture_output=True, text=True, check=False)  # noqa: S603

    if result.returncode != 0:
        logger.error("rsync failed for %s (exit %d): %s", pdb_id, result.returncode, result.stderr.strip())
        msg = f"rsync failed for {pdb_id} with exit code {result.returncode}: {result.stderr.strip()}"
        raise RuntimeError(msg)

    # Count downloaded files (non-sidecar)
    files_downloaded = sum(1 for p in entry_local.rglob("*") if p.is_file() and p.suffix not in (".crc64nvme", ".md5"))

    # Write CRC64NVME sidecars
    sidecars_written = _write_crc64nvme_sidecars(entry_local)

    return {
        "pdb_id": normed,
        "files_downloaded": files_downloaded,
        "sidecars_written": sidecars_written,
    }


# ── Batch download ───────────────────────────────────────────────────────


def download_batch(
    manifest_path: str | Path,
    output_dir: str | Path,
    workers: int = 4,
    rsync_host: str = RSYNC_HOST,
    rsync_port: int = RSYNC_PORT,
    file_types: list[str] | None = None,
    limit: int | None = None,
) -> dict[str, Any]:
    """Download all PDB entries listed in the manifest.

    :param manifest_path: path to the transfer manifest file (one PDB ID per line)
    :param output_dir: base output directory
    :param workers: number of parallel rsync workers
    :param rsync_host: rsync hostname
    :param rsync_port: rsync port
    :param file_types: file-type subdirs to include, or None for all
    :param limit: optional limit for testing
    :return: report dict with overall stats
    """
    with Path(manifest_path).open() as f:
        pdb_ids = [line.strip().lower() for line in f if line.strip() and not line.startswith("#")]

    if limit:
        pdb_ids = pdb_ids[:limit]

    logger.info("Starting download of %d PDB entries with %d workers", len(pdb_ids), workers)

    lock = threading.Lock()
    success_count = 0
    failed: list[dict[str, str]] = []
    all_stats: list[dict[str, Any]] = []

    def _download_one(pdb_id: str) -> tuple[str, Exception | None]:
        nonlocal success_count
        try:
            stats = download_entry(pdb_id, output_dir, rsync_host, rsync_port, file_types)
        except Exception as e:  # noqa: BLE001
            return pdb_id, e
        else:
            with lock:
                success_count += 1
                all_stats.append(stats)
            return pdb_id, None

    with tqdm.tqdm(total=len(pdb_ids), unit="entry", desc="Downloading", smoothing=0.01) as pbar:
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {executor.submit(_download_one, pid): pid for pid in pdb_ids}
            for future in as_completed(futures):
                pid, error = future.result()
                if error:
                    logger.error("FAILED: %s: %s", pid, error)
                    with lock:
                        failed.append({"pdb_id": pid, "error": str(error)})
                pbar.update(1)

    report: dict[str, Any] = {
        "timestamp": datetime.now(UTC).isoformat(),
        "total_attempted": len(pdb_ids),
        "succeeded": success_count,
        "failed": len(failed),
        "failures": failed,
        "entry_stats": all_stats,
    }

    report_path = Path(output_dir) / "download_report.json"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    with report_path.open("w") as f:
        json.dump(report, f, indent=2)
    logger.info("Download report written to: %s", report_path)

    logger.info(
        "SUMMARY: %d attempted, %d succeeded, %d failed",
        len(pdb_ids),
        success_count,
        len(failed),
    )

    return report


# ── CTS entry point ─────────────────────────────────────────────────────


def run_download(config: PdbRsyncSettings) -> None:
    """Main CTS entry point for Phase 2 PDB download.

    :param config: validated download settings
    """
    report = download_batch(
        manifest_path=config.manifest,
        output_dir=config.output_dir,
        workers=config.workers,
        rsync_host=config.rsync_host,
        rsync_port=config.rsync_port,
        file_types=config.file_types if config.file_types else None,
        limit=config.limit,
    )
    if report["failed"] > 0:
        msg = f"PDB download completed with {report['failed']} failures"
        raise RuntimeError(msg)


def cli() -> None:
    """CLI entry point for ``pdb_rsync_sync``."""
    run_cli(PdbRsyncSettings, run_download)


# ── Upload helper ────────────────────────────────────────────────────────


def _upload_entry_dir(
    entry_dir: Path,
    tmp_root: Path,
    bucket: str,
    staging_key_prefix: str,
) -> int:
    """Upload all files under *entry_dir* to S3, deleting each file immediately after upload.

    Empty directories are removed after all files are uploaded.  If the
    directory does not exist (e.g. the entry had no files) the function
    returns zero without raising.

    :param entry_dir: local directory for one PDB entry
    :param tmp_root: root of the temp directory (used to compute relative S3 paths)
    :param bucket: destination S3 bucket
    :param staging_key_prefix: S3 key prefix within *bucket*
    :return: number of files uploaded
    """
    if not entry_dir.exists():
        return 0
    count = 0
    for f in sorted(entry_dir.rglob("*")):
        if f.is_file():
            relative = f.relative_to(tmp_root)
            dest_prefix = f"{bucket}/{staging_key_prefix.rstrip('/')}/{relative.parent}"
            if upload_file(f, dest_prefix, show_progress=False):
                count += 1
            else:
                logger.warning("Failed to upload %s to %s", f, dest_prefix)
            f.unlink()
    shutil.rmtree(entry_dir, ignore_errors=True)
    return count


# ── Notebook / interactive entry point ──────────────────────────────────


def download_and_stage(
    *,
    staging_bucket: str,
    staging_key_prefix: str,
    manifest_s3_key: str | None = None,
    manifest_local_path: str | Path | None = None,
    workers: int = 4,
    rsync_host: str = RSYNC_HOST,
    rsync_port: int = RSYNC_PORT,
    file_types: list[str] | None = None,
    limit: int | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Download PDB entries via rsync and stage them to S3 (Phase 2).

    Exactly one of *manifest_s3_key* or *manifest_local_path* must be given.

    Downloads and uploads are pipelined per entry: each worker downloads one
    entry, immediately uploads its files to S3, then deletes the local copies
    before picking up the next entry.  At most *workers* entry directories exist
    on disk simultaneously, preventing disk exhaustion on large batches.

    This function is intended for notebook / interactive use and as a CTS
    alternative when the standard rsync CTS job is unavailable.

    :param staging_bucket: destination S3 bucket name
    :param staging_key_prefix: key prefix inside the bucket (e.g. ``"staging/run1/"``)
    :param manifest_s3_key: S3 object key of the transfer manifest within *staging_bucket*
    :param manifest_local_path: local path to the transfer manifest file
    :param workers: number of parallel download-and-upload workers
    :param rsync_host: wwPDB Beta rsync hostname
    :param rsync_port: rsync port
    :param file_types: file-type subdirs to include, or None for all
    :param limit: optional limit for testing
    :param dry_run: when ``True``, download but skip all S3 uploads
    :return: download report extended with ``staged_objects``, ``staging_key_prefix``, ``dry_run``
    """
    if manifest_s3_key is not None and manifest_local_path is not None:
        msg = "Provide exactly one of manifest_s3_key or manifest_local_path, not both"
        raise ValueError(msg)
    if manifest_s3_key is None and manifest_local_path is None:
        msg = "One of manifest_s3_key or manifest_local_path must be provided"
        raise ValueError(msg)

    with tempfile.TemporaryDirectory() as _tmpdir:
        tmp = Path(_tmpdir)
        manifest_dest = tmp / "transfer_manifest.txt"

        if manifest_s3_key is not None:
            s3 = get_s3_client()
            response = s3.get_object(Bucket=staging_bucket, Key=manifest_s3_key)
            manifest_dest.write_bytes(response["Body"].read())
            logger.info("Manifest read from S3: s3://%s/%s", staging_bucket, manifest_s3_key)
        else:
            manifest_dest.write_bytes(Path(manifest_local_path).read_bytes())  # type: ignore[arg-type]
            logger.info("Manifest read from local path: %s", manifest_local_path)

        with manifest_dest.open() as f:
            pdb_ids = [line.strip().lower() for line in f if line.strip() and not line.startswith("#")]

        if limit:
            pdb_ids = pdb_ids[:limit]

        logger.info("Starting download & stage of %d PDB entries with %d workers", len(pdb_ids), workers)

        lock = threading.Lock()
        success_count = 0
        staged_objects = 0
        failed: list[dict[str, str]] = []
        all_stats: list[dict[str, Any]] = []

        def _download_upload_one(pdb_id: str) -> tuple[str, Exception | None]:
            nonlocal success_count, staged_objects
            try:
                stats = download_entry(pdb_id, tmp, rsync_host, rsync_port, file_types)
            except Exception as e:  # noqa: BLE001
                return pdb_id, e

            if not dry_run:
                entry_local_dir = tmp / build_entry_path(pdb_id)
                count = _upload_entry_dir(entry_local_dir, tmp, staging_bucket, staging_key_prefix)
                with lock:
                    staged_objects += count

            with lock:
                success_count += 1
                all_stats.append(stats)
            return pdb_id, None

        with tqdm.tqdm(total=len(pdb_ids), unit="entry", desc="Downloading & staging", smoothing=0.01) as pbar:
            with ThreadPoolExecutor(max_workers=workers) as executor:
                futures = {executor.submit(_download_upload_one, pid): pid for pid in pdb_ids}
                for future in as_completed(futures):
                    pid, error = future.result()
                    if error:
                        logger.error("FAILED: %s: %s", pid, error)
                        with lock:
                            failed.append({"pdb_id": pid, "error": str(error)})
                    pbar.update(1)

        report: dict[str, Any] = {
            "timestamp": datetime.now(UTC).isoformat(),
            "total_attempted": len(pdb_ids),
            "succeeded": success_count,
            "failed": len(failed),
            "failures": failed,
            "entry_stats": all_stats,
        }

        if not dry_run:
            report_path = tmp / "download_report.json"
            with report_path.open("w") as f:
                json.dump(report, f, indent=2)
            if upload_file(report_path, f"{staging_bucket}/{staging_key_prefix.rstrip('/')}", show_progress=False):
                staged_objects += 1
            else:
                logger.warning("Failed to upload download report to s3://%s/%s", staging_bucket, staging_key_prefix)
            logger.info("Staged %d objects to s3://%s/%s", staged_objects, staging_bucket, staging_key_prefix)

        logger.info(
            "SUMMARY: %d attempted, %d succeeded, %d failed",
            len(pdb_ids),
            success_count,
            len(failed),
        )

    return {
        **report,
        "staged_objects": staged_objects,
        "staging_key_prefix": staging_key_prefix,
        "dry_run": dry_run,
    }
