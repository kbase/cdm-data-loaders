"""NCBI FTP assembly download pipeline (Phase 2).

Orchestrates parallel downloading of NCBI assemblies listed in a transfer
manifest.  Settings, batching, CLI entry point, and CTS integration live here;
domain-specific download logic is in :mod:`cdm_data_loaders.ncbi_ftp.assembly`.
"""

import json
import logging
import tempfile
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime
from ftplib import error_temp
from pathlib import Path
from typing import Any

import tqdm
from pydantic import AliasChoices, Field
from tenacity import before_sleep_log, retry, retry_if_exception_type, stop_after_attempt, wait_fixed
from pydantic_settings import BaseSettings, SettingsConfigDict

from cdm_data_loaders.ncbi_ftp.assembly import FTP_HOST, download_assembly_to_local
from cdm_data_loaders.pipelines.core import run_cli
from cdm_data_loaders.pipelines.cts_defaults import DEFAULT_SETTINGS_CONFIG_DICT, INPUT_MOUNT, OUTPUT_MOUNT
from cdm_data_loaders.utils.cdm_logger import get_cdm_logger
from cdm_data_loaders.utils.ftp_client import ThreadLocalFTP
from cdm_data_loaders.utils.s3 import get_s3_client, upload_file

logger = get_cdm_logger()


# ── Constants ────────────────────────────────────────────────────────────

DEFAULT_STAGING_KEY_PREFIX = "staging/"


class DownloadSettings(BaseSettings):
    """Configuration for the NCBI FTP assembly download pipeline."""

    model_config = SettingsConfigDict(**DEFAULT_SETTINGS_CONFIG_DICT)

    manifest: str = Field(
        default=f"{INPUT_MOUNT}/transfer_manifest.txt",
        description="Path to the transfer manifest file listing FTP paths to download",
        validation_alias=AliasChoices("m", "manifest"),
    )
    output_dir: str = Field(
        default=OUTPUT_MOUNT,
        description="Output directory for downloaded assembly files",
        validation_alias=AliasChoices("output-dir", "output_dir"),
    )
    threads: int = Field(
        default=4,
        ge=1,
        le=32,
        description="Number of parallel download threads",
        validation_alias=AliasChoices("t", "threads"),
    )
    ftp_host: str = Field(
        default=FTP_HOST,
        description="NCBI FTP hostname",
        validation_alias=AliasChoices("ftp-host", "ftp_host"),
    )
    limit: int | None = Field(
        default=None,
        ge=1,
        description="Limit to first N assemblies (for testing)",
        validation_alias=AliasChoices("l", "limit"),
    )


# ── Batch download ───────────────────────────────────────────────────────


def download_batch(
    manifest_path: str | Path,
    output_dir: str | Path,
    threads: int = 4,
    ftp_host: str = FTP_HOST,
    limit: int | None = None,
) -> dict[str, Any]:
    """Download all assemblies listed in the manifest.

    :param manifest_path: path to the transfer manifest file
    :param output_dir: base output directory
    :param threads: number of parallel download threads
    :param ftp_host: FTP hostname
    :param limit: optional limit for testing
    :return: report dict with overall stats
    """
    with Path(manifest_path).open() as f:
        assembly_paths = [line.strip() for line in f if line.strip() and not line.startswith("#")]

    if limit:
        assembly_paths = assembly_paths[:limit]

    logger.info("Starting download of %d assemblies with %d threads", len(assembly_paths), threads)

    pool = ThreadLocalFTP(ftp_host)
    lock = threading.Lock()
    success_count = 0
    failed: list[dict[str, str]] = []
    all_stats: list[dict[str, Any]] = []

    def _download_one(path: str) -> tuple[str, Exception | None]:
        nonlocal success_count

        @retry(
            retry=retry_if_exception_type(error_temp),
            stop=stop_after_attempt(3),
            wait=wait_fixed(5),
            reraise=True,
            before_sleep=before_sleep_log(logger, logging.WARNING),
        )
        def _attempt() -> dict[str, Any]:
            return download_assembly_to_local(path, output_dir, ftp_host=ftp_host, ftp=pool.get())

        try:
            stats = _attempt()
        except Exception as e:  # noqa: BLE001
            return path, e
        else:
            with lock:
                success_count += 1
                all_stats.append(stats)
            return path, None

    try:
        with tqdm.tqdm(total=len(assembly_paths), unit="assembly", desc="Downloading from NCBI FTP") as pbar:
            with ThreadPoolExecutor(max_workers=threads) as executor:
                futures = {executor.submit(_download_one, p): p for p in assembly_paths}
                for future in as_completed(futures):
                    path, error = future.result()
                    if error:
                        logger.error("FAILED: %s: %s", path, error)
                        with lock:
                            failed.append({"path": path, "error": str(error)})
                    pbar.update(1)
    finally:
        pool.close_all()

    report: dict[str, Any] = {
        "timestamp": datetime.now(UTC).isoformat(),
        "total_attempted": len(assembly_paths),
        "succeeded": success_count,
        "failed": len(failed),
        "failures": failed,
        "assembly_stats": all_stats,
    }

    report_path = Path(output_dir) / "download_report.json"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    with report_path.open("w") as f:
        json.dump(report, f, indent=2)
    logger.info("Download report written to: %s", report_path)

    logger.info(
        "SUMMARY: %d attempted, %d succeeded, %d failed",
        len(assembly_paths),
        success_count,
        len(failed),
    )

    return report


# ── CTS entry point ─────────────────────────────────────────────────────


def run_download(config: DownloadSettings) -> None:
    """Main CTS entry point for Phase 2 download.

    :param config: validated download settings
    """
    report = download_batch(
        manifest_path=config.manifest,
        output_dir=config.output_dir,
        threads=config.threads,
        ftp_host=config.ftp_host,
        limit=config.limit,
    )
    if report["failed"] > 0:
        msg = f"Download completed with {report['failed']} failures"
        raise RuntimeError(msg)


def cli() -> None:
    """CLI entry point for ``ncbi_ftp_sync``."""
    run_cli(DownloadSettings, run_download)


# ── Notebook / interactive entry point ──────────────────────────────────


def download_and_stage(
    *,
    bucket: str,
    staging_key_prefix: str,
    manifest_s3_key: str | None = None,
    manifest_local_path: str | Path | None = None,
    threads: int = 4,
    ftp_host: str = FTP_HOST,
    limit: int | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Download assemblies from NCBI FTP and stage them to S3 (Phase 2).

    Exactly one of *manifest_s3_key* or *manifest_local_path* must be given.

    :param bucket: destination S3 bucket name
    :param staging_key_prefix: key prefix inside the bucket (e.g. ``"staging/run1/"``)
    :param manifest_s3_key: S3 object key of the transfer manifest within *bucket*
    :param manifest_local_path: local path to the transfer manifest file
    :param threads: number of parallel download **and** upload threads
    :param ftp_host: NCBI FTP hostname
    :param limit: optional limit for testing (pass to :func:`download_batch`)
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
            response = s3.get_object(Bucket=bucket, Key=manifest_s3_key)
            manifest_dest.write_bytes(response["Body"].read())
            logger.info("Manifest read from S3: s3://%s/%s", bucket, manifest_s3_key)
        else:
            manifest_dest.write_bytes(Path(manifest_local_path).read_bytes())
            logger.info("Manifest read from local path: %s", manifest_local_path)

        report = download_batch(
            manifest_path=manifest_dest,
            output_dir=tmp,
            threads=threads,
            ftp_host=ftp_host,
            limit=limit,
        )

        staged_objects = 0

        if not dry_run:
            raw_data_dir = tmp / "raw_data"
            report_json = tmp / "download_report.json"

            upload_tasks: list[tuple[Path, str]] = []

            if raw_data_dir.exists():
                for local_file in sorted(raw_data_dir.rglob("*")):
                    if local_file.is_file():
                        relative = local_file.relative_to(tmp)
                        dest_prefix = f"{bucket}/{staging_key_prefix.rstrip('/')}/{relative.parent}"
                        upload_tasks.append((local_file, dest_prefix))

            if report_json.exists():
                upload_tasks.append((report_json, f"{bucket}/{staging_key_prefix.rstrip('/')}"))

            def _upload(task: tuple[Path, str]) -> None:
                local_file, dest = task
                upload_file(local_file, dest, show_progress=False)

            with tqdm.tqdm(total=len(upload_tasks), unit="file", desc="Staging to S3") as pbar:
                with ThreadPoolExecutor(max_workers=threads) as executor:
                    futures = [executor.submit(_upload, t) for t in upload_tasks]
                    for future in as_completed(futures):
                        future.result()
                        staged_objects += 1
                        pbar.update(1)

            logger.info("Staged %d objects to s3://%s/%s", staged_objects, bucket, staging_key_prefix)

    return {
        **report,
        "staged_objects": staged_objects,
        "staging_key_prefix": staging_key_prefix,
        "dry_run": dry_run,
    }
