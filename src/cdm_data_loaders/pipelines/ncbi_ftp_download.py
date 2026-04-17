"""NCBI FTP assembly download pipeline (Phase 2).

Orchestrates parallel downloading of NCBI assemblies listed in a transfer
manifest.  Settings, batching, CLI entry point, and CTS integration live here;
domain-specific download logic is in :mod:`cdm_data_loaders.ncbi_ftp.assembly`.
"""

import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime
from ftplib import error_temp
from pathlib import Path
from typing import Any

from pydantic import AliasChoices, Field, field_validator
from pydantic_settings import CliSuppress

from cdm_data_loaders.ncbi_ftp.assembly import FTP_HOST, download_assembly_to_local
from cdm_data_loaders.pipelines.core import run_cli
from cdm_data_loaders.pipelines.cts_defaults import INPUT_MOUNT, OUTPUT_MOUNT, CtsDefaultSettings
from cdm_data_loaders.utils.cdm_logger import get_cdm_logger
from cdm_data_loaders.utils.ftp_client import ThreadLocalFTP

logger = get_cdm_logger()


# ── Settings ─────────────────────────────────────────────────────────────


class DownloadSettings(CtsDefaultSettings):
    """Configuration for the NCBI FTP assembly download pipeline."""

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
    limit: CliSuppress[int | None] = Field(
        default=None,
        ge=1,
        description="Limit to first N assemblies (for testing)",
        validation_alias=AliasChoices("l", "limit"),
    )

    @field_validator("threads")
    @classmethod
    def validate_threads(cls, v: int) -> int:
        """Validate threads is within range.

        :param v: number of threads
        :raises ValueError: if out of range
        :return: validated thread count
        """
        if v < 1 or v > 32:  # noqa: PLR2004
            msg = f"threads must be between 1 and 32, got {v}"
            raise ValueError(msg)
        return v


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
        last_error: Exception | None = None
        for attempt in range(1, 4):
            try:
                stats = download_assembly_to_local(path, output_dir, ftp_host=ftp_host, ftp=pool.get())
            except error_temp as e:
                last_error = e
                if attempt < 3:  # noqa: PLR2004
                    logger.warning("Transient FTP error for %s, retry %d/3: %s", path, attempt, e)
                    time.sleep(5)
            except Exception as e:  # noqa: BLE001
                return path, e
            else:
                with lock:
                    success_count += 1
                    all_stats.append(stats)
                return path, None
        return path, last_error

    try:
        with ThreadPoolExecutor(max_workers=threads) as executor:
            futures = {executor.submit(_download_one, p): p for p in assembly_paths}
            for future in as_completed(futures):
                path, error = future.result()
                if error:
                    logger.error("FAILED: %s: %s", path, error)
                    with lock:
                        failed.append({"path": path, "error": str(error)})
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
