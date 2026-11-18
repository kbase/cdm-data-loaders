"""Parallel batch download and S3 staging for NCBI FTP assemblies."""

import json
import logging
import tempfile
import threading
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime
from logging import Logger, getLogger
from pathlib import Path, PurePosixPath
from typing import Any, TypedDict, Unpack

import tqdm
from tenacity import before_sleep_log, retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from cdm_data_loaders.datasets.ncbi_ftp.assembly import (
    build_accession_path,
    download_assembly_to_local,
    parse_assembly_path,
)
from cdm_data_loaders.datasets.ncbi_ftp.constants import FTP_HOST
from cdm_data_loaders.datasets.ncbi_ftp.manifest import read_manifest_lines
from cdm_data_loaders.io.ftp_client import TRANSIENT_FTP_EXCEPTIONS, ThreadLocalFTP
from cdm_data_loaders.io.s3.object_utils import get_object_bytes, upload_dir_files, upload_file

logger: Logger = getLogger(__name__)


class DownloadBatchOptions(TypedDict, total=False):
    """Optional parameters for :func:`download_batch`."""

    threads: int
    ftp_host: str
    limit: int | None
    on_assembly_downloaded: Callable[[PurePosixPath], None] | None
    progress_desc: str


class DownloadStageOptions(TypedDict, total=False):
    """Optional parameters for :func:`download_and_stage`."""

    manifest_s3_key: PurePosixPath | None
    manifest_local_path: Path | None
    threads: int
    ftp_host: str
    limit: int | None
    dry_run: bool


def _upload_assembly_dir(
    assembly_dir: Path,
    tmp_root: Path,
    bucket: PurePosixPath,
    staging_key_prefix: PurePosixPath,
) -> int:
    """Upload all files under ``assembly_dir`` to S3 and delete local copies on completion.

    :param assembly_dir: local directory for one assembly
    :type assembly_dir: Path
    :param tmp_root: temporary root directory used to compute relative S3 paths
    :type tmp_root: Path
    :param bucket: destination S3 bucket
    :type bucket: PurePosixPath
    :param staging_key_prefix: S3 key prefix within ``bucket``
    :type staging_key_prefix: PurePosixPath
    :return: number of files uploaded
    :rtype: int
    """
    if not assembly_dir.exists():
        return 0
    dest_dir = str(bucket / staging_key_prefix / assembly_dir.relative_to(tmp_root))
    uploaded_count, _ = upload_dir_files(
        assembly_dir,
        dest_dir,
        show_progress=False,
        delete_source=True,
        upload_fn=upload_file,
    )
    return uploaded_count


def download_batch(
    manifest_path: Path,
    output_dir: Path,
    **options: Unpack[DownloadBatchOptions],
) -> dict[str, Any]:
    """Download all assemblies listed in ``manifest_path`` to ``output_dir``.

    :param manifest_path: path to the transfer manifest file
    :type manifest_path: Path
    :param output_dir: base local output directory
    :type output_dir: Path
    :param options: optional ``threads``, ``ftp_host``, ``limit``, ``on_assembly_downloaded``, and ``progress_desc``
    :type options: Unpack[DownloadBatchOptions]
    :return: report dictionary with attempt, success, and failure counts
    :rtype: dict[str, Any]
    """
    threads = options.get("threads", 4)
    ftp_host = options.get("ftp_host", FTP_HOST)
    limit = options.get("limit")
    on_assembly_downloaded = options.get("on_assembly_downloaded")
    progress_desc = options.get("progress_desc", "Downloading from NCBI FTP")

    assembly_paths = [PurePosixPath(line) for line in read_manifest_lines(manifest_path)]

    if limit:
        assembly_paths = assembly_paths[:limit]

    logger.info("Starting download of %d assemblies with %d threads", len(assembly_paths), threads)

    pool = ThreadLocalFTP(ftp_host)
    lock = threading.Lock()
    success_count = 0
    failed: list[dict[str, str]] = []
    all_stats: list[dict[str, Any]] = []

    def _download_one(path: PurePosixPath) -> tuple[str, Exception | None]:
        nonlocal success_count

        @retry(
            retry=retry_if_exception_type(TRANSIENT_FTP_EXCEPTIONS),
            stop=stop_after_attempt(5),
            wait=wait_exponential(multiplier=1, min=5, max=60),
            reraise=True,
            before_sleep=before_sleep_log(logger, logging.WARNING),
        )
        def _attempt() -> dict[str, Any]:
            return download_assembly_to_local(path, output_dir, ftp_host=ftp_host, ftp=pool.get())

        try:
            stats = _attempt()
        except Exception as exc:
            logger.exception("Download failed for %s", path)
            return str(path), exc

        if on_assembly_downloaded is not None:
            on_assembly_downloaded(path)

        with lock:
            success_count += 1
            all_stats.append(stats)
        return str(path), None

    try:
        with (
            tqdm.tqdm(total=len(assembly_paths), unit="assembly", desc=progress_desc, smoothing=0.01) as pbar,
            ThreadPoolExecutor(max_workers=threads) as executor,
        ):
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

    report_path = output_dir / "download_report.json"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    with report_path.open("w", encoding="utf-8") as fh:
        json.dump(report, fh, indent=2)
    logger.info("Download report written to: %s", report_path)
    logger.info(
        "SUMMARY: %d attempted, %d succeeded, %d failed",
        len(assembly_paths),
        success_count,
        len(failed),
    )

    return report


def download_and_stage(
    *,
    bucket: PurePosixPath,
    staging_key_prefix: PurePosixPath,
    **options: Unpack[DownloadStageOptions],
) -> dict[str, Any]:
    """Download assemblies from NCBI FTP and stage them to S3.

    :param bucket: destination S3 bucket name
    :type bucket: PurePosixPath
    :param staging_key_prefix: key prefix inside ``bucket``
    :type staging_key_prefix: PurePosixPath
    :param options: manifest source (``manifest_s3_key`` or ``manifest_local_path``) and execution options
    :type options: Unpack[DownloadStageOptions]
    :return: download report extended with ``staged_objects``, ``staging_key_prefix``, and ``dry_run``
    :rtype: dict[str, Any]
    :raises ValueError: if both or neither manifest sources are provided
    """
    manifest_s3_key = options.get("manifest_s3_key")
    manifest_local_path = options.get("manifest_local_path")
    threads = options.get("threads", 4)
    ftp_host = options.get("ftp_host", FTP_HOST)
    limit = options.get("limit")
    dry_run = options.get("dry_run", False)

    if manifest_s3_key is not None and manifest_local_path is not None:
        msg = "Provide one of manifest_s3_key or manifest_local_path, not both"
        raise ValueError(msg)
    if manifest_s3_key is None and manifest_local_path is None:
        msg = "One of manifest_s3_key or manifest_local_path must be provided"
        raise ValueError(msg)

    with tempfile.TemporaryDirectory() as tmp_dir_name:
        tmp = Path(tmp_dir_name)
        manifest_dest = tmp / "transfer_manifest.txt"

        if manifest_s3_key is not None:
            manifest_dest.write_bytes(get_object_bytes(str(bucket / manifest_s3_key)))
            logger.info("Manifest read from S3: s3://%s/%s", bucket, manifest_s3_key)
        elif manifest_local_path:
            manifest_dest.write_bytes(manifest_local_path.read_bytes())
            logger.info("Manifest read from local path: %s", manifest_local_path)

        staged_objects = 0
        stage_lock = threading.Lock()

        def _stage_assembly(path: PurePosixPath) -> None:
            nonlocal staged_objects
            _db, assembly_dir_name, _accession = parse_assembly_path(path)
            assembly_local_dir = tmp / build_accession_path(assembly_dir_name)
            count = _upload_assembly_dir(assembly_local_dir, tmp, bucket, staging_key_prefix)
            with stage_lock:
                staged_objects += count

        report = download_batch(
            manifest_path=manifest_dest,
            output_dir=tmp,
            threads=threads,
            ftp_host=ftp_host,
            limit=limit,
            on_assembly_downloaded=None if dry_run else _stage_assembly,
            progress_desc="Downloading & staging",
        )

        if not dry_run:
            report_path = tmp / "download_report.json"
            if upload_file(report_path, str(bucket / staging_key_prefix), show_progress=False):
                staged_objects += 1
            else:
                logger.warning("Failed to upload download report to s3://%s/%s", bucket, staging_key_prefix)
            logger.info("Staged %d objects to s3://%s/%s", staged_objects, bucket, staging_key_prefix)

    return {
        **report,
        "staged_objects": staged_objects,
        "staging_key_prefix": staging_key_prefix,
        "dry_run": dry_run,
    }
