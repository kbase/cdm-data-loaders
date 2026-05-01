"""Orchestrate the SIFTS download and Lakehouse upload pipeline.

Typical usage (from a notebook)::

    from cdm_data_loaders.sifts.settings import SiftsSettings
    from cdm_data_loaders.sifts.run import run_sifts

    settings = SiftsSettings(lakehouse_bucket="cdm-lake")
    result = run_sifts(settings)
    for filename, fr in result.file_results.items():
        print(filename, fr.upload_status)

The pipeline:

1. Downloads one or more SIFTS TSV files from the EBI FTP server (a single
   FTP connection is reused for all files).
2. For each file, calls
   :func:`~cdm_data_loaders.utils.s3_versioned_upload.versioned_upload`
   to compare against the current Lakehouse copy.
   - Identical content → ``"unchanged"``, no S3 writes.
   - New or different content → old version archived by date, new version
     uploaded.
3. Returns a :class:`SiftsResult` summarising the outcome for every file.

By default (``sifts_files=None``) **all** 16 files published by EBI are
downloaded.  Pass a list to restrict to a subset.
"""

import tempfile
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from cdm_data_loaders.sifts.download import ALL_SIFTS_FILES, download_sifts_files
from cdm_data_loaders.sifts.settings import (
    SIFTS_ARCHIVE_PREFIX,
    SIFTS_DERIVED_DATA_PREFIX,
    SiftsSettings,
)
from cdm_data_loaders.utils.cdm_logger import get_cdm_logger
from cdm_data_loaders.utils.s3_versioned_upload import UploadResult, versioned_upload

logger = get_cdm_logger()


@dataclass
class SiftsFileResult:
    """Result for a single SIFTS file.

    :param filename: the SIFTS filename that was processed
    :param upload_status: ``"new"``, ``"archived_and_replaced"``, ``"unchanged"``, or ``"dry_run"``
    :param dest_path: S3 destination path (``bucket/key``)
    :param archive_key: S3 key of the archived old version, or ``None``
    """

    filename: str
    upload_status: str
    dest_path: str
    archive_key: str | None

    def to_dict(self) -> dict[str, Any]:  # noqa: D102
        return asdict(self)


@dataclass
class SiftsResult:
    """Result of a SIFTS pipeline run.

    :param file_results: per-file outcomes keyed by filename
    :param dry_run: True if this was a dry-run (no S3 writes)
    """

    file_results: dict[str, SiftsFileResult]
    dry_run: bool

    def to_dict(self) -> dict[str, Any]:  # noqa: D102
        return {
            "file_results": {k: asdict(v) for k, v in self.file_results.items()},
            "dry_run": self.dry_run,
        }


def run_sifts(settings: SiftsSettings) -> SiftsResult:
    """Download SIFTS mapping files and upload them to the Lakehouse.

    Downloads the files listed in ``settings.sifts_files`` (or all
    :data:`~cdm_data_loaders.sifts.download.ALL_SIFTS_FILES` when
    ``sifts_files`` is ``None``) from the EBI FTP server, then performs a
    versioned upload for each one.

    :param settings: pipeline configuration
    :return: :class:`SiftsResult` describing the outcome for every file
    """
    filenames = settings.sifts_files if settings.sifts_files is not None else ALL_SIFTS_FILES
    archive_base = (
        f"{settings.lakehouse_bucket}/{settings.lakehouse_key_prefix.strip('/')}/{SIFTS_ARCHIVE_PREFIX.strip('/')}"
    )

    logger.debug("SIFTS pipeline starting: %d file(s), dry_run=%s", len(filenames), settings.dry_run)

    result = SiftsResult(file_results={}, dry_run=settings.dry_run)

    if settings.dry_run:
        for filename in filenames:
            dest_key = f"{settings.lakehouse_key_prefix.strip('/')}/{SIFTS_DERIVED_DATA_PREFIX.strip('/')}/{filename}"
            dest_path = f"{settings.lakehouse_bucket}/{dest_key}"
            logger.debug(
                "[dry-run] would download %s and upload to s3://%s/%s", filename, settings.lakehouse_bucket, dest_key
            )
            result.file_results[filename] = SiftsFileResult(
                filename=filename,
                upload_status="dry_run",
                dest_path=dest_path,
                archive_key=None,
            )
        return result

    with tempfile.TemporaryDirectory() as tmp:
        tmp_dir = Path(tmp)
        local_paths = download_sifts_files(
            filenames=filenames,
            dest_dir=tmp_dir,
            ftp_host=settings.sifts_ftp_host,
        )
        for local_path in local_paths:
            filename = local_path.name
            dest_key = f"{settings.lakehouse_key_prefix.strip('/')}/{SIFTS_DERIVED_DATA_PREFIX.strip('/')}/{filename}"
            dest_path = f"{settings.lakehouse_bucket}/{dest_key}"
            sub_path = f"sifts/{filename}"
            upload_result: UploadResult = versioned_upload(
                local_path=local_path,
                s3_dest_path=dest_path,
                archive_base_path=archive_base,
                sub_path=sub_path,
            )
            result.file_results[filename] = SiftsFileResult(
                filename=filename,
                upload_status=upload_result.status,
                dest_path=upload_result.dest_path,
                archive_key=upload_result.archive_key,
            )

    logger.debug("SIFTS pipeline complete: %d file(s) processed", len(result.file_results))
    return result
