"""Orchestrate the SIFTS download and Lakehouse upload pipeline.

Typical usage (from a notebook)::

    from cdm_data_loaders.sifts.settings import SiftsSettings
    from cdm_data_loaders.sifts.run import run_sifts

    settings = SiftsSettings(lakehouse_bucket="cdm-lake")
    result = run_sifts(settings)
    print(result)

The pipeline:

1. Downloads ``pdb_chain_uniprot.tsv.gz`` from the EBI FTP server to a local
   temp directory.
2. Calls :func:`~cdm_data_loaders.utils.s3_versioned_upload.versioned_upload`
   to compare the file against whatever is currently in the Lakehouse.
   - Identical content → ``"unchanged"``, no S3 writes.
   - New or different content → old version archived by date, new version
     uploaded.
3. Returns a :class:`SiftsResult` dict summarising what happened.
"""

import tempfile
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from cdm_data_loaders.sifts.download import SIFTS_UNIPROT_FILE, download_sifts_file
from cdm_data_loaders.sifts.settings import (
    SIFTS_ARCHIVE_PREFIX,
    SIFTS_DERIVED_DATA_PREFIX,
    SiftsSettings,
)
from cdm_data_loaders.utils.cdm_logger import get_cdm_logger
from cdm_data_loaders.utils.s3_versioned_upload import UploadResult, versioned_upload

logger = get_cdm_logger()


@dataclass
class SiftsResult:
    """Result of a SIFTS pipeline run.

    :param filename: the SIFTS file that was processed
    :param upload_status: ``"new"``, ``"archived_and_replaced"``, or ``"unchanged"``
    :param dest_path: S3 destination path (``bucket/key``)
    :param archive_key: S3 key of the archived old version, or ``None``
    :param dry_run: True if this was a dry-run (no S3 writes)
    """

    filename: str
    upload_status: str
    dest_path: str
    archive_key: str | None
    dry_run: bool

    def to_dict(self) -> dict[str, Any]:  # noqa: D102
        return asdict(self)


def run_sifts(settings: SiftsSettings) -> SiftsResult:
    """Download the SIFTS UniProt mapping file and upload it to the Lakehouse.

    :param settings: pipeline configuration
    :return: :class:`SiftsResult` describing the outcome
    """
    filename = SIFTS_UNIPROT_FILE
    dest_key = f"{settings.lakehouse_key_prefix.strip('/')}/{SIFTS_DERIVED_DATA_PREFIX.strip('/')}/{filename}"
    dest_path = f"{settings.lakehouse_bucket}/{dest_key}"
    archive_base = (
        f"{settings.lakehouse_bucket}/{settings.lakehouse_key_prefix.strip('/')}/{SIFTS_ARCHIVE_PREFIX.strip('/')}"
    )
    sub_path = f"sifts/{filename}"

    logger.debug("SIFTS pipeline starting (dry_run=%s)", settings.dry_run)

    with tempfile.TemporaryDirectory() as tmp:
        tmp_dir = Path(tmp)

        if settings.dry_run:
            logger.debug(
                "[dry-run] would download %s and upload to s3://%s/%s", filename, settings.lakehouse_bucket, dest_key
            )
            return SiftsResult(
                filename=filename,
                upload_status="dry_run",
                dest_path=dest_path,
                archive_key=None,
                dry_run=True,
            )

        local_path = download_sifts_file(
            filename=filename,
            dest_dir=tmp_dir,
            ftp_host=settings.sifts_ftp_host,
        )
        result: UploadResult = versioned_upload(
            local_path=local_path,
            s3_dest_path=dest_path,
            archive_base_path=archive_base,
            sub_path=sub_path,
        )

    logger.debug(
        "SIFTS pipeline complete: %s -> %s (status=%s)",
        filename,
        dest_path,
        result.status,
    )
    return SiftsResult(
        filename=filename,
        upload_status=result.status,
        dest_path=result.dest_path,
        archive_key=result.archive_key,
        dry_run=False,
    )
