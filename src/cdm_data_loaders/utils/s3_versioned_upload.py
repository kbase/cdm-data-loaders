"""Versioned S3 upload utility.

Uploads a local file to a fixed S3 destination key, archiving the previous
version by date before overwriting — but only when the content has actually
changed (compared by MD5).

Typical usage::

    result = versioned_upload(
        local_path=Path("/tmp/pdb_entries.ndjson"),
        s3_dest_path="cdm-lake/tenant-general-warehouse/kbase/datasets/pdb/derived_data/rcsb/pdb_entries.ndjson",
        archive_base_path="cdm-lake/tenant-general-warehouse/kbase/datasets/pdb/derived_data/archive",
        sub_path="rcsb/pdb_entries.ndjson",
    )
    # result.status is "new", "archived_and_replaced", or "unchanged"
"""

import hashlib
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Literal

from cdm_data_loaders.utils.cdm_logger import get_cdm_logger
from cdm_data_loaders.utils.s3 import copy_object, head_object, split_s3_path, upload_file

logger = get_cdm_logger()

UploadStatus = Literal["new", "archived_and_replaced", "unchanged"]


@dataclass
class UploadResult:
    """Result of a versioned upload operation.

    :param status: one of ``"new"``, ``"archived_and_replaced"``, or ``"unchanged"``
    :param archive_key: the S3 key the old version was copied to, or ``None``
    :param dest_path: the final S3 destination path (``bucket/key`` form)
    """

    status: UploadStatus
    archive_key: str | None
    dest_path: str


def _md5_of_file(path: Path) -> str:
    """Return the hex MD5 digest of a local file.

    :param path: path to the file
    :return: hex MD5 string
    """
    h = hashlib.md5()  # noqa: S324
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def _etag_matches_md5(etag: str | None, local_md5: str) -> bool:
    """Return True if *etag* matches *local_md5*.

    S3 ETags for non-multipart uploads are quoted MD5 hex strings, e.g.
    ``'"d41d8cd98f00b204e9800998ecf8427e"'``.  Strip surrounding quotes
    before comparing.

    :param etag: S3 ETag string (may be None or quoted)
    :param local_md5: hex MD5 of the local file
    :return: True if content is unchanged
    """
    if not etag:
        return False
    return etag.strip('"') == local_md5


def versioned_upload(
    local_path: Path,
    s3_dest_path: str,
    archive_base_path: str,
    sub_path: str,
    today: date | None = None,
) -> UploadResult:
    """Upload *local_path* to *s3_dest_path*, archiving the previous version when content has changed.

    Steps:
    1. Compute MD5 of *local_path*.
    2. ``head_object`` the existing S3 object.
    3. If the ETag matches the local MD5, return ``"unchanged"`` — no upload.
    4. If an existing object was found (and content differs), copy it to
       ``archive_base_path/{today}/{sub_path}`` before overwriting.
    5. Upload the new file to *s3_dest_path*.

    :param local_path: local file to upload
    :param s3_dest_path: destination S3 path including bucket, e.g.
        ``"cdm-lake/tenant-…/pdb_entries.ndjson"``
    :param archive_base_path: S3 prefix for archive copies, e.g.
        ``"cdm-lake/tenant-…/derived_data/archive"``
    :param sub_path: relative path appended to the dated archive prefix, e.g.
        ``"rcsb/pdb_entries.ndjson"``
    :param today: override the archive date (defaults to today in UTC); useful
        for testing
    :return: :class:`UploadResult` describing what happened
    """
    if today is None:
        today = datetime.now(UTC).date()

    local_md5 = _md5_of_file(local_path)
    existing = head_object(s3_dest_path)

    if existing is not None:
        # S3 head_object doesn't expose ETag directly; fetch it via raw boto3 call
        from cdm_data_loaders.utils.s3 import get_s3_client  # noqa: PLC0415

        s3 = get_s3_client()
        bucket, key = split_s3_path(s3_dest_path)
        resp = s3.head_object(Bucket=bucket, Key=key)
        etag = resp.get("ETag", "")
        if _etag_matches_md5(etag, local_md5):
            logger.info("No change detected for %s — skipping upload", s3_dest_path)
            return UploadResult(status="unchanged", archive_key=None, dest_path=s3_dest_path)

        # Content differs — archive old version
        date_str = today.strftime("%Y-%m-%d")
        archive_path = f"{archive_base_path.rstrip('/')}/{date_str}/{sub_path.lstrip('/')}"
        logger.info("Archiving previous version: %s -> %s", s3_dest_path, archive_path)
        _ = copy_object(s3_dest_path, archive_path)

        # Upload new version — pass tags={} to force overwrite (upload_file skips if object exists with tags=None)
        dest_bucket, dest_key = split_s3_path(s3_dest_path)
        dest_dir = f"{dest_bucket}/{'/'.join(dest_key.split('/')[:-1])}"
        dest_name = dest_key.split("/")[-1]
        upload_file(local_path, dest_dir, object_name=dest_name, tags={}, show_progress=False)
        logger.info("Replaced %s with new version", s3_dest_path)
        return UploadResult(status="archived_and_replaced", archive_key=archive_path, dest_path=s3_dest_path)

    # No existing object — first upload
    dest_bucket, dest_key = split_s3_path(s3_dest_path)
    dest_dir = f"{dest_bucket}/{'/'.join(dest_key.split('/')[:-1])}"
    dest_name = dest_key.split("/")[-1]
    upload_file(local_path, dest_dir, object_name=dest_name, show_progress=False)
    logger.info("Uploaded new object: %s", s3_dest_path)
    return UploadResult(status="new", archive_key=None, dest_path=s3_dest_path)
