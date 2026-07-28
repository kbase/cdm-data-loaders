import asyncio
import logging
from pathlib import Path

import boto3

from cdm_data_loaders.utils.cdm_logger import get_cdm_logger
from cdm_data_loaders.utils.download.async_client import AsyncFileDownloader
from cdm_data_loaders.utils.download.s3_uploader import S3StreamUploader
from cdm_data_loaders.utils.download.sync_client import FileDownloader
from cdm_data_loaders.utils.link_scraper import (
    DirEntry,
    crawl_directory,
    crawl_directory_async,
    entry_destination,
    entry_s3_key,
)

GTDB_BASE_URL = "https://data.gtdb.ecogenomic.org/releases/"
S3_PREFIX = "s3://cdm-lake/tenant-general-warehouse/refdata/datasets/gtdb/raw_data/"
EXCLUDE_DIRS = {"gtdbtk_package"}

# https://data.gtdb.ecogenomic.org/releases/release214/214.0/
# https://data.gtdb.ecogenomic.org/releases/release214/214.1/
# https://data.gtdb.ecogenomic.org/releases/release220/220.0/
# https://data.gtdb.ecogenomic.org/releases/release226/226.0/
# https://data.gtdb.ecogenomic.org/releases/release232/232.0/


logger = get_cdm_logger(__name__)
logger.setLevel(logging.INFO)


def download_to_disk(output_dir: Path) -> None:
    """Crawl the release tree and download every file to local disk, verifying checksums.

    :param output_dir: local base directory to save files into
    :type output_dir: Path
    """
    files = crawl_directory(GTDB_BASE_URL, exclude_dirs=EXCLUDE_DIRS, prefer_compressed=True)
    downloader = FileDownloader(max_attempts=3, chunk_size=8192)

    for entry in files:
        dest = entry_destination(entry, GTDB_BASE_URL, output_dir)
        downloader.download(
            entry.url,
            dest,
            expected_checksum=entry.checksum.value if entry.checksum else None,
            checksum_fn=entry.checksum.algorithm if entry.checksum else None,
        )


async def download_to_disk_async(output_dir: Path) -> None:
    """Crawl the release tree and download every file to local disk concurrently.

    :param output_dir: local base directory to save files into
    :type output_dir: Path
    """
    files = await crawl_directory_async(
        GTDB_BASE_URL,
        exclude_dirs=EXCLUDE_DIRS,
        prefer_compressed=True,
        max_concurrency=8,
    )
    downloader = AsyncFileDownloader(max_attempts=3, chunk_size=8192, max_concurrency=8)

    await asyncio.gather(
        *(
            downloader.download(
                entry.url,
                entry_destination(entry, GTDB_BASE_URL, output_dir),
                expected_checksum=entry.checksum.value if entry.checksum else None,
                checksum_fn=entry.checksum.algorithm if entry.checksum else None,
            )
            for entry in files
        )
    )


def upload_to_s3(s3_prefix: str) -> None:
    """Crawl the release tree and stream every file directly into S3, verifying checksums.

    :param s3_prefix: base S3 location to save into, e.g. "s3://my-bucket/gtdb/latest"
    :type s3_prefix: str
    """
    files = crawl_directory(GTDB_BASE_URL, exclude_dirs=EXCLUDE_DIRS, prefer_compressed=True)

    s3_client = boto3.client("s3")
    uploader = S3StreamUploader(s3_client, max_attempts=3)

    for entry in files:
        s3_path = entry_s3_key(entry, GTDB_BASE_URL, s3_prefix)
        uploader.upload(
            entry.url,
            s3_path,
            expected_checksum=entry.checksum.value if entry.checksum else None,
            checksum_fn=entry.checksum.algorithm if entry.checksum else None,
        )


def route_download(
    entry: DirEntry,
    start_url: str,
    *,
    output_dir: Path | None = None,
    s3_prefix: str | None = None,
    file_downloader: FileDownloader | None = None,
    s3_uploader: S3StreamUploader | None = None,
) -> str:
    """Download a single entry to local disk or upload it directly to S3.

    Exactly one of `output_dir` / `s3_prefix` should be supplied, along with
    the matching downloader/uploader instance.

    :param entry: the file entry to transfer
    :type entry: DirEntry
    :param start_url: the root URL that was passed to crawl_directory(_async)
    :type start_url: str
    :param output_dir: local base directory to save into, defaults to None
    :type output_dir: Path | None, optional
    :param s3_prefix: base S3 location to save into, defaults to None
    :type s3_prefix: str | None, optional
    :param file_downloader: downloader to use when `output_dir` is set, defaults to None
    :type file_downloader: FileDownloader | None, optional
    :param s3_uploader: uploader to use when `s3_prefix` is set, defaults to None
    :type s3_uploader: S3StreamUploader | None, optional
    :raises ValueError: if neither or both of `output_dir`/`s3_prefix` are supplied,
        or if the matching downloader/uploader is missing
    :return: the destination the file was written to (local path or "bucket/key")
    :rtype: str
    """
    checksum = entry.checksum

    if s3_prefix is not None:
        if s3_uploader is None:
            msg = "s3_uploader is required when s3_prefix is set"
            raise ValueError(msg)
        s3_path = entry_s3_key(entry, start_url, s3_prefix)
        return s3_uploader.upload(
            entry.url,
            s3_path,
            expected_checksum=checksum.value if checksum else None,
            checksum_fn=checksum.algorithm if checksum else None,
        )

    if output_dir is not None:
        if file_downloader is None:
            msg = "file_downloader is required when output_dir is set"
            raise ValueError(msg)
        dest = entry_destination(entry, start_url, output_dir)
        result = file_downloader.download(
            entry.url,
            dest,
            expected_checksum=checksum.value if checksum else None,
            checksum_fn=checksum.algorithm if checksum else None,
        )
        return str(result)

    msg = "Either output_dir or s3_prefix must be supplied"
    raise ValueError(msg)


async def main() -> None:

    for release in ["232.0", "226.0", "220.0", "214.1", "214.0"]:
        release_dir = int(release)
        start_url: str = f"{GTDB_BASE_URL}releases{release_dir}/{release}/"
        files: list[DirEntry] = await crawl_directory_async(
            start_url,
            exclude_dirs=EXCLUDE_DIRS,
            checksum_manifest="MD5SUM.txt",
            checksum_algorithm="md5",
            prefer_compressed=True,
            max_concurrency=8,
        )

        s3_client = boto3.client("s3")
        uploader = S3StreamUploader(s3_client, max_attempts=3)
        semaphore = asyncio.Semaphore(8)

        async def _upload(entry: DirEntry):
            s3_path = entry_s3_key(entry, start_url, f"{S3_PREFIX}{release}")
            logger.info("Copying %s to %s", entry.url, s3_path)
            async with semaphore:
                await asyncio.to_thread(
                    uploader.upload,
                    entry.url,
                    s3_path,
                    expected_checksum=entry.checksum.value if entry.checksum else None,
                    checksum_fn=entry.checksum.algorithm if entry.checksum else None,
                )

        await asyncio.gather(*(_upload(entry) for entry in files))


asyncio.run(main())
