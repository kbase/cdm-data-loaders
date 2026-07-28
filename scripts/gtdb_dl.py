import asyncio

import boto3

from cdm_data_loaders.utils.download.s3_uploader import S3StreamUploader
from cdm_data_loaders.utils.link_scraper import DirEntry, crawl_directory_async, entry_s3_key

GTDB_BASE_URL = "https://data.gtdb.ecogenomic.org/releases/"
S3_PREFIX = "s3://cdm-lake/tenant-general-warehouse/refdata/gtdb/raw_data/"
EXCLUDE_DIRS = {"gtdbtk_package"}

# https://data.gtdb.ecogenomic.org/releases/release214/214.0/
# https://data.gtdb.ecogenomic.org/releases/release214/214.1/
# https://data.gtdb.ecogenomic.org/releases/release220/220.0/
# https://data.gtdb.ecogenomic.org/releases/release226/226.0/
# https://data.gtdb.ecogenomic.org/releases/release232/232.0/


async def main() -> None:

    for release in ["232.0", "226.0", "220.0", "214.1", "214.0"]:
        release_dir = int(release)
        start_url: str = f"{GTDB_BASE_URL}releases{release_dir}/{release}/"
        files: list[DirEntry] = await crawl_directory_async(
            start_url,
            exclude_dirs=EXCLUDE_DIRS,
            prefer_compressed=True,
            max_concurrency=8,
        )

        s3_client = boto3.client("s3")
        uploader = S3StreamUploader(s3_client, max_attempts=3)
        semaphore = asyncio.Semaphore(8)

        async def _upload(entry: DirEntry):
            s3_path = entry_s3_key(entry, start_url, S3_PREFIX)
            async with semaphore:
                await asyncio.to_thread(uploader.upload, entry.url, s3_path)

        await asyncio.gather(*(_upload(entry) for entry in files))


asyncio.run(main())
