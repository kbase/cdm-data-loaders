"""Shared constants and Moto seeding helpers for ``cdm_data_loaders.io.s3`` tests."""

from typing import Final

import pytest
from types_boto3_s3.client import S3Client

from cdm_data_loaders.io.s3.client import reset_s3_client
from tests.helpers.paths_and_constants import ALT_BUCKET, TEST_BUCKET

HTTP_200: Final[int] = 200
HTTP_204: Final[int] = 204

SIZE_HELLO: Final[int] = 5
SIZE_DATA: Final[int] = 4

SAMPLE_FILES: Final[list[str]] = [
    "dir_one/file1.txt",
    "dir_one/file2.txt",
    "dir_one/sub_dir/file3.txt",
    "dir_one/sub_dir/under_dir/file4.txt",
]

FILES_IN_BUCKETS: Final[dict[str, list[str]]] = {
    TEST_BUCKET: SAMPLE_FILES,
    ALT_BUCKET: ["dir_one/file1.txt"],
}

NO_PATH_FOUND: Final[str] = "No path found"
START_WITH_BUCKET_NAME: Final[str] = "s3 paths must start with the bucket name"
COULD_NOT_PARSE: Final[str] = "Could not parse out bucket and key"

TEST_PROTOCOLS: Final[list[str]] = [
    "",
    "s3://",
    "s3a://",
]


def populate_mock_s3(client: S3Client, file_list_by_bucket: dict[str, list[str]]) -> None:
    """Populate Moto S3 buckets with ``file_list_by_bucket``.

    Files under ``dir_one`` store ``"<bucket>/<key>"`` encoded as UTF-8; other keys store ``b"x"``.

    :param client: Active S3 client.
    :type client: S3Client
    :param file_list_by_bucket: Mapping from bucket name to object key list.
    :type file_list_by_bucket: dict[str, list[str]]
    """
    for bucket, file_list in file_list_by_bucket.items():
        for file in file_list:
            full_path = f"{bucket}/{file}"
            body = full_path.encode("utf-8") if file.startswith("dir_one") else b"x"
            client.put_object(Bucket=bucket, Key=file, Body=body)
            client.head_object(Bucket=bucket, Key=file)


def prep_client_init(monkeypatch: pytest.MonkeyPatch) -> None:
    """Reset the cached S3 client and seed baseline AWS environment variables.

    :param monkeypatch: Active pytest monkeypatch fixture.
    :type monkeypatch: pytest.MonkeyPatch
    """
    reset_s3_client()
    monkeypatch.setenv("AWS_ENDPOINT_URL", "http://env-endpoint.com")
    monkeypatch.delenv("AWS_ENDPOINT_URL_S3", raising=False)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "aws_access_key_id_env_var")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "aws_secret_access_key_env_var")
