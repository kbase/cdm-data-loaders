"""Tests for s3_utils.py using moto to mock AWS S3."""

import io
import json
import logging
from collections.abc import Generator
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import boto3
import pytest
from botocore.exceptions import ClientError, PartialCredentialsError
from moto import mock_aws
from requests.exceptions import ConnectionError as ConnError
from requests.exceptions import HTTPError

import cdm_data_loaders.utils.s3 as s3_utils
from cdm_data_loaders.utils.s3 import (
    CDM_LAKE_BUCKET,
    DEFAULT_EXTRA_ARGS,
    cmd_cp,
    cmd_head,
    cmd_ls,
    cmd_mb,
    copy_directory,
    copy_object,
    delete_object,
    delete_objects,
    download_file,
    get_s3_client,
    head_object,
    list_matching_objects,
    object_exists,
    reset_s3_client,
    split_s3_path,
    stream_to_s3,
    upload_dir,
    upload_file,
)
from tests.s3_helpers import strip_checksum_algorithm

HTTP_200 = 200
HTTP_204 = 204

SAMPLE_FILES = [
    "dir_one/file1.txt",
    "dir_one/file2.txt",
    "dir_one/sub_dir/file3.txt",
    "dir_one/sub_dir/under_dir/file4.txt",
]

ALT_BUCKET = "cts"  # second valid bucket from VALID_BUCKETS

FILES_IN_BUCKETS = {
    CDM_LAKE_BUCKET: SAMPLE_FILES,
    ALT_BUCKET: ["dir_one/file1.txt"],
}
BUCKETS = [CDM_LAKE_BUCKET, ALT_BUCKET]

HTTP_STATUS_OK = 200
HTTP_STATUS_NO_CONTENT = 204
SIZE_HELLO = 5
SIZE_DATA = 4


@pytest.fixture
def mock_s3_client(monkeypatch: pytest.MonkeyPatch) -> Generator[Any, Any]:
    """Yield a mocked S3 client with both valid buckets created.

    The function get_s3_client() is patched to ensure that all module functions use this client.

    Resets the cached client before and after to prevent state leaking between tests.
    """
    # Remove any real endpoint/credential env vars so moto intercepts all HTTP calls.
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.delenv("AWS_ENDPOINT_URL", raising=False)
    monkeypatch.delenv("AWS_ENDPOINT_URL_S3", raising=False)
    boto3.DEFAULT_SESSION = None

    with mock_aws():
        reset_s3_client()
        client = boto3.client("s3")
        for bucket in FILES_IN_BUCKETS:
            client.create_bucket(Bucket=bucket)

        # delete any existing client
        reset_s3_client()
        assert s3_utils._s3_client is None  # noqa: SLF001

        # patch in the client that we have just created
        with patch.object(s3_utils, "get_s3_client", return_value=client):
            yield client

        reset_s3_client()
        assert s3_utils._s3_client is None  # noqa: SLF001


@pytest.fixture
def sample_file(tmp_path: Path) -> Path:
    """Create a small temporary file for upload tests."""
    f = tmp_path / "sample.txt"
    f.write_text("hello s3")
    return f


@pytest.fixture
def sample_dir(tmp_path: Path) -> Path:
    """Create a small temporary directory tree for upload_dir tests.

    Structure (same as CDM_LAKE_BUCKET files)

    dir_one/file1.txt
    dir_one/file2.txt
    dir_one/sub_dir/file3.txt
    dir_one/sub_dir/under_dir/file4.txt
    """
    sample_dir = tmp_path / "sample_dir"
    for f in SAMPLE_FILES:
        new_file = sample_dir / f
        # ensure the parent dir exists
        new_file.parent.mkdir(parents=True, exist_ok=True)
        # add the (relative) path as the content
        new_file.write_text(f)
    return sample_dir


def populate_mock_s3(client: Any, file_list_by_bucket: dict[str, list[str]]) -> None:
    """Populate buckets with a list of files.

    File names should be a list, indexed by bucket.

    Files will be populated with the file name as bytes if the top level directory is `dir_one`;
    otherwise, the content will just be `x`.

    :param client: s3 client
    :type client: Any
    :param file_list: list of files, indexed by bucket
    :type file_list: dict[str, list[str]]
    """
    for bucket, file_list in file_list_by_bucket.items():
        for file in file_list:
            full_path = f"{bucket}/{file}"
            if file.startswith("dir_one"):
                client.put_object(Bucket=bucket, Key=file, Body=full_path.encode("utf-8"))
            else:
                client.put_object(Bucket=bucket, Key=file, Body=b"x")
            # if this errors, the transfer was not successful
            client.head_object(Bucket=bucket, Key=file)


def prep_client_init(monkeypatch: pytest.MonkeyPatch) -> None:
    """Set up environment variables to allow get_s3_client to initialize without error."""
    reset_s3_client()
    assert s3_utils._s3_client is None  # noqa: SLF001
    # set up env vars to ensure that the argument takes precedence
    monkeypatch.setenv("AWS_ENDPOINT_URL", "http://env-endpoint.com")
    monkeypatch.delenv("AWS_ENDPOINT_URL_S3", raising=False)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "aws_access_key_id_env_var")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "aws_secret_access_key_env_var")


# Client creation / reset
@mock_aws
@pytest.mark.s3
@pytest.mark.parametrize("endpoint_url", ["http://localhost", None])
def test_get_s3_client_success_via_args(endpoint_url: str | None, monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify that get_s3_client creates a client with the correct credentials and endpoint URL using args for the creds."""
    prep_client_init(monkeypatch)

    args = {
        "aws_access_key_id": "aws_access_key_id_argument",
        "aws_secret_access_key": "aws_secret_access_key_argument",
        "endpoint_url": endpoint_url,
    }

    client = get_s3_client(args)
    assert client is not None
    credentials = client._request_signer._credentials  # noqa: SLF001
    assert credentials.access_key == "aws_access_key_id_argument"
    assert credentials.secret_key == "aws_secret_access_key_argument"  # noqa: S105
    expected_endpoint = endpoint_url or "http://env-endpoint.com"
    assert client.meta.endpoint_url == expected_endpoint

    reset_s3_client()
    assert s3_utils._s3_client is None  # noqa: SLF001


@mock_aws
@pytest.mark.s3
@pytest.mark.parametrize("endpoint_url", ["http://localhost", None])
def test_get_s3_client_success_via_env(endpoint_url: str | None, monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify that get_s3_client creates a client with the correct credentials and endpoint URL using env vars for the creds."""
    prep_client_init(monkeypatch)

    args = {
        "endpoint_url": endpoint_url,
    }

    client = get_s3_client(args)
    assert client is not None
    credentials = client._request_signer._credentials  # noqa: SLF001
    assert credentials.access_key == "aws_access_key_id_env_var"
    assert credentials.secret_key == "aws_secret_access_key_env_var"  # noqa: S105
    expected_endpoint = endpoint_url or "http://env-endpoint.com"
    assert client.meta.endpoint_url == expected_endpoint

    reset_s3_client()
    assert s3_utils._s3_client is None  # noqa: SLF001


@mock_aws
@pytest.mark.s3
@pytest.mark.parametrize(
    ("aws_access_key_id", "aws_secret_access_key"), [("aws_access_key_id", None), (None, "aws_secret_access_key")]
)
def test_get_s3_client_incomplete_creds_via_args(
    aws_access_key_id: str | None, aws_secret_access_key: str | None, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify that get_s3_client raises ValueError when only one of aws_access_key_id or aws_secret_access_key is provided via arguments."""
    prep_client_init(monkeypatch)
    reset_s3_client()
    assert s3_utils._s3_client is None  # noqa: SLF001
    args = {
        "aws_access_key_id": aws_access_key_id,
        "aws_secret_access_key": aws_secret_access_key,
    }
    with pytest.raises(
        ValueError,
        match="Cannot initialise s3 client: aws_access_key_id and aws_secret_access_key must be provided together",
    ):
        get_s3_client(args)
    assert s3_utils._s3_client is None  # noqa: SLF001

    reset_s3_client()
    assert s3_utils._s3_client is None  # noqa: SLF001


@mock_aws
@pytest.mark.s3
@pytest.mark.parametrize(
    ("aws_access_key_id", "aws_secret_access_key", "error_type", "error_msg"),
    [
        (
            "aws_access_key_id",
            None,
            PartialCredentialsError,
            "Partial credentials found in env, missing: AWS_SECRET_ACCESS_KEY",
        ),
        (None, "aws_secret_access_key", ValueError, "missing configuration values: aws_access_key_id"),
        (None, None, ValueError, "missing configuration values: aws_access_key_id, aws_secret_access_key"),
    ],
)
def test_get_s3_client_incomplete_creds_via_env(
    aws_access_key_id: str | None,
    aws_secret_access_key: str | None,
    error_type: type[Exception],
    error_msg: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify that get_s3_client raises ValueError when only one of aws_access_key_id or aws_secret_access_key is provided."""
    prep_client_init(monkeypatch)
    reset_s3_client()
    assert s3_utils._s3_client is None  # noqa: SLF001
    args = {
        "aws_access_key_id": aws_access_key_id,
        "aws_secret_access_key": aws_secret_access_key,
    }
    for key, value in args.items():
        if value:
            monkeypatch.setenv(key.upper(), value)
        else:
            monkeypatch.delenv(key.upper(), raising=False)
    # ensure that the AWS config file cannot accidentally be used to provide creds
    monkeypatch.setenv("AWS_CONFIG_FILE", "/dev/null")

    with pytest.raises(
        error_type,
        match=error_msg,
    ):
        get_s3_client()
    assert s3_utils._s3_client is None  # noqa: SLF001

    reset_s3_client()
    assert s3_utils._s3_client is None  # noqa: SLF001


@mock_aws
@pytest.mark.s3
def test_get_s3_client_returns_same_instance() -> None:
    """Verify that repeated calls to get_s3_client return the exact same cached client instance."""
    reset_s3_client()
    assert s3_utils._s3_client is None  # noqa: SLF001

    args = {
        "endpoint_url": "http://localhost:9000",
        "aws_access_key_id": "key",
        "aws_secret_access_key": "secret",
    }
    client_a = get_s3_client(args)  # type: ignore[reportArgumentType]
    assert s3_utils._s3_client is not None  # noqa: SLF001
    # call again with no args - should return the stored version
    client_b = get_s3_client()
    assert client_a is client_b
    # call again with invalid args - should return the stored version, ignoring args
    client_c = get_s3_client(args={"this": "that", "pip": "pop"})
    assert client_c == client_a
    # reset the client and call
    reset_s3_client()
    assert s3_utils._s3_client is None  # noqa: SLF001
    client_d = get_s3_client(
        {
            "endpoint_url": "http://localhost:9000",
            "aws_access_key_id": "not a key",
            "aws_secret_access_key": "not a secret",
        }
    )
    assert client_d != client_a

    reset_s3_client()
    assert s3_utils._s3_client is None  # noqa: SLF001


# split_s3_path
PATH = "path"
TO = "to"
TO_FILE = "to/file.txt"
PATH_TO_FILE = f"{PATH}/{TO_FILE}"

EXPECTED = {
    "path/to": (PATH, TO),
    "path/to/": (PATH, "to/"),
    "path/to/file.txt": (PATH, TO_FILE),
    "s3://path/to": (PATH, TO),
    "s3://path/to/": (PATH, "to/"),
    "s3://path/to/file.txt": (PATH, TO_FILE),
    "s3a://path/to": (PATH, TO),
    "s3a://path/to/": (PATH, "to/"),
    "s3a://path/to/file.txt": (PATH, TO_FILE),
}

NO_PATH_FOUND = "No path found"
START_WITH_BUCKET_NAME = "s3 paths must start with the bucket name"
COULD_NOT_PARSE = "Could not parse out bucket and key"

INVALID_PATH_ERRORS = {
    "": NO_PATH_FOUND,
    "/": START_WITH_BUCKET_NAME,
    "/path": START_WITH_BUCKET_NAME,
    "/path/to/file.txt": START_WITH_BUCKET_NAME,
    "path": COULD_NOT_PARSE,
    "path/": COULD_NOT_PARSE,
    "s3://": NO_PATH_FOUND,
    "s3:///": START_WITH_BUCKET_NAME,
    "s3://path": COULD_NOT_PARSE,
    "s3://path/": COULD_NOT_PARSE,
    "s3a://": NO_PATH_FOUND,
    "s3a://path": COULD_NOT_PARSE,
    "s3a://path/": COULD_NOT_PARSE,
}


@pytest.mark.parametrize("invalid_path", list(INVALID_PATH_ERRORS.keys()))
def test_split_s3_path_errors(invalid_path: str) -> None:
    """Ensure that an error is thrown if an invalid s3 path is passed in."""
    with pytest.raises(ValueError, match=INVALID_PATH_ERRORS[invalid_path]):
        split_s3_path(invalid_path)


@pytest.mark.parametrize("valid_path", list(EXPECTED.keys()))
def test_split_s3_path_success(valid_path: str) -> None:
    """Verify that a valid path is correctly split into bucket and key."""
    (bucket, path) = split_s3_path(valid_path)
    assert (bucket, path) == EXPECTED[valid_path]


EXPECTED_ALLOW_BUCKET_ONLY = {
    "path/to": (PATH, TO),
    "path/to/": (PATH, "to/"),
    "path/to/file.txt": (PATH, TO_FILE),
    "path": (PATH, ""),
    "path/": (PATH, ""),
    "s3://path/to": (PATH, TO),
    "s3://path/to/": (PATH, "to/"),
    "s3://path/to/file.txt": (PATH, TO_FILE),
    "s3://path": (PATH, ""),
    "s3://path/": (PATH, ""),
    "s3a://path/to": (PATH, TO),
    "s3a://path/to/": (PATH, "to/"),
    "s3a://path/to/file.txt": (PATH, TO_FILE),
    "s3a://path": (PATH, ""),
    "s3a://path/": (PATH, ""),
}

INVALID_PATH_ALLOW_BUCKET_ONLY_ERRORS = {
    "": NO_PATH_FOUND,
    "/": START_WITH_BUCKET_NAME,
    "/path": START_WITH_BUCKET_NAME,
    "/path/to/file.txt": START_WITH_BUCKET_NAME,
    "s3://": NO_PATH_FOUND,
    "s3:///": START_WITH_BUCKET_NAME,
    "s3a://": NO_PATH_FOUND,
}


@pytest.mark.parametrize("invalid_path", list(INVALID_PATH_ALLOW_BUCKET_ONLY_ERRORS.keys()))
def test_split_s3_path_allow_bucket_only_errors(invalid_path: str) -> None:
    """Ensure that an error is thrown if an invalid s3 path is passed in."""
    with pytest.raises(ValueError, match=INVALID_PATH_ALLOW_BUCKET_ONLY_ERRORS[invalid_path]):
        split_s3_path(invalid_path, allow_bucket_only=True)


@pytest.mark.parametrize("valid_path", list(EXPECTED_ALLOW_BUCKET_ONLY.keys()))
def test_split_s3_path_allow_bucket_only_success(valid_path: str) -> None:
    """Verify that a valid path is correctly split into bucket and key."""
    (bucket, path) = split_s3_path(valid_path, allow_bucket_only=True)
    assert (bucket, path) == EXPECTED_ALLOW_BUCKET_ONLY[valid_path]


# list_matching_objects
@pytest.mark.parametrize("bucket", BUCKETS)
@pytest.mark.parametrize("protocol", ["", "s3://", "s3a://"])
@pytest.mark.s3
def test_list_matching_objects_lists_objects(
    mock_s3_client: Any,
    bucket: str,
    protocol: str,
) -> None:
    """Verify that all objects under a given prefix are returned, regardless of whether the protocol is supplied."""
    populate_mock_s3(mock_s3_client, FILES_IN_BUCKETS)
    contents = list_matching_objects(f"{protocol}{bucket}/dir_one")
    keys = {obj["Key"] for obj in contents}
    assert keys == {f for f in FILES_IN_BUCKETS[bucket] if f.startswith("dir_one")}


@pytest.mark.parametrize("dir_path", ["dir_one/sub_dir", "dir_one/sub_dir/", "dir_one/sub_dir/und"])
@pytest.mark.s3
def test_list_matching_objects_filters_by_prefix(
    mock_s3_client: Any,
    dir_path: str,
) -> None:
    """Check that more specific queries, including those that have 'incomplete' dir/file names, return correct results."""
    bucket = CDM_LAKE_BUCKET
    populate_mock_s3(mock_s3_client, {bucket: FILES_IN_BUCKETS[bucket]})
    contents = list_matching_objects(f"{bucket}/{dir_path}")
    keys = {obj["Key"] for obj in contents}
    # make sure this is a subset of all the files in the bucket
    assert len(keys) < len(FILES_IN_BUCKETS[bucket])
    assert keys == {f for f in FILES_IN_BUCKETS[bucket] if f.startswith(dir_path)}


@pytest.mark.parametrize("protocol", ["", "s3://", "s3a://"])
@pytest.mark.s3
def test_list_matching_objects_empty_for_missing_prefix(
    mock_s3_client: Any,
    protocol: str,
) -> None:
    """Verify that an empty list is returned when no objects match the given prefix."""
    populate_mock_s3(mock_s3_client, FILES_IN_BUCKETS)
    for bucket in FILES_IN_BUCKETS:
        contents = list_matching_objects(f"{protocol}{bucket}/nonexistent/")
        assert contents == []


N_FILES = 1005
DIR_TWO_FILES = [f"dir_two/file_{i:04d}.txt" for i in range(N_FILES)]
DIRTY_DATA = [f"dirty_data/file_{i:04d}.txt" for i in range(N_FILES)]
# pagination tests (1005 objects each, to exceed the 1000-item S3 page limit)
LOTS_OF_FILES = {
    CDM_LAKE_BUCKET: [
        *DIR_TWO_FILES,
        *DIRTY_DATA,
    ]
}

EXPECTED_FILE_LIST = {
    "di": [*FILES_IN_BUCKETS[CDM_LAKE_BUCKET], *LOTS_OF_FILES[CDM_LAKE_BUCKET]],
    "dir": [*FILES_IN_BUCKETS[CDM_LAKE_BUCKET], *LOTS_OF_FILES[CDM_LAKE_BUCKET]],
    "dir_": [*FILES_IN_BUCKETS[CDM_LAKE_BUCKET], *DIR_TWO_FILES],
    "dirty_data": DIRTY_DATA,
}


# NOTE: These tests currently compose multiple fixtures explicitly for readability.
@pytest.mark.parametrize("dir_path", EXPECTED_FILE_LIST.keys())
@pytest.mark.s3
def test_list_matching_objects_returns_more_than_1000_entries(
    mock_s3_client: Any,
    dir_path: str,
) -> None:
    """Verify that pagination is followed so that more than 1000 objects are returned."""
    populate_mock_s3(mock_s3_client, FILES_IN_BUCKETS)
    # this adds two extra dirs to CDM_LAKE_BUCKET with 1005 files in each
    populate_mock_s3(mock_s3_client, LOTS_OF_FILES)

    contents = list_matching_objects(f"{CDM_LAKE_BUCKET}/{dir_path}")
    keys = {obj["Key"] for obj in contents}
    assert keys == set(EXPECTED_FILE_LIST[dir_path])


# object_exists
@pytest.mark.parametrize("protocol", ["", "s3://", "s3a://"])
@pytest.mark.s3
def test_head_object_and_object_exists_true_and_false(mock_s3_client: Any, protocol: str) -> None:
    """Verify that object_exists returns True for an object that exists in the bucket."""
    populate_mock_s3(mock_s3_client, FILES_IN_BUCKETS)
    for bucket, file_list in FILES_IN_BUCKETS.items():
        for f in file_list:
            output = head_object(f"{protocol}{bucket}/{f}")
            assert output is not None
            assert isinstance(output["ContentLength"], int)
            assert object_exists(f"{protocol}{bucket}/{f}") is True

        nonexistent_file = f"{protocol}{bucket}/a-file-i-just-made-up.txt"
        assert object_exists(nonexistent_file) is False
        with pytest.raises(ClientError, match="404"):
            _ = head_object(nonexistent_file)


@pytest.mark.parametrize("s3_path", ["absent", "dir_one", "dir_one/", "dir_one/file1.tnt"])
@pytest.mark.parametrize("bucket", BUCKETS)
@pytest.mark.parametrize("protocol", ["", "s3://", "s3a://"])
@pytest.mark.s3
def test_object_exists_returns_false_when_absent(mock_s3_client: Any, s3_path: str, protocol: str, bucket: str) -> None:
    """Verify that object_exists returns False for an object that does not exist."""
    populate_mock_s3(mock_s3_client, FILES_IN_BUCKETS)
    assert object_exists(f"{protocol}{bucket}/{s3_path}") is False


# upload_file
@pytest.mark.parametrize("destination_dir", ["uploads", "uploads/", "some/uploads"])
@pytest.mark.parametrize("bucket", BUCKETS)
@pytest.mark.parametrize("protocol", ["", "s3://", "s3a://"])
@pytest.mark.s3
def test_upload_file_succeeds(
    mock_s3_client: Any, sample_file: Path, protocol: str, bucket: str, destination_dir: str
) -> None:
    """Verify that a file is uploaded to the correct key in the specified bucket."""
    result = upload_file(sample_file, f"{protocol}{bucket}/{destination_dir}")
    assert result is True
    obj = mock_s3_client.get_object(Bucket=bucket, Key=f"{destination_dir.removesuffix('/')}/{sample_file.name}")
    assert obj["Body"].read() == b"hello s3"


@pytest.mark.s3
def test_upload_file_uses_custom_object_name(mock_s3_client: Any, sample_file: Path) -> None:
    """Verify that the object_name argument overrides the source filename as the S3 key."""
    result = upload_file(sample_file, f"{CDM_LAKE_BUCKET}/uploads", object_name="custom.txt")
    assert result is True
    obj = mock_s3_client.get_object(Bucket=CDM_LAKE_BUCKET, Key="uploads/custom.txt")
    assert obj["Body"].read() == b"hello s3"


@pytest.mark.s3
def test_upload_file_skips_when_already_present(mock_s3_client: Any, sample_file: Path) -> None:
    """Verify that uploading a file that already exists is skipped, returns True, and leaves the object unchanged."""
    mock_s3_client.put_object(Bucket=CDM_LAKE_BUCKET, Key=f"uploads/{sample_file.name}", Body=b"old")
    result = upload_file(sample_file, f"{CDM_LAKE_BUCKET}/uploads")
    assert result is True
    # The existing object must not have been overwritten
    obj = mock_s3_client.get_object(Bucket=CDM_LAKE_BUCKET, Key=f"uploads/{sample_file.name}")
    assert obj["Body"].read() == b"old"


@pytest.mark.usefixtures("mock_s3_client")
@pytest.mark.parametrize("path_type", [str, Path])
@pytest.mark.s3
def test_upload_file_accepts_str_and_path(sample_file: Path, path_type: type[str] | type[Path]) -> None:
    """Verify that upload_file accepts both str and Path objects for the local file path."""
    result = upload_file(path_type(sample_file), f"{CDM_LAKE_BUCKET}/uploads")
    assert result is True


@pytest.mark.usefixtures("mock_s3_client")
@pytest.mark.s3
def test_upload_file_error(sample_file: Path) -> None:
    """Verify that upload_file raises ValueError when no destination directory is provided."""
    with pytest.raises(ValueError, match="No destination directory"):
        upload_file(sample_file, "")


# TODO: Missing tests
# - Upload failure (S3 error) - returns False


def make_mock_requests(
    content: bytes = b"hello world",
    status_code: int = 200,
    content_type: str = "application/octet-stream",
) -> tuple[MagicMock, MagicMock]:
    """Build a mock requests module whose .get() returns a mock response."""
    mock_response = MagicMock()
    mock_response.status_code = status_code
    mock_response.raw = io.BytesIO(content)
    mock_response.raw.decode_content = True
    mock_response.headers = {
        "content-type": content_type,
    }
    mock_response.raise_for_status = MagicMock()
    mock_response.__enter__ = lambda s: s
    mock_response.__exit__ = MagicMock(return_value=False)

    mock_requests = MagicMock()
    mock_requests.get.return_value = mock_response

    return mock_requests, mock_response


UPLOAD_TEST_KEY = "uploads/test-file.pdf"
UPLOAD_BUCKET_KEY = f"{ALT_BUCKET}/{UPLOAD_TEST_KEY}"
TEST_URL = "https://example.com/test-file.pdf"


def test_stream_to_s3_happy_path(mock_s3_client: Any) -> None:
    """File content from the HTTP response is stored correctly in S3."""
    content = b"hello world"
    mock_requests, _ = make_mock_requests(content=content)

    saved_path = stream_to_s3(TEST_URL, UPLOAD_BUCKET_KEY, mock_requests)

    mock_requests.get.assert_called_once_with(TEST_URL, stream=True)

    # s3 path including bucket returned
    assert saved_path == UPLOAD_BUCKET_KEY

    result = mock_s3_client.get_object(Bucket=ALT_BUCKET, Key=UPLOAD_TEST_KEY)
    # check the content is correct
    assert result["Body"].read() == content

    # new file shows up in list_objects
    objects = mock_s3_client.list_objects_v2(Bucket=ALT_BUCKET)["Contents"]
    keys = [obj["Key"] for obj in objects]
    assert UPLOAD_TEST_KEY in keys


@pytest.mark.parametrize("content_type", [None, "application/json", "application/pdf", "text"])
def test_stream_to_s3_sets_content_type_from_response_headers(mock_s3_client: Any, content_type: str | None) -> None:
    """ContentType metadata on the S3 object matches the HTTP response header."""
    content_type_args = {}
    if content_type:
        content_type_args["content_type"] = content_type
    mock_requests, _ = make_mock_requests(**content_type_args)

    stream_to_s3(TEST_URL, UPLOAD_BUCKET_KEY, mock_requests)

    head = mock_s3_client.head_object(Bucket=ALT_BUCKET, Key=UPLOAD_TEST_KEY)
    assert head["ContentType"] == content_type or "application/octet-stream"


def test_stream_to_s3_raises_on_http_error_status(mock_s3_client: Any) -> None:
    """An HTTP error status causes raise_for_status() to propagate an exception."""
    mock_requests, mock_response = make_mock_requests(status_code=404)
    mock_response.raise_for_status.side_effect = HTTPError("404 Not Found")

    with (
        pytest.raises(HTTPError, match="404 Not Found"),
    ):
        stream_to_s3(TEST_URL, UPLOAD_BUCKET_KEY, mock_requests)

    with pytest.raises(ClientError, match="Not Found"):
        mock_s3_client.head_object(Bucket=ALT_BUCKET, Key=UPLOAD_TEST_KEY)


def test_stream_to_s3_raises_on_connection_error(mock_s3_client: Any) -> None:
    """A network-level failure raises a ConnectionError."""
    mock_requests, _ = make_mock_requests(status_code=404)
    mock_requests.get.side_effect = ConnError("Network unreachable")

    with pytest.raises(ConnError, match="Network unreachable"):
        stream_to_s3(TEST_URL, UPLOAD_BUCKET_KEY, mock_requests)

    with pytest.raises(ClientError, match="Not Found"):
        mock_s3_client.head_object(Bucket=ALT_BUCKET, Key=UPLOAD_TEST_KEY)


# FIXME: don't upload if there is nothing there?
def test_stream_to_s3_uploads_empty_file(mock_s3_client: Any) -> None:
    """An empty HTTP response body results in an empty S3 object."""
    mock_requests, _ = make_mock_requests(content=b"")

    stream_to_s3(TEST_URL, UPLOAD_BUCKET_KEY, mock_requests)

    result = mock_s3_client.get_object(Bucket=ALT_BUCKET, Key=UPLOAD_TEST_KEY)
    assert result["Body"].read() == b""


def test_stream_to_s3_uploads_large_file(mock_s3_client: Any) -> None:
    """A large payload (>5MB) is uploaded correctly via multipart."""
    content = b"x" * (6 * 1024 * 1024)  # 6 MB
    mock_requests, _ = make_mock_requests(content=content)

    stream_to_s3(TEST_URL, UPLOAD_BUCKET_KEY, mock_requests)

    result = mock_s3_client.get_object(Bucket=ALT_BUCKET, Key=UPLOAD_TEST_KEY)
    assert result["Body"].read() == content


@pytest.mark.skip("TODO: add test(s)")
def test_stream_to_s3_accepts_custom_requests_implementation() -> None:
    """A subclassed or alternate requests module works as a drop-in."""
    # TODO: add test here?


@pytest.mark.parametrize("bucket", BUCKETS)
@pytest.mark.parametrize("protocol", ["", "s3://", "s3a://"])
@pytest.mark.s3
def test_download_file_retrieves_correct_content(
    mock_s3_client: Any, protocol: str, bucket: str, tmp_path: Path
) -> None:
    """Verify that download_file writes the correct file content to disk for each valid bucket."""
    content = b"some important content"
    mock_s3_client.put_object(Bucket=bucket, Key="remote/data.txt", Body=content, **DEFAULT_EXTRA_ARGS)
    local_file = str(tmp_path / "data.txt")
    download_file(f"{protocol}{bucket}/remote/data.txt", local_file)
    assert Path(local_file).read_bytes() == content


@pytest.mark.parametrize("path_type", [str, Path])
@pytest.mark.s3
def test_download_file_use_str_or_path_for_local_file(
    mock_s3_client: Any, tmp_path: Path, path_type: type[str] | type[Path]
) -> None:
    """Verify that download_file can create a new directory if need be."""
    content = b"some cool file stuff"
    bucket = BUCKETS[0]
    key = "to/the/door.txt"
    mock_s3_client.put_object(Bucket=bucket, Key=key, Body=content, **DEFAULT_EXTRA_ARGS)
    assert object_exists(f"{bucket}/{key}")
    local_file = tmp_path / "file.txt"
    assert not local_file.exists()
    download_file(f"{bucket}/{key}", path_type(local_file))
    assert local_file.read_bytes() == content


@pytest.mark.s3
def test_download_file_save_to_new_dir(mock_s3_client: Any, tmp_path: Path) -> None:
    """Verify that download_file can create a new directory if need be."""
    content = b"some cool file stuff"
    bucket = BUCKETS[0]
    key = "to/the/door.txt"
    mock_s3_client.put_object(Bucket=bucket, Key=key, Body=content, **DEFAULT_EXTRA_ARGS)
    assert object_exists(f"{bucket}/{key}")
    local_file = tmp_path / "some" / "convoluted" / "path" / "to" / "file.txt"
    assert not local_file.exists()
    assert not local_file.parents[2].exists()
    download_file(f"{bucket}/{key}", local_file)
    assert local_file.read_bytes() == content


@pytest.mark.s3
def test_download_file_clobbers_existing_file(mock_s3_client: Any, tmp_path: Path) -> None:
    """Verify that download_file can create a new directory if need be."""
    bucket = BUCKETS[0]
    key = "to/the/door.txt"
    local_file_content = b"some old crap"
    remote_file_content = b"some remote crap"
    local_file = tmp_path / "file.txt"
    local_file.write_bytes(local_file_content)

    mock_s3_client.put_object(Bucket=bucket, Key=key, Body=remote_file_content, **DEFAULT_EXTRA_ARGS)
    assert object_exists(f"{bucket}/{key}")

    assert local_file.exists()
    assert local_file.read_bytes() == local_file_content

    download_file(f"{bucket}/{key}", local_file)
    assert local_file.read_bytes() == remote_file_content


@pytest.mark.s3
def test_download_file_does_not_clobber_existing_file_to_mkdir(mock_s3_client: Any, tmp_path: Path) -> None:
    """Verify that download_file will not overwrite an existing file whilst trying to make a directory."""
    bucket = BUCKETS[0]
    key = "to/the/door.txt"
    mock_s3_client.put_object(Bucket=bucket, Key=key, Body=b"some crappy nonsense", **DEFAULT_EXTRA_ARGS)
    assert object_exists(f"{bucket}/{key}")
    local_file = tmp_path / "to"
    local_file.touch()

    with pytest.raises(FileExistsError, match=f"File exists: '{local_file!s}'"):
        download_file(f"{bucket}/{key}", local_file / "file.txt")


@pytest.mark.s3
@pytest.mark.usefixtures("mock_s3_client")
def test_download_file_does_not_exist(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    """Ensure that attempting to download a file that does not exist raises an error."""
    bucket = BUCKETS[0]
    key = "to/the/door.txt"
    assert not object_exists(f"{bucket}/{key}")

    with pytest.raises(
        ClientError,
        match=r"An error occurred \(404\) when calling the HeadObject",
    ):
        download_file(f"{bucket}/{key}", tmp_path / "file.txt")

    last_log_message = caplog.records[-1]
    assert "File not found" in last_log_message.message
    assert last_log_message.levelno == logging.ERROR


# TODO: Missing tests
# - Non-404 S3 error during head
# - Error during directory creation (other than FileExistsError)?
# - version_id parameter behavior


# upload_dir
@pytest.mark.parametrize("bucket", [CDM_LAKE_BUCKET, ALT_BUCKET])
@pytest.mark.s3
def test_upload_dir_uploads_recursively(mock_s3_client: Any, bucket: str, sample_dir: Path) -> None:
    """Verify that upload_dir recurses into subdirectories and uploads nested files."""
    result = upload_dir(sample_dir, f"{bucket}/remote")
    assert result is True
    keys = {obj["Key"] for obj in mock_s3_client.list_objects_v2(Bucket=bucket)["Contents"]}
    assert keys == {f"remote/{f}" for f in SAMPLE_FILES}


@pytest.mark.parametrize("path_type", [str, Path])
@pytest.mark.s3
def test_upload_dir_accepts_str_and_path(
    mock_s3_client: Any, sample_dir: Path, path_type: type[str] | type[Path]
) -> None:
    """Verify that upload_dir accepts both str and Path objects for the local directory path."""
    bucket = CDM_LAKE_BUCKET
    result = upload_dir(path_type(sample_dir), f"{bucket}/remote")
    assert result is True
    keys = {obj["Key"] for obj in mock_s3_client.list_objects_v2(Bucket=bucket)["Contents"]}
    assert keys == {f"remote/{f}" for f in SAMPLE_FILES}


@pytest.mark.usefixtures("mock_s3_client")
@pytest.mark.s3
def test_upload_dir_raises_on_empty_source() -> None:
    """Verify that upload_dir raises ValueError when no source directory is provided."""
    with pytest.raises(ValueError, match="No source directory"):
        upload_dir("", f"{CDM_LAKE_BUCKET}/remote")


@pytest.mark.usefixtures("mock_s3_client")
@pytest.mark.s3
def test_upload_dir_raises_on_empty_destination(sample_dir: Path) -> None:
    """Verify that upload_dir raises ValueError when no destination directory is provided."""
    with pytest.raises(ValueError, match="No destination directory"):
        upload_dir(sample_dir, "")


@pytest.fixture
def mocked_s3_client_no_checksum(mock_s3_client: Any) -> Any:
    """Yield the mocked S3 client with copy_object patched to strip ChecksumAlgorithm.

    This works around the moto limitation of not supporting CRC64NVME checksums,
    allowing copy_object calls that include ChecksumAlgorithm to succeed.
    """
    mock_s3_client.copy_object = strip_checksum_algorithm(mock_s3_client.copy_object)
    return mock_s3_client


# copy_object
@pytest.mark.parametrize("destination", BUCKETS)
@pytest.mark.s3
def test_copy_object(mocked_s3_client_no_checksum: Any, destination: str) -> None:
    """Verify that copy_object copies an object to a new key within the same bucket."""
    mocked_s3_client_no_checksum.put_object(Bucket=CDM_LAKE_BUCKET, Key="src/file.txt", Body=b"copy me")
    assert object_exists(f"{CDM_LAKE_BUCKET}/src/file.txt")
    response = copy_object(f"{CDM_LAKE_BUCKET}/src/file.txt", f"{destination}/dst/path/to/file.txt")

    # check both objects exist
    assert object_exists(f"{CDM_LAKE_BUCKET}/src/file.txt")
    assert object_exists(f"{destination}/dst/path/to/file.txt")

    obj = mocked_s3_client_no_checksum.get_object(Bucket=destination, Key="dst/path/to/file.txt")
    assert obj["Body"].read() == b"copy me"
    assert response["ResponseMetadata"]["HTTPStatusCode"] == HTTP_200


@pytest.mark.s3
@pytest.mark.usefixtures("mock_s3_client")
def test_copy_object_source_object_nonexistent() -> None:
    """Ensure that the code throws an error if the source object does not exist."""
    s3_path = f"{CDM_LAKE_BUCKET}/some/path/to/file"
    assert object_exists(s3_path) is False
    with pytest.raises(Exception, match="The specified key does not exist"):
        copy_object(s3_path, f"{CDM_LAKE_BUCKET}/a/different/path/to/file")


@pytest.mark.s3
@pytest.mark.usefixtures("mock_s3_client")
def test_copy_object_source_bucket_nonexistent() -> None:
    """Ensure that the code throws an error if the bucket does not exist."""
    s3_path = "some-bucket/some/path/to/file"
    assert object_exists(s3_path) is False
    with pytest.raises(Exception, match="The specified bucket does not exist"):
        copy_object(s3_path, f"{CDM_LAKE_BUCKET}/a/different/path/to/file")


@pytest.mark.s3
@pytest.mark.usefixtures("mock_s3_client")
def test_copy_file_source_object_nonexistent() -> None:
    """Ensure that the code throws an error if the source object does not exist."""
    s3_path = f"{CDM_LAKE_BUCKET}/some/path/to/file"
    assert object_exists(s3_path) is False
    with pytest.raises(Exception, match="The specified key does not exist"):
        copy_object(s3_path, f"{CDM_LAKE_BUCKET}/a/different/path/to/file")


# copy_directory tests
def put_objects(mock_s3_client: Any, bucket: str, keys: list[str], body: bytes = b"data") -> None:
    """Helper to seed objects into a bucket."""
    for key in keys:
        mock_s3_client.put_object(Bucket=bucket, Key=key, Body=body)


def list_keys(mock_s3_client: Any, bucket: str, prefix: str = "") -> set[str]:
    """Helper to list all keys in a bucket under a prefix."""
    paginator = mock_s3_client.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        keys.extend(obj["Key"] for obj in page.get("Contents", []))
    return set(keys)


@pytest.mark.s3
@pytest.mark.parametrize("source_suffix", ["", "/"])
@pytest.mark.parametrize("dest_suffix", ["", "/"])
def test_copy_directory_copies_all_objects_to_dest(
    mocked_s3_client_no_checksum: Any, source_suffix: str, dest_suffix: str
) -> None:
    """Verify that all objects under the source prefix are present in the successes dict.

    Ensure that copy works correctly with or without a slash at the end of the directory name.
    """
    mock_s3_client = mocked_s3_client_no_checksum
    populate_mock_s3(mock_s3_client, {CDM_LAKE_BUCKET: FILES_IN_BUCKETS[CDM_LAKE_BUCKET]})
    source_bucket_files = list_keys(mock_s3_client, CDM_LAKE_BUCKET)
    assert set(source_bucket_files) == set(SAMPLE_FILES)
    dest_bucket_files = list_keys(mock_s3_client, ALT_BUCKET)
    assert dest_bucket_files == set()

    successes, errors = copy_directory(
        f"s3://{CDM_LAKE_BUCKET}/dir_one{source_suffix}", f"s3://{ALT_BUCKET}/some/destination/dir{dest_suffix}"
    )

    assert errors == {}
    expected_files = {
        f"{CDM_LAKE_BUCKET}/{f}": f"{ALT_BUCKET}/some/destination/dir{f.replace('dir_one', '')}" for f in SAMPLE_FILES
    }
    assert successes == expected_files
    # ensure that the original files are still in place
    assert set(list_keys(mock_s3_client, CDM_LAKE_BUCKET)) == set(SAMPLE_FILES)
    # destination should have new files from the source
    assert set(list_keys(mock_s3_client, ALT_BUCKET)) == {
        f.removeprefix(f"{ALT_BUCKET}/") for f in expected_files.values()
    }

    # check the content
    for src, dest in expected_files.items():
        src_resp = mock_s3_client.get_object(Bucket=CDM_LAKE_BUCKET, Key=src.removeprefix(f"{CDM_LAKE_BUCKET}/"))
        assert src_resp["Body"].read() == src.encode()
        dest_resp = mock_s3_client.get_object(Bucket=ALT_BUCKET, Key=dest.removeprefix(f"{ALT_BUCKET}/"))
        assert dest_resp["Body"].read() == src.encode()


@pytest.mark.s3
def test_copy_directory_copy_within_same_bucket(mock_s3_client: Any) -> None:
    """Verify that copying between two prefixes within the same bucket works correctly."""
    populate_mock_s3(mock_s3_client, {CDM_LAKE_BUCKET: ["foo/a.txt", "foo/b.txt"]})

    successes, errors = copy_directory(f"s3://{CDM_LAKE_BUCKET}/foo", f"s3://{CDM_LAKE_BUCKET}/bar")

    assert successes == {"cdm-lake/foo/a.txt": "cdm-lake/bar/a.txt", "cdm-lake/foo/b.txt": "cdm-lake/bar/b.txt"}
    assert errors == {}
    assert list_keys(mock_s3_client, CDM_LAKE_BUCKET, prefix="bar") == {"bar/a.txt", "bar/b.txt"}
    assert list_keys(mock_s3_client, CDM_LAKE_BUCKET) == {"foo/a.txt", "foo/b.txt", "bar/a.txt", "bar/b.txt"}


@pytest.mark.s3
@pytest.mark.usefixtures("mock_s3_client")
def test_copy_directory_empty_directory_returns_empty_dicts() -> None:
    """Verify that when the source prefix matches no objects, both the successes and errors dictionaries are returned empty."""
    successes, errors = copy_directory(f"s3://{CDM_LAKE_BUCKET}/nonexistent/", f"s3://{ALT_BUCKET}/bar/")

    assert successes == {}
    assert errors == {}


@pytest.mark.s3
@pytest.mark.usefixtures("mock_s3_client")
def test_copy_directory_does_not_copy_objects_outside_prefix(mock_s3_client: Any) -> None:
    """Verify that objects whose keys share a prefix string but are not under the source directory.

    Example: 'foobar/' when copying 'foo/'.
    """
    populate_mock_s3(
        mock_s3_client,
        {
            CDM_LAKE_BUCKET: [
                "foo/a.txt",
                "foobar/should-not-be-copied.txt",
            ]
        },
    )
    copy_directory(f"s3://{CDM_LAKE_BUCKET}/foo", f"s3://{ALT_BUCKET}/bar")
    assert list_keys(mock_s3_client, ALT_BUCKET) == {"bar/a.txt"}


@pytest.mark.s3
@pytest.mark.usefixtures("mock_s3_client")
def test_copy_directory_missing_source_bucket_returns_error() -> None:
    """Verify that when the source bucket does not exist, botocore throws an error."""
    # FIXME: throws a s3.Client.exceptions.NoSuchBucket
    with pytest.raises(Exception, match="The specified bucket does not exist"):
        copy_directory("s3://nonexistent-bucket/bar", f"s3://{CDM_LAKE_BUCKET}/foo")


@pytest.mark.s3
@pytest.mark.usefixtures("mock_s3_client")
def test_copy_directory_missing_dest_bucket_records_errors(mock_s3_client: Any) -> None:
    """Verify that when the destination bucket does not exist, the errors dict contains all objects under the original dir."""
    # FIXME: throw a bucket not exists error?
    populate_mock_s3(mock_s3_client, {CDM_LAKE_BUCKET: ["foo/a.txt", "foo/b.txt"]})

    # with pytest.raises(FileNotFoundError, match="The specified bucket does not exist"):
    successes, errors = copy_directory(f"s3://{CDM_LAKE_BUCKET}/foo", "s3://nonexistent-bucket/bar")

    assert successes == {}
    assert f"{CDM_LAKE_BUCKET}/foo/a.txt" in errors
    assert f"{CDM_LAKE_BUCKET}/foo/b.txt" in errors
    assert isinstance(errors[f"{CDM_LAKE_BUCKET}/foo/a.txt"], Exception)


# delete_object
@pytest.mark.parametrize("bucket", BUCKETS)
@pytest.mark.parametrize("protocol", ["", "s3://", "s3a://"])
@pytest.mark.s3
def test_delete_object_removes_object(mock_s3_client: Any, bucket: str, protocol: str) -> None:
    """Verify that delete_object removes the object from the specified bucket."""
    mock_s3_client.put_object(Bucket=bucket, Key="to/delete.txt", Body=b"bye")
    s3_path = f"{protocol}{bucket}/to/delete.txt"
    assert object_exists(s3_path) is True

    resp = delete_object(s3_path)
    assert object_exists(s3_path) is False
    assert resp.get("ResponseMetadata", {}).get("HTTPStatusCode") == HTTP_204

    # retry the deletion
    resp = delete_object(s3_path)
    assert object_exists(s3_path) is False
    assert resp.get("ResponseMetadata", {}).get("HTTPStatusCode") == HTTP_204


# upload_file with metadata
@pytest.mark.parametrize("bucket", BUCKETS)
@pytest.mark.s3
def test_upload_file_with_metadata_attaches_metadata(mock_s3_client: Any, sample_file: Path, bucket: str) -> None:
    """Verify that upload_file with metadata stores user metadata on the uploaded object."""
    metadata = {"md5": "abc123", "source": "ncbi"}
    result = upload_file(sample_file, f"{bucket}/uploads", user_metadata=metadata)
    assert result is True

    resp = mock_s3_client.head_object(Bucket=bucket, Key=f"uploads/{sample_file.name}")
    assert resp["Metadata"]["md5"] == "abc123"
    assert resp["Metadata"]["source"] == "ncbi"


@pytest.mark.s3
def test_upload_file_with_metadata_custom_object_name(mock_s3_client: Any, sample_file: Path) -> None:
    """Verify that the object_name parameter overrides the filename."""
    result = upload_file(sample_file, f"{CDM_LAKE_BUCKET}/uploads", user_metadata={"k": "v"}, object_name="renamed.txt")
    assert result is True
    obj = mock_s3_client.get_object(Bucket=CDM_LAKE_BUCKET, Key="uploads/renamed.txt")
    assert obj["Body"].read() == b"hello s3"


@pytest.mark.s3
def test_upload_file_with_metadata_overwrites_existing(mock_s3_client: Any, sample_file: Path) -> None:
    """Verify that upload_file with metadata uploads even when the object already exists."""
    mock_s3_client.put_object(Bucket=CDM_LAKE_BUCKET, Key=f"uploads/{sample_file.name}", Body=b"old")
    result = upload_file(sample_file, f"{CDM_LAKE_BUCKET}/uploads", user_metadata={"new": "true"})
    assert result is True
    obj = mock_s3_client.get_object(Bucket=CDM_LAKE_BUCKET, Key=f"uploads/{sample_file.name}")
    assert obj["Body"].read() == b"hello s3"


@pytest.mark.usefixtures("mock_s3_client")
@pytest.mark.s3
def test_upload_file_with_metadata_raises_on_empty_destination(sample_file: Path) -> None:
    """Verify ValueError when destination_dir is empty."""
    with pytest.raises(ValueError, match="No destination directory"):
        upload_file(sample_file, "", user_metadata={"k": "v"})


@pytest.mark.usefixtures("mock_s3_client")
@pytest.mark.parametrize("path_type", [str, Path])
@pytest.mark.s3
def test_upload_file_with_metadata_accepts_str_and_path(sample_file: Path, path_type: type[str] | type[Path]) -> None:
    """Verify that upload_file with metadata accepts both str and Path."""
    result = upload_file(path_type(sample_file), f"{CDM_LAKE_BUCKET}/uploads", user_metadata={})
    assert result is True


# head_object
@pytest.mark.s3
def test_head_object_returns_info(mock_s3_client: Any) -> None:
    """Verify that head_object returns size, metadata, and checksum fields."""
    mock_s3_client.put_object(Bucket=CDM_LAKE_BUCKET, Key="info/file.txt", Body=b"hello", Metadata={"md5": "abc123"})
    result = head_object(f"{CDM_LAKE_BUCKET}/info/file.txt")
    assert result is not None
    assert result["ContentLength"] == SIZE_HELLO
    assert result["Metadata"]["md5"] == "abc123"


@pytest.mark.s3
@pytest.mark.usefixtures("mock_s3_client")
def test_head_object_raises_for_missing() -> None:
    """Verify that head_object returns None for a non-existent object."""
    with pytest.raises(ClientError, match="404"):
        _ = head_object(f"{CDM_LAKE_BUCKET}/does/not/exist.txt")


@pytest.mark.parametrize("protocol", ["", "s3://", "s3a://"])
@pytest.mark.s3
def test_head_object_with_protocols(mock_s3_client: Any, protocol: str) -> None:
    """Verify that head_object handles all valid protocol prefixes."""
    mock_s3_client.put_object(Bucket=CDM_LAKE_BUCKET, Key="proto/file.txt", Body=b"data")
    result = head_object(f"{protocol}{CDM_LAKE_BUCKET}/proto/file.txt")
    assert result is not None
    assert result["ContentLength"] == SIZE_DATA


# copy_object
@pytest.mark.parametrize("destination", BUCKETS)
@pytest.mark.s3
def test_copy_object_preserves_user_metadata(mocked_s3_client_no_checksum: Any, destination: str) -> None:
    """copy_object preserves source user metadata (MetadataDirective=COPY default)."""
    mocked_s3_client_no_checksum.put_object(
        Bucket=CDM_LAKE_BUCKET, Key="src/file.txt", Body=b"archive me", Metadata={"md5": "abc123"}
    )
    response = copy_object(
        f"{CDM_LAKE_BUCKET}/src/file.txt",
        f"{destination}/archive/file.txt",
    )
    assert response["ResponseMetadata"]["HTTPStatusCode"] == HTTP_STATUS_OK

    # source user metadata is preserved (MetadataDirective=COPY)
    resp = mocked_s3_client_no_checksum.head_object(Bucket=destination, Key="archive/file.txt")
    assert resp["Metadata"].get("md5") == "abc123"

    # verify source still exists
    assert object_exists(f"{CDM_LAKE_BUCKET}/src/file.txt")


@pytest.mark.s3
def test_copy_object_preserves_content(mocked_s3_client_no_checksum: Any) -> None:
    """Verify that the content of the copied object matches the original."""
    mocked_s3_client_no_checksum.put_object(Bucket=CDM_LAKE_BUCKET, Key="src/data.bin", Body=b"binary data")
    copy_object(
        f"{CDM_LAKE_BUCKET}/src/data.bin",
        f"{CDM_LAKE_BUCKET}/dst/data.bin",
    )
    obj = mocked_s3_client_no_checksum.get_object(Bucket=CDM_LAKE_BUCKET, Key="dst/data.bin")
    assert obj["Body"].read() == b"binary data"


# delete_object - bucket does not exist
@pytest.mark.s3
@pytest.mark.usefixtures("mock_s3_client")
def test_delete_object_no_such_bucket() -> None:
    """Verify that delete_object removes the object from the specified bucket."""
    s3_path = "fake-bucket/to/delete.txt"
    assert object_exists(s3_path) is False
    with pytest.raises(Exception, match="The specified bucket does not exist"):
        delete_object(s3_path)


# delete_objects
@pytest.mark.s3
def test_delete_objects_removes_all(mock_s3_client: Any) -> None:
    """delete_objects removes every listed key in a single call."""
    keys = ["bulk/a.txt", "bulk/b.txt", "bulk/c.txt"]
    for k in keys:
        mock_s3_client.put_object(Bucket=CDM_LAKE_BUCKET, Key=k, Body=b"data")

    errors = delete_objects(CDM_LAKE_BUCKET, keys)

    assert errors == []
    for k in keys:
        assert object_exists(f"{CDM_LAKE_BUCKET}/{k}") is False


@pytest.mark.s3
def test_delete_objects_empty_list_is_noop(mock_s3_client: Any) -> None:
    """delete_objects with an empty list makes no API call and returns no errors."""
    mock_s3_client.put_object(Bucket=CDM_LAKE_BUCKET, Key="keep/me.txt", Body=b"safe")
    errors = delete_objects(CDM_LAKE_BUCKET, [])
    assert errors == []
    assert object_exists(f"{CDM_LAKE_BUCKET}/keep/me.txt") is True


@pytest.mark.s3
def test_delete_objects_nonexistent_keys_no_error(mock_s3_client: Any) -> None:
    """Deleting keys that don't exist returns no errors (S3 delete is idempotent)."""
    errors = delete_objects(CDM_LAKE_BUCKET, ["ghost/a.txt", "ghost/b.txt"])
    assert errors == []


# CLI helper function tests

TEST_PROTOCOLS = [
    "",
    "s3://",
    "s3a://",
]

TEST_MB_PARAMS = [
    pytest.param("one-bucket", "one-bucket", id="name only"),
    pytest.param("two-bucket/", "two-bucket", id="trailing slash"),
    pytest.param("red-bucket/key", "red-bucket", id="red-bucket"),
    pytest.param("blue-bucket/foo/bar.txt", "blue-bucket", id="blue-bucket"),
]

ERROR_MB_USAGE = "Usage: s3_local.py mb s3://BUCKET"

TEST_MB_ARGS_ERROR = [
    pytest.param([], ERROR_MB_USAGE, id="no args"),
    pytest.param(["foo", "bar"], ERROR_MB_USAGE, id="two args"),
    pytest.param(["foo", "bar", "baz"], ERROR_MB_USAGE, id="three args"),
    pytest.param([""], NO_PATH_FOUND, id="missing bucket"),
    pytest.param(["/bucket"], START_WITH_BUCKET_NAME, id="preceding slash"),
    pytest.param(["/"], START_WITH_BUCKET_NAME, id="root"),
]


@pytest.mark.s3
@pytest.mark.parametrize("protocol", TEST_PROTOCOLS)
@pytest.mark.parametrize(("path", "bucket"), TEST_MB_PARAMS)
def test_cmd_mb_creates_bucket(mock_s3_client: Any, protocol: str, path: str, bucket: str) -> None:
    """CLI mb helper creates bucket for valid paths."""
    with patch("builtins.print") as mock_print:
        cmd_mb([f"{protocol}{path}"])
    mock_print.assert_called()
    mock_print.assert_any_call(f"Created bucket: {bucket}")
    mock_s3_client.head_bucket(Bucket=bucket)


@pytest.mark.s3
@pytest.mark.parametrize("protocol", TEST_PROTOCOLS)
@pytest.mark.parametrize(("path", "bucket"), TEST_MB_PARAMS)
def test_cmd_mb_handles_existing_bucket(mock_s3_client: Any, protocol: str, path: str, bucket: str) -> None:
    """CLI mb helper prints message when bucket already exists."""
    args = [f"{protocol}{path}"]
    with patch("builtins.print"):
        cmd_mb(args)
    with patch("builtins.print") as mock_print:
        cmd_mb(args)
    mock_print.assert_called()
    mock_print.assert_any_call(f"Bucket already exists: {bucket}")
    mock_s3_client.head_bucket(Bucket=bucket)


@pytest.mark.s3
@pytest.mark.usefixtures("mock_s3_client")
@pytest.mark.parametrize(("args", "err_msg"), TEST_MB_ARGS_ERROR)
def test_cmd_mb_prints_error(args: list[str], err_msg: str) -> None:
    """CLI mb helper prints usage on invalid argument list."""
    with pytest.raises(SystemExit) as exc:
        cmd_mb(args)
    assert err_msg in str(exc.value)


TEST_CP_PATHS = [
    pytest.param("bucket/path/to/file.txt", "bucket", "path/to/file.txt", id="no prefix"),
    pytest.param("s3://foo/bar/baz.qux", "foo", "bar/baz.qux", id="with prefix"),
    pytest.param("bucket/root.txt", "bucket", "root.txt", id="no prefix root file"),
    pytest.param("s3://foo/bar.baz", "foo", "bar.baz", id="with prefix root file"),
]


@pytest.mark.s3
@pytest.mark.parametrize(("path", "bucket", "key"), TEST_CP_PATHS)
def test_cmd_cp_copies_file(mock_s3_client: Any, sample_file: Path, path: str, bucket: str, key: str) -> None:
    """CLI cp helper copies local file to store."""
    mock_s3_client.create_bucket(Bucket=bucket)
    with patch("builtins.print") as mock_print:
        cmd_cp([str(sample_file), path])
    mock_print.assert_called()
    mock_print.assert_any_call(f"  {key}")
    mock_print.assert_any_call(f"Uploaded 1 files to s3://{bucket}/{key}")
    obj = mock_s3_client.get_object(Bucket=bucket, Key=key)
    assert obj["Body"].read() == b"hello s3"


TEST_CP_KEY_PREFIXES = [
    pytest.param("bucket/path/to", "bucket", "path/to/", id="no prefix"),
    pytest.param("bucket/path/to/", "bucket", "path/to/", id="no prefix trailing slash"),
    pytest.param("s3://foo/bar", "foo", "bar/", id="with prefix"),
    pytest.param("s3://foo/bar/", "foo", "bar/", id="with prefix trailing slash"),
    pytest.param("bucket", "bucket", "", id="no prefix root"),
    pytest.param("bucket/", "bucket", "", id="no prefix root trailing slash"),
    pytest.param("s3://foo", "foo", "", id="with prefix root"),
    pytest.param("s3://foo/", "foo", "", id="with prefix root trailing slash"),
]


@pytest.mark.s3
@pytest.mark.parametrize(("path", "bucket", "prefix"), TEST_CP_KEY_PREFIXES)
def test_cmd_cp_copies_dir(mock_s3_client: Any, sample_dir: Path, path: str, bucket: str, prefix: str) -> None:
    """CLI cp helper copeis local folder to store."""
    mock_s3_client.create_bucket(Bucket=bucket)
    with patch("builtins.print") as mock_print:
        cmd_cp([str(sample_dir), path])
    mock_print.assert_called()
    keys = {obj["Key"] for obj in mock_s3_client.list_objects_v2(Bucket=bucket)["Contents"]}
    mock_print.assert_any_call(f"Uploaded {len(keys)} files to s3://{Path(bucket) / prefix}/")
    assert keys == {f"{Path(prefix) / f}" for f in SAMPLE_FILES}


ERROR_CP_USAGE = "Usage: s3_local.py cp [LOCAL_DIR | LOCAL_FILE] s3://BUCKET[/PREFIX/]"

TEST_CP_ARGS_ERROR = [
    pytest.param([], ERROR_CP_USAGE, id="no args"),
    pytest.param(["foo"], ERROR_CP_USAGE, id="one arg"),
    pytest.param(["foo", "bar", "baz"], ERROR_CP_USAGE, id="three args"),
    pytest.param(["local/file.txt", ""], NO_PATH_FOUND, id="local file; missing bucket"),
    pytest.param(["local/folder", ""], NO_PATH_FOUND, id="local folder; missing bucket"),
    pytest.param(["local/folder/", ""], NO_PATH_FOUND, id="local folder/;missing bucket"),
    pytest.param(["local/file.txt", "/bucket"], START_WITH_BUCKET_NAME, id="local file; preceding slash"),
    pytest.param(["local/folder", "/bucket"], START_WITH_BUCKET_NAME, id="local folder; preceding slash"),
    pytest.param(["local/folder/", "/bucket"], START_WITH_BUCKET_NAME, id="local folder/; preceding slash"),
    pytest.param(["local/file.txt", "/"], START_WITH_BUCKET_NAME, id="local file; root"),
    pytest.param(["local/folder", "/"], START_WITH_BUCKET_NAME, id="local folder; root"),
    pytest.param(["local/folder/", "/"], START_WITH_BUCKET_NAME, id="local folder/; root"),
]


@pytest.mark.s3
@pytest.mark.usefixtures("mock_s3_client")
@pytest.mark.parametrize(("args", "err_msg"), TEST_CP_ARGS_ERROR)
def test_cmd_cp_prints_error(args: list[str], err_msg: str) -> None:
    """CLI cp helper prints usage on invalid argument list."""
    with pytest.raises(SystemExit) as exc:
        cmd_cp(args)
    assert err_msg in str(exc.value)


LS_FILES = [
    "key/prefix/file1.txt",
    "key/prefix/file2.txt",
    "key/prefix/sub/file3.txt",
]

TEST_LS_KEY_PREFIXES = [
    pytest.param("some-bucket/key/prefix/", "some-bucket", id="trailing slash"),
    pytest.param("other-bucket/key/prefix", "other-bucket", id="no trailing slash"),
    pytest.param("this-bucket/", "this-bucket", id="bucket trailing slash"),
    pytest.param("that-bucket", "that-bucket", id="bucket only"),
    pytest.param("s3://s3-bucket/key/prefix/", "s3-bucket", id="s3 protocol trailing slash"),
    pytest.param("s3a://s3a-bucket/key/prefix/", "s3a-bucket", id="s3a protocol trailing slash"),
]

ERROR_LS_USAGE = "Usage: s3_local.py ls s3://BUCKET[/PREFIX/] [--limit N]"

TEST_LS_ARGS_ERROR = [
    pytest.param([], ERROR_LS_USAGE, id="no args"),
    pytest.param([""], NO_PATH_FOUND, id="empty path"),
    pytest.param(["/bucket"], START_WITH_BUCKET_NAME, id="preceding slash"),
    pytest.param(["/"], START_WITH_BUCKET_NAME, id="root"),
]


@pytest.mark.s3
@pytest.mark.parametrize(("prefix", "bucket"), TEST_LS_KEY_PREFIXES)
def test_cmd_ls_lists_keys(mock_s3_client: Any, prefix: str, bucket: str) -> None:
    """CLI ls helper lists keys under the given prefix."""
    mock_s3_client.create_bucket(Bucket=bucket)
    for key in LS_FILES:
        mock_s3_client.put_object(Bucket=bucket, Key=key, Body=b"a")
    with patch("builtins.print") as mock_print:
        cmd_ls([prefix])
    printed_keys = {call.args[0].split()[-1] for call in mock_print.call_args_list}
    assert printed_keys == set(LS_FILES)


@pytest.mark.s3
def test_cmd_ls_respects_limit(mock_s3_client: Any) -> None:
    """CLI ls helper stops after --limit objects."""
    mock_s3_client.create_bucket(Bucket="some-bucket")
    for i in range(5):
        mock_s3_client.put_object(Bucket="some-bucket", Key=f"key/file{i}.txt", Body=b"x")

    with patch("builtins.print") as mock_print:
        cmd_ls(["some-bucket/key", "--limit", "2"])

    assert mock_print.call_count == 2  # noqa: PLR2004


@pytest.mark.s3
def test_cmd_ls_default_limit_is_20(mock_s3_client: Any) -> None:
    """CLI ls helper defaults to a limit of 20 objects."""
    bucket = "large-bucket"
    mock_s3_client.create_bucket(Bucket=bucket)
    for i in range(25):
        mock_s3_client.put_object(Bucket=bucket, Key=f"key/file{i:02d}.txt", Body=b"x")

    with patch("builtins.print") as mock_print:
        cmd_ls([f"{bucket}/key"])

    assert mock_print.call_count == 20  # noqa: PLR2004


@pytest.mark.s3
def test_cmd_ls_empty_prefix_returns_nothing(mock_s3_client: Any) -> None:
    """CLI ls helper prints nothing when no objects match the prefix."""
    bucket = "foo-bucket"
    mock_s3_client.create_bucket(Bucket=bucket)
    with patch("builtins.print") as mock_print:
        cmd_ls([f"{bucket}/nonexistent/"])

    mock_print.assert_not_called()


@pytest.mark.s3
@pytest.mark.usefixtures("mock_s3_client")
@pytest.mark.parametrize(("args", "err_msg"), TEST_LS_ARGS_ERROR)
def test_cmd_ls_prints_error(args: list[str], err_msg: str) -> None:
    """CLI ls helper raises SystemExit with a useful message on invalid arguments."""
    with pytest.raises(SystemExit) as exc:
        cmd_ls(args)
    assert err_msg in str(exc.value)


# cmd_head

ERROR_HEAD_USAGE = "Usage: s3_local.py head s3://BUCKET/KEY"

TEST_HEAD_ARGS_ERROR = [
    pytest.param([], ERROR_HEAD_USAGE, id="no args"),
    pytest.param([""], NO_PATH_FOUND, id="empty path"),
    pytest.param(["/bucket/key"], START_WITH_BUCKET_NAME, id="preceding slash"),
    pytest.param(["bucket"], COULD_NOT_PARSE, id="bucket only"),
    pytest.param(["bucket/"], COULD_NOT_PARSE, id="bucket trailing slash"),
]


@pytest.mark.s3
def test_cmd_head_prints_metadata(mock_s3_client: Any) -> None:
    """CLI head helper prints metadata for an existing object."""
    bucket = "bar-bucket"
    mock_s3_client.create_bucket(Bucket=bucket)
    mock_s3_client.put_object(
        Bucket=bucket,
        Key="some/file.txt",
        Body=b"data",
        Metadata={"md5": "abc123", "source": "ncbi"},
    )
    with patch("builtins.print") as mock_print:
        cmd_head([f"{bucket}/some/file.txt"])
    mock_print.assert_any_call(f"Metadata for {bucket}/some/file.txt:")
    # second call is the JSON dump — check both keys are present
    json_output = mock_print.call_args_list[1].args[0]
    parsed = json.loads(json_output)
    assert parsed["md5"] == "abc123"
    assert parsed["source"] == "ncbi"


@pytest.mark.s3
def test_cmd_head_prints_empty_metadata(mock_s3_client: Any) -> None:
    """CLI head helper prints empty JSON object when no metadata is set."""
    bucket = "baz-bucket"
    mock_s3_client.create_bucket(Bucket=bucket)
    mock_s3_client.put_object(Bucket=bucket, Key="bare/file.txt", Body=b"data")
    with patch("builtins.print") as mock_print:
        cmd_head([f"{bucket}/bare/file.txt"])

    mock_print.assert_any_call(f"Metadata for {bucket}/bare/file.txt:")
    json_output = mock_print.call_args_list[1].args[0]
    assert json.loads(json_output) == {}


@pytest.mark.s3
def test_cmd_head_prints_not_found_for_missing_key(mock_s3_client: Any) -> None:
    """CLI head helper prints a not-found message instead of raising for a 404."""
    bucket = "empty-bucket"
    mock_s3_client.create_bucket(Bucket=bucket)
    with patch("builtins.print") as mock_print:
        cmd_head([f"{bucket}/does/not/exist.txt"])

    mock_print.assert_called_once_with(f"File not found in store: {bucket}/does/not/exist.txt")


@pytest.mark.s3
@pytest.mark.parametrize("protocol", ["", "s3://", "s3a://"])
def test_cmd_head_handles_protocols(mock_s3_client: Any, protocol: str) -> None:
    """CLI head helper accepts all valid S3 protocol prefixes."""
    bucket = "proto-bucket"
    mock_s3_client.create_bucket(Bucket=bucket)
    mock_s3_client.put_object(Bucket=bucket, Key="proto/file.txt", Body=b"x")

    with patch("builtins.print") as mock_print:
        cmd_head([f"{protocol}{bucket}/proto/file.txt"])

    mock_print.assert_any_call(f"Metadata for {bucket}/proto/file.txt:")


@pytest.mark.s3
@pytest.mark.usefixtures("mock_s3_client")
@pytest.mark.parametrize(("args", "err_msg"), TEST_HEAD_ARGS_ERROR)
def test_cmd_head_prints_error(args: list[str], err_msg: str) -> None:
    """CLI head helper raises SystemExit with a useful message on invalid arguments."""
    with pytest.raises(SystemExit) as exc:
        cmd_head(args)
    assert err_msg in str(exc.value)
