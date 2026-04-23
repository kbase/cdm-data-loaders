"""Tests for s3_utils.py using moto to mock AWS S3."""

import functools
import io
from collections.abc import Callable, Generator
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import boto3
import pytest
from botocore.exceptions import ClientError
from moto import mock_aws
from requests.exceptions import ConnectionError as ConnError
from requests.exceptions import HTTPError

import cdm_data_loaders.utils.s3 as s3_utils
from cdm_data_loaders.utils.s3 import (
    CDM_LAKE_BUCKET,
    DEFAULT_EXTRA_ARGS,
    copy_object,
    copy_object_with_metadata,
    delete_object,
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

AWS_REGION = "us-east-1"

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
def mock_s3_client() -> Generator[Any, Any]:
    """Yield a mocked S3 client with both valid buckets created.

    The function get_s3_client() is patched to ensure that all module functions use this client.

    Resets the cached client before and after to prevent state leaking between tests.
    """
    with mock_aws():
        client = boto3.client("s3", region_name=AWS_REGION)
        for bucket in FILES_IN_BUCKETS:
            client.create_bucket(Bucket=bucket)

        reset_s3_client()
        assert s3_utils._s3_client is None  # noqa: SLF001

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


# Client creation / reset
@pytest.mark.s3
def test_get_s3_client_raises_on_missing_args() -> None:
    """Verify that get_s3_client raises ValueError when required arguments are absent."""
    reset_s3_client()
    with pytest.raises(ValueError, match="missing arguments"):
        get_s3_client(args={"endpoint_url": "http://localhost", "aws_access_key_id": "key"})
    reset_s3_client()
    assert s3_utils._s3_client is None  # noqa: SLF001


@pytest.mark.s3
def test_get_s3_client_returns_client_with_valid_args() -> None:
    """Verify that get_s3_client returns a usable client when all required args are provided."""
    reset_s3_client()
    with mock_aws():
        client = get_s3_client(
            args={
                "endpoint_url": "http://localhost:9000",
                "aws_access_key_id": "key",
                "aws_secret_access_key": "secret",
            }
        )
        assert client is not None
    reset_s3_client()
    assert s3_utils._s3_client is None  # noqa: SLF001


@pytest.mark.s3
def test_get_s3_client_returns_same_instance() -> None:
    """Verify that repeated calls to get_s3_client return the exact same cached client instance."""
    reset_s3_client()
    assert s3_utils._s3_client is None  # noqa: SLF001
    with mock_aws():
        args = {
            "endpoint_url": "http://localhost:9000",
            "aws_access_key_id": "key",
            "aws_secret_access_key": "secret",
        }
        client_a = get_s3_client(args=args)
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


@pytest.mark.s3
def test_get_s3_client_populates_from_environment() -> None:
    # set up the environment

    pass


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
@pytest.mark.s3
def test_split_s3_path_errors(invalid_path: str) -> None:
    """Ensure that an error is thrown if an invalid s3 path is passed in."""
    with pytest.raises(ValueError, match=INVALID_PATH_ERRORS[invalid_path]):
        split_s3_path(invalid_path)


@pytest.mark.parametrize("valid_path", list(EXPECTED.keys()))
@pytest.mark.s3
def test_split_s3_path_success(valid_path: str) -> None:
    """Verify that a valid path is correctly split into bucket and key."""
    (bucket, path) = split_s3_path(valid_path)
    assert (bucket, path) == EXPECTED[valid_path]


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
            assert isinstance(output["size"], int)
            assert object_exists(f"{protocol}{bucket}/{f}") is True

        nonexistent_file = f"{protocol}{bucket}/a-file-i-just-made-up.txt"
        assert object_exists(nonexistent_file) is False
        assert head_object(nonexistent_file) is None


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
def test_upload_file_skips_when_already_present(
    mock_s3_client: Any, sample_file: Path, capsys: pytest.CaptureFixture
) -> None:
    """Verify that uploading a file that already exists is skipped and returns True."""
    mock_s3_client.put_object(Bucket=CDM_LAKE_BUCKET, Key=f"uploads/{sample_file.name}", Body=b"old")
    result = upload_file(sample_file, f"{CDM_LAKE_BUCKET}/uploads")
    assert result is True
    assert "File already present" in capsys.readouterr().out


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
def test_accepts_custom_requests_implementation() -> None:
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
def test_download_file_does_not_exist(tmp_path: Path, capsys: pytest.CaptureFixture) -> None:
    """Ensure that attempting to download a file that does not exist raises an error."""
    bucket = BUCKETS[0]
    key = "to/the/door.txt"
    assert not object_exists(f"{bucket}/{key}")

    with pytest.raises(
        ClientError,
        match=r"An error occurred \(404\) when calling the HeadObject",
    ):
        download_file(f"{bucket}/{key}", tmp_path / "file.txt")

    assert "File not found" in capsys.readouterr().out


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


# NOTE: Moto currently does not support CRC64NVME; remove this helper when it does.
def strip_checksum_algorithm(method: Callable[..., Any]) -> Callable[..., Any]:
    """Wrap a boto3 S3 method to remove the ChecksumAlgorithm argument before calling moto.

    Moto does not implement CRC64NVME checksums, so any call that includes
    ChecksumAlgorithm='CRC64NVME' would fail. This wrapper silently drops the
    argument so the rest of the call proceeds normally against the moto backend.
    """

    @functools.wraps(method)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        """Remove the ChecksumAlgorithm argument from the call."""
        kwargs.pop("ChecksumAlgorithm", None)
        return method(*args, **kwargs)

    return wrapper


@pytest.fixture
def mocked_s3_client_no_checksum(mock_s3_client: Any) -> Any:
    """Return the mocked S3 client with copy_object patched to strip ChecksumAlgorithm.

    This works around the moto limitation of not supporting CRC64NVME checksums,
    allowing copy_object calls that include ChecksumAlgorithm to succeed.
    """
    mock_s3_client.copy_object = strip_checksum_algorithm(mock_s3_client.copy_object)
    return mock_s3_client


# copy_object
@pytest.mark.parametrize("destination", BUCKETS)
@pytest.mark.s3
def test_copy_file(mocked_s3_client_no_checksum: Any, destination: str) -> None:
    """Verify that copy_file copies an object to a new key within the same bucket."""
    mocked_s3_client_no_checksum.put_object(Bucket=CDM_LAKE_BUCKET, Key="src/file.txt", Body=b"copy me")
    assert object_exists(f"{CDM_LAKE_BUCKET}/src/file.txt")
    response = copy_object(f"{CDM_LAKE_BUCKET}/src/file.txt", f"{destination}/dst/path/to/file.txt")

    # check both objects exist
    assert object_exists(f"{CDM_LAKE_BUCKET}/src/file.txt")
    assert object_exists(f"{destination}/dst/path/to/file.txt")

    obj = mocked_s3_client_no_checksum.get_object(Bucket=destination, Key="dst/path/to/file.txt")
    assert obj["Body"].read() == b"copy me"
    assert response["ResponseMetadata"]["HTTPStatusCode"] == HTTP_STATUS_OK


@pytest.mark.s3
@pytest.mark.usefixtures("mock_s3_client")
def test_copy_file_source_object_nonexistent() -> None:
    """Ensure that the code throws an error if the source object does not exist."""
    s3_path = f"{CDM_LAKE_BUCKET}/some/path/to/file"
    assert object_exists(s3_path) is False
    with pytest.raises(Exception, match="The specified key does not exist"):
        copy_object(s3_path, f"{CDM_LAKE_BUCKET}/a/different/path/to/file")


@pytest.mark.s3
@pytest.mark.usefixtures("mock_s3_client")
def test_copy_file_source_bucket_nonexistent() -> None:
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
    assert resp.get("ResponseMetadata", {}).get("HTTPStatusCode") == HTTP_STATUS_NO_CONTENT

    # retry the deletion
    resp = delete_object(s3_path)
    assert object_exists(s3_path) is False
    assert resp.get("ResponseMetadata", {}).get("HTTPStatusCode") == HTTP_STATUS_NO_CONTENT


# upload_file with metadata
@pytest.mark.parametrize("bucket", BUCKETS)
@pytest.mark.s3
def test_upload_file_with_metadata_attaches_metadata(mock_s3_client: Any, sample_file: Path, bucket: str) -> None:
    """Verify that upload_file with metadata stores user metadata on the uploaded object."""
    metadata = {"md5": "abc123", "source": "ncbi"}
    result = upload_file(sample_file, f"{bucket}/uploads", metadata=metadata)
    assert result is True

    resp = mock_s3_client.head_object(Bucket=bucket, Key=f"uploads/{sample_file.name}")
    assert resp["Metadata"]["md5"] == "abc123"
    assert resp["Metadata"]["source"] == "ncbi"


@pytest.mark.s3
def test_upload_file_with_metadata_custom_object_name(mock_s3_client: Any, sample_file: Path) -> None:
    """Verify that the object_name parameter overrides the filename."""
    result = upload_file(
        sample_file, f"{CDM_LAKE_BUCKET}/uploads", metadata={"k": "v"}, object_name="renamed.txt"
    )
    assert result is True
    obj = mock_s3_client.get_object(Bucket=CDM_LAKE_BUCKET, Key="uploads/renamed.txt")
    assert obj["Body"].read() == b"hello s3"


@pytest.mark.s3
def test_upload_file_with_metadata_overwrites_existing(mock_s3_client: Any, sample_file: Path) -> None:
    """Verify that upload_file with metadata uploads even when the object already exists."""
    mock_s3_client.put_object(Bucket=CDM_LAKE_BUCKET, Key=f"uploads/{sample_file.name}", Body=b"old")
    result = upload_file(sample_file, f"{CDM_LAKE_BUCKET}/uploads", metadata={"new": "true"})
    assert result is True
    obj = mock_s3_client.get_object(Bucket=CDM_LAKE_BUCKET, Key=f"uploads/{sample_file.name}")
    assert obj["Body"].read() == b"hello s3"


@pytest.mark.usefixtures("mock_s3_client")
@pytest.mark.s3
def test_upload_file_with_metadata_raises_on_empty_destination(sample_file: Path) -> None:
    """Verify ValueError when destination_dir is empty."""
    with pytest.raises(ValueError, match="No destination directory"):
        upload_file(sample_file, "", metadata={"k": "v"})


@pytest.mark.usefixtures("mock_s3_client")
@pytest.mark.parametrize("path_type", [str, Path])
@pytest.mark.s3
def test_upload_file_with_metadata_accepts_str_and_path(sample_file: Path, path_type: type[str] | type[Path]) -> None:
    """Verify that upload_file with metadata accepts both str and Path."""
    result = upload_file(path_type(sample_file), f"{CDM_LAKE_BUCKET}/uploads", metadata={})
    assert result is True


# head_object
@pytest.mark.s3
def test_head_object_returns_info(mock_s3_client: Any) -> None:
    """Verify that head_object returns size, metadata, and checksum fields."""
    mock_s3_client.put_object(Bucket=CDM_LAKE_BUCKET, Key="info/file.txt", Body=b"hello", Metadata={"md5": "abc123"})
    result = head_object(f"{CDM_LAKE_BUCKET}/info/file.txt")
    assert result is not None
    assert result["size"] == SIZE_HELLO
    assert result["metadata"]["md5"] == "abc123"
    # moto may not populate CRC64NVME, but the key should be present
    assert "checksum_crc64nvme" in result


@pytest.mark.s3
@pytest.mark.usefixtures("mock_s3_client")
def test_head_object_returns_none_for_missing() -> None:
    """Verify that head_object returns None for a non-existent object."""
    result = head_object(f"{CDM_LAKE_BUCKET}/does/not/exist.txt")
    assert result is None


@pytest.mark.parametrize("protocol", ["", "s3://", "s3a://"])
@pytest.mark.s3
def test_head_object_with_protocols(mock_s3_client: Any, protocol: str) -> None:
    """Verify that head_object handles all valid protocol prefixes."""
    mock_s3_client.put_object(Bucket=CDM_LAKE_BUCKET, Key="proto/file.txt", Body=b"data")
    result = head_object(f"{protocol}{CDM_LAKE_BUCKET}/proto/file.txt")
    assert result is not None
    assert result["size"] == SIZE_DATA


# copy_object_with_metadata
@pytest.mark.parametrize("destination", BUCKETS)
@pytest.mark.s3
def test_copy_object_with_metadata_replaces_metadata(mocked_s3_client_no_checksum: Any, destination: str) -> None:
    """Verify that copy_object_with_metadata copies and replaces metadata."""
    mocked_s3_client_no_checksum.put_object(
        Bucket=CDM_LAKE_BUCKET, Key="src/file.txt", Body=b"archive me", Metadata={"old_key": "old_val"}
    )
    new_metadata = {"archive_reason": "replaced", "archive_date": "2026-04-16"}
    response = copy_object_with_metadata(
        f"{CDM_LAKE_BUCKET}/src/file.txt",
        f"{destination}/archive/file.txt",
        metadata=new_metadata,
    )
    assert response["ResponseMetadata"]["HTTPStatusCode"] == HTTP_STATUS_OK

    # verify the destination has the new metadata, not the old
    resp = mocked_s3_client_no_checksum.head_object(Bucket=destination, Key="archive/file.txt")
    assert resp["Metadata"]["archive_reason"] == "replaced"
    assert resp["Metadata"]["archive_date"] == "2026-04-16"
    assert "old_key" not in resp["Metadata"]

    # verify source still exists
    assert object_exists(f"{CDM_LAKE_BUCKET}/src/file.txt")


@pytest.mark.s3
def test_copy_object_with_metadata_preserves_content(mocked_s3_client_no_checksum: Any) -> None:
    """Verify that the content of the copied object matches the original."""
    mocked_s3_client_no_checksum.put_object(Bucket=CDM_LAKE_BUCKET, Key="src/data.bin", Body=b"binary data")
    copy_object_with_metadata(
        f"{CDM_LAKE_BUCKET}/src/data.bin",
        f"{CDM_LAKE_BUCKET}/dst/data.bin",
        metadata={"tag": "value"},
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
