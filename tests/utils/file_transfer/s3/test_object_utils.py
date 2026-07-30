"""Tests for s3_utils.py using moto to mock AWS S3."""

import dataclasses
import io
import logging
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError

from cdm_data_loaders.utils.file_transfer.checksums import ChecksumEntry
from cdm_data_loaders.utils.file_transfer.s3 import object_utils
from cdm_data_loaders.utils.file_transfer.s3.object_utils import (
    CHECKSUM_ALGORITHM_METADATA_KEY,
    CHECKSUM_VALUE_METADATA_KEY,
    DEFAULT_EXTRA_ARGS,
    S3ObjectInfo,
    SkipDecision,
    checksum_metadata,
    copy_directory,
    copy_object,
    decide_skip,
    delete_object,
    delete_objects,
    download_file,
    extract_stored_checksum,
    get_existing_object_info,
    head_object,
    list_matching_objects,
    object_exists,
    upload_dir,
    upload_file,
)
from tests.utils.file_transfer.s3.conftest import (
    ALT_BUCKET,
    BUCKETS,
    FILES_IN_BUCKETS,
    HTTP_200,
    HTTP_204,
    SAMPLE_FILES,
    SIZE_DATA,
    SIZE_HELLO,
    TEST_BUCKET,
    populate_mock_s3,
)


# S3ObjectInfo dataclass
def test_s3objectinfo_pass_construction_and_fields() -> None:
    """S3ObjectInfo stores its fields exactly as provided."""
    info = S3ObjectInfo(size=100, etag='"abc123"', metadata={"foo": "bar"})
    assert info.size == 100
    assert info.etag == '"abc123"'
    assert info.metadata == {"foo": "bar"}


def test_s3objectinfo_pass_equality() -> None:
    """Two S3ObjectInfo instances with identical fields compare equal."""
    a = S3ObjectInfo(size=1, etag="e", metadata={})
    b = S3ObjectInfo(size=1, etag="e", metadata={})
    assert a == b


def test_s3objectinfo_fail_frozen_immutable() -> None:
    """Mutating a field on S3ObjectInfo raises FrozenInstanceError."""
    info = S3ObjectInfo(size=1, etag="e", metadata={})
    with pytest.raises(dataclasses.FrozenInstanceError):
        info.size = 2  # type: ignore[misc]


def test_s3objectinfo_fail_not_hashable_due_to_mutable_metadata_field() -> None:
    """Although the dataclass is frozen, it is not actually hashable because `metadata` is a dict."""
    info = S3ObjectInfo(size=1, etag="e", metadata={})
    with pytest.raises(TypeError):
        hash(info)


# SkipDecision dataclass
def test_skipdecision_pass_confident_defaults_true() -> None:
    """SkipDecision.confident defaults to True when not explicitly provided."""
    decision = SkipDecision(skip=True, reason="test")
    assert decision.confident is True


def test_skipdecision_pass_explicit_confident_false() -> None:
    """SkipDecision.confident can be explicitly set to False."""
    decision = SkipDecision(skip=False, reason="test", confident=False)
    assert decision.confident is False


def test_skipdecision_fail_frozen_immutable() -> None:
    """Mutating a field on SkipDecision raises FrozenInstanceError."""
    decision = SkipDecision(skip=True, reason="x")
    with pytest.raises(dataclasses.FrozenInstanceError):
        decision.skip = False  # type: ignore[misc]


def test_skipdecision_pass_hashable() -> None:
    """All fields on SkipDecision are hashable primitives, so it is usable in sets/dict keys."""
    a = SkipDecision(skip=True, reason="x", confident=True)
    b = SkipDecision(skip=True, reason="x", confident=True)
    assert hash(a) == hash(b)
    assert len({a, b}) == 1


# extract_stored_checksum
def test_extract_stored_checksum_pass_returns_entry_when_both_keys_present() -> None:
    """A complete checksum recorded in metadata is recovered as a ChecksumEntry."""
    existing = S3ObjectInfo(
        size=1,
        etag="e",
        metadata={CHECKSUM_ALGORITHM_METADATA_KEY: "sha256", CHECKSUM_VALUE_METADATA_KEY: "abc"},
    )
    assert extract_stored_checksum(existing) == ChecksumEntry(algorithm="sha256", value="abc")


@pytest.mark.parametrize(
    "metadata",
    [
        {},
        {CHECKSUM_ALGORITHM_METADATA_KEY: "sha256"},
        {CHECKSUM_VALUE_METADATA_KEY: "abc"},
        {CHECKSUM_ALGORITHM_METADATA_KEY: "", CHECKSUM_VALUE_METADATA_KEY: "abc"},
        {CHECKSUM_ALGORITHM_METADATA_KEY: "sha256", CHECKSUM_VALUE_METADATA_KEY: ""},
    ],
    ids=["empty", "missing_value", "missing_algorithm", "empty_algorithm", "empty_value"],
)
def test_extract_stored_checksum_pass_returns_none_when_incomplete(metadata: dict[str, str]) -> None:
    """Any missing or empty-string component of the stored checksum results in None, not a partial value."""
    existing = S3ObjectInfo(size=1, etag="e", metadata=metadata)
    assert extract_stored_checksum(existing) is None


def test_extract_stored_checksum_pass_ignores_unrelated_metadata() -> None:
    """Extraction only looks at the two dedicated checksum keys, ignoring other metadata present."""
    existing = S3ObjectInfo(
        size=1,
        etag="e",
        metadata={
            CHECKSUM_ALGORITHM_METADATA_KEY: "md5",
            CHECKSUM_VALUE_METADATA_KEY: "def456",
            "source": "ncbi",
        },
    )
    assert extract_stored_checksum(existing) == ChecksumEntry(algorithm="md5", value="def456")


# checksum_metadata
def test_checksum_metadata_pass_builds_expected_dict() -> None:
    """checksum_metadata produces a dict keyed by the module's dedicated metadata keys."""
    checksum = ChecksumEntry(algorithm="sha256", value="deadbeef")
    result = checksum_metadata(checksum)
    assert result == {
        CHECKSUM_ALGORITHM_METADATA_KEY: "sha256",
        CHECKSUM_VALUE_METADATA_KEY: "deadbeef",
    }


def test_checksum_metadata_pass_roundtrips_with_extract_stored_checksum() -> None:
    """A checksum built into metadata via checksum_metadata is fully recoverable via extract_stored_checksum."""
    checksum = ChecksumEntry(algorithm="crc64nvme", value="YWJjMTIz")
    metadata = checksum_metadata(checksum)
    existing = S3ObjectInfo(size=1, etag="e", metadata=metadata)
    assert extract_stored_checksum(existing) == checksum


# decide_skip
def test_decide_skip_pass_no_existing_object_never_skips() -> None:
    """When no object exists at the destination, the upload is never skipped."""
    decision = decide_skip(None, remote_size=100, expected_checksum=ChecksumEntry("sha256", "abc"))
    assert decision.skip is False
    assert "does not exist" in decision.reason


def test_decide_skip_pass_matching_stored_checksum_skips_confidently() -> None:
    """A matching stored checksum results in a confident skip."""
    existing = S3ObjectInfo(
        size=100,
        etag="e",
        metadata={CHECKSUM_ALGORITHM_METADATA_KEY: "sha256", CHECKSUM_VALUE_METADATA_KEY: "abcdef"},
    )
    expected = ChecksumEntry(algorithm="sha256", value="abcdef")
    decision = decide_skip(existing, remote_size=100, expected_checksum=expected)
    assert decision.skip is True
    assert decision.confident is True
    assert "checksum matches" in decision.reason


def test_decide_skip_pass_checksum_comparison_is_case_insensitive() -> None:
    """Checksum value comparison ignores case (hex digests may differ in case)."""
    existing = S3ObjectInfo(
        size=100,
        etag="e",
        metadata={CHECKSUM_ALGORITHM_METADATA_KEY: "sha256", CHECKSUM_VALUE_METADATA_KEY: "ABCDEF"},
    )
    expected = ChecksumEntry(algorithm="sha256", value="abcdef")
    decision = decide_skip(existing, remote_size=None, expected_checksum=expected)
    assert decision.skip is True


def test_decide_skip_fail_checksum_value_mismatch_does_not_skip() -> None:
    """A differing checksum value results in a confident non-skip, with both values in the reason."""
    existing = S3ObjectInfo(
        size=100,
        etag="e",
        metadata={CHECKSUM_ALGORITHM_METADATA_KEY: "sha256", CHECKSUM_VALUE_METADATA_KEY: "111111"},
    )
    expected = ChecksumEntry(algorithm="sha256", value="222222")
    decision = decide_skip(existing, remote_size=100, expected_checksum=expected)
    assert decision.skip is False
    assert decision.confident is True
    assert "sha256:111111" in decision.reason
    assert "sha256:222222" in decision.reason


def test_decide_skip_fail_checksum_algorithm_mismatch_does_not_skip() -> None:
    """A differing algorithm name is treated as a mismatch, even if the raw value strings are identical."""
    existing = S3ObjectInfo(
        size=100,
        etag="e",
        metadata={CHECKSUM_ALGORITHM_METADATA_KEY: "md5", CHECKSUM_VALUE_METADATA_KEY: "same-value"},
    )
    expected = ChecksumEntry(algorithm="sha256", value="same-value")
    decision = decide_skip(existing, remote_size=None, expected_checksum=expected)
    assert decision.skip is False


def test_decide_skip_pass_falls_back_to_size_when_no_stored_checksum(caplog: pytest.LogCaptureFixture) -> None:
    """When an expected checksum is given but the existing object has none, a warning is logged and size is compared."""
    existing = S3ObjectInfo(size=100, etag="e", metadata={})
    expected = ChecksumEntry(algorithm="sha256", value="abcdef")
    with caplog.at_level(logging.WARNING):
        decision = decide_skip(existing, remote_size=100, expected_checksum=expected)
    assert decision.skip is True
    assert decision.confident is False
    assert "size matches" in decision.reason
    assert any("no stored checksum metadata" in r.message for r in caplog.records)


def test_decide_skip_fail_size_mismatch_when_no_stored_checksum() -> None:
    """Falling back to size comparison still correctly detects a mismatch."""
    existing = S3ObjectInfo(size=100, etag="e", metadata={})
    expected = ChecksumEntry(algorithm="sha256", value="abcdef")
    decision = decide_skip(existing, remote_size=50, expected_checksum=expected)
    assert decision.skip is False
    assert decision.confident is True
    assert "size mismatch" in decision.reason


def test_decide_skip_fail_no_checksum_or_size_available_with_expected_checksum() -> None:
    """If nothing at all can be compared (no stored checksum, no remote size), the upload is never skipped."""
    existing = S3ObjectInfo(size=100, etag="e", metadata={})
    expected = ChecksumEntry(algorithm="sha256", value="abcdef")
    decision = decide_skip(existing, remote_size=None, expected_checksum=expected)
    assert decision.skip is False
    assert decision.confident is False
    assert "no checksum or size available" in decision.reason


def test_decide_skip_pass_size_only_match_skips_unconfidently() -> None:
    """With no expected checksum at all, a matching size still triggers a skip, but flagged unconfident."""
    existing = S3ObjectInfo(size=100, etag="e", metadata={})
    decision = decide_skip(existing, remote_size=100, expected_checksum=None)
    assert decision.skip is True
    assert decision.confident is False
    assert "size matches" in decision.reason


def test_decide_skip_fail_size_only_mismatch_does_not_skip() -> None:
    """A size mismatch with no checksum available results in a confident non-skip."""
    existing = S3ObjectInfo(size=100, etag="e", metadata={})
    decision = decide_skip(existing, remote_size=99, expected_checksum=None)
    assert decision.skip is False
    assert decision.confident is True
    assert "size mismatch" in decision.reason


def test_decide_skip_fail_nothing_to_compare_never_skips() -> None:
    """With no checksum and no remote size at all, re-uploading is preferred over an unverifiable skip."""
    existing = S3ObjectInfo(size=100, etag="e", metadata={})
    decision = decide_skip(existing, remote_size=None, expected_checksum=None)
    assert decision.skip is False
    assert decision.confident is False
    assert "no checksum or size available" in decision.reason


def test_decide_skip_pass_partial_stored_checksum_metadata_falls_back_to_size() -> None:
    """A partially-recorded checksum (only one of the two keys) is treated as absent, not as a comparison error."""
    existing = S3ObjectInfo(size=100, etag="e", metadata={CHECKSUM_ALGORITHM_METADATA_KEY: "sha256"})
    expected = ChecksumEntry(algorithm="sha256", value="abcdef")
    decision = decide_skip(existing, remote_size=100, expected_checksum=expected)
    assert decision.skip is True
    assert decision.confident is False


# get_existing_object_info
@pytest.mark.s3
def test_get_existing_object_info_pass_returns_info_for_existing_object(mock_s3_client: Any) -> None:
    """Verify size, etag, and lowercased metadata are all populated for an existing object."""
    mock_s3_client.put_object(Bucket=TEST_BUCKET, Key="info/file.txt", Body=b"hello", Metadata={"MD5": "abc123"})
    info = get_existing_object_info(TEST_BUCKET, "info/file.txt")
    assert info is not None
    assert info.size == SIZE_HELLO
    assert info.etag
    assert info.metadata == {"md5": "abc123"}


@pytest.mark.s3
@pytest.mark.usefixtures("mock_s3_client")
def test_get_existing_object_info_pass_returns_none_for_missing_object() -> None:
    """Verify None is returned (not an exception) for a missing object."""
    assert get_existing_object_info(TEST_BUCKET, "does/not/exist.txt") is None


@pytest.mark.s3
@pytest.mark.usefixtures("mock_s3_client")
def test_get_existing_object_info_pass_metadata_defaults_to_empty_dict(mock_s3_client: Any) -> None:
    """An object with no user metadata yields an empty metadata dict, not None."""
    mock_s3_client.put_object(Bucket=TEST_BUCKET, Key="no-metadata.txt", Body=b"x")
    info = get_existing_object_info(TEST_BUCKET, "no-metadata.txt")
    assert info is not None
    assert info.metadata == {}


@pytest.mark.parametrize("error_code", ["404", "NoSuchKey", "NotFound", "NoSuchBucket"])
@pytest.mark.s3
def test_get_existing_object_info_pass_recognizes_all_not_found_error_codes(
    mock_s3_client: Any, monkeypatch: pytest.MonkeyPatch, error_code: str
) -> None:
    """NOT_FOUND_ERROR_CODES are treated as 'not found', even with a non-404 HTTP status."""

    def raise_not_found(**kwargs: Any) -> None:
        raise ClientError(
            {"Error": {"Code": error_code}, "ResponseMetadata": {"HTTPStatusCode": 400}},
            "HeadObject",
        )

    monkeypatch.setattr(mock_s3_client, "head_object", raise_not_found)
    assert get_existing_object_info(TEST_BUCKET, "some/key.txt") is None


@pytest.mark.s3
def test_get_existing_object_info_pass_recognizes_404_status_with_unknown_error_code(
    mock_s3_client: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A 404 HTTP status is treated as 'not found' even if the error code itself isn't recognized."""

    def raise_404_status(**kwargs: Any) -> None:
        raise ClientError(
            {"Error": {"Code": "SomeWeirdCode"}, "ResponseMetadata": {"HTTPStatusCode": 404}},
            "HeadObject",
        )

    monkeypatch.setattr(mock_s3_client, "head_object", raise_404_status)
    assert get_existing_object_info(TEST_BUCKET, "some/key.txt") is None


@pytest.mark.s3
def test_get_existing_object_info_fail_non_not_found_error_propagates(
    mock_s3_client: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A ClientError that is neither a recognized not-found code nor a 404 status is re-raised."""

    def raise_access_denied(**kwargs: Any) -> None:
        raise ClientError(
            {"Error": {"Code": "AccessDenied"}, "ResponseMetadata": {"HTTPStatusCode": 403}},
            "HeadObject",
        )

    monkeypatch.setattr(mock_s3_client, "head_object", raise_access_denied)
    with pytest.raises(ClientError, match="AccessDenied"):
        get_existing_object_info(TEST_BUCKET, "some/key.txt")


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
    bucket = TEST_BUCKET
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
    TEST_BUCKET: [
        *DIR_TWO_FILES,
        *DIRTY_DATA,
    ]
}

EXPECTED_FILE_LIST = {
    "di": [*FILES_IN_BUCKETS[TEST_BUCKET], *LOTS_OF_FILES[TEST_BUCKET]],
    "dir": [*FILES_IN_BUCKETS[TEST_BUCKET], *LOTS_OF_FILES[TEST_BUCKET]],
    "dir_": [*FILES_IN_BUCKETS[TEST_BUCKET], *DIR_TWO_FILES],
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
    # this adds two extra dirs to TEST_BUCKET with 1005 files in each
    populate_mock_s3(mock_s3_client, LOTS_OF_FILES)

    contents = list_matching_objects(f"{TEST_BUCKET}/{dir_path}")
    keys = {obj["Key"] for obj in contents}
    assert keys == set(EXPECTED_FILE_LIST[dir_path])


@pytest.mark.s3
def test_list_matching_objects_pass_respects_custom_max_keys(mock_s3_client: Any) -> None:
    """A small custom max_keys value still results in complete results via pagination."""
    populate_mock_s3(mock_s3_client, {TEST_BUCKET: FILES_IN_BUCKETS[TEST_BUCKET]})
    contents = list_matching_objects(f"{TEST_BUCKET}/dir_one", max_keys=1)
    keys = {obj["Key"] for obj in contents}
    assert keys == {f for f in FILES_IN_BUCKETS[TEST_BUCKET] if f.startswith("dir_one")}


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
    result = upload_file(sample_file, f"{TEST_BUCKET}/uploads", object_name="custom.txt")
    assert result is True
    obj = mock_s3_client.get_object(Bucket=TEST_BUCKET, Key="uploads/custom.txt")
    assert obj["Body"].read() == b"hello s3"


@pytest.mark.s3
def test_upload_file_skips_when_already_present(mock_s3_client: Any, sample_file: Path) -> None:
    """Verify that uploading a file that already exists is skipped, returns True, and leaves the object unchanged."""
    mock_s3_client.put_object(Bucket=TEST_BUCKET, Key=f"uploads/{sample_file.name}", Body=b"old")
    result = upload_file(sample_file, f"{TEST_BUCKET}/uploads")
    assert result is True
    # The existing object must not have been overwritten
    obj = mock_s3_client.get_object(Bucket=TEST_BUCKET, Key=f"uploads/{sample_file.name}")
    assert obj["Body"].read() == b"old"


@pytest.mark.usefixtures("mock_s3_client")
@pytest.mark.parametrize("path_type", [str, Path])
@pytest.mark.s3
def test_upload_file_accepts_str_and_path(sample_file: Path, path_type: type[str] | type[Path]) -> None:
    """Verify that upload_file accepts both str and Path objects for the local file path."""
    result = upload_file(path_type(sample_file), f"{TEST_BUCKET}/uploads")
    assert result is True


@pytest.mark.usefixtures("mock_s3_client")
@pytest.mark.s3
def test_upload_file_error(sample_file: Path) -> None:
    """Verify that upload_file raises ValueError when no destination directory is provided."""
    with pytest.raises(ValueError, match="No destination directory"):
        upload_file(sample_file, "")


@pytest.mark.usefixtures("mock_s3_client")
@pytest.mark.s3
def test_upload_file_fail_upload_error_returns_false(sample_file: Path, caplog: pytest.LogCaptureFixture) -> None:
    """A failure during the actual upload (e.g. destination bucket does not exist) returns False, not an exception."""
    with caplog.at_level(logging.ERROR):
        result = upload_file(sample_file, "nonexistent-bucket/uploads")
    assert result is False
    assert any("Error uploading to s3" in r.message for r in caplog.records)


@pytest.mark.s3
def test_upload_file_with_metadata_still_applies_default_extra_args(mock_s3_client: Any, sample_file: Path) -> None:
    """DEFAULT_EXTRA_ARGS (e.g. ChecksumAlgorithm) is preserved even when user_metadata is also supplied."""
    upload_file(sample_file, f"{TEST_BUCKET}/uploads", user_metadata={"k": "v"})
    resp = mock_s3_client.head_object(Bucket=TEST_BUCKET, Key=f"uploads/{sample_file.name}", ChecksumMode="ENABLED")
    assert "ChecksumCRC64NVME" in resp
    assert resp["Metadata"]["k"] == "v"


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


# def test_stream_to_s3_happy_path(mock_s3_client: Any) -> None:
#     """File content from the HTTP response is stored correctly in S3."""
#     content = b"hello world"
#     mock_requests, _ = make_mock_requests(content=content)

#     saved_path = stream_to_s3(TEST_URL, UPLOAD_BUCKET_KEY, mock_requests)

#     mock_requests.get.assert_called_once_with(TEST_URL, stream=True)

#     # s3 path including bucket returned
#     assert saved_path == UPLOAD_BUCKET_KEY

#     result = mock_s3_client.get_object(Bucket=ALT_BUCKET, Key=UPLOAD_TEST_KEY)
#     # check the content is correct
#     assert result["Body"].read() == content

#     # new file shows up in list_objects
#     objects = mock_s3_client.list_objects_v2(Bucket=ALT_BUCKET)["Contents"]
#     keys = [obj["Key"] for obj in objects]
#     assert UPLOAD_TEST_KEY in keys


# @pytest.mark.parametrize("content_type", [None, "application/json", "application/pdf", "text"])
# def test_stream_to_s3_sets_content_type_from_response_headers(mock_s3_client: Any, content_type: str | None) -> None:
#     """ContentType metadata on the S3 object matches the HTTP response header."""
#     content_type_args = {}
#     if content_type:
#         content_type_args["content_type"] = content_type
#     mock_requests, _ = make_mock_requests(**content_type_args)

#     stream_to_s3(TEST_URL, UPLOAD_BUCKET_KEY, mock_requests)

#     head = mock_s3_client.head_object(Bucket=ALT_BUCKET, Key=UPLOAD_TEST_KEY)
#     assert head["ContentType"] == content_type or "application/octet-stream"


# def test_stream_to_s3_raises_on_http_error_status(mock_s3_client: Any) -> None:
#     """An HTTP error status causes raise_for_status() to propagate an exception."""
#     mock_requests, mock_response = make_mock_requests(status_code=404)
#     mock_response.raise_for_status.side_effect = HTTPError("404 Not Found")

#     with (
#         pytest.raises(HTTPError, match="404 Not Found"),
#     ):
#         stream_to_s3(TEST_URL, UPLOAD_BUCKET_KEY, mock_requests)

#     with pytest.raises(ClientError, match="Not Found"):
#         mock_s3_client.head_object(Bucket=ALT_BUCKET, Key=UPLOAD_TEST_KEY)


# def test_stream_to_s3_raises_on_connection_error(mock_s3_client: Any) -> None:
#     """A network-level failure raises a ConnectionError."""
#     mock_requests, _ = make_mock_requests(status_code=404)
#     mock_requests.get.side_effect = ConnError("Network unreachable")

#     with pytest.raises(ConnError, match="Network unreachable"):
#         stream_to_s3(TEST_URL, UPLOAD_BUCKET_KEY, mock_requests)

#     with pytest.raises(ClientError, match="Not Found"):
#         mock_s3_client.head_object(Bucket=ALT_BUCKET, Key=UPLOAD_TEST_KEY)


# # FIXME: don't upload if there is nothing there?
# def test_stream_to_s3_uploads_empty_file(mock_s3_client: Any) -> None:
#     """An empty HTTP response body results in an empty S3 object."""
#     mock_requests, _ = make_mock_requests(content=b"")

#     stream_to_s3(TEST_URL, UPLOAD_BUCKET_KEY, mock_requests)

#     result = mock_s3_client.get_object(Bucket=ALT_BUCKET, Key=UPLOAD_TEST_KEY)
#     assert result["Body"].read() == b""


# def test_stream_to_s3_uploads_large_file(mock_s3_client: Any) -> None:
#     """A large payload (>5MB) is uploaded correctly via multipart."""
#     content = b"x" * (6 * 1024 * 1024)  # 6 MB
#     mock_requests, _ = make_mock_requests(content=content)

#     stream_to_s3(TEST_URL, UPLOAD_BUCKET_KEY, mock_requests)

#     result = mock_s3_client.get_object(Bucket=ALT_BUCKET, Key=UPLOAD_TEST_KEY)
#     assert result["Body"].read() == content


# @pytest.mark.skip("TODO: add test(s)")
# def test_stream_to_s3_accepts_custom_requests_implementation() -> None:
#     """A subclassed or alternate requests module works as a drop-in."""
#     # TODO: add test here?


# @pytest.mark.parametrize("bucket", BUCKETS)
# @pytest.mark.parametrize("protocol", ["", "s3://", "s3a://"])
# @pytest.mark.s3
# def test_download_file_retrieves_correct_content(
#     mock_s3_client: Any, protocol: str, bucket: str, tmp_path: Path
# ) -> None:
#     """Verify that download_file writes the correct file content to disk for each valid bucket."""
#     content = b"some important content"
#     mock_s3_client.put_object(Bucket=bucket, Key="remote/data.txt", Body=content, **DEFAULT_EXTRA_ARGS)
#     local_file = str(tmp_path / "data.txt")
#     download_file(f"{protocol}{bucket}/remote/data.txt", local_file)
#     assert Path(local_file).read_bytes() == content


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
        FileNotFoundError,
        match=f"File not found: {bucket}/{key}",
    ):
        download_file(f"{bucket}/{key}", tmp_path / "file.txt")


@pytest.mark.s3
def test_download_file_fail_non_404_error_propagates(
    mock_s3_client: Any, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """A non-404 ClientError while checking object existence propagates rather than being treated as 'not found'."""

    def raise_access_denied(**kwargs: Any) -> None:
        raise ClientError(
            {"Error": {"Code": "AccessDenied"}, "ResponseMetadata": {"HTTPStatusCode": 403}},
            "HeadObject",
        )

    monkeypatch.setattr(mock_s3_client, "head_object", raise_access_denied)
    with pytest.raises(ClientError, match="AccessDenied"):
        download_file(f"{TEST_BUCKET}/some/key.txt", tmp_path / "file.txt")


@pytest.mark.s3
@pytest.mark.usefixtures("mock_s3_client")
def test_download_file_fail_directory_creation_error_other_than_exists_propagates(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """A directory-creation failure other than 'already exists' (e.g. permission denied) is logged and re-raised."""
    err_msg = "Permission denied"

    def raise_permission_error(self: Path, *args: Any, **kwargs: Any) -> None:
        raise PermissionError(err_msg)

    monkeypatch.setattr(Path, "mkdir", raise_permission_error)

    with pytest.raises(PermissionError, match=err_msg):
        download_file(f"{TEST_BUCKET}/some/key.txt", tmp_path / "newdir" / "file.txt")

    assert any("Could not save s3 file" in r.message for r in caplog.records)


@pytest.mark.s3
def test_download_file_pass_version_id_downloads_specific_version(mock_s3_client: Any, tmp_path: Path) -> None:
    """Passing version_id downloads the specified version rather than the latest one."""
    bucket = TEST_BUCKET
    key = "versioned/file.txt"
    mock_s3_client.put_bucket_versioning(Bucket=bucket, VersioningConfiguration={"Status": "Enabled"})

    first = mock_s3_client.put_object(Bucket=bucket, Key=key, Body=b"version one")
    mock_s3_client.put_object(Bucket=bucket, Key=key, Body=b"version two")
    version_id = first["VersionId"]

    local_file = tmp_path / "file.txt"
    download_file(f"{bucket}/{key}", local_file, version_id=version_id)
    assert local_file.read_bytes() == b"version one"

    # sanity check: no version_id downloads the latest
    local_file_latest = tmp_path / "latest.txt"
    download_file(f"{bucket}/{key}", local_file_latest)
    assert local_file_latest.read_bytes() == b"version two"


@pytest.mark.s3
def test_download_file_fail_progress_bar_total_reflects_latest_not_requested_version(
    mock_s3_client: Any, tmp_path: Path
) -> None:
    """Documents a current quirk: the progress bar total always reflects the LATEST object version's
    size, even when downloading an older version_id whose actual size differs, because
    get_existing_object_info() is called without version_id.
    """
    bucket = TEST_BUCKET
    key = "versioned/file2.txt"
    mock_s3_client.put_bucket_versioning(Bucket=bucket, VersioningConfiguration={"Status": "Enabled"})

    first = mock_s3_client.put_object(Bucket=bucket, Key=key, Body=b"short")
    latest_body = b"a much longer body than the first version"
    mock_s3_client.put_object(Bucket=bucket, Key=key, Body=latest_body)
    version_id = first["VersionId"]

    captured_totals: list[int | None] = []
    original_make_progress_bar = object_utils.make_progress_bar

    def spy_make_progress_bar(*, total: int | None, **kwargs: Any) -> Any:
        captured_totals.append(total)
        return original_make_progress_bar(total=total, **kwargs)

    with patch.object(object_utils, "make_progress_bar", side_effect=spy_make_progress_bar):
        local_file = tmp_path / "file2.txt"
        download_file(f"{bucket}/{key}", local_file, version_id=version_id)

    assert local_file.read_bytes() == b"short"
    assert captured_totals[0] == len(latest_body)
    assert captured_totals[0] != len(b"short")


# upload_dir
@pytest.mark.parametrize("bucket", [TEST_BUCKET, ALT_BUCKET])
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
    bucket = TEST_BUCKET
    result = upload_dir(path_type(sample_dir), f"{bucket}/remote")
    assert result is True
    keys = {obj["Key"] for obj in mock_s3_client.list_objects_v2(Bucket=bucket)["Contents"]}
    assert keys == {f"remote/{f}" for f in SAMPLE_FILES}


@pytest.mark.usefixtures("mock_s3_client")
@pytest.mark.s3
def test_upload_dir_raises_on_empty_source() -> None:
    """Verify that upload_dir raises ValueError when no source directory is provided."""
    with pytest.raises(ValueError, match="No source directory"):
        upload_dir("", f"{TEST_BUCKET}/remote")


@pytest.mark.usefixtures("mock_s3_client")
@pytest.mark.s3
def test_upload_dir_raises_on_empty_destination(sample_dir: Path) -> None:
    """Verify that upload_dir raises ValueError when no destination directory is provided."""
    with pytest.raises(ValueError, match="No destination directory"):
        upload_dir(sample_dir, "")


@pytest.mark.s3
def test_upload_dir_pass_respects_custom_file_glob(mock_s3_client: Any, sample_dir: Path) -> None:
    """A custom file_glob restricts which files get uploaded."""
    result = upload_dir(sample_dir, f"{TEST_BUCKET}/remote", file_glob="dir_one/*.txt")
    assert result is True
    keys = {obj["Key"] for obj in mock_s3_client.list_objects_v2(Bucket=TEST_BUCKET)["Contents"]}
    assert keys == {"remote/dir_one/file1.txt", "remote/dir_one/file2.txt"}


@pytest.mark.s3
def test_upload_dir_pass_empty_directory_returns_true_and_uploads_nothing(mock_s3_client: Any, tmp_path: Path) -> None:
    """An empty source directory (no matching files) returns True without uploading anything."""
    empty_dir = tmp_path / "empty"
    empty_dir.mkdir()
    result = upload_dir(empty_dir, f"{TEST_BUCKET}/remote")
    assert result is True
    assert "Contents" not in mock_s3_client.list_objects_v2(Bucket=TEST_BUCKET)


@pytest.mark.usefixtures("mock_s3_client")
@pytest.mark.s3
def test_upload_dir_fail_partial_failure_returns_false(sample_dir: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """If any individual file upload fails, upload_dir returns False even though other files succeeded."""
    original_upload_file = object_utils.upload_file
    call_count = {"n": 0}

    def flaky_upload_file(*args: Any, **kwargs: Any) -> bool:
        call_count["n"] += 1
        if call_count["n"] == 1:
            return False
        return original_upload_file(*args, **kwargs)

    monkeypatch.setattr(object_utils, "upload_file", flaky_upload_file)
    result = object_utils.upload_dir(sample_dir, f"{TEST_BUCKET}/remote")
    assert result is False
    assert call_count["n"] == len(SAMPLE_FILES)


# copy_object
@pytest.mark.parametrize("destination", BUCKETS)
@pytest.mark.s3
def test_copy_object(mocked_s3_client_no_checksum: Any, destination: str) -> None:
    """Verify that copy_object copies an object to a new key within the same bucket."""
    mocked_s3_client_no_checksum.put_object(Bucket=TEST_BUCKET, Key="src/file.txt", Body=b"copy me")
    assert object_exists(f"{TEST_BUCKET}/src/file.txt")
    response = copy_object(f"{TEST_BUCKET}/src/file.txt", f"{destination}/dst/path/to/file.txt")

    # check both objects exist
    assert object_exists(f"{TEST_BUCKET}/src/file.txt")
    assert object_exists(f"{destination}/dst/path/to/file.txt")

    obj = mocked_s3_client_no_checksum.get_object(Bucket=destination, Key="dst/path/to/file.txt")
    assert obj["Body"].read() == b"copy me"
    assert response["ResponseMetadata"]["HTTPStatusCode"] == HTTP_200


@pytest.mark.s3
@pytest.mark.usefixtures("mock_s3_client")
def test_copy_object_source_object_nonexistent() -> None:
    """Ensure that the code throws an error if the source object does not exist."""
    s3_path = f"{TEST_BUCKET}/some/path/to/file"
    assert object_exists(s3_path) is False
    with pytest.raises(Exception, match="The specified key does not exist"):
        copy_object(s3_path, f"{TEST_BUCKET}/a/different/path/to/file")


@pytest.mark.s3
@pytest.mark.usefixtures("mock_s3_client")
def test_copy_object_source_bucket_nonexistent() -> None:
    """Ensure that the code throws an error if the bucket does not exist."""
    s3_path = "some-bucket/some/path/to/file"
    assert object_exists(s3_path) is False
    with pytest.raises(Exception, match="The specified bucket does not exist"):
        copy_object(s3_path, f"{TEST_BUCKET}/a/different/path/to/file")


@pytest.mark.s3
@pytest.mark.usefixtures("mock_s3_client")
def test_copy_file_source_object_nonexistent() -> None:
    """Ensure that the code throws an error if the source object does not exist."""
    s3_path = f"{TEST_BUCKET}/some/path/to/file"
    assert object_exists(s3_path) is False
    with pytest.raises(Exception, match="The specified key does not exist"):
        copy_object(s3_path, f"{TEST_BUCKET}/a/different/path/to/file")


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
    populate_mock_s3(mock_s3_client, {TEST_BUCKET: FILES_IN_BUCKETS[TEST_BUCKET]})
    source_bucket_files = list_keys(mock_s3_client, TEST_BUCKET)
    assert set(source_bucket_files) == set(SAMPLE_FILES)
    dest_bucket_files = list_keys(mock_s3_client, ALT_BUCKET)
    assert dest_bucket_files == set()

    successes, errors = copy_directory(
        f"s3://{TEST_BUCKET}/dir_one{source_suffix}", f"s3://{ALT_BUCKET}/some/destination/dir{dest_suffix}"
    )

    assert errors == {}
    expected_files = {
        f"{TEST_BUCKET}/{f}": f"{ALT_BUCKET}/some/destination/dir{f.replace('dir_one', '')}" for f in SAMPLE_FILES
    }
    assert successes == expected_files
    # ensure that the original files are still in place
    assert set(list_keys(mock_s3_client, TEST_BUCKET)) == set(SAMPLE_FILES)
    # destination should have new files from the source
    assert set(list_keys(mock_s3_client, ALT_BUCKET)) == {
        f.removeprefix(f"{ALT_BUCKET}/") for f in expected_files.values()
    }

    # check the content
    for src, dest in expected_files.items():
        src_resp = mock_s3_client.get_object(Bucket=TEST_BUCKET, Key=src.removeprefix(f"{TEST_BUCKET}/"))
        assert src_resp["Body"].read() == src.encode()
        dest_resp = mock_s3_client.get_object(Bucket=ALT_BUCKET, Key=dest.removeprefix(f"{ALT_BUCKET}/"))
        assert dest_resp["Body"].read() == src.encode()


@pytest.mark.s3
def test_copy_directory_copy_within_same_bucket(mock_s3_client: Any) -> None:
    """Verify that copying between two prefixes within the same bucket works correctly."""
    populate_mock_s3(mock_s3_client, {TEST_BUCKET: ["foo/a.txt", "foo/b.txt"]})

    successes, errors = copy_directory(f"s3://{TEST_BUCKET}/foo", f"s3://{TEST_BUCKET}/bar")

    assert successes == {
        f"{TEST_BUCKET}/foo/a.txt": f"{TEST_BUCKET}/bar/a.txt",
        f"{TEST_BUCKET}/foo/b.txt": f"{TEST_BUCKET}/bar/b.txt",
    }
    assert errors == {}
    assert list_keys(mock_s3_client, TEST_BUCKET, prefix="bar") == {"bar/a.txt", "bar/b.txt"}
    assert list_keys(mock_s3_client, TEST_BUCKET) == {"foo/a.txt", "foo/b.txt", "bar/a.txt", "bar/b.txt"}


@pytest.mark.s3
@pytest.mark.usefixtures("mock_s3_client")
def test_copy_directory_empty_directory_returns_empty_dicts() -> None:
    """Verify that when the source prefix matches no objects, both the successes and errors dictionaries are returned empty."""
    successes, errors = copy_directory(f"s3://{TEST_BUCKET}/nonexistent/", f"s3://{ALT_BUCKET}/bar/")

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
            TEST_BUCKET: [
                "foo/a.txt",
                "foobar/should-not-be-copied.txt",
            ]
        },
    )
    copy_directory(f"s3://{TEST_BUCKET}/foo", f"s3://{ALT_BUCKET}/bar")
    assert list_keys(mock_s3_client, ALT_BUCKET) == {"bar/a.txt"}


@pytest.mark.s3
@pytest.mark.usefixtures("mock_s3_client")
def test_copy_directory_missing_source_bucket_returns_error() -> None:
    """Verify that when the source bucket does not exist, botocore throws an error."""
    # FIXME: throws a s3.Client.exceptions.NoSuchBucket
    with pytest.raises(Exception, match="The specified bucket does not exist"):
        copy_directory("s3://nonexistent-bucket/bar", f"s3://{TEST_BUCKET}/foo")


@pytest.mark.s3
@pytest.mark.usefixtures("mock_s3_client")
def test_copy_directory_missing_dest_bucket_records_errors(mock_s3_client: Any) -> None:
    """Verify that when the destination bucket does not exist, the errors dict contains all objects under the original dir."""
    # FIXME: throw a bucket not exists error?
    populate_mock_s3(mock_s3_client, {TEST_BUCKET: ["foo/a.txt", "foo/b.txt"]})

    # with pytest.raises(FileNotFoundError, match="The specified bucket does not exist"):
    successes, errors = copy_directory(f"s3://{TEST_BUCKET}/foo", "s3://nonexistent-bucket/bar")

    assert successes == {}
    assert f"{TEST_BUCKET}/foo/a.txt" in errors
    assert f"{TEST_BUCKET}/foo/b.txt" in errors
    assert isinstance(errors[f"{TEST_BUCKET}/foo/a.txt"], Exception)


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
    result = upload_file(sample_file, f"{TEST_BUCKET}/uploads", user_metadata={"k": "v"}, object_name="renamed.txt")
    assert result is True
    obj = mock_s3_client.get_object(Bucket=TEST_BUCKET, Key="uploads/renamed.txt")
    assert obj["Body"].read() == b"hello s3"


@pytest.mark.s3
def test_upload_file_with_metadata_overwrites_existing(mock_s3_client: Any, sample_file: Path) -> None:
    """Verify that upload_file with metadata uploads even when the object already exists."""
    mock_s3_client.put_object(Bucket=TEST_BUCKET, Key=f"uploads/{sample_file.name}", Body=b"old")
    result = upload_file(sample_file, f"{TEST_BUCKET}/uploads", user_metadata={"new": "true"})
    assert result is True
    obj = mock_s3_client.get_object(Bucket=TEST_BUCKET, Key=f"uploads/{sample_file.name}")
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
    result = upload_file(path_type(sample_file), f"{TEST_BUCKET}/uploads", user_metadata={})
    assert result is True


# head_object
@pytest.mark.s3
def test_head_object_returns_info(mock_s3_client: Any) -> None:
    """Verify that head_object returns size, metadata, and checksum fields."""
    mock_s3_client.put_object(Bucket=TEST_BUCKET, Key="info/file.txt", Body=b"hello", Metadata={"md5": "abc123"})
    result = head_object(f"{TEST_BUCKET}/info/file.txt")
    assert result is not None
    assert result["ContentLength"] == SIZE_HELLO
    assert result["Metadata"]["md5"] == "abc123"


@pytest.mark.s3
@pytest.mark.usefixtures("mock_s3_client")
def test_head_object_raises_for_missing() -> None:
    """Verify that head_object returns None for a non-existent object."""
    with pytest.raises(ClientError, match="404"):
        _ = head_object(f"{TEST_BUCKET}/does/not/exist.txt")


@pytest.mark.parametrize("protocol", ["", "s3://", "s3a://"])
@pytest.mark.s3
def test_head_object_with_protocols(mock_s3_client: Any, protocol: str) -> None:
    """Verify that head_object handles all valid protocol prefixes."""
    mock_s3_client.put_object(Bucket=TEST_BUCKET, Key="proto/file.txt", Body=b"data")
    result = head_object(f"{protocol}{TEST_BUCKET}/proto/file.txt")
    assert result is not None
    assert result["ContentLength"] == SIZE_DATA


# copy_object
@pytest.mark.parametrize("destination", BUCKETS)
@pytest.mark.s3
def test_copy_object_preserves_user_metadata(mocked_s3_client_no_checksum: Any, destination: str) -> None:
    """copy_object preserves source user metadata (MetadataDirective=COPY default)."""
    mocked_s3_client_no_checksum.put_object(
        Bucket=TEST_BUCKET, Key="src/file.txt", Body=b"archive me", Metadata={"md5": "abc123"}
    )
    response = copy_object(
        f"{TEST_BUCKET}/src/file.txt",
        f"{destination}/archive/file.txt",
    )
    assert response["ResponseMetadata"]["HTTPStatusCode"] == HTTP_200

    # source user metadata is preserved (MetadataDirective=COPY)
    resp = mocked_s3_client_no_checksum.head_object(Bucket=destination, Key="archive/file.txt")
    assert resp["Metadata"].get("md5") == "abc123"

    # verify source still exists
    assert object_exists(f"{TEST_BUCKET}/src/file.txt")


@pytest.mark.s3
def test_copy_object_preserves_content(mocked_s3_client_no_checksum: Any) -> None:
    """Verify that the content of the copied object matches the original."""
    mocked_s3_client_no_checksum.put_object(Bucket=TEST_BUCKET, Key="src/data.bin", Body=b"binary data")
    copy_object(
        f"{TEST_BUCKET}/src/data.bin",
        f"{TEST_BUCKET}/dst/data.bin",
    )
    obj = mocked_s3_client_no_checksum.get_object(Bucket=TEST_BUCKET, Key="dst/data.bin")
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


# delete_objects
@pytest.mark.s3
def test_delete_objects_pass_splits_into_batches_of_1000(mock_s3_client: Any) -> None:
    """More than 1000 keys are deleted via multiple batched API calls, each capped at 1000 objects."""
    total_keys = 1005
    keys = [f"batch/file_{i:04d}.txt" for i in range(total_keys)]
    for k in keys:
        mock_s3_client.put_object(Bucket=TEST_BUCKET, Key=k, Body=b"x")

    original_delete_objects = mock_s3_client.delete_objects
    call_sizes: list[int] = []

    def spy_delete_objects(**kwargs: Any) -> Any:
        call_sizes.append(len(kwargs["Delete"]["Objects"]))
        return original_delete_objects(**kwargs)

    mock_s3_client.delete_objects = spy_delete_objects

    errors = delete_objects(TEST_BUCKET, keys)

    assert errors == []
    assert call_sizes == [1000, 5]
    for k in keys:
        assert object_exists(f"{TEST_BUCKET}/{k}") is False


@pytest.mark.s3
def test_delete_objects_pass_propagates_per_key_errors_from_response(mock_s3_client: Any) -> None:
    """Per-key errors returned in the S3 response's Errors field are surfaced in the returned list."""
    fake_error = {"Key": "bad/key.txt", "Code": "AccessDenied", "Message": "Access Denied"}
    original_delete_objects = mock_s3_client.delete_objects

    def mock_delete_objects(**kwargs: Any) -> Any:
        resp = original_delete_objects(**kwargs)
        resp["Errors"] = [fake_error]
        return resp

    mock_s3_client.delete_objects = mock_delete_objects
    errors = delete_objects(TEST_BUCKET, ["some/key.txt"])
    assert errors == [fake_error]


@pytest.mark.s3
def test_delete_objects_removes_all(mock_s3_client: Any) -> None:
    """delete_objects removes every listed key in a single call."""
    keys = ["bulk/a.txt", "bulk/b.txt", "bulk/c.txt"]
    for k in keys:
        mock_s3_client.put_object(Bucket=TEST_BUCKET, Key=k, Body=b"data")

    errors = delete_objects(TEST_BUCKET, keys)

    assert errors == []
    for k in keys:
        assert object_exists(f"{TEST_BUCKET}/{k}") is False


@pytest.mark.s3
def test_delete_objects_empty_list_is_noop(mock_s3_client: Any) -> None:
    """delete_objects with an empty list makes no API call and returns no errors."""
    mock_s3_client.put_object(Bucket=TEST_BUCKET, Key="keep/me.txt", Body=b"safe")
    errors = delete_objects(TEST_BUCKET, [])
    assert errors == []
    assert object_exists(f"{TEST_BUCKET}/keep/me.txt") is True


@pytest.mark.s3
def test_delete_objects_nonexistent_keys_no_error(mock_s3_client: Any) -> None:
    """Deleting keys that don't exist returns no errors (S3 delete is idempotent)."""
    errors = delete_objects(TEST_BUCKET, ["ghost/a.txt", "ghost/b.txt"])
    assert errors == []
