"""Unit tests for streamer.py (streamer HTTP resources directly into S3)."""

import hashlib
import io
import logging
from dataclasses import dataclass
from typing import Any, Final, Generator
from unittest.mock import MagicMock, patch

import pytest
import requests

from cdm_data_loaders.utils.file_transfer.checksums import HashingReader
from cdm_data_loaders.utils.file_transfer.core import (
    ChecksumMismatchError,
    DownloadError,
    NonRetryableDownloadError,
)
from cdm_data_loaders.utils.file_transfer.s3 import streamer
from cdm_data_loaders.utils.file_transfer.s3.object_utils import (
    CHECKSUM_ALGORITHM_METADATA_KEY,
    CHECKSUM_VALUE_METADATA_KEY,
    DEFAULT_EXTRA_ARGS,
    S3ObjectInfo,
    SkipDecision,
)
from cdm_data_loaders.utils.file_transfer.s3.streamer import (
    DEFAULT_CHECKSUM_ALGORITHM,
    S3StreamUploader,
    S3UploadCore,
    stream_to_s3,
)
from tests.conftest import DEFAULT_VCR_CONFIG
import cdm_data_loaders.utils.file_transfer.s3.client as s3_client

TEST_URL: Final[str] = "https://example.com/test-file.pdf"
TEST_BUCKET: Final[str] = "test-bucket"
TEST_KEY: Final[str] = "path/to/file.pdf"
S3_PATH: Final[str] = f"{TEST_BUCKET}/{TEST_KEY}"
TEST_ECHO_URL: Final[str] = "https://httpbin.org/bytes/1024"  # deterministic-size real endpoint


@pytest.fixture
def requests_session() -> Generator[requests.Session, Any]:
    """A requests session."""
    with requests.Session() as s:
        yield s


@pytest.fixture(scope="module")
def vcr_config() -> dict[str, Any]:
    """VCR config for tests that make HTTP requests."""
    return {**DEFAULT_VCR_CONFIG}


@dataclass
class FakeResponse:
    """Minimal stand-in for requests.Response, for tests that only need status_code/reason."""

    status_code: int
    reason: str = "reason"


def make_mock_get_response(
    content: bytes = b"hello world",
    status_code: int = 200,
    headers: dict[str, str] | None = None,
) -> MagicMock:
    """Build a mock streamed requests.Response usable as a context manager."""
    response = MagicMock()
    response.status_code = status_code
    response.reason = "OK" if status_code < 400 else "Error"  # noqa: PLR2004
    response.headers = (
        headers
        if headers is not None
        else {"content-length": str(len(content)), "content-type": "application/octet-stream"}
    )
    response.raw = MagicMock()
    response.raw.read = MagicMock(side_effect=[content, b""])
    response.raise_for_status = MagicMock()
    response.__enter__ = MagicMock(return_value=response)
    response.__exit__ = MagicMock(return_value=False)
    return response


def make_mock_requests_module(
    get_response: MagicMock | None = None, head_response: MagicMock | None = None
) -> MagicMock:
    """Build a mock requests module whose get()/head() return the given mock responses."""
    mock_requests = MagicMock()
    mock_requests.get.return_value = get_response if get_response is not None else make_mock_get_response()
    mock_requests.head.return_value = (
        head_response if head_response is not None else make_mock_get_response(content=b"")
    )
    mock_requests.exceptions = requests.exceptions
    return mock_requests


def draining_upload_fileobj(fileobj: Any, *_args: Any, **_kwargs: Any) -> None:
    """Simulate s3transfer actually reading the fileobj, so any wrapping HashingReader sees real bytes."""
    while True:
        chunk = fileobj.read(65536)
        if not chunk:
            break


@pytest.fixture
def fake_s3_client() -> MagicMock:
    """A bare MagicMock standing in for a boto3 S3 client, with upload_fileobj draining its input."""
    client = MagicMock()
    client.upload_fileobj = MagicMock(side_effect=draining_upload_fileobj)
    client.delete_object = MagicMock()
    return client


"""Tests for S3UploadCore.validate_response"""


@pytest.mark.parametrize("status", [200, 201, 204, 299, 300, 399])
def test_validate_response_pass_no_raise_for_non_error_status(status: int) -> None:
    """Statuses below 400 do not raise."""
    S3UploadCore.validate_response(FakeResponse(status_code=status))  # pyright: ignore[reportArgumentType]


@pytest.mark.parametrize("status", [400, 401, 403, 404, 499])
def test_validate_response_fail_client_error_raises_non_retryable(status: int) -> None:
    """4xx statuses raise NonRetryableDownloadError, including the status code and reason."""
    with pytest.raises(NonRetryableDownloadError, match=f"Client error: {status}"):
        S3UploadCore.validate_response(FakeResponse(status_code=status, reason="Bad"))  # pyright: ignore[reportArgumentType]


@pytest.mark.parametrize("status", [500, 502, 503, 599])
def test_validate_response_fail_server_error_raises_retryable(status: int) -> None:
    """5xx statuses raise DownloadError (retryable), including the status code and reason."""
    with pytest.raises(DownloadError, match=f"Server error: {status}"):
        S3UploadCore.validate_response(FakeResponse(status_code=status, reason="Boom"))  # pyright: ignore[reportArgumentType]


"""Tests for S3UploadCore.check_hash"""


@pytest.mark.vcr
def test_check_hash_pass_with_real_response(requests_session: requests.Session, fake_s3_client: MagicMock) -> None:
    """check_hash should not delete the object when the real response matches the expected checksum."""
    resp = requests_session.get(TEST_ECHO_URL, stream=True)
    resp.raw.decode_content = True

    hasher = HashingReader(resp.raw, "sha256")
    body = hasher.read(-1)
    expected = hashlib.sha256(body).hexdigest()

    S3UploadCore.check_hash(TEST_URL, TEST_BUCKET, TEST_KEY, expected, "sha256", hasher, fake_s3_client)

    fake_s3_client.delete_object.assert_not_called()


@pytest.mark.vcr
def test_check_hash_fail_with_real_response_deletes_and_raises(
    requests_session: requests.Session, fake_s3_client: MagicMock
) -> None:
    """A deliberately wrong expected checksum against a real response should delete and raise."""
    resp = requests_session.get(TEST_ECHO_URL, stream=True)
    resp.raw.decode_content = True

    hasher = HashingReader(resp.raw, "sha256")
    hasher.read(-1)
    wrong_expected = "0" * 64

    with pytest.raises(ChecksumMismatchError):
        S3UploadCore.check_hash(TEST_URL, TEST_BUCKET, TEST_KEY, wrong_expected, "sha256", hasher, fake_s3_client)

    fake_s3_client.delete_object.assert_called_once_with(Bucket=TEST_BUCKET, Key=TEST_KEY)


def test_check_hash_pass_matching_checksum_does_not_raise_or_delete(fake_s3_client: MagicMock) -> None:
    """A matching checksum does not raise and does not delete the uploaded object."""
    data = b"some content"
    hasher = HashingReader(io.BytesIO(data), "sha256")
    hasher.read(-1)
    expected = hashlib.sha256(data).hexdigest()

    S3UploadCore.check_hash(TEST_URL, TEST_BUCKET, TEST_KEY, expected, "sha256", hasher, fake_s3_client)

    fake_s3_client.delete_object.assert_not_called()


def test_check_hash_pass_comparison_is_case_insensitive(fake_s3_client: MagicMock) -> None:
    """Checksum comparison ignores case on both sides."""
    data = b"case test"
    hasher = HashingReader(io.BytesIO(data), "sha256")
    hasher.read(-1)
    expected = hashlib.sha256(data).hexdigest().upper()

    S3UploadCore.check_hash(TEST_URL, TEST_BUCKET, TEST_KEY, expected, "sha256", hasher, fake_s3_client)
    fake_s3_client.delete_object.assert_not_called()


def test_check_hash_fail_mismatch_deletes_object_and_raises(fake_s3_client: MagicMock) -> None:
    """A mismatched checksum deletes the uploaded object and raises ChecksumMismatchError with both digests."""
    data = b"actual content"
    hasher = HashingReader(io.BytesIO(data), "sha256")
    hasher.read(-1)
    wrong_expected = "0" * 64

    with pytest.raises(ChecksumMismatchError, match=f"expected={wrong_expected}"):
        S3UploadCore.check_hash(TEST_URL, TEST_BUCKET, TEST_KEY, wrong_expected, "sha256", hasher, fake_s3_client)

    fake_s3_client.delete_object.assert_called_once_with(Bucket=TEST_BUCKET, Key=TEST_KEY)


"""Tests for S3UploadCore.get_content_length"""


def test_get_content_length_pass_returns_int_when_present() -> None:
    """A valid content-length header is parsed as an int."""
    response = MagicMock(headers={"content-length": "12345"})
    assert S3UploadCore.get_content_length(response) == 12345


def test_get_content_length_pass_returns_none_when_missing() -> None:
    """A missing content-length header returns None."""
    response = MagicMock(headers={})
    assert S3UploadCore.get_content_length(response) is None


def test_get_content_length_pass_returns_none_when_unparseable() -> None:
    """A non-numeric content-length header returns None instead of raising."""
    response = MagicMock(headers={"content-length": "not-a-number"})
    assert S3UploadCore.get_content_length(response) is None


"""Tests for S3UploadCore.get_remote_size"""


def test_get_remote_size_pass_returns_size_on_success() -> None:
    """A successful HEAD request with a content-length header returns the parsed size."""
    mock_requests = make_mock_requests_module(head_response=MagicMock(headers={"content-length": "999"}))
    mock_requests.head.return_value.raise_for_status = MagicMock()
    assert S3UploadCore.get_remote_size(mock_requests, TEST_URL, None, None) == 999


def test_get_remote_size_pass_passes_extra_headers_and_no_timeout_key_when_none() -> None:
    """extra_headers are forwarded; timeout is omitted from kwargs when None."""
    mock_requests = make_mock_requests_module(head_response=MagicMock(headers={"content-length": "1"}))
    S3UploadCore.get_remote_size(mock_requests, TEST_URL, {"X-Test": "1"}, None)
    _, kwargs = mock_requests.head.call_args
    assert kwargs["headers"] == {"X-Test": "1"}
    assert "timeout" not in kwargs


def test_get_remote_size_pass_passes_timeout_when_given() -> None:
    """Timeout is included in the HEAD request kwargs when explicitly provided."""
    mock_requests = make_mock_requests_module(head_response=MagicMock(headers={"content-length": "1"}))
    S3UploadCore.get_remote_size(mock_requests, TEST_URL, None, 5.0)
    _, kwargs = mock_requests.head.call_args
    assert kwargs["timeout"] == 5.0


def test_get_remote_size_pass_missing_content_length_returns_none() -> None:
    """A successful HEAD request without a content-length header returns None."""
    mock_requests = make_mock_requests_module(head_response=MagicMock(headers={}))
    assert S3UploadCore.get_remote_size(mock_requests, TEST_URL, None, None) is None


def test_get_remote_size_pass_unparseable_content_length_returns_none() -> None:
    """A non-numeric content-length header returns None."""
    mock_requests = make_mock_requests_module(head_response=MagicMock(headers={"content-length": "bad"}))
    assert S3UploadCore.get_remote_size(mock_requests, TEST_URL, None, None) is None


def test_get_remote_size_pass_head_exception_returns_none_and_warns(caplog: pytest.LogCaptureFixture) -> None:
    """Any exception from the HEAD request (network error, raise_for_status, etc.) results in None with a warning logged."""
    mock_requests = MagicMock()
    mock_requests.head.side_effect = requests.exceptions.ConnectionError("unreachable")

    with caplog.at_level(logging.WARNING):
        result = S3UploadCore.get_remote_size(mock_requests, TEST_URL, None, None)

    assert result is None
    assert any("HEAD request failed" in r.message for r in caplog.records)


def test_get_remote_size_pass_raise_for_status_error_returns_none() -> None:
    """An HTTP error status surfaced via raise_for_status() is treated the same as a network failure."""
    head_response = MagicMock(headers={"content-length": "1"})
    head_response.raise_for_status.side_effect = requests.exceptions.HTTPError("404")
    mock_requests = make_mock_requests_module(head_response=head_response)
    assert S3UploadCore.get_remote_size(mock_requests, TEST_URL, None, None) is None


"""Tests for S3UploadCore.check_existing"""


def test_check_existing_pass_no_object_returns_false_without_size_check() -> None:
    """When no object exists, False is returned and no remote-size lookup is attempted."""
    with patch.object(streamer, "get_existing_object_info", return_value=None) as mock_get_info:
        mock_requests = make_mock_requests_module()
        result = S3UploadCore.check_existing(
            TEST_URL, TEST_BUCKET, TEST_KEY, MagicMock(), mock_requests, None, None, None
        )
    assert result is False
    mock_get_info.assert_called_once_with(TEST_BUCKET, TEST_KEY)
    mock_requests.head.assert_not_called()


def test_check_existing_pass_uses_expected_size_without_head_request() -> None:
    """When expected_size is supplied, get_remote_size (and thus a HEAD request) is skipped entirely."""
    existing = S3ObjectInfo(size=100, etag="e", metadata={})
    with (
        patch.object(streamer, "get_existing_object_info", return_value=existing),
        patch.object(streamer, "decide_skip", return_value=SkipDecision(skip=True, reason="ok")) as mock_decide,
    ):
        mock_requests = make_mock_requests_module()
        S3UploadCore.check_existing(
            TEST_URL, TEST_BUCKET, TEST_KEY, MagicMock(), mock_requests, None, None, None, expected_size=100
        )
    mock_requests.head.assert_not_called()
    mock_decide.assert_called_once_with(existing, 100, None)


def test_check_existing_pass_falls_back_to_head_request_when_no_expected_size() -> None:
    """Without expected_size, a HEAD request is made to determine the remote size for comparison."""
    existing = S3ObjectInfo(size=100, etag="e", metadata={})
    with (
        patch.object(streamer, "get_existing_object_info", return_value=existing),
        patch.object(streamer, "decide_skip", return_value=SkipDecision(skip=False, reason="ok")),
    ):
        mock_requests = make_mock_requests_module(head_response=MagicMock(headers={"content-length": "42"}))
        S3UploadCore.check_existing(TEST_URL, TEST_BUCKET, TEST_KEY, MagicMock(), mock_requests, None, None, None)
    mock_requests.head.assert_called_once()


@pytest.mark.parametrize(
    ("skip", "confident", "expected_level"),
    [
        (True, True, logging.INFO),
        (True, False, logging.WARNING),
        (False, True, logging.INFO),
        (False, False, logging.WARNING),
    ],
)
def test_check_existing_pass_logs_at_level_matching_confidence(
    caplog: pytest.LogCaptureFixture, skip: bool, confident: bool, expected_level: int
) -> None:
    """Log level reflects the confidence of the skip decision, regardless of whether skip is True or False."""
    existing = S3ObjectInfo(size=100, etag="e", metadata={})
    with (
        patch.object(streamer, "get_existing_object_info", return_value=existing),
        patch.object(
            streamer, "decide_skip", return_value=SkipDecision(skip=skip, reason="reason text", confident=confident)
        ),
        caplog.at_level(logging.INFO),
    ):
        result = S3UploadCore.check_existing(
            TEST_URL, TEST_BUCKET, TEST_KEY, MagicMock(), make_mock_requests_module(), None, None, None, expected_size=1
        )
    assert result is skip
    assert caplog.records[-1].levelno == expected_level
    assert "reason text" in caplog.records[-1].message


"""Fixtures/tests for S3UploadCore.perform_upload"""


@pytest.fixture
def no_skip_check(monkeypatch: pytest.MonkeyPatch) -> None:
    """Force skip_if_exists=False semantics by making check_existing always return False if called."""
    monkeypatch.setattr(S3UploadCore, "check_existing", MagicMock(return_value=False))


def test_perform_upload_pass_uploads_and_returns_bucket_key(fake_s3_client: MagicMock) -> None:
    """A basic, non-checksummed upload succeeds and returns 'bucket/key'."""
    mock_requests = make_mock_requests_module()
    result = S3UploadCore.perform_upload(
        TEST_URL, S3_PATH, fake_s3_client, mock_requests, None, None, None, None, skip_if_exists=False
    )
    assert result == f"{TEST_BUCKET}/{TEST_KEY}"
    fake_s3_client.upload_fileobj.assert_called_once()
    mock_requests.get.assert_called_once_with(TEST_URL, stream=True, headers={})


def test_perform_upload_pass_skip_if_exists_true_and_can_skip_returns_early(fake_s3_client: MagicMock) -> None:
    """When check_existing reports the upload can be skipped, no HTTP GET or S3 upload happens."""
    mock_requests = make_mock_requests_module()
    with patch.object(S3UploadCore, "check_existing", return_value=True):
        result = S3UploadCore.perform_upload(
            TEST_URL, S3_PATH, fake_s3_client, mock_requests, None, None, None, None, skip_if_exists=True, force=False
        )
    assert result == f"{TEST_BUCKET}/{TEST_KEY}"
    mock_requests.get.assert_not_called()
    fake_s3_client.upload_fileobj.assert_not_called()


def test_perform_upload_pass_force_bypasses_skip_check(fake_s3_client: MagicMock) -> None:
    """force=True skips the existence check entirely, even with skip_if_exists=True."""
    mock_requests = make_mock_requests_module()
    with patch.object(S3UploadCore, "check_existing") as mock_check:
        S3UploadCore.perform_upload(
            TEST_URL, S3_PATH, fake_s3_client, mock_requests, None, None, None, None, skip_if_exists=True, force=True
        )
    mock_check.assert_not_called()
    fake_s3_client.upload_fileobj.assert_called_once()


def test_perform_upload_pass_skip_if_exists_false_does_not_check_existing(fake_s3_client: MagicMock) -> None:
    """skip_if_exists=False never invokes the existence check."""
    mock_requests = make_mock_requests_module()
    with patch.object(S3UploadCore, "check_existing") as mock_check:
        S3UploadCore.perform_upload(
            TEST_URL, S3_PATH, fake_s3_client, mock_requests, None, None, None, None, skip_if_exists=False
        )
    mock_check.assert_not_called()


@pytest.mark.usefixtures("no_skip_check")
def test_perform_upload_fail_client_error_status_raises_and_does_not_upload(fake_s3_client: MagicMock) -> None:
    """A 4xx response raises NonRetryableDownloadError and never reaches upload_fileobj."""
    mock_requests = make_mock_requests_module(get_response=make_mock_get_response(status_code=404))
    with pytest.raises(NonRetryableDownloadError, match="Client error: 404"):
        S3UploadCore.perform_upload(
            TEST_URL, S3_PATH, fake_s3_client, mock_requests, None, None, None, None, skip_if_exists=False
        )
    fake_s3_client.upload_fileobj.assert_not_called()


@pytest.mark.usefixtures("no_skip_check")
def test_perform_upload_fail_server_error_status_raises(fake_s3_client: MagicMock) -> None:
    """A 5xx response raises DownloadError."""
    mock_requests = make_mock_requests_module(get_response=make_mock_get_response(status_code=503))
    with pytest.raises(DownloadError, match="Server error: 503"):
        S3UploadCore.perform_upload(
            TEST_URL, S3_PATH, fake_s3_client, mock_requests, None, None, None, None, skip_if_exists=False
        )


@pytest.mark.usefixtures("no_skip_check")
def test_perform_upload_pass_checksum_verification_success(fake_s3_client: MagicMock) -> None:
    """A correct expected_checksum verifies successfully against the actual streamed bytes."""
    content = b"verify me please"
    expected = hashlib.sha256(content).hexdigest()
    mock_requests = make_mock_requests_module(get_response=make_mock_get_response(content=content))

    result = S3UploadCore.perform_upload(
        TEST_URL, S3_PATH, fake_s3_client, mock_requests, None, None, expected, "sha256", skip_if_exists=False
    )

    assert result == f"{TEST_BUCKET}/{TEST_KEY}"
    fake_s3_client.delete_object.assert_not_called()


@pytest.mark.usefixtures("no_skip_check")
def test_perform_upload_fail_checksum_mismatch_deletes_object_and_raises(fake_s3_client: MagicMock) -> None:
    """A checksum mismatch deletes the uploaded object and raises ChecksumMismatchError."""
    content = b"actual bytes"
    wrong = "0" * 64
    mock_requests = make_mock_requests_module(get_response=make_mock_get_response(content=content))

    with pytest.raises(ChecksumMismatchError):
        S3UploadCore.perform_upload(
            TEST_URL, S3_PATH, fake_s3_client, mock_requests, None, None, wrong, "sha256", skip_if_exists=False
        )
    fake_s3_client.delete_object.assert_called_once_with(Bucket=TEST_BUCKET, Key=TEST_KEY)


@pytest.mark.usefixtures("no_skip_check")
def test_perform_upload_pass_records_checksum_metadata_in_extra_args(fake_s3_client: MagicMock) -> None:
    """A successful, checksummed upload records the checksum in the object's ExtraArgs Metadata."""
    content = b"metadata check"
    expected = hashlib.sha256(content).hexdigest()
    mock_requests = make_mock_requests_module(get_response=make_mock_get_response(content=content))

    S3UploadCore.perform_upload(
        TEST_URL, S3_PATH, fake_s3_client, mock_requests, None, None, expected, "sha256", skip_if_exists=False
    )

    _, kwargs = fake_s3_client.upload_fileobj.call_args
    metadata = kwargs["ExtraArgs"]["Metadata"]
    assert metadata[CHECKSUM_ALGORITHM_METADATA_KEY] == "sha256"
    assert metadata[CHECKSUM_VALUE_METADATA_KEY] == expected


@pytest.mark.usefixtures("no_skip_check")
def test_perform_upload_pass_merges_extra_args_with_defaults(fake_s3_client: MagicMock) -> None:
    """Caller-supplied extra_args are merged with DEFAULT_EXTRA_ARGS, not replacing them."""
    mock_requests = make_mock_requests_module()
    S3UploadCore.perform_upload(
        TEST_URL, S3_PATH, fake_s3_client, mock_requests, None, {"ACL": "private"}, None, None, skip_if_exists=False
    )
    _, kwargs = fake_s3_client.upload_fileobj.call_args
    extra_args = kwargs["ExtraArgs"]
    assert extra_args["ACL"] == "private"
    for key, value in DEFAULT_EXTRA_ARGS.items():
        assert extra_args[key] == value


@pytest.mark.usefixtures("no_skip_check")
def test_perform_upload_pass_content_type_from_response_header(fake_s3_client: MagicMock) -> None:
    """ContentType is set from the response's content-type header when present."""
    mock_requests = make_mock_requests_module(
        get_response=make_mock_get_response(headers={"content-type": "application/pdf"})
    )
    S3UploadCore.perform_upload(
        TEST_URL, S3_PATH, fake_s3_client, mock_requests, None, None, None, None, skip_if_exists=False
    )
    _, kwargs = fake_s3_client.upload_fileobj.call_args
    assert kwargs["ExtraArgs"]["ContentType"] == "application/pdf"


@pytest.mark.usefixtures("no_skip_check")
def test_perform_upload_pass_content_type_defaults_when_missing(fake_s3_client: MagicMock) -> None:
    """ContentType defaults to application/octet-stream when the response has no content-type header."""
    mock_requests = make_mock_requests_module(get_response=make_mock_get_response(headers={}))
    S3UploadCore.perform_upload(
        TEST_URL, S3_PATH, fake_s3_client, mock_requests, None, None, None, None, skip_if_exists=False
    )
    _, kwargs = fake_s3_client.upload_fileobj.call_args
    assert kwargs["ExtraArgs"]["ContentType"] == "application/octet-stream"


@pytest.mark.usefixtures("no_skip_check")
def test_perform_upload_pass_progress_desc_defaults_to_key(fake_s3_client: MagicMock) -> None:
    """When progress_desc is not given, the progress bar's desc defaults to the destination key."""
    mock_requests = make_mock_requests_module()
    with patch.object(streamer, "make_progress_bar", wraps=streamer.make_progress_bar) as spy:
        S3UploadCore.perform_upload(
            TEST_URL, S3_PATH, fake_s3_client, mock_requests, None, None, None, None, skip_if_exists=False
        )
    _, kwargs = spy.call_args
    assert kwargs["desc"] == TEST_KEY


@pytest.mark.usefixtures("no_skip_check")
def test_perform_upload_pass_progress_desc_override_respected(fake_s3_client: MagicMock) -> None:
    """An explicit progress_desc overrides the default (destination key)."""
    mock_requests = make_mock_requests_module()
    with patch.object(streamer, "make_progress_bar", wraps=streamer.make_progress_bar) as spy:
        S3UploadCore.perform_upload(
            TEST_URL,
            S3_PATH,
            fake_s3_client,
            mock_requests,
            None,
            None,
            None,
            None,
            skip_if_exists=False,
            progress_desc="my-custom-label",
        )
    _, kwargs = spy.call_args
    assert kwargs["desc"] == "my-custom-label"


@pytest.mark.usefixtures("no_skip_check")
def test_perform_upload_pass_expected_size_prioritized_over_content_length_header(fake_s3_client: MagicMock) -> None:
    """expected_size takes priority over the response's content-length header for sizing the transfer."""
    mock_requests = make_mock_requests_module(get_response=make_mock_get_response(headers={"content-length": "10"}))
    with patch.object(streamer, "build_transfer_config", wraps=streamer.build_transfer_config) as spy:
        S3UploadCore.perform_upload(
            TEST_URL,
            S3_PATH,
            fake_s3_client,
            mock_requests,
            None,
            None,
            None,
            None,
            skip_if_exists=False,
            expected_size=999,
        )
    args, _ = spy.call_args
    assert args[0] == 999


@pytest.mark.usefixtures("no_skip_check")
def test_perform_upload_pass_uses_content_length_header_when_no_expected_size(fake_s3_client: MagicMock) -> None:
    """Falls back to the response's content-length header when expected_size is not given."""
    mock_requests = make_mock_requests_module(get_response=make_mock_get_response(headers={"content-length": "77"}))
    with patch.object(streamer, "build_transfer_config", wraps=streamer.build_transfer_config) as spy:
        S3UploadCore.perform_upload(
            TEST_URL, S3_PATH, fake_s3_client, mock_requests, None, None, None, None, skip_if_exists=False
        )
    args, _ = spy.call_args
    assert args[0] == 77


@pytest.mark.usefixtures("no_skip_check")
def test_perform_upload_pass_file_size_none_when_neither_available(fake_s3_client: MagicMock) -> None:
    """file_size is None when neither expected_size nor a content-length header is available."""
    mock_requests = make_mock_requests_module(get_response=make_mock_get_response(headers={}))
    with patch.object(streamer, "build_transfer_config", wraps=streamer.build_transfer_config) as spy:
        S3UploadCore.perform_upload(
            TEST_URL, S3_PATH, fake_s3_client, mock_requests, None, None, None, None, skip_if_exists=False
        )
    args, _ = spy.call_args
    assert args[0] is None


@pytest.mark.usefixtures("no_skip_check")
def test_perform_upload_pass_transfer_config_kwargs_forwarded(fake_s3_client: MagicMock) -> None:
    """transfer_config_kwargs are forwarded to build_transfer_config."""
    mock_requests = make_mock_requests_module()
    with patch.object(streamer, "build_transfer_config", wraps=streamer.build_transfer_config) as spy:
        S3UploadCore.perform_upload(
            TEST_URL,
            S3_PATH,
            fake_s3_client,
            mock_requests,
            None,
            None,
            None,
            None,
            skip_if_exists=False,
            transfer_config_kwargs={"max_concurrency": 3},
        )
    _, kwargs = spy.call_args
    assert kwargs["max_concurrency"] == 3


@pytest.mark.usefixtures("no_skip_check")
def test_perform_upload_pass_get_request_uses_stream_and_headers(fake_s3_client: MagicMock) -> None:
    """The GET request is always made with stream=True and forwards extra_headers."""
    mock_requests = make_mock_requests_module()
    S3UploadCore.perform_upload(
        TEST_URL,
        S3_PATH,
        fake_s3_client,
        mock_requests,
        {"Authorization": "Bearer x"},
        None,
        None,
        None,
        skip_if_exists=False,
    )
    mock_requests.get.assert_called_once_with(TEST_URL, stream=True, headers={"Authorization": "Bearer x"})


@pytest.mark.usefixtures("no_skip_check")
def test_perform_upload_pass_timeout_omitted_when_none(fake_s3_client: MagicMock) -> None:
    """The GET request omits the timeout kwarg entirely when timeout is None."""
    mock_requests = make_mock_requests_module()
    S3UploadCore.perform_upload(
        TEST_URL, S3_PATH, fake_s3_client, mock_requests, None, None, None, None, skip_if_exists=False, timeout=None
    )
    _, kwargs = mock_requests.get.call_args
    assert "timeout" not in kwargs


@pytest.mark.usefixtures("no_skip_check")
def test_perform_upload_pass_timeout_included_when_given(fake_s3_client: MagicMock) -> None:
    """The GET request includes the timeout kwarg when explicitly provided."""
    mock_requests = make_mock_requests_module()
    S3UploadCore.perform_upload(
        TEST_URL, S3_PATH, fake_s3_client, mock_requests, None, None, None, None, skip_if_exists=False, timeout=12.5
    )
    _, kwargs = mock_requests.get.call_args
    assert kwargs["timeout"] == 12.5


@pytest.mark.usefixtures("no_skip_check")
def test_perform_upload_pass_callback_is_synchronized(fake_s3_client: MagicMock) -> None:
    """The Callback passed to upload_fileobj is a SynchronizedCallback wrapping the progress bar's update."""
    mock_requests = make_mock_requests_module()
    S3UploadCore.perform_upload(
        TEST_URL, S3_PATH, fake_s3_client, mock_requests, None, None, None, None, skip_if_exists=False
    )
    _, kwargs = fake_s3_client.upload_fileobj.call_args
    assert isinstance(kwargs["Callback"], streamer.SynchronizedCallback)


"""Tests for stream_to_s3"""


def test_stream_to_s3_pass_resolves_checksum_fn_and_delegates(fake_s3_client: MagicMock) -> None:
    """expected_checksum with no checksum_fn resolves to DEFAULT_CHECKSUM_ALGORITHM before delegating."""
    with patch.object(
        streamer.S3UploadCore, "perform_upload", return_value=f"{TEST_BUCKET}/{TEST_KEY}"
    ) as mock_perform:
        stream_to_s3(TEST_URL, S3_PATH, s3_client=fake_s3_client, expected_checksum="abc123")
    _, kwargs = mock_perform.call_args
    assert kwargs["checksum_fn"] == DEFAULT_CHECKSUM_ALGORITHM


def test_stream_to_s3_fail_invalid_checksum_fn_raises_before_delegating(fake_s3_client: MagicMock) -> None:
    """An unsupported checksum_fn raises ValueError before perform_upload is ever called."""
    with (
        patch.object(streamer.S3UploadCore, "perform_upload") as mock_perform,
        pytest.raises(ValueError, match="not supported"),
    ):
        stream_to_s3(TEST_URL, S3_PATH, s3_client=fake_s3_client, expected_checksum="abc", checksum_fn="not-real")
    mock_perform.assert_not_called()


def test_stream_to_s3_pass_uses_provided_s3_client_without_calling_get_s3_client(fake_s3_client: MagicMock) -> None:
    """When s3_client is explicitly provided, get_s3_client() is never called."""
    with (
        patch.object(s3_client, "get_s3_client") as mock_get_client,
        patch.object(streamer.S3UploadCore, "perform_upload", return_value="x") as mock_perform,
    ):
        stream_to_s3(TEST_URL, S3_PATH, s3_client=fake_s3_client)
    mock_get_client.assert_not_called()
    _, kwargs = mock_perform.call_args
    assert kwargs["s3_client"] is fake_s3_client


def test_stream_to_s3_pass_uses_get_s3_client_when_not_provided() -> None:
    """When s3_client is not provided, get_s3_client() supplies the default client."""
    default_client = MagicMock()
    with (
        patch.object(s3_client, "get_s3_client", return_value=default_client) as mock_get_client,
        patch.object(streamer.S3UploadCore, "perform_upload", return_value="x") as mock_perform,
    ):
        stream_to_s3(TEST_URL, S3_PATH)
    mock_get_client.assert_called_once()
    _, kwargs = mock_perform.call_args
    assert kwargs["s3_client"] is default_client


def test_stream_to_s3_pass_forwards_all_optional_arguments(fake_s3_client: MagicMock) -> None:
    """All optional stream_to_s3 arguments are forwarded verbatim to perform_upload."""
    mock_requests = make_mock_requests_module()
    with patch.object(streamer.S3UploadCore, "perform_upload", return_value="x") as mock_perform:
        stream_to_s3(
            TEST_URL,
            S3_PATH,
            s3_client=fake_s3_client,
            requests_module=mock_requests,
            extra_headers={"H": "1"},
            extra_args={"ACL": "private"},
            show_progress=True,
            progress_desc="desc",
            skip_if_exists=False,
            force=True,
            expected_size=123,
            transfer_config_kwargs={"max_concurrency": 2},
        )
    _, kwargs = mock_perform.call_args
    assert kwargs["extra_headers"] == {"H": "1"}
    assert kwargs["extra_args"] == {"ACL": "private"}
    assert kwargs["show_progress"] is True
    assert kwargs["progress_desc"] == "desc"
    assert kwargs["skip_if_exists"] is False
    assert kwargs["force"] is True
    assert kwargs["expected_size"] == 123
    assert kwargs["transfer_config_kwargs"] == {"max_concurrency": 2}
    assert kwargs["requests_module"] is mock_requests


"""Tests for S3StreamUploader.__init__"""


def test_s3streamuploader_pass_uses_provided_s3_client(fake_s3_client: MagicMock) -> None:
    """A provided s3_client is stored directly, without calling get_s3_client()."""
    with patch.object(s3_client, "get_s3_client") as mock_get_client:
        uploader = S3StreamUploader(s3_client=fake_s3_client)
    mock_get_client.assert_not_called()
    assert uploader.s3_client is fake_s3_client


def test_s3streamuploader_pass_defaults_to_get_s3_client_when_none() -> None:
    """When s3_client is not provided, get_s3_client() supplies the default client."""
    default_client = MagicMock()
    with patch.object(s3_client, "get_s3_client", return_value=default_client):
        uploader = S3StreamUploader()
    assert uploader.s3_client is default_client


def test_s3streamuploader_pass_none_dict_arguments_default_to_empty(fake_s3_client: MagicMock) -> None:
    """extra_args and transfer_config_kwargs default to empty dicts, not None."""
    uploader = S3StreamUploader(s3_client=fake_s3_client, extra_args=None, transfer_config_kwargs=None)
    assert uploader.extra_args == {}
    assert uploader.transfer_config_kwargs == {}


def test_s3streamuploader_pass_stores_configuration_defaults(fake_s3_client: MagicMock) -> None:
    """Constructor arguments are stored on the instance as given."""
    uploader = S3StreamUploader(
        s3_client=fake_s3_client,
        default_checksum_fn="md5",
        default_show_progress=True,
        default_skip_if_exists=False,
    )
    assert uploader.default_checksum_fn == "md5"
    assert uploader.default_show_progress is True
    assert uploader.default_skip_if_exists is False


def test_s3streamuploader_pass_retryable_exceptions_include_expected_types() -> None:
    """RETRYABLE_EXCEPTIONS covers timeouts, connection errors, and DownloadError, but not the non-retryable errors."""
    assert requests.exceptions.Timeout in S3StreamUploader.RETRYABLE_EXCEPTIONS
    assert requests.exceptions.ConnectionError in S3StreamUploader.RETRYABLE_EXCEPTIONS
    assert DownloadError in S3StreamUploader.RETRYABLE_EXCEPTIONS
    assert NonRetryableDownloadError not in S3StreamUploader.RETRYABLE_EXCEPTIONS
    assert ChecksumMismatchError not in S3StreamUploader.RETRYABLE_EXCEPTIONS


"""Tests for S3StreamUploader.upload (retry orchestration)"""


@pytest.fixture
def fast_uploader(fake_s3_client: MagicMock) -> S3StreamUploader:
    """An uploader configured with zero backoff, so retry tests run near-instantly."""
    return S3StreamUploader(s3_client=fake_s3_client, max_attempts=3, min_backoff=0, max_backoff=0)


def test_upload_pass_resolves_checksum_fn(fast_uploader: S3StreamUploader) -> None:
    """upload() resolves checksum_fn via resolve_checksum_fn, falling back to default_checksum_fn."""
    fast_uploader._upload_once = MagicMock(return_value="ok")  # noqa: SLF001
    fast_uploader.upload(TEST_URL, S3_PATH, expected_checksum="abc")
    args = fast_uploader._upload_once.call_args.args  # noqa: SLF001
    assert args[4] == fast_uploader.default_checksum_fn


def test_upload_pass_show_progress_and_skip_if_exists_fall_back_to_defaults(fast_uploader: S3StreamUploader) -> None:
    """When show_progress/skip_if_exists are None, the instance defaults are used."""
    fast_uploader.default_show_progress = True
    fast_uploader.default_skip_if_exists = False
    fast_uploader._upload_once = MagicMock(return_value="ok")  # noqa: SLF001

    fast_uploader.upload(TEST_URL, S3_PATH)

    args = fast_uploader._upload_once.call_args.args  # noqa: SLF001
    assert args[5] is True  # show_progress
    assert args[7] is False  # skip_if_exists


def test_upload_pass_explicit_overrides_respected(fast_uploader: S3StreamUploader) -> None:
    """Explicit show_progress/skip_if_exists override the instance defaults."""
    fast_uploader.default_show_progress = False
    fast_uploader.default_skip_if_exists = True
    fast_uploader._upload_once = MagicMock(return_value="ok")  # noqa: SLF001

    fast_uploader.upload(TEST_URL, S3_PATH, show_progress=True, skip_if_exists=False)

    args = fast_uploader._upload_once.call_args.args  # noqa: SLF001
    assert args[5] is True
    assert args[7] is False


def test_upload_pass_succeeds_on_first_attempt(fast_uploader: S3StreamUploader) -> None:
    """A successful first attempt returns immediately with a single call."""
    fast_uploader._upload_once = MagicMock(return_value=f"{TEST_BUCKET}/{TEST_KEY}")  # noqa: SLF001
    result = fast_uploader.upload(TEST_URL, S3_PATH)
    assert result == f"{TEST_BUCKET}/{TEST_KEY}"
    assert fast_uploader._upload_once.call_count == 1  # noqa: SLF001


def test_upload_pass_retries_on_download_error_then_succeeds(fast_uploader: S3StreamUploader) -> None:
    """DownloadError triggers retries, and a later successful attempt returns its result."""
    fast_uploader._upload_once = MagicMock(  # noqa: SLF001
        side_effect=[DownloadError("first"), DownloadError("second"), f"{TEST_BUCKET}/{TEST_KEY}"]
    )
    result = fast_uploader.upload(TEST_URL, S3_PATH)
    assert result == f"{TEST_BUCKET}/{TEST_KEY}"
    assert fast_uploader._upload_once.call_count == 3  # noqa: SLF001


@pytest.mark.parametrize(
    "exc",
    [requests.exceptions.Timeout("timed out"), requests.exceptions.ConnectionError("refused")],
)
def test_upload_pass_retries_on_requests_exceptions(fast_uploader: S3StreamUploader, exc: Exception) -> None:
    """requests.exceptions.Timeout and ConnectionError are also retried, not just DownloadError."""
    fast_uploader._upload_once = MagicMock(side_effect=[exc, f"{TEST_BUCKET}/{TEST_KEY}"])  # noqa: SLF001
    result = fast_uploader.upload(TEST_URL, S3_PATH)
    assert result == f"{TEST_BUCKET}/{TEST_KEY}"
    assert fast_uploader._upload_once.call_count == 2  # noqa: SLF001


def test_upload_fail_exhausts_retries_and_reraises(fast_uploader: S3StreamUploader) -> None:
    """After max_attempts consecutive DownloadErrors, the last exception is re-raised (reraise=True)."""
    fast_uploader._upload_once = MagicMock(side_effect=DownloadError("always fails"))  # noqa: SLF001
    with pytest.raises(DownloadError, match="always fails"):
        fast_uploader.upload(TEST_URL, S3_PATH)
    assert fast_uploader._upload_once.call_count == 3  # noqa: SLF001, matches max_attempts


def test_upload_fail_non_retryable_error_propagates_immediately(fast_uploader: S3StreamUploader) -> None:
    """NonRetryableDownloadError is not retried; it propagates after exactly one attempt."""
    fast_uploader._upload_once = MagicMock(side_effect=NonRetryableDownloadError("bad request"))  # noqa: SLF001
    with pytest.raises(NonRetryableDownloadError, match="bad request"):
        fast_uploader.upload(TEST_URL, S3_PATH)
    assert fast_uploader._upload_once.call_count == 1  # noqa: SLF001


def test_upload_fail_checksum_mismatch_propagates_immediately(fast_uploader: S3StreamUploader) -> None:
    """ChecksumMismatchError is not retried; it propagates after exactly one attempt."""
    fast_uploader._upload_once = MagicMock(side_effect=ChecksumMismatchError("mismatch"))  # noqa: SLF001
    with pytest.raises(ChecksumMismatchError, match="mismatch"):
        fast_uploader.upload(TEST_URL, S3_PATH)
    assert fast_uploader._upload_once.call_count == 1  # noqa: SLF001


def test_upload_fail_unrecognized_exception_not_retried(fast_uploader: S3StreamUploader) -> None:
    """An exception type outside RETRYABLE_EXCEPTIONS is not retried by tenacity, regardless of _upload_once's own wrapping."""
    fast_uploader._upload_once = MagicMock(side_effect=ValueError("unexpected"))  # noqa: SLF001
    with pytest.raises(ValueError, match="unexpected"):
        fast_uploader.upload(TEST_URL, S3_PATH)
    assert fast_uploader._upload_once.call_count == 1  # noqa: SLF001


"""Tests for S3StreamUploader._upload_once"""


def test_upload_once_pass_delegates_to_perform_upload_with_instance_config(fake_s3_client: MagicMock) -> None:
    """_upload_once calls S3UploadCore.perform_upload with the instance's stored configuration."""
    uploader = S3StreamUploader(
        s3_client=fake_s3_client,
        timeout=15.0,
        extra_args={"ACL": "private"},
        transfer_config_kwargs={"max_concurrency": 4},
    )
    with patch.object(
        streamer.S3UploadCore, "perform_upload", return_value=f"{TEST_BUCKET}/{TEST_KEY}"
    ) as mock_perform:
        result = uploader._upload_once(  # noqa: SLF001
            TEST_URL, S3_PATH, None, None, None, False, None, True, False, None
        )
    assert result == f"{TEST_BUCKET}/{TEST_KEY}"
    _, kwargs = mock_perform.call_args
    assert kwargs["s3_client"] is fake_s3_client
    assert kwargs["timeout"] == 15.0
    assert kwargs["extra_args"] == {"ACL": "private"}
    assert kwargs["transfer_config_kwargs"] == {"max_concurrency": 4}


def test_upload_once_fail_non_retryable_error_logged_and_reraised_unwrapped(
    fake_s3_client: MagicMock, caplog: pytest.LogCaptureFixture
) -> None:
    """NonRetryableDownloadError is logged and re-raised as-is (not wrapped in DownloadError)."""
    uploader = S3StreamUploader(s3_client=fake_s3_client)
    with (
        patch.object(streamer.S3UploadCore, "perform_upload", side_effect=NonRetryableDownloadError("bad")),
        caplog.at_level(logging.ERROR),
        pytest.raises(NonRetryableDownloadError, match="bad"),
    ):
        uploader._upload_once(TEST_URL, S3_PATH, None, None, None, False, None, True, False, None)  # noqa: SLF001
    assert any("retry not possible" in r.message for r in caplog.records)


def test_upload_once_fail_checksum_mismatch_logged_and_reraised_unwrapped(
    fake_s3_client: MagicMock, caplog: pytest.LogCaptureFixture
) -> None:
    """ChecksumMismatchError is logged and re-raised as-is (not wrapped in DownloadError)."""
    uploader = S3StreamUploader(s3_client=fake_s3_client)
    with (
        patch.object(streamer.S3UploadCore, "perform_upload", side_effect=ChecksumMismatchError("mismatch")),
        caplog.at_level(logging.ERROR),
        pytest.raises(ChecksumMismatchError, match="mismatch"),
    ):
        uploader._upload_once(TEST_URL, S3_PATH, None, None, None, False, None, True, False, None)  # noqa: SLF001
    assert any("retry not possible" in r.message for r in caplog.records)


def test_upload_once_fail_unexpected_exception_wrapped_in_download_error(
    fake_s3_client: MagicMock, caplog: pytest.LogCaptureFixture
) -> None:
    """Any other exception is logged, wrapped in DownloadError, and chained via `from exc`."""
    original = ValueError("network hiccup")
    uploader = S3StreamUploader(s3_client=fake_s3_client)
    with (
        patch.object(streamer.S3UploadCore, "perform_upload", side_effect=original),
        caplog.at_level(logging.ERROR),
        pytest.raises(DownloadError, match="network hiccup") as exc_info,
    ):
        uploader._upload_once(TEST_URL, S3_PATH, None, None, None, False, None, True, False, None)  # noqa: SLF001
    assert exc_info.value.__cause__ is original
    assert any("retry possible" in r.message for r in caplog.records)
