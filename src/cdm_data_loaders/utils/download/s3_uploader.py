"""Stream HTTP resources directly into S3, without buffering to local disk.

`S3UploadCore` holds the pure request/upload/verify logic shared by the
plain `stream_to_s3` function and the retrying `S3StreamUploader` class,
mirroring the `DownloadCore` / `FileDownloader` split used for local downloads.
"""

from logging import WARNING, Logger, getLogger
from types import ModuleType
from typing import Any

import requests
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from cdm_data_loaders.utils.download.checksums import HashingReader, resolve_checksum_fn
from cdm_data_loaders.utils.download.core import (
    ChecksumMismatchError,
    DownloadError,
    NonRetryableDownloadError,
)
from cdm_data_loaders.utils.s3 import split_s3_path

logger: Logger = getLogger(__name__)

DEFAULT_CHECKSUM_ALGORITHM = "sha256"


class S3UploadCore:
    """
    Core request/upload/verify logic for streaming an HTTP resource into S3.
    """

    @staticmethod
    def validate_response(response: requests.Response) -> None:
        """Check an HTTP response's status code and raise on error.

        :param response: the streamed HTTP response to validate
        :type response: requests.Response
        :raises NonRetryableDownloadError: for 4xx client errors
        :raises DownloadError: for 5xx server errors
        """
        status = response.status_code
        if 400 <= status < 500:  # noqa: PLR2004
            msg = f"Client error: {status} {response.reason}"
            raise NonRetryableDownloadError(msg)
        if status >= 500:  # noqa: PLR2004
            msg = f"Server error: {status} {response.reason}"
            raise DownloadError(msg)

    @staticmethod
    def check_hash(  # noqa: PLR0913
        url: str,
        bucket: str,
        key: str,
        expected_checksum: str,
        checksum_fn: str | None,
        hasher: HashingReader,
        s3_client: Any,
    ) -> None:
        """Compare the expected checksum to the one computed while uploading.

        Deletes the uploaded object before raising if the checksums do not match.

        :param url: source URL the data was streamed from, used in error messages
        :type url: str
        :param bucket: destination S3 bucket
        :type bucket: str
        :param key: destination S3 key
        :type key: str
        :param expected_checksum: expected digest
        :type expected_checksum: str
        :param checksum_fn: hashlib algorithm name used, for error messages
        :type checksum_fn: str | None
        :param hasher: the HashingReader used during the upload
        :type hasher: HashingReader
        :param s3_client: a boto3 S3 client, used to delete the object on mismatch
        :type s3_client: Any
        :raises ChecksumMismatchError: if the checksums do not match
        """
        actual = hasher.hexdigest()
        if actual.lower() != expected_checksum.lower():
            s3_client.delete_object(Bucket=bucket, Key=key)
            msg = (
                f"{url}: {checksum_fn} checksum mismatch uploading to {bucket}/{key}: "
                f"expected={expected_checksum}, actual={actual}"
            )
            raise ChecksumMismatchError(msg)
        logger.info("%s: %s checksum verified for %s/%s", url, checksum_fn, bucket, key)

    @staticmethod
    def perform_upload(  # noqa: PLR0913
        url: str,
        s3_path: str,
        s3_client: Any,
        requests_module: ModuleType,
        extra_headers: dict[str, str] | None,
        extra_args: dict[str, Any] | None,
        expected_checksum: str | None,
        checksum_fn: str | None,
        timeout: float | None = None,
    ) -> str:
        """Stream `url` into `s3_path`, optionally verifying a checksum in-flight.

        If `expected_checksum` is supplied and does not match the uploaded
        data, the uploaded object is deleted before raising.

        :param url: address of the object to transfer to s3
        :type url: str
        :param s3_path: save path on s3, as 's3://bucket/key' or 'bucket/key'
        :type s3_path: str
        :param s3_client: a boto3 S3 client
        :type s3_client: Any
        :param requests_module: module implementing requests.get and returning a response
        :type requests_module: ModuleType
        :param extra_headers: extra headers to pass to the GET request
        :type extra_headers: dict[str, str] | None
        :param extra_args: extra S3 ExtraArgs to merge into the upload (e.g. ACL, Metadata)
        :type extra_args: dict[str, Any] | None
        :param expected_checksum: expected digest of the downloaded bytes, or
            None to skip verification
        :type expected_checksum: str | None
        :param checksum_fn: hashlib algorithm name used to compute/verify
            `expected_checksum`; required if `expected_checksum` is set
        :type checksum_fn: str | None
        :param timeout: request timeout in seconds, defaults to None (library default)
        :type timeout: float | None, optional
        :raises NonRetryableDownloadError: for 4xx client errors
        :raises DownloadError: for 5xx server errors or other transport failures
        :raises ChecksumMismatchError: if the uploaded data does not match `expected_checksum`
        :return: path of the file on s3, in the form bucket/key
        :rtype: str
        """
        bucket, key = split_s3_path(s3_path)
        get_kwargs: dict[str, Any] = {"stream": True, "headers": extra_headers or {}}
        if timeout is not None:
            get_kwargs["timeout"] = timeout

        with requests_module.get(url, **get_kwargs) as response:
            S3UploadCore.validate_response(response)

            hasher = HashingReader(response.raw, checksum_fn) if expected_checksum else response.raw

            s3_client.upload_fileobj(
                # raw stream from urllib3 (or a HashingReader wrapping it)
                hasher,
                bucket,
                key,
                ExtraArgs={
                    **(extra_args or {}),
                    "ContentType": response.headers.get("content-type", "application/octet-stream"),
                },
            )

            if expected_checksum:
                S3UploadCore.check_hash(url, bucket, key, expected_checksum, checksum_fn, hasher, s3_client)

        logger.info("%s: upload to s3 successful", url, extra={"s3_path": f"{bucket}/{key}"})
        return f"{bucket}/{key}"


def stream_to_s3(  # noqa: PLR0913
    url: str,
    s3_path: str,
    s3_client: Any,
    requests_module: ModuleType = requests,
    extra_headers: dict[str, str] | None = None,
    extra_args: dict[str, Any] | None = None,
    expected_checksum: str | None = None,
    checksum_fn: str | None = None,
) -> str:
    """Stream directly from an HTTP download to s3, optionally verifying a checksum in-flight.

    :param url: address of the object to transfer to s3
    :type url: str
    :param s3_path: save path on s3, as 's3://bucket/key' or 'bucket/key'
    :type s3_path: str
    :param s3_client: a boto3 S3 client
    :type s3_client: Any
    :param requests_module: module implementing requests.get and returning a
        response, defaults to the real `requests` module (injectable for testing)
    :type requests_module: ModuleType, optional
    :param extra_headers: extra headers to pass to the GET request, defaults to None
    :type extra_headers: dict[str, str] | None, optional
    :param extra_args: extra S3 ExtraArgs to merge in (e.g. ACL, Metadata), defaults to None
    :type extra_args: dict[str, Any] | None, optional
    :param expected_checksum: expected digest of the downloaded bytes, defaults to None
    :type expected_checksum: str | None, optional
    :param checksum_fn: hashlib algorithm name used to compute/verify
        `expected_checksum`; defaults to `DEFAULT_CHECKSUM_ALGORITHM` if
        `expected_checksum` is given without it
    :type checksum_fn: str | None, optional
    :raises ValueError: if `checksum_fn` (or its default) is not a supported hashlib algorithm
    :raises NonRetryableDownloadError: for 4xx client errors
    :raises DownloadError: for 5xx server errors or other transport failures
    :raises ChecksumMismatchError: if the uploaded data does not match `expected_checksum`
    :return: path of the file on s3, in the form bucket/key
    :rtype: str
    """
    checksum_fn = resolve_checksum_fn(expected_checksum, checksum_fn, DEFAULT_CHECKSUM_ALGORITHM)
    return S3UploadCore.perform_upload(
        url,
        s3_path,
        s3_client,
        requests_module,
        extra_headers,
        extra_args,
        expected_checksum,
        checksum_fn,
    )


class S3StreamUploader:
    """
    Streams HTTP resources into S3 with retry support, mirroring FileDownloader's semantics.
    """

    RETRYABLE_EXCEPTIONS = (
        requests.exceptions.Timeout,
        requests.exceptions.ConnectionError,
        DownloadError,
    )

    def __init__(  # noqa: PLR0913
        self,
        s3_client: Any,
        requests_module: ModuleType = requests,
        max_attempts: int = 5,
        min_backoff: int = 1,
        max_backoff: int = 30,
        timeout: float = 30.0,
        extra_args: dict[str, Any] | None = None,
        default_checksum_fn: str = DEFAULT_CHECKSUM_ALGORITHM,
    ) -> None:
        """Initialise an S3 streaming uploader.

        :param s3_client: a boto3 S3 client
        :type s3_client: Any
        :param requests_module: module implementing requests.get, defaults to `requests`
        :type requests_module: ModuleType, optional
        :param max_attempts: how many times to retry the upload, defaults to 5
        :type max_attempts: int, optional
        :param min_backoff: minimum backoff for retries, defaults to 1
        :type min_backoff: int, optional
        :param max_backoff: maximum backoff for retries, defaults to 30
        :type max_backoff: int, optional
        :param timeout: request timeout in seconds, defaults to 30.0
        :type timeout: float, optional
        :param extra_args: extra S3 ExtraArgs applied to every upload, defaults to None
        :type extra_args: dict[str, Any] | None, optional
        :param default_checksum_fn: algorithm used when `upload()` is given an
            expected checksum without an explicit algorithm, defaults to
            `DEFAULT_CHECKSUM_ALGORITHM`
        :type default_checksum_fn: str, optional
        """
        self.s3_client = s3_client
        self.requests = requests_module
        self.timeout = timeout
        self.extra_args = extra_args or {}
        self.default_checksum_fn = default_checksum_fn

        self._retry = retry(
            retry=retry_if_exception_type(self.RETRYABLE_EXCEPTIONS),
            stop=stop_after_attempt(max_attempts),
            wait=wait_exponential(min=min_backoff, max=max_backoff),
            reraise=True,
            before_sleep=before_sleep_log(logger, WARNING),
        )

    def upload(
        self,
        url: str,
        s3_path: str,
        extra_headers: dict[str, str] | None = None,
        expected_checksum: str | None = None,
        checksum_fn: str | None = None,
    ) -> str:
        """Stream `url` into `s3_path` with retries and optional checksum verification.

        :param url: address of the object to transfer to s3
        :type url: str
        :param s3_path: save path on s3, as 's3://bucket/key' or 'bucket/key'
        :type s3_path: str
        :param extra_headers: extra headers to pass to the GET request, defaults to None
        :type extra_headers: dict[str, str] | None, optional
        :param expected_checksum: expected digest of the downloaded bytes, defaults to None
        :type expected_checksum: str | None, optional
        :param checksum_fn: hashlib algorithm name used to compute/verify
            `expected_checksum`; defaults to `self.default_checksum_fn` if
            `expected_checksum` is given without it
        :type checksum_fn: str | None, optional
        :raises ValueError: if `checksum_fn` (or its default) is not a supported hashlib algorithm
        :raises NonRetryableDownloadError: for 4xx client errors (not retried)
        :raises DownloadError: for 5xx server errors or other transport failures (retried)
        :raises ChecksumMismatchError: if the uploaded data does not match `expected_checksum` (not retried)
        :return: path of the file on s3, in the form bucket/key
        :rtype: str
        """
        checksum_fn = resolve_checksum_fn(expected_checksum, checksum_fn, self.default_checksum_fn)

        @self._retry
        def _once() -> str:
            return self._upload_once(url, s3_path, extra_headers, expected_checksum, checksum_fn)

        return _once()

    def _upload_once(
        self,
        url: str,
        s3_path: str,
        extra_headers: dict[str, str] | None,
        expected_checksum: str | None,
        checksum_fn: str | None,
    ) -> str:
        """Perform a single upload attempt, translating unexpected errors into `DownloadError`.

        :param url: address of the object to transfer to s3
        :type url: str
        :param s3_path: save path on s3, as 's3://bucket/key' or 'bucket/key'
        :type s3_path: str
        :param extra_headers: extra headers to pass to the GET request
        :type extra_headers: dict[str, str] | None
        :param expected_checksum: expected digest of the downloaded bytes, or None
        :type expected_checksum: str | None
        :param checksum_fn: resolved hashlib algorithm name, or None
        :type checksum_fn: str | None
        :raises NonRetryableDownloadError: for 4xx client errors
        :raises ChecksumMismatchError: if the uploaded data does not match `expected_checksum`
        :raises DownloadError: for any other failure, wrapping the original exception
        :return: path of the file on s3, in the form bucket/key
        :rtype: str
        """
        try:
            return S3UploadCore.perform_upload(
                url,
                s3_path,
                self.s3_client,
                self.requests,
                extra_headers,
                self.extra_args,
                expected_checksum,
                checksum_fn,
                timeout=self.timeout,
            )
        except (NonRetryableDownloadError, ChecksumMismatchError) as exc:
            logger.exception("%s: %s; retry not possible", url, exc.args[0], extra={"url": url})
            raise
        except Exception as exc:
            logger.exception("%s: %s; retry possible", url, exc, extra={"url": url})
            raise DownloadError(str(exc)) from exc
