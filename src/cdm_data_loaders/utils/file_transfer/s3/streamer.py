"""Stream HTTP resources directly into S3, without buffering to local disk."""

from dataclasses import dataclass
from logging import WARNING, Logger, getLogger
from types import ModuleType
from typing import Any, Final

import requests
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from cdm_data_loaders.utils.file_transfer.checksums import ChecksumEntry, HashingReader, resolve_checksum_fn
from cdm_data_loaders.utils.file_transfer.core import (
    ChecksumMismatchError,
    DownloadError,
    NonRetryableDownloadError,
)
from cdm_data_loaders.utils.file_transfer.progress import SynchronizedCallback, make_progress_bar
from cdm_data_loaders.utils.file_transfer.s3 import client
from cdm_data_loaders.utils.file_transfer.s3.client import split_s3_path
from cdm_data_loaders.utils.file_transfer.s3.object_utils import (
    DEFAULT_EXTRA_ARGS,
    checksum_metadata,
    decide_skip,
    get_existing_object_info,
)
from cdm_data_loaders.utils.file_transfer.s3.transfer_config import build_transfer_config

logger: Logger = getLogger(__name__)

DEFAULT_CHECKSUM_ALGORITHM: Final[str] = "sha256"


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
    def get_content_length(response: requests.Response) -> int | None:
        """Extract a response's Content-Length header as an int, if present.

        Used both to size a determinate progress bar and to select a safe
        multipart chunksize; when absent (e.g. chunked transfer-encoding),
        the file size is treated as unknown by both.

        :param response: the HTTP response to inspect
        :type response: requests.Response
        :return: the content length in bytes, or None if not present/parseable
        :rtype: int | None
        """
        content_length = response.headers.get("content-length")
        if content_length is None:
            return None
        try:
            return int(content_length)
        except ValueError:
            return None

    @staticmethod
    def get_remote_size(
        requests_module: ModuleType,
        url: str,
        extra_headers: dict[str, str] | None,
        timeout: float | None,
    ) -> int | None:
        """Get the size of a remote resource via an HTTP HEAD request, without downloading it.

        Used to pre-check whether a source file's size matches an object
        already in S3, before deciding whether a full download/upload is needed.

        :param requests_module: module implementing requests.head
        :type requests_module: ModuleType
        :param url: address of the resource to inspect
        :type url: str
        :param extra_headers: extra headers to pass to the HEAD request
        :type extra_headers: dict[str, str] | None
        :param timeout: request timeout in seconds, or None for the library default
        :type timeout: float | None
        :return: the resource's size in bytes, or None if it could not be determined
            (e.g. the server doesn't support HEAD, or omits Content-Length)
        :rtype: int | None
        """
        kwargs: dict[str, Any] = {"headers": extra_headers or {}}
        if timeout is not None:
            kwargs["timeout"] = timeout

        try:
            response = requests_module.head(url, **kwargs)
            response.raise_for_status()
        except Exception:
            logger.warning("%s: HEAD request failed; cannot pre-check remote size", url, exc_info=True)
            return None

        content_length = response.headers.get("content-length")
        if content_length is None:
            return None
        try:
            return int(content_length)
        except ValueError:
            return None

    @staticmethod
    def check_existing(  # noqa: PLR0913
        url: str,
        bucket: str,
        key: str,
        s3_client: Any,
        requests_module: ModuleType,
        extra_headers: dict[str, str] | None,
        expected_checksum_entry: ChecksumEntry | None,
        timeout: float | None,
        expected_size: int | None = None,
    ) -> bool:
        """Check whether `bucket`/`key` already holds an equivalent file, and can be skipped.

        Fetches S3 object metadata via `head_object` and, if an object
        exists, compares it against the source using (in order of
        preference) a checksum recorded in the object's metadata by a
        previous upload, or the source's size.

        :param url: address of the source file, used for the HEAD size check and logging
        :type url: str
        :param bucket: destination S3 bucket
        :type bucket: str
        :param key: destination S3 key
        :type key: str
        :param s3_client: a boto3 S3 client
        :type s3_client: Any
        :param requests_module: module implementing requests.head
        :type requests_module: ModuleType
        :param extra_headers: extra headers to pass to the HEAD request
        :type extra_headers: dict[str, str] | None
        :param expected_checksum_entry: the checksum the source file is
            expected to have, if known, or None
        :type expected_checksum_entry: ChecksumEntry | None
        :param timeout: request timeout in seconds, or None for the library default
        :type timeout: float | None
        :param expected_size: known size of the source file in bytes, if
            already available (e.g. from a directory listing), avoiding a
            separate HEAD request; defaults to None
        :type expected_size: int | None, optional
        :return: True if the upload can be safely skipped, False otherwise
        :rtype: bool
        """
        existing = get_existing_object_info(bucket, key)
        if existing is None:
            return False

        remote_size = expected_size
        if remote_size is None:
            remote_size = S3UploadCore.get_remote_size(requests_module, url, extra_headers, timeout)

        decision = decide_skip(existing, remote_size, expected_checksum_entry)

        log = logger.info if decision.confident else logger.warning
        if decision.skip:
            log("%s: skipping upload to %s/%s (%s)", url, bucket, key, decision.reason)
        else:
            log("%s: will (re-)upload to %s/%s (%s)", url, bucket, key, decision.reason)

        return decision.skip

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
        show_progress: bool = False,  # noqa: FBT001, FBT002
        progress_desc: str | None = None,
        skip_if_exists: bool = True,  # noqa: FBT001, FBT002
        force: bool = False,  # noqa: FBT001, FBT002
        expected_size: int | None = None,
        transfer_config_kwargs: dict[str, Any] | None = None,
    ) -> str:
        """Stream `url` into `s3_path`, skipping, verifying, and sizing the transfer as configured.

        If `expected_checksum` is supplied and does not match the uploaded
        data, the uploaded object is deleted before raising. If a checksum
        is supplied, it is also recorded as object metadata on successful
        upload, so future calls with `skip_if_exists=True` can rely on it.

        The multipart chunksize used for the transfer is automatically
        scaled up (via `transfer_config.build_transfer_config`) if the file
        is large enough that S3's default 8MB chunksize would exceed the
        10,000-part limit, avoiding "Part number must be an integer between
        1 and 10000" errors on very large files.

        :param url: address of the object to transfer to s3
        :type url: str
        :param s3_path: save path on s3, as 's3://bucket/key' or 'bucket/key'
        :type s3_path: str
        :param s3_client: a boto3 S3 client
        :type s3_client: Any
        :param requests_module: module implementing requests.get/requests.head
        :type requests_module: ModuleType
        :param extra_headers: extra headers to pass to the GET/HEAD requests
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
        :param show_progress: if True, display a tqdm progress bar tracking
            bytes uploaded to S3, defaults to False
        :type show_progress: bool, optional
        :param progress_desc: label for the progress bar; defaults to the
            destination S3 key if not supplied
        :type progress_desc: str | None, optional
        :param skip_if_exists: if True, check S3 for an existing object
            before uploading and skip the transfer if it appears to already
            match the source, defaults to True
        :type skip_if_exists: bool, optional
        :param force: if True, always upload regardless of what already
            exists in S3, defaults to False
        :type force: bool, optional
        :param expected_size: known size of the source file in bytes, if
            already available (e.g. from a directory listing); used to
            avoid a HEAD request during the skip check and to compute the
            multipart chunksize before the transfer starts, defaults to None
        :type expected_size: int | None, optional
        :param transfer_config_kwargs: extra `TransferConfig` keyword
            arguments (e.g. `max_concurrency`), merged with the
            automatically-computed `multipart_chunksize`, defaults to None
        :type transfer_config_kwargs: dict[str, Any] | None, optional
        :raises NonRetryableDownloadError: for 4xx client errors
        :raises DownloadError: for 5xx server errors or other transport failures
        :raises ChecksumMismatchError: if the uploaded data does not match `expected_checksum`
        :return: path of the file on s3, in the form bucket/key
        :rtype: str
        """
        bucket, key = split_s3_path(s3_path)
        expected_checksum_entry = (
            ChecksumEntry(algorithm=checksum_fn, value=expected_checksum) if expected_checksum and checksum_fn else None
        )

        if skip_if_exists and not force:
            can_skip = S3UploadCore.check_existing(
                url,
                bucket,
                key,
                s3_client,
                requests_module,
                extra_headers,
                expected_checksum_entry,
                timeout,
                expected_size=expected_size,
            )
            if can_skip:
                return f"{bucket}/{key}"

        merged_extra_args = {**DEFAULT_EXTRA_ARGS, **(extra_args or {})}
        if expected_checksum_entry is not None:
            merged_extra_args["Metadata"] = {
                **merged_extra_args.get("Metadata", {}),
                **checksum_metadata(expected_checksum_entry),
            }

        get_kwargs: dict[str, Any] = {"stream": True, "headers": extra_headers or {}}
        if timeout is not None:
            get_kwargs["timeout"] = timeout

        with requests_module.get(url, **get_kwargs) as response:
            S3UploadCore.validate_response(response)

            # Prefer a size already known ahead of time (e.g. from a directory
            # listing) over the response header, since it's available even if
            # the server response doesn't include Content-Length.
            file_size = expected_size if expected_size is not None else S3UploadCore.get_content_length(response)
            transfer_config = build_transfer_config(file_size, **(transfer_config_kwargs or {}))

            hasher = HashingReader(response.raw, checksum_fn) if expected_checksum else response.raw

            with make_progress_bar(
                total=file_size,
                desc=progress_desc or key,
                disable=not show_progress,
            ) as pbar:
                s3_client.upload_fileobj(
                    # raw stream from urllib3 (or a HashingReader wrapping it)
                    hasher,
                    bucket,
                    key,
                    ExtraArgs={
                        **merged_extra_args,
                        "ContentType": response.headers.get("content-type", "application/octet-stream"),
                    },
                    Config=transfer_config,
                    Callback=SynchronizedCallback(pbar.update),
                )

            if expected_checksum:
                S3UploadCore.check_hash(url, bucket, key, expected_checksum, checksum_fn, hasher, s3_client)

        logger.info("%s: upload to s3 successful", url, extra={"s3_path": f"{bucket}/{key}"})
        return f"{bucket}/{key}"


def stream_to_s3(  # noqa: PLR0913
    url: str,
    s3_path: str,
    s3_client: Any = None,
    requests_module: ModuleType = requests,
    extra_headers: dict[str, str] | None = None,
    extra_args: dict[str, Any] | None = None,
    expected_checksum: str | None = None,
    checksum_fn: str | None = None,
    show_progress: bool = False,  # noqa: FBT001, FBT002
    progress_desc: str | None = None,
    skip_if_exists: bool = True,  # noqa: FBT001, FBT002
    force: bool = False,  # noqa: FBT001, FBT002
    expected_size: int | None = None,
    transfer_config_kwargs: dict[str, Any] | None = None,
) -> str:
    """Stream directly from an HTTP download to s3, with skip-if-exists, checksum, progress, and large-file support.

    :param url: address of the object to transfer to s3
    :type url: str
    :param s3_path: save path on s3, as 's3://bucket/key' or 'bucket/key'
    :type s3_path: str
    :param s3_client: a boto3 S3 client
    :type s3_client: Any
    :param requests_module: module implementing requests.get/requests.head and
        returning a response, defaults to the real `requests` module (injectable for testing)
    :type requests_module: ModuleType, optional
    :param extra_headers: extra headers to pass to the GET/HEAD requests, defaults to None
    :type extra_headers: dict[str, str] | None, optional
    :param extra_args: extra S3 ExtraArgs to merge in (e.g. ACL, Metadata), defaults to None
    :type extra_args: dict[str, Any] | None, optional
    :param expected_checksum: expected digest of the downloaded bytes, defaults to None
    :type expected_checksum: str | None, optional
    :param checksum_fn: hashlib algorithm name used to compute/verify
        `expected_checksum`; defaults to `DEFAULT_CHECKSUM_ALGORITHM` if
        `expected_checksum` is given without it
    :type checksum_fn: str | None, optional
    :param show_progress: if True, display a tqdm progress bar tracking
        bytes uploaded to S3, defaults to False
    :type show_progress: bool, optional
    :param progress_desc: label for the progress bar; defaults to the
        destination S3 key if not supplied
    :type progress_desc: str | None, optional
    :param skip_if_exists: if True, check S3 for an existing object before
        uploading and skip the transfer if it appears to already match the
        source, defaults to True
    :type skip_if_exists: bool, optional
    :param force: if True, always upload regardless of what already exists
        in S3, defaults to False
    :type force: bool, optional
    :param expected_size: known size of the source file in bytes, if already
        available (e.g. from a directory listing); used to avoid a HEAD
        request during the skip check and to size the multipart transfer
        before it starts, defaults to None
    :type expected_size: int | None, optional
    :param transfer_config_kwargs: extra `TransferConfig` keyword arguments
        (e.g. `max_concurrency`), merged with the automatically-computed
        `multipart_chunksize`, defaults to None
    :type transfer_config_kwargs: dict[str, Any] | None, optional
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
        s3_client=s3_client or client.get_s3_client(),
        requests_module=requests_module,
        extra_headers=extra_headers,
        extra_args=extra_args,
        expected_checksum=expected_checksum,
        checksum_fn=checksum_fn,
        show_progress=show_progress,
        progress_desc=progress_desc,
        skip_if_exists=skip_if_exists,
        force=force,
        expected_size=expected_size,
        transfer_config_kwargs=transfer_config_kwargs,
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
        s3_client: Any = None,
        requests_module: ModuleType = requests,
        max_attempts: int = 5,
        min_backoff: int = 1,
        max_backoff: int = 30,
        timeout: float = 30.0,
        extra_args: dict[str, Any] | None = None,
        default_checksum_fn: str = DEFAULT_CHECKSUM_ALGORITHM,
        default_show_progress: bool = False,  # noqa: FBT001, FBT002
        default_skip_if_exists: bool = True,  # noqa: FBT001, FBT002
        transfer_config_kwargs: dict[str, Any] | None = None,
    ) -> None:
        """Initialise an S3 streaming uploader.

        :param s3_client: a boto3 S3 client
        :type s3_client: Any
        :param requests_module: module implementing requests.get/requests.head, defaults to `requests`
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
        :param default_show_progress: whether `upload()` shows a progress bar
            when not explicitly overridden per call, defaults to False
        :type default_show_progress: bool, optional
        :param default_skip_if_exists: whether `upload()` checks S3 for an
            existing, matching object before uploading, when not explicitly
            overridden per call, defaults to True
        :type default_skip_if_exists: bool, optional
        :param transfer_config_kwargs: extra `TransferConfig` keyword
            arguments applied to every upload (e.g. `max_concurrency`),
            merged with the automatically-computed `multipart_chunksize`
            for each file, defaults to None
        :type transfer_config_kwargs: dict[str, Any] | None, optional
        """
        self.s3_client = s3_client or client.get_s3_client()
        self.requests = requests_module
        self.timeout = timeout
        self.extra_args = extra_args or {}
        self.default_checksum_fn = default_checksum_fn
        self.default_show_progress = default_show_progress
        self.default_skip_if_exists = default_skip_if_exists
        self.transfer_config_kwargs = transfer_config_kwargs or {}

        self._retry = retry(
            retry=retry_if_exception_type(self.RETRYABLE_EXCEPTIONS),
            stop=stop_after_attempt(max_attempts),
            wait=wait_exponential(min=min_backoff, max=max_backoff),
            reraise=True,
            before_sleep=before_sleep_log(logger, WARNING),
        )

    def upload(  # noqa: PLR0913
        self,
        url: str,
        s3_path: str,
        extra_headers: dict[str, str] | None = None,
        expected_checksum: str | None = None,
        checksum_fn: str | None = None,
        show_progress: bool | None = None,  # noqa: FBT001
        progress_desc: str | None = None,
        skip_if_exists: bool | None = None,  # noqa: FBT001
        force: bool = False,  # noqa: FBT001, FBT002
        expected_size: int | None = None,
    ) -> str:
        """Stream `url` into `s3_path` with retries, skip-if-exists, checksum, progress, and large-file support.

        :param url: address of the object to transfer to s3
        :type url: str
        :param s3_path: save path on s3, as 's3://bucket/key' or 'bucket/key'
        :type s3_path: str
        :param extra_headers: extra headers to pass to the GET/HEAD requests, defaults to None
        :type extra_headers: dict[str, str] | None, optional
        :param expected_checksum: expected digest of the downloaded bytes, defaults to None
        :type expected_checksum: str | None, optional
        :param checksum_fn: hashlib algorithm name used to compute/verify
            `expected_checksum`; defaults to `self.default_checksum_fn` if
            `expected_checksum` is given without it
        :type checksum_fn: str | None, optional
        :param show_progress: if True, display a tqdm progress bar tracking
            bytes uploaded to S3; defaults to `self.default_show_progress` if not given
        :type show_progress: bool | None, optional
        :param progress_desc: label for the progress bar; defaults to the
            destination S3 key if not supplied
        :type progress_desc: str | None, optional
        :param skip_if_exists: if True, check S3 for an existing, matching
            object before uploading and skip the transfer if found; defaults
            to `self.default_skip_if_exists` if not given
        :type skip_if_exists: bool | None, optional
        :param force: if True, always upload regardless of what already
            exists in S3, defaults to False
        :type force: bool, optional
        :param expected_size: known size of the source file in bytes, if
            already available (e.g. from a directory listing); used to
            avoid a HEAD request during the skip check and to size the
            multipart transfer before it starts, defaults to None
        :type expected_size: int | None, optional
        :raises ValueError: if `checksum_fn` (or its default) is not a supported hashlib algorithm
        :raises NonRetryableDownloadError: for 4xx client errors (not retried)
        :raises DownloadError: for 5xx server errors or other transport failures (retried)
        :raises ChecksumMismatchError: if the uploaded data does not match `expected_checksum` (not retried)
        :return: path of the file on s3, in the form bucket/key
        :rtype: str
        """
        checksum_fn = resolve_checksum_fn(expected_checksum, checksum_fn, self.default_checksum_fn)
        show_progress = self.default_show_progress if show_progress is None else show_progress
        skip_if_exists = self.default_skip_if_exists if skip_if_exists is None else skip_if_exists

        @self._retry
        def _once() -> str:
            return self._upload_once(
                url,
                s3_path,
                extra_headers,
                expected_checksum,
                checksum_fn,
                show_progress,
                progress_desc,
                skip_if_exists,
                force,
                expected_size,
            )

        return _once()

    def _upload_once(  # noqa: PLR0913
        self,
        url: str,
        s3_path: str,
        extra_headers: dict[str, str] | None,
        expected_checksum: str | None,
        checksum_fn: str | None,
        show_progress: bool,  # noqa: FBT001
        progress_desc: str | None,
        skip_if_exists: bool,  # noqa: FBT001
        force: bool,  # noqa: FBT001
        expected_size: int | None,
    ) -> str:
        """Perform a single upload attempt, translating unexpected errors into `DownloadError`.

        :param url: address of the object to transfer to s3
        :type url: str
        :param s3_path: save path on s3, as 's3://bucket/key' or 'bucket/key'
        :type s3_path: str
        :param extra_headers: extra headers to pass to the GET/HEAD requests
        :type extra_headers: dict[str, str] | None
        :param expected_checksum: expected digest of the downloaded bytes, or None
        :type expected_checksum: str | None
        :param checksum_fn: resolved hashlib algorithm name, or None
        :type checksum_fn: str | None
        :param show_progress: whether to display a tqdm progress bar for this attempt
        :type show_progress: bool
        :param progress_desc: label for the progress bar, or None to use the S3 key
        :type progress_desc: str | None
        :param skip_if_exists: whether to check S3 for an existing, matching object first
        :type skip_if_exists: bool
        :param force: if True, always upload regardless of what already exists in S3
        :type force: bool
        :param expected_size: known size of the source file in bytes, or None
        :type expected_size: int | None
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
                s3_client=self.s3_client,
                requests_module=self.requests,
                extra_headers=extra_headers,
                extra_args=self.extra_args,
                expected_checksum=expected_checksum,
                checksum_fn=checksum_fn,
                timeout=self.timeout,
                show_progress=show_progress,
                progress_desc=progress_desc,
                skip_if_exists=skip_if_exists,
                force=force,
                expected_size=expected_size,
                transfer_config_kwargs=self.transfer_config_kwargs,
            )
        except (NonRetryableDownloadError, ChecksumMismatchError) as exc:
            logger.exception("%s: %s; retry not possible", url, exc.args[0], extra={"url": url})
            raise
        except Exception as exc:
            logger.exception("%s: %s; retry possible", url, exc, extra={"url": url})
            raise DownloadError(str(exc)) from exc
