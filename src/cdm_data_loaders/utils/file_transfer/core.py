"""File downloader with tenacity for retry support."""

from collections.abc import AsyncIterable, Callable, Generator, Iterable
from contextlib import contextmanager
from logging import Logger, getLogger
from pathlib import Path
from typing import Any, Never

import httpx
from frozendict import frozendict
from tenacity import (
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from cdm_data_loaders.utils.checksums import new_hasher, validate_checksum_fn

logger: Logger = getLogger(__name__)


class DownloadError(Exception):
    """Retryable download error."""


class NonRetryableDownloadError(Exception):
    """Retryable download error."""


class ChecksumMismatchError(NonRetryableDownloadError):
    """Checksum validation failed."""


HTTP_CLIENT_DEFAULTS = frozendict(
    {
        "timeout": httpx.Timeout(30.0),
        "limits": httpx.Limits(
            max_connections=20,
            max_keepalive_connections=10,
        ),
        "follow_redirects": True,
    }
)


RETRY_DEFAULTS = frozendict(
    {
        "max_attempts": 5,
        "min_backoff": 1,
        "max_backoff": 30,
    }
)


class DownloadCore:
    """HTTP file downloader with retries and checksum testing."""

    RETRYABLE_EXCEPTIONS = (
        httpx.TimeoutException,
        httpx.TransportError,
        DownloadError,
    )

    @staticmethod
    def retry_kwargs(max_attempts: int, min_backoff: int, max_backoff: int) -> dict[str, Any]:
        """Build common tenacity retry configuration for sync and async downloaders."""
        return {
            "retry": retry_if_exception_type(DownloadCore.RETRYABLE_EXCEPTIONS),
            "stop": stop_after_attempt(max_attempts),
            "wait": wait_exponential(min=min_backoff, max=max_backoff),
            "reraise": True,
        }

    @staticmethod
    def handle_download_exception(exc: Exception, url: str, log: Logger) -> Never:
        """Log a download failure and re-raise or wrap it in ``DownloadError``."""
        if isinstance(exc, NonRetryableDownloadError):
            log.exception("%s: %s; retry not possible", url, exc.args[0], extra={"url": url})  # noqa: LOG004
            raise exc
        log.exception("%s: %s; retry possible", url, exc.args[0], extra={"url": url})  # noqa: LOG004
        raise DownloadError(str(exc)) from exc

    @staticmethod
    def validate_args(
        destination: str | Path,
        checksum_fn: str | None = None,
        extra_headers: dict[str, Any] | None = None,
    ) -> tuple[Path, str, dict[str, Any]]:
        """Validate download arguments, apply defaults, and convert them to the appropriate format.

        If no checksum function is supplied, it will be set to "sha256".

        :param destination: location to save file to
        :type destination: str | Path
        :param checksum_fn: function for calculating file checksum
        :type checksum_fn: str | None, optional
        :param extra_headers: extra headers to add to the httpx request, defaults to None
        :type extra_headers: dict[str, Any] | None, optional
        :return: arguments spruced up and in shape!
        :rtype: tuple[Path, str, dict[str, Any]]
        """
        if not checksum_fn:
            checksum_fn = "sha256"

        validated_checksum_fn = validate_checksum_fn(checksum_fn, include_extras=False)
        destination = Path(destination)
        destination.parent.mkdir(parents=True, exist_ok=True)

        if not extra_headers:
            extra_headers = {}

        return (destination, validated_checksum_fn, extra_headers)

    @staticmethod
    def validate_response(response: httpx.Response) -> bool:
        """Check the response from the server and act accordingly."""
        status = response.status_code

        # not modified since last accessed
        if status == 304:  # noqa: PLR2004
            return False

        # successful response
        if 200 <= status < 300:  # noqa: PLR2004
            return True

        # client error
        if 400 <= status < 500:  # noqa: PLR2004
            msg = f"Client error: {status} {response.reason_phrase}"
            raise NonRetryableDownloadError(msg)

        # server errors
        msg = f"Server error: {status} {response.reason_phrase}"
        raise DownloadError(msg)

    @staticmethod
    def check_should_download(response: httpx.Response, url: str, log: Logger) -> bool:
        """Validate ``response`` and log when the resource is unmodified (HTTP 304).

        :param response: HTTP response from the server
        :type response: httpx.Response
        :param url: download URL
        :type url: str
        :param log: logger instance
        :type log: Logger
        :return: ``True`` if the response body should be downloaded, ``False`` on HTTP 304
        :rtype: bool
        """
        if not DownloadCore.validate_response(response):
            log.info("%s: resource has not been modified", url, extra={"url": url})
            return False
        return True

    @staticmethod
    def log_download_success(url: str, destination: Path, log: Logger) -> Path:
        """Log a successful download and return ``destination``.

        :param url: download URL
        :type url: str
        :param destination: saved file path
        :type destination: Path
        :param log: logger instance
        :type log: Logger
        :return: ``destination``
        :rtype: Path
        """
        log.info("%s: download successful", url, extra={"path": str(destination)})
        return destination

    @staticmethod
    def check_hash(
        url: str,
        expected_checksum: str,
        checksum_fn: str,
        hash_obj,  #: hashlib._Hash,
    ) -> None:
        """Compare the expected to actual checksum.

        :param url: download URL
        :type url: str
        :param expected_checksum: expected checksum
        :type expected_checksum: str
        :param checksum_fn: checksum algorithm
        :type checksum_fn: str
        :param hash_obj: hash object for calculating checksum
        :type hash_obj: bool
        :raises ChecksumMismatchError: if the checksum is incorrect
        """
        if hash_obj:
            # check the checksum matches expectations
            actual = hash_obj.hexdigest()
            if actual.lower() != expected_checksum.lower():
                msg = f"{url}: Checksum mismatch: expected={expected_checksum}, actual={actual}"
                raise ChecksumMismatchError(msg)
            logger.info("%s: %s checksum matches", url, checksum_fn)

    @staticmethod
    @contextmanager
    def open_hash_writer(
        url: str,
        destination: Path,
        expected_checksum: str | None,
        checksum_fn: str,
    ) -> Generator[Callable[[bytes], None]]:
        """Open ``destination`` for binary writing, update an optional hasher per chunk, and verify on exit.

        :param url: download URL
        :type url: str
        :param destination: file path to write to
        :type destination: Path
        :param expected_checksum: expected checksum (if known)
        :type expected_checksum: str | None
        :param checksum_fn: checksum algorithm name
        :type checksum_fn: str
        :yield: callback accepting a ``bytes`` chunk to write and hash
        :rtype: Generator[Callable[[bytes], None]]
        """
        hash_obj = new_hasher(checksum_fn) if expected_checksum else None
        with destination.open("wb") as fh:

            def _write_chunk(chunk: bytes) -> None:
                fh.write(chunk)
                if hash_obj:
                    hash_obj.update(chunk)

            yield _write_chunk

        if hash_obj and expected_checksum:
            DownloadCore.check_hash(url, expected_checksum, checksum_fn, hash_obj)

    @staticmethod
    def write_and_hash(
        url: str,
        destination: Path,
        chunks: Iterable[bytes],
        expected_checksum: str | None,
        checksum_fn: str,
    ) -> None:
        """Write downloaded data to disk and accrue data for calculating the checksum of the download.

        :param url: URL to download from
        :type url: str
        :param destination: where to save the file to
        :type destination: Path
        :param chunks: iterable chunks of downloaded data
        :type chunks: Iterable[bytes]
        :param expected_checksum: expected checksum (if known), defaults to None
        :type expected_checksum: str | None, optional
        :param checksum_fn: function used for calculating the checksum, defaults to "sha256"
        :type checksum_fn: str, optional
        :raises ChecksumMismatchError: if the checksum of the downloaded data does not match the expected checksum.
        """
        with DownloadCore.open_hash_writer(url, destination, expected_checksum, checksum_fn) as write_chunk:
            for chunk in chunks:
                write_chunk(chunk)

    @staticmethod
    async def write_and_hash_async(
        url: str,
        destination: Path,
        chunks: AsyncIterable[bytes],
        expected_checksum: str | None,
        checksum_fn: str,
    ) -> None:
        """Write downloaded data to disk and accrue data for calculating the checksum of the download.

        :param url: URL to download from
        :type url: str
        :param destination: where to save the file to
        :type destination: Path
        :param chunks: iterable chunks of downloaded data
        :type chunks: AsyncIterable[bytes]
        :param expected_checksum: expected checksum (if known), defaults to None
        :type expected_checksum: str | None, optional
        :param checksum_fn: function used for calculating the checksum, defaults to "sha256"
        :type checksum_fn: str, optional
        :raises ChecksumMismatchError: if the checksum of the downloaded data does not match the expected checksum.
        """
        with DownloadCore.open_hash_writer(url, destination, expected_checksum, checksum_fn) as write_chunk:
            async for chunk in chunks:
                write_chunk(chunk)


class BaseFileDownloader:
    """Shared base class for synchronous and asynchronous HTTP file downloaders."""

    RETRYABLE_EXCEPTIONS = DownloadCore.RETRYABLE_EXCEPTIONS

    def __init__(self, client: Any, *, chunk_size: int = 8192) -> None:  # noqa: ANN401
        """Initialize shared HTTP client and chunk size attributes.

        :param client: configured sync or async httpx client
        :type client: Any
        :param chunk_size: download stream chunk size in bytes
        :type chunk_size: int
        """
        self.client = client
        self.chunk_size = chunk_size
