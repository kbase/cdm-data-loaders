"""Shared retry, response validation, and streaming checksum logic for HTTP downloaders."""

from collections.abc import AsyncIterable, Callable, Generator, Iterable
from contextlib import contextmanager
from http import HTTPStatus
from logging import WARNING, Logger, getLogger
from pathlib import Path
from typing import Any, Final, TypedDict

import httpx
from frozendict import frozendict
from tenacity import (
    before_sleep_log,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from cdm_data_loaders.utils.checksums import Hasher, new_hasher, validate_checksum_fn

logger: Logger = getLogger(__name__)

DEFAULT_CHECKSUM_FN: Final[str] = "sha256"
DEFAULT_CHUNK_SIZE: Final[int] = 8192


class DownloadError(Exception):
    """Retryable download error."""


class NonRetryableDownloadError(Exception):
    """Non-retryable client or validation error."""


class ChecksumMismatchError(NonRetryableDownloadError):
    """Checksum validation failed."""


class DownloaderOptions(TypedDict, total=False):
    """Optional retry and streaming parameters for HTTP file downloaders."""

    max_attempts: int
    min_backoff: int
    max_backoff: int
    chunk_size: int


class AsyncDownloaderOptions(DownloaderOptions, total=False):
    """Optional retry, streaming, and concurrency parameters for :class:`AsyncFileDownloader`."""

    max_concurrency: int | None


HTTP_CLIENT_DEFAULTS: Final[frozendict[str, Any]] = frozendict(
    {
        "timeout": httpx.Timeout(30.0),
        "limits": httpx.Limits(
            max_connections=20,
            max_keepalive_connections=10,
        ),
        "follow_redirects": True,
    }
)


RETRY_DEFAULTS: Final[frozendict[str, int]] = frozendict(
    {
        "max_attempts": 5,
        "min_backoff": 1,
        "max_backoff": 30,
    }
)


class DownloadCore:
    """HTTP file downloader helpers for retries, status checks, and streaming checksums."""

    RETRYABLE_EXCEPTIONS: Final[tuple[type[BaseException], ...]] = (
        httpx.TimeoutException,
        httpx.TransportError,
        DownloadError,
    )

    @staticmethod
    def retry_kwargs(max_attempts: int, min_backoff: int, max_backoff: int, log: Logger) -> dict[str, Any]:
        """Build tenacity retry configuration shared by sync and async downloaders.

        :param max_attempts: maximum attempts before re-raising
        :type max_attempts: int
        :param min_backoff: minimum exponential backoff in seconds
        :type min_backoff: int
        :param max_backoff: maximum exponential backoff in seconds
        :type max_backoff: int
        :param log: logger used for pre-sleep retry warnings
        :type log: Logger
        :return: dictionary of keyword arguments for ``tenacity.retry`` or ``AsyncRetrying``
        :rtype: dict[str, Any]
        """
        return {
            "retry": retry_if_exception_type(DownloadCore.RETRYABLE_EXCEPTIONS),
            "stop": stop_after_attempt(max_attempts),
            "wait": wait_exponential(min=min_backoff, max=max_backoff),
            "reraise": True,
            "before_sleep": before_sleep_log(log, WARNING),
        }

    @staticmethod
    @contextmanager
    def capture_download_errors(url: str, log: Logger) -> Generator[None]:
        """Translate transport and HTTP errors into logged :class:`DownloadError` or :class:`NonRetryableDownloadError`.

        :param url: target URL
        :type url: str
        :param log: logger instance
        :type log: Logger
        :yield: ``None``
        :rtype: Generator[None]
        :raises NonRetryableDownloadError: when a non-retryable client or checksum error occurs
        :raises DownloadError: when a retryable transport, timeout, or server error occurs
        """
        try:
            yield
        except NonRetryableDownloadError as exc:
            detail = exc.args[0] if exc.args else str(exc)
            log.exception("%s: %s; retry not possible", url, detail, extra={"url": url})
            raise
        except Exception as exc:
            detail = exc.args[0] if exc.args else str(exc)
            log.exception("%s: %s; retry possible", url, detail, extra={"url": url})
            raise DownloadError(str(exc)) from exc

    @staticmethod
    def validate_args(
        destination: str | Path,
        checksum_fn: str | None = None,
        extra_headers: dict[str, Any] | None = None,
    ) -> tuple[Path, str, dict[str, Any]]:
        """Validate download arguments, apply defaults, and ensure parent directories exist.

        :param destination: location to save the downloaded file
        :type destination: str | Path
        :param checksum_fn: checksum algorithm name (defaults to ``"sha256"`` when omitted)
        :type checksum_fn: str | None
        :param extra_headers: optional extra HTTP request headers
        :type extra_headers: dict[str, Any] | None
        :return: tuple of ``(destination_path, validated_checksum_fn, extra_headers)``
        :rtype: tuple[Path, str, dict[str, Any]]
        """
        validated_checksum_fn = validate_checksum_fn(checksum_fn or DEFAULT_CHECKSUM_FN, include_extras=False)
        destination_path = Path(destination)
        destination_path.parent.mkdir(parents=True, exist_ok=True)
        return (destination_path, validated_checksum_fn, extra_headers or {})

    @staticmethod
    def validate_response(response: httpx.Response) -> bool:
        """Validate an HTTP response status code.

        :param response: HTTP response from the server
        :type response: httpx.Response
        :return: ``True`` on 2xx, ``False`` on 304 Not Modified
        :rtype: bool
        :raises NonRetryableDownloadError: on 4xx client errors
        :raises DownloadError: on unexpected 3xx or 5xx responses
        """
        status = response.status_code

        if status == HTTPStatus.NOT_MODIFIED:
            return False

        if HTTPStatus.OK <= status < HTTPStatus.MULTIPLE_CHOICES:
            return True

        if HTTPStatus.BAD_REQUEST <= status < HTTPStatus.INTERNAL_SERVER_ERROR:
            msg = f"Client error: {status} {response.reason_phrase}"
            raise NonRetryableDownloadError(msg)

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
        """Log a completed download and return ``destination``.

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
        hash_obj: Hasher | None,
    ) -> None:
        """Compare the computed digest against ``expected_checksum``.

        :param url: download URL
        :type url: str
        :param expected_checksum: expected hexadecimal digest
        :type expected_checksum: str
        :param checksum_fn: checksum algorithm name
        :type checksum_fn: str
        :param hash_obj: populated hasher instance, or ``None`` when hashing was skipped
        :type hash_obj: Hasher | None
        :raises ChecksumMismatchError: if the digest does not match ``expected_checksum``
        """
        if hash_obj is not None:
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
                if hash_obj is not None:
                    hash_obj.update(chunk)

            yield _write_chunk

        if hash_obj is not None and expected_checksum:
            DownloadCore.check_hash(url, expected_checksum, checksum_fn, hash_obj)

    @staticmethod
    def write_and_hash(
        url: str,
        destination: Path,
        chunks: Iterable[bytes],
        expected_checksum: str | None,
        checksum_fn: str,
    ) -> None:
        """Write downloaded byte chunks to ``destination`` and verify ``expected_checksum`` if supplied.

        :param url: URL to download from
        :type url: str
        :param destination: file path to save to
        :type destination: Path
        :param chunks: iterable chunks of downloaded data
        :type chunks: Iterable[bytes]
        :param expected_checksum: expected checksum if known
        :type expected_checksum: str | None
        :param checksum_fn: checksum algorithm name
        :type checksum_fn: str
        :raises ChecksumMismatchError: if the computed checksum does not match ``expected_checksum``
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
        """Write async byte chunks to ``destination`` and verify ``expected_checksum`` if supplied.

        :param url: URL to download from
        :type url: str
        :param destination: file path to save to
        :type destination: Path
        :param chunks: async iterable chunks of downloaded data
        :type chunks: AsyncIterable[bytes]
        :param expected_checksum: expected checksum if known
        :type expected_checksum: str | None
        :param checksum_fn: checksum algorithm name
        :type checksum_fn: str
        :raises ChecksumMismatchError: if the computed checksum does not match ``expected_checksum``
        """
        with DownloadCore.open_hash_writer(url, destination, expected_checksum, checksum_fn) as write_chunk:
            async for chunk in chunks:
                write_chunk(chunk)


class BaseFileDownloader[ClientT: (httpx.Client, httpx.AsyncClient)]:
    """Shared base class for synchronous and asynchronous HTTP file downloaders."""

    RETRYABLE_EXCEPTIONS = DownloadCore.RETRYABLE_EXCEPTIONS

    def __init__(self, client: ClientT, *, chunk_size: int = DEFAULT_CHUNK_SIZE) -> None:
        """Initialize shared HTTP client and chunk size attributes.

        :param client: configured sync or async httpx client
        :type client: ClientT
        :param chunk_size: download stream chunk size in bytes
        :type chunk_size: int
        """
        self.client = client
        self.chunk_size = chunk_size

    @staticmethod
    def _build_retry_kwargs(options: DownloaderOptions, log: Logger) -> dict[str, Any]:
        """Extract retry configuration from ``options`` with defaults from ``RETRY_DEFAULTS``.

        :param options: downloader option mapping
        :type options: DownloaderOptions
        :param log: logger used for pre-sleep retry warnings
        :type log: Logger
        :return: keyword arguments for ``tenacity.retry`` or ``AsyncRetrying``
        :rtype: dict[str, Any]
        """
        return DownloadCore.retry_kwargs(
            options.get("max_attempts", RETRY_DEFAULTS["max_attempts"]),
            options.get("min_backoff", RETRY_DEFAULTS["min_backoff"]),
            options.get("max_backoff", RETRY_DEFAULTS["max_backoff"]),
            log,
        )
