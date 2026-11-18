"""Synchronous HTTP file and JSON download client."""

from logging import Logger, getLogger
from pathlib import Path
from typing import Any, Unpack

import httpx
from tenacity import retry

from cdm_data_loaders.io.core import (
    DEFAULT_CHUNK_SIZE,
    HTTP_CLIENT_DEFAULTS,
    BaseFileDownloader,
    DownloadCore,
    DownloaderOptions,
)

logger: Logger = getLogger(__name__)


def get_httpx_client() -> httpx.Client:
    """Build a synchronous :class:`httpx.Client` configured with ``HTTP_CLIENT_DEFAULTS``.

    :return: configured HTTP client
    :rtype: httpx.Client
    """
    return httpx.Client(**HTTP_CLIENT_DEFAULTS)


def fetch_json(
    url: str,
    *,
    timeout: float | None = None,
    extra_headers: dict[str, str] | None = None,
    **options: Unpack[DownloaderOptions],
) -> dict[str, Any]:
    """Fetch and decode a JSON object from ``url`` using a managed :class:`httpx.Client` with retries.

    :param url: URL to request
    :type url: str
    :param timeout: optional request timeout in seconds overriding ``HTTP_CLIENT_DEFAULTS``
    :type timeout: float | None
    :param extra_headers: optional HTTP request headers
    :type extra_headers: dict[str, str] | None
    :param options: optional retry settings forwarded to :class:`FileDownloader`
    :type options: Unpack[DownloaderOptions]
    :return: decoded JSON dictionary
    :rtype: dict[str, Any]
    """
    client_kwargs: dict[str, Any] = dict(HTTP_CLIENT_DEFAULTS)
    if timeout is not None:
        client_kwargs["timeout"] = httpx.Timeout(timeout)
    with httpx.Client(**client_kwargs) as http_client:
        return FileDownloader(client=http_client, **options).fetch_json(url, extra_headers=extra_headers)


class FileDownloader(BaseFileDownloader[httpx.Client]):
    """Synchronous HTTP downloader with retries and streaming checksum verification."""

    def __init__(
        self,
        client: httpx.Client | None = None,
        **options: Unpack[DownloaderOptions],
    ) -> None:
        """Initialize a synchronous download client.

        :param client: optional pre-configured :class:`httpx.Client`
        :type client: httpx.Client | None
        :param options: optional ``max_attempts``, ``min_backoff``, ``max_backoff``, and ``chunk_size``
        :type options: Unpack[DownloaderOptions]
        """
        super().__init__(
            client or get_httpx_client(),
            chunk_size=options.get("chunk_size", DEFAULT_CHUNK_SIZE),
        )
        self._retry = retry(**self._build_retry_kwargs(options, logger))

    def fetch_json(
        self,
        url: str,
        *,
        extra_headers: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        """Fetch and decode a JSON object from ``url`` with retry on transient errors.

        :param url: URL to request
        :type url: str
        :param extra_headers: optional request headers
        :type extra_headers: dict[str, str] | None
        :return: decoded JSON payload
        :rtype: dict[str, Any]
        :raises DownloadError: after exhausting retries on transport, timeout, or 5xx errors
        :raises NonRetryableDownloadError: on 4xx client errors
        """
        headers = extra_headers or {}

        @self._retry
        def _once() -> dict[str, Any]:
            with DownloadCore.capture_download_errors(url, logger):
                response = self.client.get(url, headers=headers)
                DownloadCore.validate_response(response)
                payload: dict[str, Any] = response.json()
            return payload

        return _once()

    def download(
        self,
        url: str,
        destination: str | Path,
        expected_checksum: str | None = None,
        checksum_fn: str | None = None,
        extra_headers: dict[str, Any] | None = None,
    ) -> Path | None:
        """Download a file from ``url`` to ``destination`` on disk.

        Returns the saved file path on 2xx responses, or ``None`` on HTTP 304 Not Modified.

        :param url: URL to download from
        :type url: str
        :param destination: local destination path
        :type destination: str | Path
        :param expected_checksum: expected checksum if known
        :type expected_checksum: str | None
        :param checksum_fn: checksum algorithm name (defaults to ``"sha256"`` when omitted)
        :type checksum_fn: str | None
        :param extra_headers: optional extra request headers
        :type extra_headers: dict[str, Any] | None
        :return: saved file path, or ``None`` on HTTP 304
        :rtype: Path | None
        """
        dest_path, validated_fn, headers = DownloadCore.validate_args(destination, checksum_fn, extra_headers)

        @self._retry
        def _once() -> Path | None:
            return self._download_once(url, dest_path, expected_checksum, validated_fn, headers)

        return _once()

    def _download_once(
        self,
        url: str,
        destination: Path,
        expected_checksum: str | None,
        checksum_fn: str,
        extra_headers: dict[str, Any],
    ) -> Path | None:
        """Execute a single synchronous download attempt.

        :param url: URL to download from
        :type url: str
        :param destination: local destination path
        :type destination: Path
        :param expected_checksum: expected checksum if known
        :type expected_checksum: str | None
        :param checksum_fn: validated checksum algorithm name
        :type checksum_fn: str
        :param extra_headers: request headers
        :type extra_headers: dict[str, Any]
        :return: saved file path, or ``None`` on HTTP 304
        :rtype: Path | None
        """
        with (
            DownloadCore.capture_download_errors(url, logger),
            self.client.stream("GET", url, headers=extra_headers) as response,
        ):
            if not DownloadCore.check_should_download(response, url, logger):
                return None

            chunks = response.iter_bytes(chunk_size=self.chunk_size)
            DownloadCore.write_and_hash(
                url,
                destination,
                chunks,
                expected_checksum=expected_checksum,
                checksum_fn=checksum_fn,
            )

        return DownloadCore.log_download_success(url, destination, logger)
