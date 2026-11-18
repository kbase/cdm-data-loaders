"""Asynchronous HTTP file download client."""

import asyncio
import contextlib
from logging import Logger, getLogger
from pathlib import Path
from typing import Any, Unpack

import httpx
from tenacity import AsyncRetrying

from cdm_data_loaders.io.core import (
    DEFAULT_CHUNK_SIZE,
    HTTP_CLIENT_DEFAULTS,
    AsyncDownloaderOptions,
    BaseFileDownloader,
    DownloadCore,
)

logger: Logger = getLogger(__name__)


def get_async_httpx_client() -> httpx.AsyncClient:
    """Build an asynchronous :class:`httpx.AsyncClient` configured with ``HTTP_CLIENT_DEFAULTS``.

    :return: configured async HTTP client
    :rtype: httpx.AsyncClient
    """
    return httpx.AsyncClient(**HTTP_CLIENT_DEFAULTS)


class AsyncFileDownloader(BaseFileDownloader[httpx.AsyncClient]):
    """Asynchronous HTTP downloader with retries, concurrency limits, and checksum verification."""

    def __init__(
        self,
        client: httpx.AsyncClient | None = None,
        **options: Unpack[AsyncDownloaderOptions],
    ) -> None:
        """Initialize an asynchronous download client.

        :param client: optional pre-configured :class:`httpx.AsyncClient`
        :type client: httpx.AsyncClient | None
        :param options: optional ``max_attempts``, ``min_backoff``, ``max_backoff``, ``chunk_size``, and ``max_concurrency``
        :type options: Unpack[AsyncDownloaderOptions]
        """
        super().__init__(
            client or get_async_httpx_client(),
            chunk_size=options.get("chunk_size", DEFAULT_CHUNK_SIZE),
        )
        max_concurrency = options.get("max_concurrency")
        self.semaphore = asyncio.Semaphore(max_concurrency) if max_concurrency else None
        self._retry = AsyncRetrying(**self._build_retry_kwargs(options, logger))

    async def download(
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

        async with self.semaphore or contextlib.nullcontext():
            return await self._download_with_retry(
                url,
                dest_path,
                expected_checksum,
                validated_fn,
                headers,
            )

    async def _download_with_retry(
        self,
        url: str,
        destination: Path,
        expected_checksum: str | None,
        checksum_fn: str,
        extra_headers: dict[str, Any],
    ) -> Path | None:
        """Run :meth:`_download_once` inside the configured ``AsyncRetrying`` loop.

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
        :raises RuntimeError: if the retry iterator yields zero attempts
        """
        async for attempt in self._retry:
            with attempt:
                return await self._download_once(url, destination, expected_checksum, checksum_fn, extra_headers)
        msg = "Iterator exhausted, unreachable code reached!"
        raise RuntimeError(msg)

    async def _download_once(
        self,
        url: str,
        destination: Path,
        expected_checksum: str | None,
        checksum_fn: str,
        extra_headers: dict[str, Any],
    ) -> Path | None:
        """Execute a single asynchronous download attempt.

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
        with DownloadCore.capture_download_errors(url, logger):
            async with self.client.stream("GET", url, headers=extra_headers) as response:
                if not DownloadCore.check_should_download(response, url, logger):
                    return None

                chunks = response.aiter_bytes(chunk_size=self.chunk_size)
                await DownloadCore.write_and_hash_async(
                    url,
                    destination,
                    chunks,
                    expected_checksum=expected_checksum,
                    checksum_fn=checksum_fn,
                )

        return DownloadCore.log_download_success(url, destination, logger)
