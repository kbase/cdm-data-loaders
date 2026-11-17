"""Fixtures for testing sync and async HTTP file downloaders."""

from collections.abc import Callable
from pathlib import Path
from typing import cast

import httpx
import pytest

from cdm_data_loaders.io.async_client import AsyncFileDownloader
from cdm_data_loaders.io.sync_client import FileDownloader
from tests.cdm_data_loaders.io._helpers import DownloaderAdapter


@pytest.fixture(params=["sync", "async"])
def downloader_adapter(request: pytest.FixtureRequest) -> DownloaderAdapter:
    """Return a ``DownloaderAdapter`` wrapping either ``FileDownloader`` or ``AsyncFileDownloader``.

    :param request: Active pytest fixture request.
    :type request: pytest.FixtureRequest
    :return: Configured ``DownloaderAdapter``.
    :rtype: DownloaderAdapter
    """

    def make_sync(handler: Callable[[httpx.Request], httpx.Response], **kwargs: object) -> FileDownloader:
        client = httpx.Client(transport=httpx.MockTransport(handler))
        return FileDownloader(client, **kwargs)  # pyright: ignore[reportArgumentType]

    def make_async(handler: Callable[[httpx.Request], httpx.Response], **kwargs: object) -> AsyncFileDownloader:
        client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        return AsyncFileDownloader(client, **kwargs)  # pyright: ignore[reportArgumentType]

    async def download_sync(downloader: object, *args: object, **kwargs: object) -> Path | None:
        sync_dl = cast("FileDownloader", downloader)
        return sync_dl.download(*args, **kwargs)  # pyright: ignore[reportArgumentType]

    async def download_async(downloader: object, *args: object, **kwargs: object) -> Path | None:
        async_dl = cast("AsyncFileDownloader", downloader)
        return await async_dl.download(*args, **kwargs)  # pyright: ignore[reportArgumentType]

    if request.param == "sync":
        return DownloaderAdapter(name="sync", make_downloader=make_sync, download=download_sync)
    return DownloaderAdapter(name="async", make_downloader=make_async, download=download_async)
