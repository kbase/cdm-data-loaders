"""Shared adapter helper for testing sync and async HTTP downloaders."""

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from pathlib import Path

from cdm_data_loaders.io.async_client import AsyncFileDownloader
from cdm_data_loaders.io.sync_client import FileDownloader


@dataclass(frozen=True, slots=True)
class DownloaderAdapter:
    """Uniform test interface wrapping sync ``FileDownloader`` and async ``AsyncFileDownloader``."""

    name: str
    make_downloader: Callable[..., FileDownloader | AsyncFileDownloader]
    download: Callable[..., Awaitable[Path | None]]
