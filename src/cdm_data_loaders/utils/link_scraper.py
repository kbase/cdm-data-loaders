"""Recursive directory-listing scraper for Apache/lighttpd-style autoindex pages."""

import asyncio
from dataclasses import dataclass
from logging import Logger, getLogger
from pathlib import Path
from urllib.parse import urljoin, urlparse

import httpx
from bs4 import BeautifulSoup

from cdm_data_loaders.utils.s3 import split_s3_path

logger: Logger = getLogger(__name__)

# Extensions that indicate a "compressed" duplicate of another file.
# e.g. "bac120_taxonomy.tsv.gz" is a compressed version of "bac120_taxonomy.tsv"
COMPRESSION_EXTS: frozenset[str] = frozenset({".gz", ".bz2", ".xz", ".zst", ".zip", ".lz4"})


@dataclass(frozen=True, slots=True)
class DirEntry:
    """A single entry (file or directory) found in a directory listing."""

    name: str
    url: str
    is_dir: bool


def _strip_compression_ext(name: str) -> str | None:
    """Return the base name with a compression suffix removed, or None if not compressed."""
    p = Path(name)
    if p.suffix in COMPRESSION_EXTS:
        return p.stem
    return None


def parse_directory_listing(html: str, page_url: str) -> list[DirEntry]:
    """Parse an autoindex-style HTML page into a list of DirEntry objects.

    Skips "Parent Directory" links entirely. Uses the "Type" column (Directory/File)
    to classify entries rather than the icon, since it's more reliable to select.

    :param html: raw HTML of the directory listing page
    :param page_url: the URL the page was fetched from (used to resolve relative hrefs)
    :return: list of DirEntry, in document order
    """
    soup = BeautifulSoup(html, "html.parser")
    entries: list[DirEntry] = []

    for row in soup.select("table tbody tr"):
        name_cell = row.find("td", class_="n")
        type_cell = row.find("td", class_="t")
        if not name_cell or not type_cell:
            continue

        link = name_cell.find("a")
        if not link or not link.get("href"):
            continue

        href = link["href"]
        name = link.get_text(strip=True)
        entry_type = type_cell.get_text(strip=True)

        # Ignore "Parent Directory" / "../" links entirely
        if href in ("../", "..") or name.lower() == "parent directory":
            continue

        is_dir = entry_type.lower() == "directory"
        url = urljoin(page_url, href)
        entries.append(DirEntry(name=name.rstrip("/"), url=url, is_dir=is_dir))

    return entries


def dedupe_compressed(entries: list[DirEntry], *, prefer_compressed: bool = True) -> list[DirEntry]:
    """Collapse pairs like (foo.tsv, foo.tsv.gz) down to a single entry.

    Directories are never deduped against each other/files.

    :param entries: entries from a single directory listing
    :param prefer_compressed: if True, keep the compressed file when both exist;
        otherwise keep the uncompressed one
    :return: deduplicated list, preserving original ordering
    """
    groups: dict[str, list[DirEntry]] = {}
    order: list[str] = []

    for entry in entries:
        if entry.is_dir:
            key = f"__dir__::{entry.name}"
        else:
            base = _strip_compression_ext(entry.name)
            key = base if base is not None else entry.name

        if key not in groups:
            order.append(key)
            groups[key] = []
        groups[key].append(entry)

    result: list[DirEntry] = []
    for key in order:
        group = groups[key]
        if len(group) == 1:
            result.append(group[0])
            continue

        compressed = [e for e in group if _strip_compression_ext(e.name) is not None]
        uncompressed = [e for e in group if _strip_compression_ext(e.name) is None]

        if prefer_compressed and compressed:
            chosen = compressed[0]
        elif uncompressed:
            chosen = uncompressed[0]
        else:
            chosen = group[0]

        skipped = [e.name for e in group if e is not chosen]
        logger.info("Deduped %s in favour of %s (skipped: %s)", key, chosen.name, skipped)
        result.append(chosen)

    return result


def _normalise_dir_url(url: str) -> str:
    return url if url.endswith("/") else url + "/"


def crawl_directory(
    start_url: str,
    *,
    exclude_dirs: set[str] | None = None,
    prefer_compressed: bool = True,
    client: httpx.Client | None = None,
) -> list[DirEntry]:
    """Recursively crawl a directory-listing tree, returning all *file* entries found.

    :param start_url: root directory listing URL to start crawling from
    :param exclude_dirs: set of directory *names* (not paths) to skip entirely
    :param prefer_compressed: passed through to `dedupe_compressed`
    :param client: optional pre-configured httpx.Client
    :return: flat list of file DirEntry objects across the whole tree
    """
    exclude_dirs = exclude_dirs or set()
    own_client = client is None
    client = client or httpx.Client(follow_redirects=True, timeout=30.0)
    files: list[DirEntry] = []

    try:
        _crawl_sync(start_url, exclude_dirs, prefer_compressed, client, files)
    finally:
        if own_client:
            client.close()

    return files


def _crawl_sync(
    url: str,
    exclude_dirs: set[str],
    prefer_compressed: bool,
    client: httpx.Client,
    files: list[DirEntry],
) -> None:
    url = _normalise_dir_url(url)
    logger.info("Listing %s", url)

    response = client.get(url)
    response.raise_for_status()

    entries = parse_directory_listing(response.text, url)
    entries = dedupe_compressed(entries, prefer_compressed=prefer_compressed)

    for entry in entries:
        if entry.is_dir:
            if entry.name in exclude_dirs:
                logger.info("Skipping excluded directory: %s", entry.name)
                continue
            _crawl_sync(entry.url, exclude_dirs, prefer_compressed, client, files)
        else:
            files.append(entry)


async def crawl_directory_async(
    start_url: str,
    *,
    exclude_dirs: set[str] | None = None,
    prefer_compressed: bool = True,
    client: httpx.AsyncClient | None = None,
    max_concurrency: int | None = None,
) -> list[DirEntry]:
    """Async version of `crawl_directory`; subdirectories are crawled concurrently.

    :param start_url: root directory listing URL to start crawling from
    :param exclude_dirs: set of directory *names* (not paths) to skip entirely
    :param prefer_compressed: passed through to `dedupe_compressed`
    :param client: optional pre-configured httpx.AsyncClient
    :param max_concurrency: cap on concurrent listing fetches
    :return: flat list of file DirEntry objects across the whole tree
    """
    exclude_dirs = exclude_dirs or set()
    own_client = client is None
    client = client or httpx.AsyncClient(follow_redirects=True, timeout=30.0)
    semaphore = asyncio.Semaphore(max_concurrency) if max_concurrency else None

    files: list[DirEntry] = []
    lock = asyncio.Lock()

    async def _fetch(url: str) -> httpx.Response:
        if semaphore:
            async with semaphore:
                return await client.get(url)
        return await client.get(url)

    async def _crawl(url: str) -> None:
        url = _normalise_dir_url(url)
        logger.info("Listing %s", url)

        response = await _fetch(url)
        response.raise_for_status()

        entries = parse_directory_listing(response.text, url)
        entries = dedupe_compressed(entries, prefer_compressed=prefer_compressed)

        subdir_urls: list[str] = []
        for entry in entries:
            if entry.is_dir:
                if entry.name in exclude_dirs:
                    logger.info("Skipping excluded directory: %s", entry.name)
                    continue
                subdir_urls.append(entry.url)
            else:
                async with lock:
                    files.append(entry)

        if subdir_urls:
            await asyncio.gather(*(_crawl(u) for u in subdir_urls))

    try:
        await _crawl(start_url)
    finally:
        if own_client:
            await client.aclose()

    return files


def entry_destination(entry: DirEntry, start_url: str, output_dir: Path) -> Path:
    """Compute a local destination path that mirrors the remote directory structure.

    :param entry: the file entry to compute a path for
    :param start_url: the root URL that was passed to crawl_directory(_async)
    :param output_dir: local base directory to save into
    :return: local path preserving the remote directory layout below start_url
    """
    start_path = urlparse(_normalise_dir_url(start_url)).path
    entry_path = urlparse(entry.url).path
    rel = entry_path[len(start_path) :].lstrip("/")
    return output_dir / rel


def entry_s3_key(entry: DirEntry, start_url: str, s3_prefix: str) -> str:
    """Compute an S3 destination path ('bucket/key') that mirrors the remote directory structure.

    :param entry: the file entry to compute a path for
    :param start_url: the root URL that was passed to crawl_directory(_async)
    :param s3_prefix: base S3 location to save into, e.g. 's3://my-bucket/gtdb/latest'
    :return: S3 path in the form 'bucket/key'
    """
    start_path = urlparse(_normalise_dir_url(start_url)).path
    entry_path = urlparse(entry.url).path
    rel = entry_path[len(start_path) :].lstrip("/")

    bucket, base_key = split_s3_path(s3_prefix)
    key = f"{base_key.rstrip('/')}/{rel}" if base_key else rel
    return f"{bucket}/{key}"
