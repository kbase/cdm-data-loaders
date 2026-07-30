"""Recursive directory-listing scraper for Apache/lighttpd-style autoindex pages.

Parsing and deduplication logic is kept free of network I/O so it can be
unit tested directly against sample HTML/text fixtures. `crawl_directory`
and `crawl_directory_async` are thin I/O drivers built on top of that logic,
sharing all per-page processing via `process_listing`.
"""

import asyncio
import dataclasses
from dataclasses import dataclass
from logging import Logger, getLogger
from pathlib import Path
from urllib.parse import urljoin, urlparse

import httpx
from bs4 import BeautifulSoup

from cdm_data_loaders.utils.file_transfer.checksums import ChecksumEntry, validate_checksum_fn
from cdm_data_loaders.utils.file_transfer.s3.client import split_s3_path

logger: Logger = getLogger(__name__)

# Extensions that indicate a "compressed" duplicate of another file.
# e.g. "bac120_taxonomy.tsv.gz" is a compressed version of "bac120_taxonomy.tsv"
COMPRESSION_EXTS: frozenset[str] = frozenset({".gz", ".bz2", ".xz", ".zst", ".zip", ".lz4"})

# Default name/algorithm for the checksum manifest file present in each directory.
DEFAULT_CHECKSUM_MANIFEST = "MD5SUM.txt"
DEFAULT_CHECKSUM_ALGORITHM = "md5"


@dataclass(frozen=True, slots=True)
class DirEntry:
    """A single entry (file or directory) found in a directory listing.

    :param name: display name of the entry, with any trailing "/" removed
    :type name: str
    :param url: absolute URL of the entry
    :type url: str
    :param is_dir: whether the entry is a directory
    :type is_dir: bool
    :param checksum: expected checksum for this entry, if found in a manifest
        file elsewhere in the same directory; None for directories or files
        not covered by a manifest
    :type checksum: ChecksumEntry | None
    """

    name: str
    url: str
    is_dir: bool
    checksum: ChecksumEntry | None = None


@dataclass(frozen=True, slots=True)
class ListingResult:
    """The result of processing a single directory listing page.

    :param files: file entries in this directory, excluding any checksum
        manifest file and with compressed duplicates already collapsed
    :type files: list[DirEntry]
    :param subdirs: subdirectory entries to recurse into, excluding any
        directories named in `exclude_dirs`
    :type subdirs: list[DirEntry]
    :param manifest_url: absolute URL of the checksum manifest file found in
        this directory, or None if none was present
    :type manifest_url: str | None
    """

    files: list[DirEntry]
    subdirs: list[DirEntry]
    manifest_url: str | None


def _strip_compression_ext(name: str) -> str | None:
    """Return `name` with a known compression suffix removed, if it has one.

    :param name: file name to inspect
    :type name: str
    :return: the base name without the compression suffix, or None if `name`
        does not end in a recognised compression extension
    :rtype: str | None
    """
    suffix = Path(name).suffix
    if suffix in COMPRESSION_EXTS:
        return Path(name).stem
    return None


def _normalise_dir_url(url: str) -> str:
    """Ensure a directory URL has a trailing slash.

    :param url: directory URL to normalise
    :type url: str
    :return: `url` with a trailing "/" appended if it did not already have one
    :rtype: str
    """
    return url if url.endswith("/") else url + "/"


def parse_directory_listing(html: str, page_url: str) -> list[DirEntry]:
    """Parse an autoindex-style HTML page into a list of DirEntry objects.

    Skips "Parent Directory" links entirely. Classifies entries using the
    "Type" column (Directory/File) rather than the row's icon, since it has
    a small, reliable set of values compared to the many icon names used
    (folder.svg, txt.svg, tree.svg, etc).

    :param html: raw HTML of the directory listing page
    :type html: str
    :param page_url: URL the page was fetched from, used to resolve relative hrefs
    :type page_url: str
    :return: list of DirEntry, in document order
    :rtype: list[DirEntry]
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

        if href in ("../", "..") or name.lower() == "parent directory":
            continue

        is_dir = entry_type.lower() == "directory"
        url = urljoin(page_url, href)
        entries.append(DirEntry(name=name.rstrip("/"), url=url, is_dir=is_dir))

    return entries


def dedupe_compressed(entries: list[DirEntry], *, prefer_compressed: bool = True) -> list[DirEntry]:
    """Collapse pairs like (foo.tsv, foo.tsv.gz) down to a single entry.

    Directories are grouped separately and are never deduped against files.

    :param entries: entries from a single directory listing
    :type entries: list[DirEntry]
    :param prefer_compressed: if True, keep the compressed file when both a
        compressed and uncompressed version exist; otherwise keep the
        uncompressed one, defaults to True
    :type prefer_compressed: bool, optional
    :return: deduplicated list, preserving original ordering
    :rtype: list[DirEntry]
    """
    groups: dict[str, list[DirEntry]] = {}
    order: list[str] = []

    for entry in entries:
        key = f"__dir__::{entry.name}" if entry.is_dir else (_strip_compression_ext(entry.name) or entry.name)
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


def process_listing(
    html: str,
    page_url: str,
    *,
    exclude_dirs: set[str] | None = None,
    prefer_compressed: bool = True,
    checksum_manifest: str | None = DEFAULT_CHECKSUM_MANIFEST,
) -> ListingResult:
    """Parse, dedupe, and split a single directory listing page.

    Pure function containing all the per-page logic shared by the sync and
    async crawlers, so it can be unit tested without any network access.

    :param html: raw HTML of the directory listing page
    :type html: str
    :param page_url: URL the page was fetched from, used to resolve relative hrefs
    :type page_url: str
    :param exclude_dirs: directory names to exclude from the returned subdirs, defaults to None
    :type exclude_dirs: set[str] | None, optional
    :param prefer_compressed: passed through to `dedupe_compressed`, defaults to True
    :type prefer_compressed: bool, optional
    :param checksum_manifest: file name treated as a checksum manifest in this
        directory, or None to disable checksum manifest detection, defaults
        to `DEFAULT_CHECKSUM_MANIFEST`
    :type checksum_manifest: str | None, optional
    :return: the files, subdirectories, and manifest URL found on this page
    :rtype: ListingResult
    """
    exclude_dirs = exclude_dirs or set()

    entries = parse_directory_listing(html, page_url)

    manifest_url = None
    if checksum_manifest:
        manifest_entry = next((e for e in entries if not e.is_dir and e.name == checksum_manifest), None)
        if manifest_entry:
            manifest_url = manifest_entry.url

    entries = dedupe_compressed(entries, prefer_compressed=prefer_compressed)

    files: list[DirEntry] = []
    subdirs: list[DirEntry] = []
    for entry in entries:
        if entry.is_dir:
            if entry.name in exclude_dirs:
                logger.info("Skipping excluded directory: %s", entry.name)
                continue
            subdirs.append(entry)
        elif entry.name != checksum_manifest:
            files.append(entry)

    return ListingResult(files=files, subdirs=subdirs, manifest_url=manifest_url)


def parse_checksum_file(text: str, base_url: str, algorithm: str) -> dict[str, ChecksumEntry]:
    """Parse a `*SUM.txt`-style checksum manifest (md5sum/sha256sum -c format).

    Expected line format: "<hex digest>  ./relative/path", one entry per
    line (matches the output of GNU coreutils' md5sum/sha256sum/etc).

    :param text: raw contents of the checksum manifest file
    :type text: str
    :param base_url: URL of the directory the manifest was fetched from;
        relative paths inside the file are resolved against this
    :type base_url: str
    :param algorithm: hashlib algorithm name the digests in this file were
        generated with, e.g. "md5", "sha256"
    :type algorithm: str
    :raises ValueError: if `algorithm` is not a supported hashlib algorithm
    :return: mapping of absolute file URL to its expected ChecksumEntry
    :rtype: dict[str, ChecksumEntry]
    """
    algorithm = validate_checksum_fn(algorithm)
    checksums: dict[str, ChecksumEntry] = {}

    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue

        parts = line.split(None, 1)
        if len(parts) != 2:  # noqa: PLR2004
            continue

        digest, rel_path = parts
        rel_path = rel_path.strip().removeprefix("./")
        url = urljoin(base_url, rel_path)
        checksums[url] = ChecksumEntry(algorithm=algorithm, value=digest.lower())

    return checksums


def _with_checksums(files: list[DirEntry], checksums: dict[str, ChecksumEntry]) -> list[DirEntry]:
    """Attach checksum information to a list of file entries.

    :param files: file entries to annotate
    :type files: list[DirEntry]
    :param checksums: mapping of absolute file URL to its expected checksum
    :type checksums: dict[str, ChecksumEntry]
    :return: new list of DirEntry, each with `checksum` populated from `checksums` if present
    :rtype: list[DirEntry]
    """
    return [dataclasses.replace(f, checksum=checksums.get(f.url)) for f in files]


def crawl_directory(  # noqa: PLR0913
    start_url: str,
    *,
    exclude_dirs: set[str] | None = None,
    prefer_compressed: bool = True,
    client: httpx.Client | None = None,
    checksum_manifest: str | None = DEFAULT_CHECKSUM_MANIFEST,
    checksum_algorithm: str = DEFAULT_CHECKSUM_ALGORITHM,
) -> list[DirEntry]:
    """Recursively crawl a directory-listing tree, returning all file entries found.

    :param start_url: root directory listing URL to start crawling from
    :type start_url: str
    :param exclude_dirs: directory names to skip entirely, wherever in the
        tree they appear, defaults to None
    :type exclude_dirs: set[str] | None, optional
    :param prefer_compressed: passed through to `dedupe_compressed`, defaults to True
    :type prefer_compressed: bool, optional
    :param client: pre-configured httpx.Client to use for requests, defaults to None
    :type client: httpx.Client | None, optional
    :param checksum_manifest: file name treated as a checksum manifest in each
        directory, or None to disable checksum lookup, defaults to
        `DEFAULT_CHECKSUM_MANIFEST`
    :type checksum_manifest: str | None, optional
    :param checksum_algorithm: hashlib algorithm used to generate `checksum_manifest`,
        defaults to `DEFAULT_CHECKSUM_ALGORITHM`
    :type checksum_algorithm: str, optional
    :return: flat list of file DirEntry objects across the whole tree, with
        `checksum` populated where a manifest covered them
    :rtype: list[DirEntry]
    """
    exclude_dirs = exclude_dirs or set()
    own_client = client is None
    client = client or httpx.Client(follow_redirects=True, timeout=30.0)

    files: list[DirEntry] = []
    checksums: dict[str, ChecksumEntry] = {}

    try:
        _crawl_sync(
            start_url,
            client,
            exclude_dirs,
            prefer_compressed,
            checksum_manifest,
            checksum_algorithm,
            files,
            checksums,
        )
    finally:
        if own_client:
            client.close()

    return _with_checksums(files, checksums)


def _crawl_sync(  # noqa: PLR0913
    url: str,
    client: httpx.Client,
    exclude_dirs: set[str],
    prefer_compressed: bool,  # noqa: FBT001
    checksum_manifest: str | None,
    checksum_algorithm: str,
    files: list[DirEntry],
    checksums: dict[str, ChecksumEntry],
) -> None:
    """Recursively crawl a single directory and its subdirectories, synchronously.

    :param url: directory URL to list
    :type url: str
    :param client: httpx.Client used to perform requests
    :type client: httpx.Client
    :param exclude_dirs: directory names to skip entirely
    :type exclude_dirs: set[str]
    :param prefer_compressed: passed through to `dedupe_compressed`
    :type prefer_compressed: bool
    :param checksum_manifest: file name treated as a checksum manifest, or None to disable
    :type checksum_manifest: str | None
    :param checksum_algorithm: hashlib algorithm used to generate `checksum_manifest`
    :type checksum_algorithm: str
    :param files: accumulator list that discovered file entries are appended to
    :type files: list[DirEntry]
    :param checksums: accumulator mapping that discovered checksums are merged into
    :type checksums: dict[str, ChecksumEntry]
    """
    url = _normalise_dir_url(url)
    logger.info("Listing %s", url)

    response = client.get(url)
    response.raise_for_status()

    result = process_listing(
        response.text,
        url,
        exclude_dirs=exclude_dirs,
        prefer_compressed=prefer_compressed,
        checksum_manifest=checksum_manifest,
    )

    if result.manifest_url:
        logger.info("Found checksum manifest %s", result.manifest_url)
        manifest_response = client.get(result.manifest_url)
        manifest_response.raise_for_status()
        checksums.update(parse_checksum_file(manifest_response.text, url, checksum_algorithm))

    files.extend(result.files)

    for subdir in result.subdirs:
        _crawl_sync(
            subdir.url,
            client,
            exclude_dirs,
            prefer_compressed,
            checksum_manifest,
            checksum_algorithm,
            files,
            checksums,
        )


async def crawl_directory_async(  # noqa: PLR0913
    start_url: str,
    *,
    exclude_dirs: set[str] | None = None,
    prefer_compressed: bool = True,
    client: httpx.AsyncClient | None = None,
    max_concurrency: int | None = None,
    checksum_manifest: str | None = DEFAULT_CHECKSUM_MANIFEST,
    checksum_algorithm: str = DEFAULT_CHECKSUM_ALGORITHM,
) -> list[DirEntry]:
    """Async version of `crawl_directory`; subdirectories are crawled concurrently.

    :param start_url: root directory listing URL to start crawling from
    :type start_url: str
    :param exclude_dirs: directory names to skip entirely, wherever in the
        tree they appear, defaults to None
    :type exclude_dirs: set[str] | None, optional
    :param prefer_compressed: passed through to `dedupe_compressed`, defaults to True
    :type prefer_compressed: bool, optional
    :param client: pre-configured httpx.AsyncClient to use for requests, defaults to None
    :type client: httpx.AsyncClient | None, optional
    :param max_concurrency: cap on concurrent listing/manifest fetches, defaults to None
    :type max_concurrency: int | None, optional
    :param checksum_manifest: file name treated as a checksum manifest in each
        directory, or None to disable checksum lookup, defaults to
        `DEFAULT_CHECKSUM_MANIFEST`
    :type checksum_manifest: str | None, optional
    :param checksum_algorithm: hashlib algorithm used to generate `checksum_manifest`,
        defaults to `DEFAULT_CHECKSUM_ALGORITHM`
    :type checksum_algorithm: str, optional
    :return: flat list of file DirEntry objects across the whole tree, with
        `checksum` populated where a manifest covered them
    :rtype: list[DirEntry]
    """
    exclude_dirs = exclude_dirs or set()
    own_client = client is None
    client = client or httpx.AsyncClient(follow_redirects=True, timeout=30.0)
    semaphore = asyncio.Semaphore(max_concurrency) if max_concurrency else None

    files: list[DirEntry] = []
    checksums: dict[str, ChecksumEntry] = {}
    lock = asyncio.Lock()

    try:
        await _crawl_async(
            start_url,
            client,
            semaphore,
            lock,
            exclude_dirs,
            prefer_compressed,
            checksum_manifest,
            checksum_algorithm,
            files,
            checksums,
        )
    finally:
        if own_client:
            await client.aclose()

    return _with_checksums(files, checksums)


async def _fetch_async(client: httpx.AsyncClient, semaphore: asyncio.Semaphore | None, url: str) -> httpx.Response:
    """Perform a GET request, respecting an optional concurrency limit.

    :param client: httpx.AsyncClient used to perform the request
    :type client: httpx.AsyncClient
    :param semaphore: concurrency limiter to acquire before the request, or None
    :type semaphore: asyncio.Semaphore | None
    :param url: URL to fetch
    :type url: str
    :return: the HTTP response
    :rtype: httpx.Response
    """
    if semaphore:
        async with semaphore:
            return await client.get(url)
    return await client.get(url)


async def _crawl_async(  # noqa: PLR0913
    url: str,
    client: httpx.AsyncClient,
    semaphore: asyncio.Semaphore | None,
    lock: asyncio.Lock,
    exclude_dirs: set[str],
    prefer_compressed: bool,  # noqa: FBT001
    checksum_manifest: str | None,
    checksum_algorithm: str,
    files: list[DirEntry],
    checksums: dict[str, ChecksumEntry],
) -> None:
    """Recursively crawl a single directory and its subdirectories, concurrently.

    :param url: directory URL to list
    :type url: str
    :param client: httpx.AsyncClient used to perform requests
    :type client: httpx.AsyncClient
    :param semaphore: concurrency limiter shared across the whole crawl, or None
    :type semaphore: asyncio.Semaphore | None
    :param lock: lock guarding concurrent writes to `files`/`checksums`
    :type lock: asyncio.Lock
    :param exclude_dirs: directory names to skip entirely
    :type exclude_dirs: set[str]
    :param prefer_compressed: passed through to `dedupe_compressed`
    :type prefer_compressed: bool
    :param checksum_manifest: file name treated as a checksum manifest, or None to disable
    :type checksum_manifest: str | None
    :param checksum_algorithm: hashlib algorithm used to generate `checksum_manifest`
    :type checksum_algorithm: str
    :param files: accumulator list that discovered file entries are appended to
    :type files: list[DirEntry]
    :param checksums: accumulator mapping that discovered checksums are merged into
    :type checksums: dict[str, ChecksumEntry]
    """
    url = _normalise_dir_url(url)
    logger.info("Listing %s", url)

    response = await _fetch_async(client, semaphore, url)
    response.raise_for_status()

    result = process_listing(
        response.text,
        url,
        exclude_dirs=exclude_dirs,
        prefer_compressed=prefer_compressed,
        checksum_manifest=checksum_manifest,
    )

    if result.manifest_url:
        logger.info("Found checksum manifest %s", result.manifest_url)
        manifest_response = await _fetch_async(client, semaphore, result.manifest_url)
        manifest_response.raise_for_status()
        parsed = parse_checksum_file(manifest_response.text, url, checksum_algorithm)
        async with lock:
            checksums.update(parsed)

    async with lock:
        files.extend(result.files)

    if result.subdirs:
        await asyncio.gather(
            *(
                _crawl_async(
                    subdir.url,
                    client,
                    semaphore,
                    lock,
                    exclude_dirs,
                    prefer_compressed,
                    checksum_manifest,
                    checksum_algorithm,
                    files,
                    checksums,
                )
                for subdir in result.subdirs
            )
        )


def _relative_path(entry: DirEntry, start_url: str) -> str:
    """Compute an entry's path relative to the crawl's start URL.

    :param entry: the entry to compute a relative path for
    :type entry: DirEntry
    :param start_url: the root URL that was passed to crawl_directory(_async)
    :type start_url: str
    :return: the entry's path, relative to `start_url`, with no leading "/"
    :rtype: str
    """
    start_path = urlparse(_normalise_dir_url(start_url)).path
    entry_path = urlparse(entry.url).path
    return entry_path[len(start_path) :].lstrip("/")


def entry_destination(entry: DirEntry, start_url: str, output_dir: Path) -> Path:
    """Compute a local destination path that mirrors the remote directory structure.

    :param entry: the file entry to compute a path for
    :type entry: DirEntry
    :param start_url: the root URL that was passed to crawl_directory(_async)
    :type start_url: str
    :param output_dir: local base directory to save into
    :type output_dir: Path
    :return: local path preserving the remote directory layout below start_url
    :rtype: Path
    """
    return output_dir / _relative_path(entry, start_url)


def entry_s3_key(entry: DirEntry, start_url: str, s3_prefix: str) -> str:
    """Compute an S3 destination path that mirrors the remote directory structure.

    :param entry: the file entry to compute a path for
    :type entry: DirEntry
    :param start_url: the root URL that was passed to crawl_directory(_async)
    :type start_url: str
    :param s3_prefix: base S3 location to save into, e.g. "s3://my-bucket/gtdb/latest"
    :type s3_prefix: str
    :return: S3 path in the form "bucket/key"
    :rtype: str
    """
    bucket, base_key = split_s3_path(s3_prefix)
    rel = _relative_path(entry, start_url)
    key = f"{base_key.rstrip('/')}/{rel}" if base_key else rel
    return f"{bucket}/{key}"
