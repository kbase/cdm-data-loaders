"""Helpers for treating dlt ``FileItemDict`` objects as the unit of pipeline input.

Readers accept a local path (``str`` or ``Path``) or a ``FileItemDict`` emitted by the dlt ``filesystem``
resource. ``FileItemDict`` works for any fsspec protocol (``file``, ``s3``, ``memory``, ...), so readers that
only ever call ``FileItemDict.open`` work the same way for local and remote files.

``FileItemDict.open`` (with dlt's default ``compression="auto"``) decompresses gzip when the listing marked the
file as gzip-encoded. dlt sets that from the file name (``*.gz``, ``*.tgz``) for every protocol, so no
repository-specific gzip handling is needed. Build ``FileItemDict`` objects with dlt's ``glob_files`` (as
``as_file_item`` does) rather than by hand: a hand-built dict has no ``encoding`` key and will not be decompressed.
"""

import errno
import glob
import os
import posixpath
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path, PurePosixPath
from typing import IO, Any

import fsspec
from dlt.common.storages.fsspec_filesystem import FileItemDict, glob_files

FileSource = str | Path | FileItemDict
"""A local file path or a dlt file item."""


def as_file_item(file: FileSource) -> FileItemDict:
    """Return ``file`` as a ``FileItemDict``.

    A ``FileItemDict`` is returned unchanged. A local path is listed with dlt's ``glob_files``, so the result
    carries the same fields (including ``encoding``) as items emitted by the dlt ``filesystem`` resource. Its
    ``relative_path`` is the file name.

    :param file: local path or dlt file item
    :type file: FileSource
    :raises IsADirectoryError: if ``file`` is a directory
    :raises FileNotFoundError: if ``file`` does not exist
    :return: file item for ``file``
    :rtype: FileItemDict
    """
    if isinstance(file, FileItemDict):
        return file

    path = Path(file).resolve()
    if path.is_dir():
        raise IsADirectoryError(errno.EISDIR, os.strerror(errno.EISDIR), str(file))

    local_fs = fsspec.filesystem("file")
    # escape the name so that glob metacharacters ([, ], *, ?) in file names are matched literally
    matches = list(glob_files(local_fs, str(path.parent), glob.escape(path.name)))
    if not matches:
        raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), str(file))
    return FileItemDict(matches[0], local_fs)


def join_url(bucket_url: str, *parts: str) -> str:
    """Join path parts onto a bucket URL (``s3://bucket/prefix``, ``file:///dir``, ...) or a local path.

    URLs are joined with ``/``; local paths with the operating system's separator.

    :param bucket_url: bucket URL or local directory path
    :type bucket_url: str
    :param parts: path parts to append
    :type parts: str
    :return: joined URL or path
    :rtype: str
    """
    if "://" in bucket_url:
        return posixpath.join(bucket_url, *parts)
    return str(Path(bucket_url, *parts))


def source_id(file: FileSource) -> str:
    """Return the identifier recorded for ``file`` in output tables.

    This is the file's path relative to the listed bucket or directory (``relative_path``), which does not
    depend on where the input lives. For a local path, it is the file name.

    :param file: local path or dlt file item
    :type file: FileSource
    :return: source identifier
    :rtype: str
    """
    if isinstance(file, FileItemDict):
        return file["relative_path"]
    return Path(file).name


def source_url(file: FileSource | IO[bytes]) -> str:
    """Return a human-readable location for ``file``, for log messages.

    :param file: local path, dlt file item, or open handle
    :type file: FileSource | IO[bytes]
    :return: ``file_url`` for a file item, the path as given for a local path, otherwise the handle's name
    :rtype: str
    """
    if isinstance(file, FileItemDict):
        return file["file_url"]
    if isinstance(file, (str, Path)):
        return str(file)
    return str(getattr(file, "name", file))


@contextmanager
def open_binary(file: FileSource | IO[bytes]) -> Generator[IO[bytes]]:
    """Open ``file`` for binary reading, decompressing gzip files.

    An already-open binary handle is yielded as-is and is not closed on exit, so callers can share one handle
    between several readers (e.g. read the XML head matter, ``seek(0)``, then stream the file).

    :param file: local path, dlt file item, or open binary handle
    :type file: FileSource | IO[bytes]
    :yield: open binary handle
    :rtype: Generator[IO[bytes]]
    """
    if isinstance(file, (str, Path, FileItemDict)):
        with as_file_item(file).open(mode="rb") as fh:
            yield fh
        return
    yield file


def is_local(file_item: FileItemDict) -> bool:
    """Return True if ``file_item`` is on the local filesystem.

    :param file_item: dlt file item
    :type file_item: FileItemDict
    :return: whether the item's fsspec filesystem is the local filesystem
    :rtype: bool
    """
    protocol: Any = file_item.fsspec.protocol
    protocols = (protocol,) if isinstance(protocol, str) else tuple(protocol)
    return "file" in protocols


@contextmanager
def local_copy(file_item: FileItemDict, dest_dir: Path) -> Generator[Path]:
    """Yield a local path for ``file_item``, downloading it first if it is not on the local filesystem.

    Local files are not copied: their native path is yielded. Remote files are downloaded, as stored (not
    decompressed), to ``dest_dir / relative_path`` and deleted on exit. Keeping the relative path means files
    with the same name in different directories do not overwrite each other.

    :param file_item: dlt file item
    :type file_item: FileItemDict
    :param dest_dir: directory to download remote files into
    :type dest_dir: Path
    :raises ValueError: if the item's relative path would place the download outside ``dest_dir``
    :yield: local path to the file
    :rtype: Generator[Path]
    """
    if is_local(file_item):
        yield Path(file_item.local_file_path)
        return

    dest_root = dest_dir.resolve()
    dest = (dest_root / PurePosixPath(file_item["relative_path"])).resolve()
    if not dest.is_relative_to(dest_root):
        msg = f"Relative path {file_item['relative_path']!r} resolves outside the download directory"
        raise ValueError(msg)

    dest.parent.mkdir(parents=True, exist_ok=True)
    file_item.fsspec.get(file_item["file_url"], str(dest))
    try:
        yield dest
    finally:
        dest.unlink(missing_ok=True)
