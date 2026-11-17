"""URL protocol inspection, path joining, and S3 bucket/key parsing helpers."""

import posixpath
from pathlib import Path
from typing import Final

from fsspec.core import split_protocol

FILE_PROTOCOL: Final[str] = "file"


def url_protocol(url: str) -> str:
    """Return the canonical protocol of a path or URL, treating plain paths as ``'file'``.

    :param url: path or URL
    :type url: str
    :return: lowercase protocol name
    :rtype: str
    """
    protocol, _ = split_protocol(url)
    return (protocol or FILE_PROTOCOL).lower()


def is_local_url(url: str) -> bool:
    """Return whether a path or URL uses the local ``'file'`` protocol.

    :param url: path or URL
    :type url: str
    :return: ``True`` if ``url`` uses the local filesystem protocol, else ``False``
    :rtype: bool
    """
    return url_protocol(url) == FILE_PROTOCOL


def join_url(bucket_url: str, *parts: str) -> str:
    """Join path segments onto a URL or local filesystem path.

    URLs containing ``://`` join with ``/``; local paths join with the OS path separator.

    :param bucket_url: base URL or local directory path
    :type bucket_url: str
    :param parts: path segments to append
    :type parts: str
    :return: joined URL or local path string
    :rtype: str
    """
    if "://" in bucket_url:
        return posixpath.join(bucket_url, *parts)
    return str(Path(bucket_url, *parts))


def split_s3_path(s3_path: str, *, allow_bucket_only: bool = False) -> tuple[str, str | None]:
    """Split an S3 path (``s3://bucket/key`` or ``bucket/key``) into ``(bucket, key)``.

    :param s3_path: S3 path including the bucket name
    :type s3_path: str
    :param allow_bucket_only: whether to permit a bucket-only path with no object key
    :type allow_bucket_only: bool
    :return: tuple of ``(bucket, key)``
    :rtype: tuple[str, str | None]
    :raises ValueError: if ``s3_path`` is empty, starts with ``/``, or lacks a required key
    """
    if "://" in s3_path:
        _, unprefixed_path = s3_path.split("://", 1)
    else:
        unprefixed_path = s3_path

    if not unprefixed_path:
        err_msg = f"Invalid path: '{s3_path}\nNo path found"
        raise ValueError(err_msg)

    if unprefixed_path.startswith("/"):
        err_msg = f"Invalid path: '{s3_path}'\ns3 paths must start with the bucket name"
        raise ValueError(err_msg)

    expected_parts = 2
    path_parts = unprefixed_path.split("/", 1)
    if allow_bucket_only and (len(path_parts) == 1 or (len(path_parts) == expected_parts and not path_parts[1])):
        return (path_parts[0], "")

    if len(path_parts) != expected_parts or not path_parts[1]:
        err_msg = f"Invalid path: '{s3_path}'\nCould not parse out bucket and key"
        raise ValueError(err_msg)

    return (path_parts[0], path_parts[1])
