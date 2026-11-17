"""Unit tests for ``cdm_data_loaders.utils.urls``."""

from pathlib import Path

import pytest

from cdm_data_loaders.utils.urls import (
    FILE_PROTOCOL,
    is_local_url,
    join_url,
    split_s3_path,
    url_protocol,
)


@pytest.mark.parametrize(
    ("url", "expected"),
    [
        pytest.param("/var/data/file.tsv", FILE_PROTOCOL, id="local-posix-path"),
        pytest.param("relative/path.xml", FILE_PROTOCOL, id="relative-path"),
        pytest.param("file:///var/data/file.tsv", FILE_PROTOCOL, id="file-uri"),
        pytest.param("S3://my-bucket/key", "s3", id="uppercase-s3-uri"),
        pytest.param("https://example.org/data.gz", "https", id="https-uri"),
    ],
)
def test_url_protocol(url: str, expected: str) -> None:
    """Return lowercase protocol names and default plain paths to ``file``."""
    assert url_protocol(url) == expected


@pytest.mark.parametrize(
    ("url", "expected"),
    [
        pytest.param("/var/data/file.json", True, id="local-path"),
        pytest.param("file:///var/data/file.json", True, id="file-scheme"),
        pytest.param("s3://bucket/key", False, id="s3-scheme"),
        pytest.param("https://host/item", False, id="https-scheme"),
    ],
)
def test_is_local_url(url: str, expected: bool) -> None:
    """Classify local filesystem paths and ``file://`` URLs as local."""
    assert is_local_url(url) is expected


@pytest.mark.parametrize(
    ("base", "parts", "expected"),
    [
        pytest.param(
            "s3://my-bucket/prefix",
            ("sub", "item.parquet"),
            "s3://my-bucket/prefix/sub/item.parquet",
            id="s3-remote-url",
        ),
        pytest.param(
            "/var/data",
            ("sub", "item.parquet"),
            str(Path("/var/data", "sub", "item.parquet")),
            id="local-path",
        ),
    ],
)
def test_join_url_remote_and_local(base: str, parts: tuple[str, ...], expected: str) -> None:
    """Join remote URLs with POSIX slashes and local paths with ``Path``."""
    assert join_url(base, *parts) == expected


@pytest.mark.parametrize(
    ("s3_path", "allow_bucket_only", "expected"),
    [
        pytest.param("s3://bucket/dir/file.txt", False, ("bucket", "dir/file.txt"), id="s3-uri"),
        pytest.param("bucket/dir/file.txt", False, ("bucket", "dir/file.txt"), id="bare-bucket-key"),
        pytest.param("s3://bucket", True, ("bucket", ""), id="bucket-only-no-slash"),
        pytest.param("s3://bucket/", True, ("bucket", ""), id="bucket-only-trailing-slash"),
    ],
)
def test_split_s3_path_valid(
    s3_path: str,
    allow_bucket_only: bool,
    expected: tuple[str, str],
) -> None:
    """Split valid S3 URIs and bare bucket/key paths into ``(bucket, key)``."""
    assert split_s3_path(s3_path, allow_bucket_only=allow_bucket_only) == expected


@pytest.mark.parametrize(
    ("s3_path", "match"),
    [
        pytest.param("", "No path found", id="empty"),
        pytest.param("s3://", "No path found", id="scheme-only"),
        pytest.param("/leading/slash", "must start with the bucket name", id="leading-slash"),
        pytest.param("s3://bucket", "Could not parse out bucket and key", id="missing-key"),
        pytest.param("s3://bucket/", "Could not parse out bucket and key", id="empty-key"),
    ],
)
def test_split_s3_path_invalid(s3_path: str, match: str) -> None:
    """Raise ``ValueError`` when an S3 path lacks a bucket or required object key."""
    with pytest.raises(ValueError, match=match):
        split_s3_path(s3_path)
