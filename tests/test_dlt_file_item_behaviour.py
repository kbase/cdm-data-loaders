"""Characterisation tests pinning the dlt ``filesystem`` / ``FileItemDict`` behaviour that the readers rely on.

The readers open every input with ``FileItemDict.open(mode="rb")`` and dlt's default ``compression="auto"``.
These tests fail if a dlt or fsspec upgrade changes how files are listed, decompressed or opened.
"""

import gzip
from typing import Final

import pytest

from tests.fs_backends import S3_DRIVERS, FsBackend

CONTENT: Final[bytes] = b'<?xml version="1.0"?>\n<root><entry>caf\xc3\xa9</entry></root>\n'
GZ_CONTENT: Final[bytes] = gzip.compress(CONTENT)


@pytest.mark.parametrize(
    "relative_path",
    ["a.xml.gz", "a.jsonl.gz", "a.tsv.gz", "a.csv.gz", "a.gz", "a.dat.gz", "a.tar.gz", "a.tgz", "sub/dir/a.xml.gz"],
)
def test_file_item_open_pass_auto_gzip_by_suffix(fs_backend: FsBackend, relative_path: str) -> None:
    """Verify gzip files are listed with encoding "gzip" and decompressed by open() on every backend."""
    fs_backend.put(relative_path, GZ_CONTENT)

    item = fs_backend.file_item(relative_path)
    with item.open(mode="rb") as fh:
        data = fh.read()

    assert item["encoding"] == "gzip"
    assert data == CONTENT


@pytest.mark.parametrize(
    ("relative_path", "stored", "expected_encoding"),
    [
        pytest.param("plain.xml", CONTENT, None, id="plain"),
        pytest.param("upper.XML.GZ", GZ_CONTENT, None, id="upper_case_gz_suffix"),
        pytest.param("mixed.xml.Gz", GZ_CONTENT, None, id="mixed_case_gz_suffix"),
        pytest.param("compressed.xml.bz2", b"not really bz2", "bzip2", id="bz2_suffix"),
    ],
)
def test_file_item_open_pass_not_decompressed(
    fs_backend: FsBackend, relative_path: str, stored: bytes, expected_encoding: str | None
) -> None:
    """Verify only lowercase gzip suffixes are decompressed: other files are returned exactly as stored."""
    fs_backend.put(relative_path, stored)

    item = fs_backend.file_item(relative_path)
    with item.open(mode="rb") as fh:
        data = fh.read()

    assert item.get("encoding") == expected_encoding
    assert data == stored


@pytest.mark.parametrize("relative_path", ["plain.xml", "compressed.xml.gz"])
def test_file_item_open_pass_seek0_rewinds_handle(fs_backend: FsBackend, relative_path: str) -> None:
    """Verify seek(0) rewinds a partly read handle to the start of the (decompressed) content."""
    fs_backend.put(relative_path, GZ_CONTENT if relative_path.endswith(".gz") else CONTENT)

    with fs_backend.file_item(relative_path).open(mode="rb") as fh:
        head = fh.read(10)
        seekable = fh.seekable()
        fh.seek(0)
        data = fh.read()

    assert head == CONTENT[:10]
    assert seekable is True
    assert data == CONTENT


@pytest.mark.parametrize("relative_path", ["plain.jsonl", "compressed.jsonl.gz"])
def test_file_item_open_pass_text_mode_utf8(fs_backend: FsBackend, relative_path: str) -> None:
    """Verify text mode decodes UTF-8 (including multi-byte characters) on every backend."""
    fs_backend.put(relative_path, GZ_CONTENT if relative_path.endswith(".gz") else CONTENT)

    with fs_backend.file_item(relative_path).open(mode="rt", encoding="utf-8") as fh:
        text = fh.read()

    assert text == CONTENT.decode("utf-8")


@pytest.mark.parametrize(
    "relative_path",
    ["with space.xml", "plus+sign.xml", "per%cent.xml", "brack[et].xml", "caf\u00e9.xml", "sub dir/nested.xml"],
)
def test_file_item_listing_pass_awkward_names_round_trip(fs_backend: FsBackend, relative_path: str) -> None:
    """Verify file names with spaces, URL and glob metacharacters, and non-ASCII characters list and open."""
    fs_backend.put(relative_path, CONTENT)

    items = fs_backend.file_items()
    with items[0].open(mode="rb") as fh:
        data = fh.read()

    assert [item["relative_path"] for item in items] == [relative_path]
    assert items[0]["file_name"] == relative_path.rsplit("/", 1)[-1]
    assert data == CONTENT


def test_file_item_open_fail_gz_name_plain_content(fs_backend: FsBackend) -> None:
    """Verify a .gz file that does not contain gzip data raises BadGzipFile on every backend."""
    fs_backend.put("not_gzip.xml.gz", CONTENT)

    with (
        pytest.raises(gzip.BadGzipFile, match="Not a gzipped file"),
        fs_backend.file_item("not_gzip.xml.gz").open(mode="rb") as fh,
    ):
        fh.read()


def test_file_item_open_fail_removed_after_listing(fs_backend: FsBackend) -> None:
    """Verify opening a file that was deleted after it was listed raises FileNotFoundError on every backend."""
    fs_backend.put("gone.xml", CONTENT)
    item = fs_backend.file_item("gone.xml")
    fs_backend.fs.rm(fs_backend.path("gone.xml"))

    with pytest.raises(FileNotFoundError), item.open(mode="rb") as fh:
        fh.read()


def test_file_item_open_pass_s3_content_encoding_gzip_decompressed_once(fs_backend: FsBackend) -> None:
    """Verify S3 Content-Encoding metadata is ignored: a .gz object stored with it is decompressed exactly once."""
    if fs_backend.name not in S3_DRIVERS:
        pytest.skip("Content-Encoding metadata only exists on S3")
    fs_backend.put("encoded.xml.gz", GZ_CONTENT, ContentEncoding="gzip")

    with fs_backend.file_item("encoded.xml.gz").open(mode="rb") as fh:
        data = fh.read()

    assert data == CONTENT
