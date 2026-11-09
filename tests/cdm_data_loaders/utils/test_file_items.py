"""Tests for the FileItemDict helpers in cdm_data_loaders.utils.file_items."""

import gzip
import io
from pathlib import Path
from typing import Final

import fsspec
import pytest
from dlt.common.storages.fsspec_filesystem import FileItemDict
from dlt.sources.filesystem import filesystem

from cdm_data_loaders.utils.file_items import (
    as_file_item,
    is_local,
    join_url,
    local_copy,
    open_binary,
    source_id,
    source_url,
)
from tests.fs_backends import FsBackend

CONTENT: Final[bytes] = b"id\tname\n1\tcaf\xc3\xa9\n"
GZ_CONTENT: Final[bytes] = gzip.compress(CONTENT)


def _write(directory: Path, name: str, data: bytes) -> Path:
    """Write ``data`` to ``directory / name`` and return the path."""
    path = directory / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)
    return path


def _local_items(directory: Path) -> list[FileItemDict]:
    """List every file under a local directory with the dlt filesystem resource."""
    return list(filesystem(bucket_url=str(directory), file_glob="**/*"))


"""as_file_item"""


def test_as_file_item_pass_file_item_returned_unchanged(fs_backend: FsBackend) -> None:
    """Verify a FileItemDict is returned as the same object."""
    fs_backend.put("a.tsv", CONTENT)
    item = fs_backend.file_item("a.tsv")

    assert as_file_item(item) is item


@pytest.mark.parametrize("path_type", [str, Path], ids=["str", "Path"])
@pytest.mark.parametrize(
    ("name", "stored", "expected_encoding"),
    [
        pytest.param("data.tsv", CONTENT, None, id="plain"),
        pytest.param("data.tsv.gz", GZ_CONTENT, "gzip", id="gzip"),
        pytest.param("we[ird] *name?.tsv", CONTENT, None, id="glob_metacharacters"),
    ],
)
def test_as_file_item_pass_local_path(
    tmp_path: Path, path_type: type, name: str, stored: bytes, expected_encoding: str | None
) -> None:
    """Verify a local path becomes a file item with dlt listing fields, and opens with gzip decompressed."""
    path = _write(tmp_path, name, stored)

    item = as_file_item(path_type(path))
    with item.open(mode="rb") as fh:
        data = fh.read()

    assert isinstance(item, FileItemDict)
    assert item["file_name"] == name
    assert item["relative_path"] == name
    assert item["file_url"] == path.resolve().as_uri()
    assert item["size_in_bytes"] == len(stored)
    assert item.get("encoding") == expected_encoding
    assert data == CONTENT


def test_as_file_item_pass_relative_path_resolved(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify a path relative to the working directory resolves to an absolute file URL."""
    path = _write(tmp_path, "sub/data.tsv", CONTENT)
    monkeypatch.chdir(tmp_path)

    item = as_file_item(Path("sub") / "data.tsv")

    assert item["relative_path"] == "data.tsv"
    assert item["file_url"] == path.resolve().as_uri()


def test_as_file_item_fail_missing_file(tmp_path: Path) -> None:
    """Verify a missing file raises FileNotFoundError naming the path."""
    missing = tmp_path / "missing.tsv"

    with pytest.raises(FileNotFoundError, match="No such file or directory") as exc_info:
        as_file_item(missing)

    assert exc_info.value.filename == str(missing)


def test_as_file_item_fail_directory(tmp_path: Path) -> None:
    """Verify a directory raises IsADirectoryError."""
    with pytest.raises(IsADirectoryError):
        as_file_item(tmp_path)


"""join_url"""


@pytest.mark.parametrize(
    ("bucket_url", "parts", "expected"),
    [
        pytest.param("s3://bucket/prefix", ("proteins",), "s3://bucket/prefix/proteins", id="s3"),
        pytest.param("s3://bucket/prefix/", ("a", "b.jsonl"), "s3://bucket/prefix/a/b.jsonl", id="s3_trailing_slash"),
        pytest.param("memory:///root", ("proteins",), "memory:///root/proteins", id="memory"),
        pytest.param("file:///data/in", ("proteins",), "file:///data/in/proteins", id="file_url"),
    ],
)
def test_join_url_pass_url_joined_with_slash(bucket_url: str, parts: tuple[str, ...], expected: str) -> None:
    """Verify URLs are joined with a forward slash."""
    assert join_url(bucket_url, *parts) == expected


def test_join_url_pass_local_path_joined_with_os_separator(tmp_path: Path) -> None:
    """Verify a local path is joined with the operating system's path rules."""
    assert join_url(str(tmp_path), "proteins", "a.jsonl") == str(tmp_path / "proteins" / "a.jsonl")


"""source_id and source_url"""


@pytest.mark.parametrize(
    ("file", "expected"),
    [
        pytest.param(Path("/some/dir/data.tsv"), "data.tsv", id="Path"),
        pytest.param("relative/dir/data.tsv.gz", "data.tsv.gz", id="str"),
    ],
)
def test_source_id_pass_local_path_is_file_name(file: str | Path, expected: str) -> None:
    """Verify the source id of a local path is its file name, without touching the filesystem."""
    assert source_id(file) == expected


@pytest.mark.parametrize("relative_path", ["data.tsv", "sub dir/caf\u00e9 + more.tsv"])
def test_source_id_pass_file_item_is_relative_path(fs_backend: FsBackend, relative_path: str) -> None:
    """Verify the source id of a file item is its relative path, the same on every backend."""
    fs_backend.put(relative_path, CONTENT)

    assert source_id(fs_backend.file_item(relative_path)) == relative_path




@pytest.mark.parametrize(
    ("file", "expected"),
    [
        pytest.param(Path("some/data.tsv"), str(Path("some/data.tsv")), id="path"),
        pytest.param("some/data.tsv", "some/data.tsv", id="str"),
    ],
)
def test_source_url_pass_path(file: Path | str, expected: str) -> None:
    """Verify source_url returns a local path or string as given."""
    assert source_url(file) == expected


def test_source_url_pass_file_item(fs_backend: FsBackend) -> None:
    """Verify source_url gives a file item's file_url."""
    fs_backend.put("data.tsv", CONTENT)
    item = fs_backend.file_item("data.tsv")

    assert source_url(item) == item["file_url"]


"""open_binary"""


@pytest.mark.parametrize("name", ["data.tsv", "data.tsv.gz"])
def test_open_binary_pass_local_path(tmp_path: Path, name: str) -> None:
    """Verify a local path is opened for binary reading, with gzip decompressed."""
    path = _write(tmp_path, name, GZ_CONTENT if name.endswith(".gz") else CONTENT)

    with open_binary(path) as fh:
        assert fh.read() == CONTENT


@pytest.mark.parametrize("relative_path", ["data.tsv", "sub/data.tsv.gz"])
def test_open_binary_pass_file_item(fs_backend: FsBackend, relative_path: str) -> None:
    """Verify a file item is opened for binary reading on every backend, with gzip decompressed."""
    fs_backend.put(relative_path, GZ_CONTENT if relative_path.endswith(".gz") else CONTENT)

    with open_binary(fs_backend.file_item(relative_path)) as fh:
        assert fh.read() == CONTENT


def test_open_binary_pass_open_handle_yielded_and_not_closed() -> None:
    """Verify an open handle is yielded unchanged and left open on exit."""
    handle = io.BytesIO(CONTENT)

    with open_binary(handle) as fh:
        assert fh is handle

    assert not handle.closed
    assert handle.read() == CONTENT


def test_open_binary_fail_missing_file(tmp_path: Path) -> None:
    """Verify a missing local file raises FileNotFoundError."""
    with pytest.raises(FileNotFoundError), open_binary(tmp_path / "missing.tsv"):
        pass


"""is_local"""


def test_is_local_pass_matches_backend(fs_backend: FsBackend) -> None:
    """Verify is_local is True only for items on the local filesystem."""
    fs_backend.put("data.tsv", CONTENT)

    assert is_local(fs_backend.file_item("data.tsv")) is fs_backend.is_local


"""local_copy"""


@pytest.mark.parametrize("relative_path", ["data.tsv", "sub/data.tsv.gz"])
def test_local_copy_pass_local_file_not_copied(tmp_path: Path, relative_path: str) -> None:
    """Verify a local file yields its own path and nothing is written to the download directory."""
    path = _write(tmp_path / "input", relative_path, CONTENT)
    item = next(item for item in _local_items(tmp_path / "input") if item["relative_path"] == relative_path)
    dest_dir = tmp_path / "downloads"
    dest_dir.mkdir()

    with local_copy(item, dest_dir) as local_path:
        assert local_path == path.resolve()
        assert local_path.read_bytes() == CONTENT

    assert path.exists()
    assert list(dest_dir.iterdir()) == []


@pytest.mark.parametrize(
    ("relative_path", "stored"),
    [
        pytest.param("data.tsv", CONTENT, id="plain"),
        pytest.param("sub/dir/data.tsv.gz", GZ_CONTENT, id="nested_gzip_kept_compressed"),
    ],
)
def test_local_copy_pass_remote_file_downloaded_and_removed(
    remote_fs_backend: FsBackend, tmp_path: Path, relative_path: str, stored: bytes
) -> None:
    """Verify a remote file is downloaded as stored to dest_dir/relative_path and deleted on exit."""
    remote_fs_backend.put(relative_path, stored)
    item = remote_fs_backend.file_item(relative_path)

    with local_copy(item, tmp_path) as local_path:
        assert local_path == (tmp_path / relative_path).resolve()
        assert local_path.read_bytes() == stored

    assert not local_path.exists()
    assert remote_fs_backend.fs.exists(remote_fs_backend.path(relative_path))


def test_local_copy_pass_same_name_in_different_dirs_kept_apart(remote_fs_backend: FsBackend, tmp_path: Path) -> None:
    """Verify files with the same name in different directories are downloaded to different local paths."""
    remote_fs_backend.put_all({"a/data.tsv": b"from a", "b/data.tsv": b"from b"})
    item_a, item_b = remote_fs_backend.file_items()

    with local_copy(item_a, tmp_path) as path_a, local_copy(item_b, tmp_path) as path_b:
        assert path_a != path_b
        assert path_a.read_bytes() == b"from a"
        assert path_b.read_bytes() == b"from b"


def test_local_copy_fail_remote_missing_object(remote_fs_backend: FsBackend, tmp_path: Path) -> None:
    """Verify a remote file deleted after listing raises FileNotFoundError and leaves no partial download."""
    remote_fs_backend.put("gone.tsv", CONTENT)
    item = remote_fs_backend.file_item("gone.tsv")
    remote_fs_backend.fs.rm(remote_fs_backend.path("gone.tsv"))

    with pytest.raises(FileNotFoundError), local_copy(item, tmp_path):
        pass

    assert not (tmp_path / "gone.tsv").exists()


def test_local_copy_fail_relative_path_outside_dest_dir(tmp_path: Path) -> None:
    """Verify a relative path that escapes the download directory is rejected before anything is written."""
    memory_fs = fsspec.filesystem("memory")
    item = FileItemDict(
        {"file_name": "x.tsv", "relative_path": "../x.tsv", "file_url": "memory:///x.tsv", "mime_type": "text/tsv"},
        memory_fs,
    )
    dest_dir = tmp_path / "downloads"
    dest_dir.mkdir()

    with pytest.raises(ValueError, match="resolves outside the download directory"), local_copy(item, dest_dir):
        pass

    assert not (tmp_path / "x.tsv").exists()
