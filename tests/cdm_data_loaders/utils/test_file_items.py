"""Tests for cdm_data_loaders.utils.file_items."""

import gzip
from collections.abc import Generator
from pathlib import Path
from uuid import uuid4

import fsspec
import pytest
from dlt.common.storages.fsspec_filesystem import FileItemDict
from dlt.sources.filesystem import filesystem

from cdm_data_loaders.utils.file_items import (
    compression_mode,
    local_copy,
    open_input,
    source_label,
    source_name,
)

CONTENT = b"<root><entry>one</entry></root>\n"


def file_items(bucket_url: str, file_glob: str = "**/*") -> list[FileItemDict]:
    """List the file items dlt's filesystem resource yields for bucket_url (iteration flattens the pages)."""
    return list(filesystem(bucket_url=bucket_url, file_glob=file_glob))


@pytest.fixture
def local_tree(tmp_path: Path) -> Path:
    """Local directory containing a plain and a gzipped file, one in a subdirectory."""
    (tmp_path / "sub").mkdir()
    (tmp_path / "plain.xml").write_bytes(CONTENT)
    (tmp_path / "sub" / "zipped.xml.gz").write_bytes(gzip.compress(CONTENT))
    return tmp_path


@pytest.fixture
def memory_tree() -> Generator[str]:
    """Remote-like (memory://) directory containing a plain and a gzipped file, one in a subdirectory."""
    root = f"memory://file_items_{uuid4().hex}"
    fs = fsspec.filesystem("memory")
    fs.pipe(f"{root}/plain.xml", CONTENT)
    fs.pipe(f"{root}/sub/zipped.xml.gz", gzip.compress(CONTENT))
    yield root
    fs.rm(root, recursive=True)


@pytest.mark.parametrize(
    ("file_name", "expected"),
    [("a.xml.gz", "enable"), ("a.xml", "disable"), ("a.gz.xml", "disable"), ("gz", "disable")],
)
def test_compression_mode_pass(file_name: str, expected: str) -> None:
    """Gzip compression is enabled only for names ending in .gz."""
    assert compression_mode(file_name) == expected


@pytest.mark.parametrize("tree", ["local_tree", "memory_tree"])
def test_open_input_pass_file_items(tree: str, request: pytest.FixtureRequest) -> None:
    """Plain and gzipped file items, local or remote, are opened and decompressed transparently."""
    items = file_items(str(request.getfixturevalue(tree)))
    assert sorted(item["relative_path"] for item in items) == ["plain.xml", "sub/zipped.xml.gz"]
    for item in items:
        with open_input(item) as fh:
            assert fh.read() == CONTENT


@pytest.mark.parametrize("path_type", [str, Path])
@pytest.mark.parametrize("file_name", ["plain.xml", "sub/zipped.xml.gz"])
def test_open_input_pass_local_paths(local_tree: Path, file_name: str, path_type: type) -> None:
    """Local paths, as str or Path, are opened and decompressed transparently."""
    with open_input(path_type(local_tree / file_name)) as fh:
        assert fh.read() == CONTENT


@pytest.mark.parametrize("tree", ["local_tree", "memory_tree"])
def test_source_name_and_label_pass_file_items(tree: str, request: pytest.FixtureRequest) -> None:
    """File items are named by their base name and labelled by their relative path."""
    items = {item["relative_path"]: item for item in file_items(str(request.getfixturevalue(tree)))}
    assert source_name(items["sub/zipped.xml.gz"]) == "zipped.xml.gz"
    assert source_label(items["sub/zipped.xml.gz"]) == "sub/zipped.xml.gz"
    assert source_name(items["plain.xml"]) == "plain.xml"
    assert source_label(items["plain.xml"]) == "plain.xml"


@pytest.mark.parametrize("path_type", [str, Path])
def test_source_name_and_label_pass_local_paths(local_tree: Path, path_type: type) -> None:
    """Local paths are named by their base name and labelled with the path as given."""
    path = path_type(local_tree / "sub" / "zipped.xml.gz")
    assert source_name(path) == "zipped.xml.gz"
    assert source_label(path) == str(path)


def test_local_copy_pass_local_item_not_copied(local_tree: Path) -> None:
    """Local file items are used in place."""
    items = {item["relative_path"]: item for item in file_items(str(local_tree))}
    with local_copy(items["sub/zipped.xml.gz"]) as path:
        assert path == local_tree / "sub" / "zipped.xml.gz"
    assert path.exists()


def test_local_copy_pass_remote_item_copied_and_removed(memory_tree: str) -> None:
    """Remote file items are copied, undecompressed and with their original name, then deleted on exit."""
    items = {item["relative_path"]: item for item in file_items(memory_tree)}
    with local_copy(items["sub/zipped.xml.gz"]) as path:
        assert path.name == "zipped.xml.gz"
        # still compressed: the copy is byte-for-byte, not decompressed
        assert gzip.decompress(path.read_bytes()) == CONTENT
    assert not path.exists()
    assert not path.parent.exists()


def test_local_copy_fail_copy_removed_on_error(memory_tree: str) -> None:
    """The temporary copy is removed even if the caller raises."""
    items = {item["relative_path"]: item for item in file_items(memory_tree)}
    copied: list[Path] = []

    def _copy_then_fail() -> None:
        with local_copy(items["plain.xml"]) as path:
            copied.append(path)
            err_msg = "boom"
            raise RuntimeError(err_msg)

    with pytest.raises(RuntimeError, match="boom"):
        _copy_then_fail()
    assert copied
    assert not copied[0].exists()
