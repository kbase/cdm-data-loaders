"""Tests for entity_files, which lists one entity's JSONL files on any filesystem."""

import logging
from pathlib import Path
from typing import Final

import pytest

from cdm_data_loaders.pipelines.jsonlines.base import entity_files
from tests.fs_backends import FsBackend

JSONL_GLOB: Final[str] = "*.jsonl*"
WARNING_PREFIX: Final[str] = "Entity directory does not exist, skipping"


def test_entity_files_pass_lists_only_the_entity_subdirectory(fs_backend: FsBackend) -> None:
    """Only files in <bucket>/<table_name> matching the glob are listed, on every backend."""
    fs_backend.put_all(
        {
            "proteins/a.jsonl": b"{}\n",
            "proteins/b.jsonl.gz": b"",
            "proteins/notes.txt": b"",
            "proteins_v2/c.jsonl": b"{}\n",
            "d.jsonl": b"{}\n",
        }
    )

    resource = entity_files(fs_backend.bucket_url, "proteins", JSONL_GLOB, credentials=fs_backend.fs)

    assert resource.name == "filesystem"
    assert sorted(item["relative_path"] for item in resource) == ["a.jsonl", "b.jsonl.gz"]


def test_entity_files_pass_missing_entity_warns_and_lists_nothing(
    fs_backend: FsBackend, caplog: pytest.LogCaptureFixture
) -> None:
    """A missing entity subdirectory gives an empty resource and a warning, on every backend."""
    fs_backend.put("other/a.jsonl", b"{}\n")

    resource = entity_files(fs_backend.bucket_url, "proteins", JSONL_GLOB, credentials=fs_backend.fs)

    assert list(resource) == []
    assert resource.name == "proteins_files"
    warnings = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
    assert any(message.startswith(WARNING_PREFIX) and "proteins" in message for message in warnings)


def test_entity_files_pass_empty_entity_lists_nothing(fs_backend: FsBackend) -> None:
    """An entity subdirectory with no files lists nothing (object stores have no empty directories)."""
    if fs_backend.has_directories:
        fs_backend.fs.makedirs(fs_backend.path("proteins"), exist_ok=True)

    resource = entity_files(fs_backend.bucket_url, "proteins", JSONL_GLOB, credentials=fs_backend.fs)

    assert list(resource) == []


@pytest.mark.parametrize("as_url", [False, True], ids=["path", "file_url"])
def test_entity_files_pass_local_directory_without_credentials(tmp_path: Path, as_url: bool) -> None:
    """A local directory, given as a path or a file:// URL, needs no credentials."""
    (tmp_path / "proteins").mkdir()
    (tmp_path / "proteins" / "a.jsonl").write_text("{}\n", encoding="utf-8")
    bucket_url = tmp_path.as_uri() if as_url else str(tmp_path)

    resource = entity_files(bucket_url, "proteins", JSONL_GLOB)

    assert [item["relative_path"] for item in resource] == ["a.jsonl"]
