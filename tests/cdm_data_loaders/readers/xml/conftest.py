"""Shared fixtures and helpers for the XML reader tests."""

import gzip
from collections.abc import Callable, Iterable
from pathlib import Path
from typing import Any

import pytest
from dlt.extract.items import DataItemWithMeta, TableNameMeta


def write_xml_file(directory: Path, filename: str, content: str, *, gzip_compress: bool = False) -> Path:
    """Write an XML string to `directory / filename`, optionally gzip-compressed."""
    file_path = directory / filename
    data = content.encode("utf-8")
    if gzip_compress:
        with gzip.open(file_path, "wb") as f:
            f.write(data)
    else:
        file_path.write_bytes(data)
    return file_path


def table_and_data(items: Iterable[DataItemWithMeta]) -> list[tuple[str, Any]]:
    """Flatten a sequence of DataItemWithMeta into (table_name, data) pairs for easy assertion."""
    result: list[tuple[str, Any]] = []
    for item in items:
        assert isinstance(item, DataItemWithMeta)
        assert isinstance(item.meta, TableNameMeta)
        result.append((item.meta.table_name, item.data))
    return result


@pytest.fixture
def xml_path_factory(tmp_path: Path) -> Callable[..., Path]:
    """Return a factory that writes XML content to a real file, optionally gzip-compressed."""

    def _make(content: str, *, gzip_compress: bool = False, filename: str = "data.xml") -> Path:
        target_name = f"{filename}.gz" if gzip_compress else filename
        return write_xml_file(tmp_path, target_name, content, gzip_compress=gzip_compress)

    return _make
