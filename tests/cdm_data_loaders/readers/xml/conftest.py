"""Shared fixtures and helpers for the XML reader tests."""

import gzip
from collections.abc import Callable, Iterable
from pathlib import Path
from typing import Any

import pytest
from dlt.extract.items import DataItemWithMeta, TableNameMeta
from lxml.etree import XMLSyntaxError

MISMATCHED_TAG_XML = '<library><book id="1"><title>Broken</title></library>'

XML_OPEN_ERROR_CASES = [
    pytest.param("missing_file", FileNotFoundError, None, id="missing-file"),
    pytest.param("missing_gzip_file", FileNotFoundError, None, id="missing-gzip-file"),
    pytest.param("empty_file", XMLSyntaxError, None, id="empty-file"),
    pytest.param("malformed_xml", XMLSyntaxError, None, id="malformed-xml"),
    pytest.param("non_gzip_with_gz_ext", OSError, "Not a gzipped file", id="non-gzip-with-gz-ext"),
]


def prepare_xml_error_path(scenario: str, tmp_path: Path, xml_path_factory: Callable[..., Path]) -> Path:
    """Create or return the path for one of the standard XML open/parse failure scenarios."""
    if scenario == "missing_file":
        return tmp_path / "does_not_exist.xml"
    if scenario == "missing_gzip_file":
        return tmp_path / "does_not_exist.xml.gz"
    if scenario == "empty_file":
        return xml_path_factory("")
    if scenario == "malformed_xml":
        return xml_path_factory("not xml at all <<<")
    if scenario == "mismatched_tag":
        return xml_path_factory(MISMATCHED_TAG_XML)
    if scenario == "non_gzip_with_gz_ext":
        return xml_path_factory("<library/>", gzip_compress=False, filename="not_really_gzipped.xml.gz")
    msg = f"Unknown scenario: {scenario}"
    raise ValueError(msg)


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
