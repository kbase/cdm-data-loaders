"""Pytest fixtures for XML reader tests."""

from collections.abc import Callable
from pathlib import Path

import pytest

from tests.helpers.xml_samples import write_xml_file


@pytest.fixture
def xml_path_factory(tmp_path: Path) -> Callable[..., Path]:
    """Return a factory writing XML content to a temporary file, with optional gzip compression.

    :param tmp_path: Pytest temporary directory.
    :type tmp_path: Path
    :return: Factory writing XML files.
    :rtype: Callable[..., Path]
    """

    def _make(content: str, *, gzip_compress: bool = False, filename: str = "data.xml") -> Path:
        target_name = f"{filename}.gz" if gzip_compress else filename
        return write_xml_file(tmp_path, target_name, content, gzip_compress=gzip_compress)

    return _make
