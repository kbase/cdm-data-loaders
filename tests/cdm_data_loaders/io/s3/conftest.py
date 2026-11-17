"""Fixtures for ``cdm_data_loaders.io.s3`` tests."""

from pathlib import Path

import pytest

from tests.cdm_data_loaders.io.s3._helpers import SAMPLE_FILES


@pytest.fixture
def sample_file(tmp_path: Path) -> Path:
    """Create a small temporary file for upload tests.

    :param tmp_path: Per-test temporary directory.
    :type tmp_path: Path
    :return: Path to ``sample.txt``.
    :rtype: Path
    """
    f = tmp_path / "sample.txt"
    f.write_text("hello s3", encoding="utf-8")
    return f


@pytest.fixture
def sample_dir(tmp_path: Path) -> Path:
    """Create a small temporary directory tree matching ``SAMPLE_FILES`` for directory upload tests.

    :param tmp_path: Per-test temporary directory.
    :type tmp_path: Path
    :return: Root directory containing ``SAMPLE_FILES``.
    :rtype: Path
    """
    root_dir = tmp_path / "sample_dir"
    for rel_path in SAMPLE_FILES:
        new_file = root_dir / rel_path
        new_file.parent.mkdir(parents=True, exist_ok=True)
        new_file.write_text(rel_path, encoding="utf-8")
    return root_dir
