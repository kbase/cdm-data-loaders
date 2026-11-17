"""Pytest collection hooks for documentation generation and content tests."""

from pathlib import Path

import pytest

from tests.docs.helpers import should_ignore_docs_collect


def pytest_ignore_collect(collection_path: Path, config: pytest.Config) -> bool | None:
    """Skip collecting ``tests/docs`` modules when ``-m`` excludes ``docs``-only tests.

    Prevents importing documentation-group dependencies (such as ``zensical`` and
    ``markdown``) when running non-docs test suites via ``-m "not docs"``.

    :param collection_path: Candidate path considered by pytest collection.
    :type collection_path: Path
    :param config: Active pytest configuration object.
    :type config: pytest.Config
    :return: ``True`` to ignore ``collection_path`` when ``docs`` is deselected, otherwise ``None``.
    :rtype: bool | None
    """
    _ = collection_path
    return should_ignore_docs_collect(config.option.markexpr)
