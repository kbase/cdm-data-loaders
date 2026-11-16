"""Pytest collection hooks for documentation generation and content tests."""

from functools import partial
from pathlib import Path

import pytest
from _pytest.mark.expression import Expression


def _matches_docs_marker(name: str, **_kwargs: str | int | bool | None) -> bool:
    """Evaluate a marker identifier as ``True`` iff it equals ``docs``.

    :param name: Marker identifier passed by ``Expression.evaluate``.
    :type name: str
    :param _kwargs: Optional marker keyword arguments passed by ``Expression.evaluate``.
    :type _kwargs: str | int | bool | None
    :return: ``True`` when ``name == "docs"``.
    :rtype: bool
    """
    return name == "docs"


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
    stripped = config.option.markexpr.strip()
    if not stripped:
        return None
    try:
        compiled = Expression.compile(stripped)
    except SyntaxError:
        return None
    if not compiled.evaluate(partial(_matches_docs_marker)):
        return True
    return None
