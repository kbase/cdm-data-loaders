"""Unit tests for ``load_patterns`` in ``cdm_data_loaders.datasets.all_the_bacteria.osf``."""

import re
from pathlib import Path

import pytest

from cdm_data_loaders.datasets.all_the_bacteria.osf import load_patterns


@pytest.fixture
def pattern_file(tmp_path: Path) -> Path:
    """Create a sample pattern file for ``load_patterns`` tests.

    :param tmp_path: Pytest temporary directory.
    :type tmp_path: Path
    :return: Path to the temporary pattern file.
    :rtype: Path
    """
    path = tmp_path / "patterns.txt"
    path.write_text("hello world\nfoo.bar\nstarts with*\n2+2=4\n", encoding="utf-8")
    return path


@pytest.mark.parametrize(
    ("candidate", "expected_match"),
    [
        pytest.param("hello world", True, id="exact-hello"),
        pytest.param("foo.bar", True, id="exact-dot"),
        pytest.param("2+2=4", True, id="exact-plus"),
        pytest.param("hello world!!!", False, id="trailing-chars"),
        pytest.param("well, hello world", False, id="leading-chars"),
        pytest.param("fooXbar", False, id="dot-is-literal"),
        pytest.param("22=4", False, id="plus-is-literal"),
        pytest.param("starts with", True, id="wildcard-empty-suffix"),
        pytest.param("starts with anything", True, id="wildcard-alpha-suffix"),
        pytest.param("starts with 123!@#", True, id="wildcard-symbol-suffix"),
        pytest.param("ends with", False, id="wildcard-wrong-prefix"),
    ],
)
def test_load_patterns_exact_match(pattern_file: Path, candidate: str, expected_match: bool) -> None:
    """Plain text lines match against their literal content or trailing wildcard."""
    pattern = load_patterns(pattern_file)
    assert isinstance(pattern, re.Pattern)
    assert bool(pattern.match(candidate)) is expected_match


@pytest.mark.parametrize(
    ("candidate", "expected_match"),
    [
        pytest.param("hello", True, id="hello"),
        pytest.param("world", True, id="world"),
        pytest.param("", False, id="empty-string"),
    ],
)
def test_load_patterns_blank_lines_are_ignored(tmp_path: Path, candidate: str, expected_match: bool) -> None:
    """Blank lines in the pattern file are skipped."""
    path = tmp_path / "patterns.txt"
    path.write_text("\nhello\n\nworld\n", encoding="utf-8")
    pattern = load_patterns(path)
    assert isinstance(pattern, re.Pattern)
    assert bool(pattern.match(candidate)) is expected_match


@pytest.mark.parametrize("candidate", ["", "anything at all"], ids=["empty", "non-empty"])
def test_load_patterns_only_wildcard_matches_anything(tmp_path: Path, candidate: str) -> None:
    """A file containing only '*' produces a pattern matching any string."""
    path = tmp_path / "patterns.txt"
    path.write_text("*\n", encoding="utf-8")
    pattern = load_patterns(path)
    assert isinstance(pattern, re.Pattern)
    assert pattern.match(candidate)


@pytest.mark.parametrize(
    ("candidate", "expected_match"),
    [
        pytest.param("cat", True, id="cat"),
        pytest.param("dog", True, id="dog"),
        pytest.param("bird", True, id="bird"),
        pytest.param("fish", False, id="fish"),
    ],
)
def test_load_patterns_alternation(tmp_path: Path, candidate: str, expected_match: bool) -> None:
    """Each line in the file becomes an alternative in the combined regex."""
    path = tmp_path / "patterns.txt"
    path.write_text("cat\ndog\nbird\n", encoding="utf-8")
    pattern = load_patterns(path)
    assert isinstance(pattern, re.Pattern)
    assert bool(pattern.match(candidate)) is expected_match


def test_load_patterns_no_file_returns_none(tmp_path: Path) -> None:
    """Loading a missing file returns None."""
    assert load_patterns(tmp_path / "some" / "path") is None


def test_load_patterns_touched_file_returns_none(tmp_path: Path) -> None:
    """Loading a zero-byte file returns None."""
    path = tmp_path / "patterns.txt"
    path.touch()
    assert load_patterns(path) is None


def test_load_patterns_empty_file_returns_none(tmp_path: Path) -> None:
    """Loading a whitespace-only file returns None."""
    path = tmp_path / "patterns.txt"
    path.write_text("\n\n   \t\n  \n   \n\t\t\n", encoding="utf-8")
    assert load_patterns(path) is None
