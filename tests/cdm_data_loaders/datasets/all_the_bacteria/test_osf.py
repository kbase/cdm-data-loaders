"""Unit tests for ``cdm_data_loaders.datasets.all_the_bacteria.osf``."""

import logging
import re
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from cdm_data_loaders.datasets.all_the_bacteria.osf import (
    ALL_ATB_FILE_NAME,
    download_osf_files,
    download_osf_index_tsv,
    iter_atb_download_links,
    load_patterns,
)


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


@patch("cdm_data_loaders.datasets.all_the_bacteria.osf.FileDownloader")
@patch("cdm_data_loaders.datasets.all_the_bacteria.osf.fetch_json")
def test_download_osf_index_tsv_fetches_link_and_downloads(
    mock_fetch_json: MagicMock,
    mock_downloader_cls: MagicMock,
    tmp_path: Path,
) -> None:
    """Fetch OSF metadata JSON and download all_atb_files.tsv into raw_data_dir."""
    mock_fetch_json.return_value = {"data": {"links": {"download": "https://osf.io/download/r6gcp/"}}}
    raw_dir = tmp_path / "raw"
    result = download_osf_index_tsv(raw_dir)

    expected_dest = raw_dir / ALL_ATB_FILE_NAME
    assert result == expected_dest
    mock_downloader_cls.return_value.download.assert_called_once_with(
        url="https://osf.io/download/r6gcp/",
        destination=expected_dest,
    )


def test_download_osf_index_tsv_missing_link_raises(tmp_path: Path) -> None:
    """Raise RuntimeError when the OSF metadata JSON lacks data.links.download."""
    with (
        patch("cdm_data_loaders.datasets.all_the_bacteria.osf.fetch_json", return_value={"data": {"links": {}}}),
        pytest.raises(RuntimeError, match="Could not find download URL"),
    ):
        download_osf_index_tsv(tmp_path / "raw")


def test_iter_atb_download_links_filters_and_warns_on_extra_column(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Yield matching rows and log WARNING when non-required headers change."""
    tsv = tmp_path / "index.tsv"
    tsv.write_text(
        "project\tfilename\turl\tmd5\textra_col\n"
        "AllTheBacteria/Metadata\tspecies.tsv.gz\thttps://osf.io/1\taaa\tx\n"
        "AllTheBacteria/Other\tskip.tsv.gz\thttps://osf.io/2\tbbb\ty\n",
        encoding="utf-8",
    )
    with caplog.at_level(logging.WARNING):
        batches = list(iter_atb_download_links(tsv, re.compile(r"^AllTheBacteria/Metadata$")))

    assert batches == [
        [
            {
                "project": "AllTheBacteria/Metadata",
                "filename": "species.tsv.gz",
                "url": "https://osf.io/1",
                "md5": "aaa",
                "extra_col": "x",
            }
        ]
    ]
    assert "ATB file index TSV headers have changed" in caplog.text


@pytest.mark.parametrize(
    ("tsv_text", "match"),
    [
        pytest.param("project\tproject_id\tfilename\turl\tmd5\tsize(MB)\n", "No valid TSV data found", id="empty-rows"),
        pytest.param("project\tfilename\turl\nA\tf.tsv\thttps://osf.io/1\n", "Missing required", id="missing-md5-col"),
    ],
)
def test_iter_atb_download_links_invalid_tsv(tmp_path: Path, tsv_text: str, match: str) -> None:
    """Raise RuntimeError on empty TSV bodies or missing required headers."""
    tsv = tmp_path / "bad.tsv"
    tsv.write_text(tsv_text, encoding="utf-8")
    with pytest.raises(RuntimeError, match=match):
        list(iter_atb_download_links(tsv, re.compile(r".*")))


@patch("cdm_data_loaders.datasets.all_the_bacteria.osf.FileDownloader")
def test_download_osf_files_records_successes_and_skips_failures(
    mock_downloader_cls: MagicMock,
    tmp_path: Path,
) -> None:
    """Populate 'path' on downloaded file records and continue past single-file download errors."""
    mock_client = mock_downloader_cls.return_value

    def side_effect(*, url: str, destination: Path, expected_checksum: str, checksum_fn: str) -> Path:
        assert checksum_fn == "md5"
        if url.endswith("/bad"):
            raise OSError
        assert expected_checksum == "abc"
        return destination

    mock_client.download.side_effect = side_effect
    files = [
        {"project": "AllTheBacteria/Metadata/", "filename": "ok.tsv.gz", "url": "https://osf.io/ok", "md5": "abc"},
        {"project": "AllTheBacteria/Assembly/", "filename": "bad.tar.xz", "url": "https://osf.io/bad", "md5": "def"},
    ]
    succeeded = download_osf_files(tmp_path, files)
    assert succeeded == [
        {
            "project": "AllTheBacteria/Metadata/",
            "filename": "ok.tsv.gz",
            "url": "https://osf.io/ok",
            "md5": "abc",
            "path": str(tmp_path / "Metadata" / "ok.tsv.gz"),
        }
    ]
