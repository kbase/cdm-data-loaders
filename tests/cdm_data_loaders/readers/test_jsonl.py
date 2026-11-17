"""Unit tests for ``iter_file_jsonl_records`` and ``stream_jsonl_lines`` in ``cdm_data_loaders.readers.jsonl``."""

import gzip
from collections.abc import Callable
from pathlib import Path
from typing import Any, Final

import pytest
from dlt.sources.filesystem import filesystem

from cdm_data_loaders.readers.jsonl import (
    iter_file_jsonl_records,
    stream_jsonl_lines,
)
from tests._support.fs_backends import FsBackend

BACKEND_FILES: Final[dict[str, bytes]] = {
    "a.jsonl": b'{"widget_id": "caf\xc3\xa9"}\r\n\r\n{broken\n',
    "sub/b.jsonl.gz": gzip.compress(b'{"widget_id": "b"}\n'),
    "sub/deep/nested/dir/c.jsonl": b'{"widget_id": "c"}\n',
}


def read_items(directory: Path, file_glob: str = "*.jsonl*") -> list[Any]:
    """List the ``dlt`` filesystem items in ``directory``.

    :param directory: Root directory to scan.
    :type directory: Path
    :param file_glob: Glob pattern for matching files.
    :type file_glob: str
    :return: Materialized file item dictionaries.
    :rtype: list[Any]
    """
    return list(filesystem(bucket_url=str(directory), file_glob=file_glob))


def read_records(directory: Path, buffer_size: int = 10) -> list[dict[str, Any]]:
    """Stream every line record in ``directory``, flattened across pages.

    :param directory: Root directory containing JSONL files.
    :type directory: Path
    :param buffer_size: Maximum records per page.
    :type buffer_size: int
    :return: Flattened list of line record dicts.
    :rtype: list[dict[str, Any]]
    """
    return [
        record for page in stream_jsonl_lines(iter(read_items(directory)), buffer_size=buffer_size) for record in page
    ]


def test_stream_jsonl_lines_pass_yields_pages_of_line_records(tmp_path: Path) -> None:
    """stream_jsonl_lines yields pages of at most buffer_size records with provenance."""
    directory = tmp_path / "widget"
    directory.mkdir()
    lines = [f'{{"widget_id": "w{i}", "count": {i}}}' for i in range(3)]
    (directory / "batch.jsonl").write_text("\n".join(lines) + "\n", encoding="utf-8")

    pages = list(stream_jsonl_lines(iter(read_items(directory)), buffer_size=2))

    assert [len(page) for page in pages] == [2, 1]
    first = pages[0][0]
    assert first["record"] == {"widget_id": "w0", "count": 0}
    assert first["parse_error"] is None
    assert first["source_file"] == "batch.jsonl"
    assert first["line_no"] == 1


def test_stream_jsonl_lines_pass_blank_lines_are_skipped(tmp_path: Path) -> None:
    """Blank and whitespace-only lines produce no output records."""
    directory = tmp_path / "widget"
    directory.mkdir()
    (directory / "blank.jsonl").write_text('{"widget_id": "a"}\n\n   \n{"widget_id": "b"}\n', encoding="utf-8")

    records = read_records(directory)

    assert [record["record"]["widget_id"] for record in records] == ["a", "b"]


def test_stream_jsonl_lines_fail_malformed_json_line_captured_not_raised(tmp_path: Path) -> None:
    """A line that is not valid JSON sets record to None and fills parse_error without raising."""
    directory = tmp_path / "widget"
    directory.mkdir()
    (directory / "bad.jsonl").write_text("{not valid json\n", encoding="utf-8")

    records = read_records(directory)

    assert len(records) == 1
    assert records[0]["record"] is None
    assert records[0]["parse_error"] is not None
    assert records[0]["raw_record"] == "{not valid json"
    assert records[0]["line_no"] == 1


def test_stream_jsonl_lines_pass_empty_file_yields_nothing(tmp_path: Path) -> None:
    """An empty file produces no output records."""
    directory = tmp_path / "widget"
    directory.mkdir()
    (directory / "empty.jsonl").write_text("", encoding="utf-8")

    assert read_records(directory) == []


def test_stream_jsonl_lines_pass_line_numbers_restart_per_file(tmp_path: Path) -> None:
    """Line numbers restart at 1 in each file and source_file names the file."""
    directory = tmp_path / "widget"
    directory.mkdir()
    (directory / "first.jsonl").write_text('{"widget_id": "a"}\n{"widget_id": "b"}\n', encoding="utf-8")
    (directory / "second.jsonl").write_text('{"widget_id": "c"}\n', encoding="utf-8")

    records = read_records(directory)

    by_source: dict[str, list[int]] = {}
    for record in records:
        by_source.setdefault(Path(record["source_file"]).name, []).append(record["line_no"])

    assert by_source == {"first.jsonl": [1, 2], "second.jsonl": [1]}


def test_stream_jsonl_lines_pass_gzip_file_is_decompressed(
    tmp_path: Path,
    write_gzip_file: Callable[[Path, str, str], Path],
) -> None:
    """A .gz-suffixed file is decompressed before its lines are read."""
    directory = tmp_path / "widget"
    write_gzip_file(directory, "compressed.jsonl.gz", '{"widget_id": "a", "count": 1}\n')

    records = read_records(directory)

    assert len(records) == 1
    assert records[0]["record"] == {"widget_id": "a", "count": 1}
    assert records[0]["parse_error"] is None


def test_iter_file_jsonl_records_pass_yields_records_with_line_numbers_and_skips_blanks(tmp_path: Path) -> None:
    """iter_file_jsonl_records yields parsed and malformed records with 1-based line_no and source_file."""
    directory = tmp_path / "widget"
    directory.mkdir()
    (directory / "sample.jsonl").write_text('{"id": 1}\n\n{bad\n{"id": 2}\n', encoding="utf-8")
    file_item = read_items(directory)[0]

    records = list(iter_file_jsonl_records(file_item))

    assert [r["line_no"] for r in records] == [1, 3, 4]
    assert records[0] == {
        "record": {"id": 1},
        "raw_record": '{"id": 1}',
        "parse_error": None,
        "source_file": "sample.jsonl",
        "line_no": 1,
    }
    assert records[1]["record"] is None
    assert records[1]["raw_record"] == "{bad"
    assert records[1]["parse_error"] is not None
    assert records[1]["source_file"] == "sample.jsonl"
    assert records[2] == {
        "record": {"id": 2},
        "raw_record": '{"id": 2}',
        "parse_error": None,
        "source_file": "sample.jsonl",
        "line_no": 4,
    }


@pytest.mark.parametrize(
    "content",
    [
        pytest.param("", id="empty-file"),
        pytest.param("\n\n\r\n", id="newlines-only"),
        pytest.param("   \t  ", id="spaces-and-tabs-only"),
        pytest.param("  \n\t\t\r\n   \n\t ", id="mixed-blank-and-whitespace-lines"),
    ],
)
def test_iter_file_jsonl_records_pass_empty_or_whitespace_only_file_yields_nothing(
    tmp_path: Path,
    content: str,
) -> None:
    """Empty files and files containing only blank or whitespace lines yield no records."""
    directory = tmp_path / "widget"
    directory.mkdir()
    (directory / "empty.jsonl").write_text(content, encoding="utf-8")
    file_item = read_items(directory)[0]

    assert list(iter_file_jsonl_records(file_item)) == []


def test_iter_file_jsonl_records_pass_skips_whitespace_lines_and_strips_padding(tmp_path: Path) -> None:
    """Blank/whitespace lines are skipped and surrounding whitespace and CRLF are stripped."""
    directory = tmp_path / "widget"
    directory.mkdir()
    raw_bytes = (
        b"\n"
        b"   \t  \r\n"
        b'  \t {"widget_id": "caf\xc3\xa9", "count": 1} \t \r\n'
        b"\t\t\n"
        b"   {malformed json   \r\n"
        b"   \n"
        b'{"widget_id": "b", "count": 2}'
        b"\n   \t\r\n"
    )
    (directory / "padded.jsonl").write_bytes(raw_bytes)
    file_item = read_items(directory)[0]

    records = list(iter_file_jsonl_records(file_item))

    assert [r["line_no"] for r in records] == [3, 5, 7]
    assert records[0] == {
        "record": {"widget_id": "caf\u00e9", "count": 1},
        "raw_record": '{"widget_id": "caf\u00e9", "count": 1}',
        "parse_error": None,
        "source_file": "padded.jsonl",
        "line_no": 3,
    }
    assert records[1]["record"] is None
    assert records[1]["raw_record"] == "{malformed json"
    assert records[1]["parse_error"] is not None
    assert records[1]["source_file"] == "padded.jsonl"
    assert records[2] == {
        "record": {"widget_id": "b", "count": 2},
        "raw_record": '{"widget_id": "b", "count": 2}',
        "parse_error": None,
        "source_file": "padded.jsonl",
        "line_no": 7,
    }


@pytest.mark.parametrize(
    "raw_line",
    [
        pytest.param("{not valid json", id="unclosed-brace"),
        pytest.param('{"widget_id": "a",}', id="trailing-comma"),
        pytest.param("{'widget_id': 'a'}", id="single-quoted-keys"),
        pytest.param("unquoted_token", id="bare-identifier"),
    ],
)
def test_iter_file_jsonl_records_fail_malformed_json_captures_error(tmp_path: Path, raw_line: str) -> None:
    """Malformed JSON lines set record to None and populate parse_error without raising."""
    directory = tmp_path / "widget"
    directory.mkdir()
    (directory / "bad.jsonl").write_text(f"{raw_line}\n", encoding="utf-8")
    file_item = read_items(directory)[0]

    records = list(iter_file_jsonl_records(file_item))

    assert len(records) == 1
    assert records[0]["record"] is None
    assert records[0]["raw_record"] == raw_line
    assert isinstance(records[0]["parse_error"], str)
    assert records[0]["parse_error"]
    assert records[0]["source_file"] == "bad.jsonl"
    assert records[0]["line_no"] == 1


def test_iter_file_jsonl_records_pass_decompresses_gzip_and_preserves_relative_path(
    tmp_path: Path,
    write_gzip_file: Callable[[Path, str, str], Path],
) -> None:
    """Gzipped JSONL files in nested directories are decompressed and report their relative source_file."""
    nested = tmp_path / "sub" / "nested"
    write_gzip_file(nested, "data.jsonl.gz", '{"widget_id": "a", "count": 1}\n\n{"widget_id": "b", "count": 2}\n')
    file_item = read_items(tmp_path, file_glob="**/*.jsonl*")[0]

    records = list(iter_file_jsonl_records(file_item))

    assert records == [
        {
            "record": {"widget_id": "a", "count": 1},
            "raw_record": '{"widget_id": "a", "count": 1}',
            "parse_error": None,
            "source_file": "sub/nested/data.jsonl.gz",
            "line_no": 1,
        },
        {
            "record": {"widget_id": "b", "count": 2},
            "raw_record": '{"widget_id": "b", "count": 2}',
            "parse_error": None,
            "source_file": "sub/nested/data.jsonl.gz",
            "line_no": 3,
        },
    ]


def test_stream_jsonl_lines_pass_file_items_any_backend(fs_backend: FsBackend) -> None:
    """Line records from file items on any backend carry relative-path provenance and decompress .gz files."""
    fs_backend.put_all(BACKEND_FILES)

    records = [record for page in stream_jsonl_lines(iter(fs_backend.file_items()), buffer_size=10) for record in page]

    assert [(r["source_file"], r["line_no"], r["record"], r["raw_record"]) for r in records] == [
        ("a.jsonl", 1, {"widget_id": "caf\u00e9"}, '{"widget_id": "caf\u00e9"}'),
        ("a.jsonl", 3, None, "{broken"),
        ("sub/b.jsonl.gz", 1, {"widget_id": "b"}, '{"widget_id": "b"}'),
        ("sub/deep/nested/dir/c.jsonl", 1, {"widget_id": "c"}, '{"widget_id": "c"}'),
    ]
    assert [r["parse_error"] is None for r in records] == [True, False, True, True]
