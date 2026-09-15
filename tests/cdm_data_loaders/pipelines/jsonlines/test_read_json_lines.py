"""Tests for _read_jsonl_lines."""

from collections.abc import Callable
from pathlib import Path
from typing import Any

from dlt.sources.filesystem import filesystem

from cdm_data_loaders.pipelines.jsonlines.pipeline import _read_jsonl_lines


def read_dir(directory: Path, file_glob: str = "*.jsonl*", buffer_size: int = 100) -> list[dict[str, Any]]:
    """Read every file in directory matching file_glob. Return the flattened list of line records."""
    items = list(filesystem(bucket_url=str(directory), file_glob=file_glob))
    pages = list(_read_jsonl_lines(iter(items), buffer_size))
    return [record for page in pages for record in page]


def test_read_jsonl_lines_pass_single_file_all_valid_lines(tmp_path: Path) -> None:
    """Every non-blank line in one file is read, in order, with no parse error."""
    directory = tmp_path / "widget"
    directory.mkdir()
    (directory / "valid.jsonl").write_text(
        '{"widget_id": "a", "count": 1}\n{"widget_id": "b", "count": 2}\n', encoding="utf-8"
    )

    items = read_dir(directory)

    assert len(items) == 2
    assert items[0]["record"] == {"widget_id": "a", "count": 1}
    assert items[0]["parse_error"] is None
    assert items[0]["line_no"] == 1
    assert items[1]["record"] == {"widget_id": "b", "count": 2}
    assert items[1]["line_no"] == 2


def test_read_jsonl_lines_pass_blank_and_whitespace_lines_are_skipped(tmp_path: Path) -> None:
    """Blank and whitespace-only lines produce no output rows."""
    directory = tmp_path / "widget"
    directory.mkdir()
    (directory / "blank.jsonl").write_text(
        '{"widget_id": "a", "count": 1}\n\n   \n{"widget_id": "b", "count": 2}\n', encoding="utf-8"
    )

    items = read_dir(directory)

    assert [item["record"]["widget_id"] for item in items] == ["a", "b"]


def test_read_jsonl_lines_fail_malformed_json_line_captured_not_raised(tmp_path: Path) -> None:
    """A line that is not valid JSON sets record to None and fills parse_error. It does not raise."""
    directory = tmp_path / "widget"
    directory.mkdir()
    (directory / "bad.jsonl").write_text("{not valid json\n", encoding="utf-8")

    items = read_dir(directory)

    assert len(items) == 1
    assert items[0]["record"] is None
    assert items[0]["parse_error"] is not None
    assert items[0]["raw_line"] == "{not valid json"
    assert items[0]["line_no"] == 1


def test_read_jsonl_lines_pass_empty_file_yields_nothing(tmp_path: Path) -> None:
    """An empty file produces no output rows."""
    directory = tmp_path / "widget"
    directory.mkdir()
    (directory / "empty.jsonl").write_text("", encoding="utf-8")

    assert read_dir(directory) == []


def test_read_jsonl_lines_pass_multiple_files_restart_line_numbers(tmp_path: Path) -> None:
    """Line numbers restart at 1 in each file. source_file names the file the line came from."""
    directory = tmp_path / "widget"
    directory.mkdir()
    (directory / "file_a.jsonl").write_text('{"widget_id": "a", "count": 1}\n', encoding="utf-8")
    (directory / "file_b.jsonl").write_text('{"widget_id": "b", "count": 2}\n', encoding="utf-8")

    items = read_dir(directory)
    by_source = {Path(item["source_file"]).name: item for item in items}

    assert by_source["file_a.jsonl"]["line_no"] == 1
    assert by_source["file_a.jsonl"]["record"]["widget_id"] == "a"
    assert by_source["file_b.jsonl"]["line_no"] == 1
    assert by_source["file_b.jsonl"]["record"]["widget_id"] == "b"


def test_read_jsonl_lines_pass_gzip_file_is_decompressed(
    tmp_path: Path, write_gzip_jsonl_file: Callable[[Path, str, list[str]], Path]
) -> None:
    """A file named with a .gz suffix is decompressed before its lines are read."""
    directory = tmp_path / "widget"
    write_gzip_jsonl_file(directory, "compressed.jsonl.gz", ['{"widget_id": "a", "count": 1}'])

    items = read_dir(directory)

    assert len(items) == 1
    assert items[0]["record"] == {"widget_id": "a", "count": 1}
    assert items[0]["parse_error"] is None


def test_read_jsonl_lines_pass_no_matching_files_yields_nothing(tmp_path: Path) -> None:
    """Files that do not match the glob pattern are not read."""
    directory = tmp_path / "widget"
    directory.mkdir()
    (directory / "not_jsonl.txt").write_text("ignored", encoding="utf-8")

    assert read_dir(directory) == []


def test_read_jsonl_lines_pass_buffer_size_controls_page_size(tmp_path: Path) -> None:
    """buffer_size controls how many line records each yielded page carries."""
    directory = tmp_path / "widget"
    directory.mkdir()
    lines = [f'{{"widget_id": "w{i}", "count": {i}}}' for i in range(5)]
    (directory / "batch.jsonl").write_text("\n".join(lines) + "\n", encoding="utf-8")

    items = list(filesystem(bucket_url=str(directory), file_glob="*.jsonl*"))
    pages = list(_read_jsonl_lines(iter(items), buffer_size=2))

    assert [len(page) for page in pages] == [2, 2, 1]
    assert [record["line_no"] for page in pages for record in page] == [1, 2, 3, 4, 5]
