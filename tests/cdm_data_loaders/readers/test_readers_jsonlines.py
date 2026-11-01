"""Tests for the JSONL line-reading helpers in readers.jsonlines."""

import gzip
from collections.abc import Callable
from pathlib import Path
from typing import Any, Final

import pyarrow as pa
from dlt.sources.filesystem import filesystem

from cdm_data_loaders.core.transforms import TableTransform
from cdm_data_loaders.readers.jsonlines import (
    _iter_file_jsonl_records,
    parse_jsonl_line,
    route_jsonl_lines,
    stream_jsonl_lines,
)
from tests.fs_backends import FsBackend


def test_parse_jsonl_line_pass_valid_json_returns_record() -> None:
    """A valid JSON line parses into the record with no error."""
    result = parse_jsonl_line('{"widget_id": "a", "count": 1}')

    assert result == {
        "record": {"widget_id": "a", "count": 1},
        "raw_record": '{"widget_id": "a", "count": 1}',
        "parse_error": None,
    }


def test_parse_jsonl_line_fail_malformed_json_captures_error() -> None:
    """A line that is not valid JSON sets record to None and fills parse_error."""
    result = parse_jsonl_line("{not valid json")

    assert result["record"] is None
    assert result["raw_record"] == "{not valid json"
    assert result["parse_error"] is not None


def read_items(directory: Path, file_glob: str = "*.jsonl*") -> list[Any]:
    """List the filesystem items in a directory."""
    return list(filesystem(bucket_url=str(directory), file_glob=file_glob))


def read_records(directory: Path, buffer_size: int = 10) -> list[dict[str, Any]]:
    """Stream every line record in a directory, flattened across pages."""
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
    """A line that is not valid JSON sets record to None and fills parse_error; it does not raise."""
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
    """Line numbers restart at 1 in each file; source_file names the file."""
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
    tmp_path: Path, write_gzip_file: Callable[[Path, str, str], Path]
) -> None:
    """A .gz-suffixed file is decompressed before its lines are read."""
    directory = tmp_path / "widget"
    write_gzip_file(directory, "compressed.jsonl.gz", '{"widget_id": "a", "count": 1}\n')

    records = read_records(directory)

    assert len(records) == 1
    assert records[0]["record"] == {"widget_id": "a", "count": 1}
    assert records[0]["parse_error"] is None


def test_route_jsonl_lines_pass_routes_valid_and_invalid_rows(tmp_path: Path) -> None:
    """Valid lines route to the table; parse failures route to _rejected."""
    directory = tmp_path / "widget"
    directory.mkdir()
    (directory / "data.jsonl").write_text('{"widget_id": "a"}\n{broken\n{"widget_id": "b"}\n', encoding="utf-8")

    pages = list(
        route_jsonl_lines(
            iter(read_items(directory)),
            buffer_size=10,
            table_name_of=lambda file_item: Path(file_item["file_url"]).parent.name,
        )
    )

    by_table: dict[str, list[dict[str, Any]]] = {}
    for page in pages:
        by_table.setdefault(page.meta.table_name, []).extend(page.data)

    assert list(by_table["widget"]) == [{"widget_id": "a"}, {"widget_id": "b"}]
    rejected = by_table["_rejected"]
    assert len(rejected) == 1
    assert "record" not in rejected[0]
    assert rejected[0]["table_name"] == "widget"
    assert rejected[0]["raw_record"] == "{broken"
    assert rejected[0]["line_no"] == 2
    assert rejected[0]["source_file"] == "data.jsonl"
    assert rejected[0]["error_detail"] is not None


def test_route_jsonl_lines_pass_skips_blank_lines_and_flushes_at_buffer_size(tmp_path: Path) -> None:
    """route_jsonl_lines skips blank lines, preserves line numbers, and flushes pages at buffer_size."""
    directory = tmp_path / "widget"
    directory.mkdir()
    (directory / "data.jsonl").write_text(
        '{"widget_id": "a"}\n\n   \n{"widget_id": "b"}\n{"widget_id": "c"}\n',
        encoding="utf-8",
    )

    pages = list(
        route_jsonl_lines(
            iter(read_items(directory)),
            buffer_size=2,
            table_name_of=lambda _file_item: "widget",
        )
    )

    assert [len(page.data) for page in pages] == [2, 1]
    assert [row["widget_id"] for page in pages for row in page.data] == ["a", "b", "c"]


def test_iter_file_jsonl_records_pass_yields_records_with_line_numbers_and_skips_blanks(tmp_path: Path) -> None:
    """_iter_file_jsonl_records yields parsed and malformed records with 1-based line_no and source_file."""
    directory = tmp_path / "widget"
    directory.mkdir()
    (directory / "sample.jsonl").write_text('{"id": 1}\n\n{bad\n{"id": 2}\n', encoding="utf-8")
    file_item = read_items(directory)[0]

    records = list(_iter_file_jsonl_records(file_item))

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


# local and non-local filesystems

BACKEND_FILES: Final[dict[str, bytes]] = {
    "a.jsonl": b'{"widget_id": "caf\xc3\xa9"}\r\n\r\n{broken\n',
    "sub/b.jsonl.gz": gzip.compress(b'{"widget_id": "b"}\n'),
}


def test_stream_jsonl_lines_pass_file_items_any_backend(fs_backend: FsBackend) -> None:
    """Line records from file items on any backend carry relative-path provenance; .gz files are decompressed."""
    fs_backend.put_all(BACKEND_FILES)

    records = [record for page in stream_jsonl_lines(iter(fs_backend.file_items()), buffer_size=10) for record in page]

    assert [(r["source_file"], r["line_no"], r["record"], r["raw_record"]) for r in records] == [
        ("a.jsonl", 1, {"widget_id": "caf\u00e9"}, '{"widget_id": "caf\u00e9"}'),
        ("a.jsonl", 3, None, "{broken"),
        ("sub/b.jsonl.gz", 1, {"widget_id": "b"}, '{"widget_id": "b"}'),
    ]
    assert [r["parse_error"] is None for r in records] == [True, False, True]


def test_route_jsonl_lines_pass_file_items_any_backend(fs_backend: FsBackend) -> None:
    """Routing works the same for file items on any backend, with .gz files decompressed."""
    fs_backend.put_all(BACKEND_FILES)

    pages = list(route_jsonl_lines(iter(fs_backend.file_items()), buffer_size=10, table_name_of=lambda _: "widget"))

    by_table: dict[str, list[dict[str, Any]]] = {}
    for page in pages:
        by_table.setdefault(page.meta.table_name, []).extend(page.data)
    assert by_table["widget"] == [{"widget_id": "caf\u00e9"}, {"widget_id": "b"}]
    assert [(r["table_name"], r["source_file"], r["line_no"]) for r in by_table["_rejected"]] == [
        ("widget", "a.jsonl", 3),
    ]


def test_route_jsonl_lines_pass_applies_table_transform_and_schema_map(tmp_path: Path) -> None:
    """route_jsonl_lines filters, transforms, splits, and coerces rows when given a TableTransform."""
    directory = tmp_path / "widget"
    directory.mkdir()
    (directory / "data.jsonl").write_text(
        '{"widget_id": "a", "count": 0, "tags": ["t1"]}\n'
        '{"widget_id": "b", "count": 2, "tags": ["t2", "t3"]}\n'
        "{broken\n",
        encoding="utf-8",
    )
    transform = TableTransform(
        transform_fn=lambda row: (
            {}
            if row["count"] == 0
            else {
                "widget": [{"widget_id": row["widget_id"], "count": row["count"]}],
                "widget_tag": [{"widget_id": row["widget_id"], "tag": tag} for tag in row["tags"]],
            }
        ),
        table_to_schema_map={
            "widget": pa.schema([pa.field("widget_id", pa.string()), pa.field("count", pa.int64())]),
            "widget_tag": pa.schema([pa.field("widget_id", pa.string()), pa.field("tag", pa.string())]),
        },
    )

    pages = list(
        route_jsonl_lines(
            iter(read_items(directory)),
            buffer_size=10,
            table_name_of=lambda _: "widget",
            table_transform=transform,
        )
    )

    by_table = {page.meta.table_name: page.data for page in pages}
    assert isinstance(by_table["widget"], pa.Table)
    assert by_table["widget"].to_pylist() == [{"widget_id": "b", "count": 2}]
    assert isinstance(by_table["widget_tag"], pa.Table)
    assert by_table["widget_tag"].to_pylist() == [
        {"widget_id": "b", "tag": "t2"},
        {"widget_id": "b", "tag": "t3"},
    ]
    assert isinstance(by_table["_rejected"], pa.Table)
    rejected_rows = by_table["_rejected"].to_pylist()
    assert len(rejected_rows) == 1
    assert rejected_rows[0]["table_name"] == "widget"
    assert rejected_rows[0]["raw_record"] == "{broken"
