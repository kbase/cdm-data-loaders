"""Tests for the JSONL line-reading helpers in readers.jsonlines."""

import gzip
import json
from collections.abc import Callable
from pathlib import Path
from typing import Any, Final

import pyarrow as pa
import pytest
from dlt.extract.items import DataItemWithMeta, TableNameMeta
from dlt.sources.filesystem import filesystem
from pydantic import ValidationError

from cdm_data_loaders.pipelines.jsonlines.settings import JsonlExtractSettings
from cdm_data_loaders.readers.jsonlines import (
    iter_file_jsonl_records,
    make_page_router,
    route_jsonl_lines,
    stream_jsonl_lines,
)
from cdm_data_loaders.utils.buffer import REJECTED_PYARROW_SCHEMA
from tests.fs_backends import FsBackend
from tests.helpers import make_cts_settings, register_temp_module
from tests.jsonlines_fixtures import make_item


def make_extract_settings(base_dir: Path, **overrides: Any) -> JsonlExtractSettings:  # noqa: ANN401
    """Build a JsonlExtractSettings rooted at base_dir/_settings with default table_name='widget'."""
    defaults: dict[str, Any] = {
        "dataset_name": "test_dataset",
        "table_name": "widget",
        "buffer_size": 10,
    }
    defaults.update(overrides)
    return make_cts_settings(JsonlExtractSettings, base_dir / "_settings", **defaults)


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

    pages = list(route_jsonl_lines(iter(read_items(directory)), make_extract_settings(tmp_path)))

    by_table = {page.meta.table_name: page.data for page in pages}

    assert list(by_table["widget"]) == [{"widget_id": "a"}, {"widget_id": "b"}]
    assert isinstance(by_table["_rejected"], pa.Table)
    assert by_table["_rejected"].schema == REJECTED_PYARROW_SCHEMA
    rejected = by_table["_rejected"].to_pylist()
    assert len(rejected) == 1
    assert "record" not in rejected[0]
    assert rejected[0]["table_name"] == "widget"
    assert rejected[0]["raw_record"] == "{broken"
    expected_rejected_line_no = 2
    assert rejected[0]["line_no"] == expected_rejected_line_no
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

    pages = list(route_jsonl_lines(iter(read_items(directory)), make_extract_settings(tmp_path, buffer_size=2)))

    assert [len(page.data) for page in pages] == [2, 1]
    assert [row["widget_id"] for page in pages for row in page.data] == ["a", "b", "c"]


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
        pytest.param("", id="empty_file"),
        pytest.param("\n\n\r\n", id="newlines_only"),
        pytest.param("   \t  ", id="spaces_and_tabs_only"),
        pytest.param("  \n\t\t\r\n   \n\t ", id="mixed_blank_and_whitespace_lines"),
    ],
)
def test_iter_file_jsonl_records_pass_empty_or_whitespace_only_file_yields_nothing(
    tmp_path: Path, content: str
) -> None:
    """Empty files and files containing only blank or whitespace lines yield no records."""
    directory = tmp_path / "widget"
    directory.mkdir()
    (directory / "empty.jsonl").write_text(content, encoding="utf-8")
    file_item = read_items(directory)[0]

    assert list(iter_file_jsonl_records(file_item)) == []


def test_iter_file_jsonl_records_pass_skips_whitespace_lines_and_strips_padding(tmp_path: Path) -> None:
    """Leading, middle, and trailing blank/whitespace lines are skipped; surrounding whitespace and CRLF are stripped."""
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
        pytest.param("{not valid json", id="unclosed_brace"),
        pytest.param('{"widget_id": "a",}', id="trailing_comma"),
        pytest.param("{'widget_id': 'a'}", id="single_quoted_keys"),
        pytest.param("unquoted_token", id="bare_identifier"),
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
    tmp_path: Path, write_gzip_file: Callable[[Path, str, str], Path]
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


# local and non-local filesystems

BACKEND_FILES: Final[dict[str, bytes]] = {
    "a.jsonl": b'{"widget_id": "caf\xc3\xa9"}\r\n\r\n{broken\n',
    "sub/b.jsonl.gz": gzip.compress(b'{"widget_id": "b"}\n'),
    "sub/deep/nested/dir/c.jsonl": b'{"widget_id": "c"}\n',
}


def test_stream_jsonl_lines_pass_file_items_any_backend(fs_backend: FsBackend) -> None:
    """Line records from file items on any backend carry relative-path provenance; .gz files are decompressed."""
    fs_backend.put_all(BACKEND_FILES)

    records = [record for page in stream_jsonl_lines(iter(fs_backend.file_items()), buffer_size=10) for record in page]

    assert [(r["source_file"], r["line_no"], r["record"], r["raw_record"]) for r in records] == [
        ("a.jsonl", 1, {"widget_id": "caf\u00e9"}, '{"widget_id": "caf\u00e9"}'),
        ("a.jsonl", 3, None, "{broken"),
        ("sub/b.jsonl.gz", 1, {"widget_id": "b"}, '{"widget_id": "b"}'),
        ("sub/deep/nested/dir/c.jsonl", 1, {"widget_id": "c"}, '{"widget_id": "c"}'),
    ]
    assert [r["parse_error"] is None for r in records] == [True, False, True, True]


def test_route_jsonl_lines_pass_file_items_any_backend(fs_backend: FsBackend, tmp_path: Path) -> None:
    """Routing works the same for file items on any backend, with .gz files decompressed."""
    fs_backend.put_all(BACKEND_FILES)

    pages = list(route_jsonl_lines(iter(fs_backend.file_items()), make_extract_settings(tmp_path)))

    by_table = {page.meta.table_name: page.data for page in pages}
    assert by_table["widget"] == [{"widget_id": "caf\u00e9"}, {"widget_id": "b"}, {"widget_id": "c"}]
    assert isinstance(by_table["_rejected"], pa.Table)
    assert [(r["table_name"], r["source_file"], r["line_no"]) for r in by_table["_rejected"].to_pylist()] == [
        ("widget", "a.jsonl", 3),
    ]


def test_route_jsonl_lines_pass_applies_table_transform_and_schema_map(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """route_jsonl_lines filters, transforms, splits, and coerces rows when given transform and schema_map."""
    directory = tmp_path / "widget"
    directory.mkdir()
    (directory / "data.jsonl").write_text(
        '{"widget_id": "a", "count": 0, "tags": ["t1"]}\n'
        '{"widget_id": "b", "count": 2, "tags": ["t2", "t3"]}\n'
        "{broken\n",
        encoding="utf-8",
    )
    transform = lambda row: (  # noqa: E731
        {}
        if row["count"] == 0
        else {
            "widget": [{"widget_id": row["widget_id"], "count": row["count"]}],
            "widget_tag": [{"widget_id": row["widget_id"], "tag": tag} for tag in row["tags"]],
        }
    )
    mod_name = register_temp_module(
        monkeypatch,
        "_test_route_jsonl_lines_transforms",
        TABLE_TRANSFORMS={"widget": transform},
        OUTPUT_SCHEMAS={
            "widget": pa.schema([pa.field("widget_id", pa.string()), pa.field("count", pa.int64())]),
            "widget_tag": pa.schema([pa.field("widget_id", pa.string()), pa.field("tag", pa.string())]),
        },
    )
    settings = make_extract_settings(tmp_path, transforms_module=mod_name, output_schemas_module=mod_name)

    pages = list(route_jsonl_lines(iter(read_items(directory)), settings))

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


def test_route_jsonl_lines_pass_buffers_across_files_up_to_buffer_size(tmp_path: Path) -> None:
    """route_jsonl_lines accumulates rows across files for the same table before flushing at buffer_size."""
    directory = tmp_path / "widget"
    directory.mkdir()
    (directory / "first.jsonl").write_text('{"widget_id": "a"}\n', encoding="utf-8")
    (directory / "second.jsonl").write_text('{"widget_id": "b"}\n', encoding="utf-8")

    pages = list(route_jsonl_lines(iter(read_items(directory)), make_extract_settings(tmp_path, buffer_size=2)))

    assert [len(page.data) for page in pages] == [2]
    assert sorted(row["widget_id"] for page in pages for row in page.data) == ["a", "b"]


def run_validator(validator: Callable[[list[dict[str, Any]]], Any], item: dict[str, Any]) -> list[DataItemWithMeta]:
    """Run the validator on a single-item page. Return the tagged results."""
    return list(validator([item]))


def make_router(model: Any, **kwargs: Any) -> Callable[[list[dict[str, Any]]], Any]:  # noqa: ANN401
    """Build a router for the widget table using a model, mirroring the pydantic pipeline."""

    def _resolve_error(record: dict[str, Any]) -> str | None:
        try:
            model.model_validate(record)
        except ValidationError as error:
            return json.dumps(error.errors(), default=str)
        return None

    return make_page_router(
        "widget",
        kwargs.get("buffer_size", 10),
        _resolve_error,
        **{key: value for key, value in kwargs.items() if key != "buffer_size"},
    )


def tagged_pages(results: list[DataItemWithMeta]) -> list[tuple[str, int]]:
    """Return (table_name, row_count) pairs for tagged pages."""
    return [(r.meta.table_name, len(r.data)) for r in results]


def test_make_page_router_pass_valid_record_routed_to_main_table(widget_model: Any) -> None:  # noqa: ANN401
    """A record that passes validation goes to the main table, dumped from the validated model."""
    validator = make_router(widget_model)
    item = make_item(record={"widget_id": "a", "count": 5}, raw_record='{"widget_id": "a", "count": 5}')

    results = run_validator(validator, item)

    assert len(results) == 1
    result = results[0]
    assert isinstance(result.meta, TableNameMeta)
    assert result.meta.table_name == "widget"
    assert result.data == [{"widget_id": "a", "count": 5}]


def test_make_page_router_fail_json_parse_error_routed_to_rejected_without_model_validation(
    widget_model: Any,  # noqa: ANN401
) -> None:
    """A record with a parse_error goes to _rejected. The model is not called."""
    validator = make_router(widget_model)
    item = make_item(record=None, raw_record="{not json", parse_error="Expecting value: line 1 column 1 (char 0)")

    results = run_validator(validator, item)

    assert len(results) == 1
    result = results[0]
    assert result.meta.table_name == "_rejected"
    assert isinstance(result.data, pa.Table)
    assert result.data.schema == REJECTED_PYARROW_SCHEMA
    assert result.data.to_pylist() == [
        {
            "table_name": "widget",
            "error_detail": "Expecting value: line 1 column 1 (char 0)",
            "line_no": 1,
            "raw_record": "{not json",
            "source_file": "file.jsonl",
        }
    ]


@pytest.mark.parametrize(
    "record",
    [
        pytest.param({"count": 5}, id="missing_required_widget_id"),
        pytest.param({"widget_id": "a", "count": -1}, id="count_below_minimum_boundary"),
        pytest.param({"widget_id": "a", "count": "not-a-number"}, id="count_not_coercible"),
        pytest.param({"widget_id": "", "count": 1}, id="widget_id_below_min_length_boundary"),
    ],
)
def test_make_page_router_fail_pydantic_validation_error_routed_to_rejected(
    widget_model: Any,  # noqa: ANN401
    record: dict[str, Any],
) -> None:
    """A record that parses as JSON but fails model validation goes to _rejected, with error detail."""
    validator = make_router(widget_model)
    item = make_item(record=record, raw_record=json.dumps(record))

    results = run_validator(validator, item)

    assert len(results) == 1
    result = results[0]
    assert result.meta.table_name == "_rejected"
    assert isinstance(result.data, pa.Table)
    assert result.data.schema == REJECTED_PYARROW_SCHEMA
    rejected_rows = result.data.to_pylist()
    assert len(rejected_rows) == 1
    assert rejected_rows[0]["table_name"] == "widget"
    assert rejected_rows[0]["raw_record"] == json.dumps(record)
    error_detail = json.loads(rejected_rows[0]["error_detail"])
    assert isinstance(error_detail, list)
    assert len(error_detail) >= 1
    assert all({"loc", "msg", "type"} <= set(error.keys()) for error in error_detail)


def test_make_page_router_pass_mixed_batch_routes_each_record_independently(
    widget_model: Any,  # noqa: ANN401
) -> None:
    """A mixed batch routes each record on its own. Valid and invalid records do not affect each other."""
    validator = make_router(widget_model)
    items = [
        make_item(record={"widget_id": "a", "count": 1}, raw_record='{"widget_id": "a", "count": 1}', line_no=1),
        make_item(record={"widget_id": "b", "count": -1}, raw_record='{"widget_id": "b", "count": -1}', line_no=2),
        make_item(record={"widget_id": "c", "count": 3}, raw_record='{"widget_id": "c", "count": 3}', line_no=3),
        make_item(record=None, raw_record="{bad", parse_error="boom", line_no=4),
    ]

    results = list(validator(items))

    valid_rows = [r for r in results if r.meta.table_name == "widget"]
    assert len(valid_rows) == 1
    assert [row["widget_id"] for row in valid_rows[0].data] == ["a", "c"]

    rejected_rows = [r for r in results if r.meta.table_name == "_rejected"]
    assert len(rejected_rows) == 1
    assert isinstance(rejected_rows[0].data, pa.Table)
    assert {row["line_no"] for row in rejected_rows[0].data.to_pylist()} == {2, 4}


def test_make_page_router_fail_all_invalid_none_reach_main_table(
    widget_model: Any,  # noqa: ANN401
) -> None:
    """When every record in a batch is invalid, none reach the main table."""
    validator = make_router(widget_model)
    items = [
        make_item(record={"count": 1}, raw_record='{"count": 1}', line_no=1),
        make_item(record={"widget_id": "b", "count": -5}, raw_record='{"widget_id": "b", "count": -5}', line_no=2),
        make_item(record=None, raw_record="not json at all", parse_error="bad json", line_no=3),
    ]

    results = list(validator(items))

    assert len(results) == 1
    assert results[0].meta.table_name == "_rejected"
    assert isinstance(results[0].data, pa.Table)
    assert len(results[0].data) == len(items)


def test_make_page_router_pass_buffer_size_controls_batching(
    widget_model: Any,  # noqa: ANN401
) -> None:
    """buffer_size controls how many rows land in each yielded page per table."""
    validator = make_router(widget_model, buffer_size=2)
    items = [
        make_item(record={"widget_id": f"w{i}", "count": 1}, raw_record=f'{{"count": {i}}}', line_no=i + 1)
        for i in range(5)
    ]

    pages = tagged_pages(list(validator(items)))

    assert pages == [("widget", 2), ("widget", 2), ("widget", 1)]


def test_make_page_router_pass_use_pyarrow_false_yields_lists_of_dicts(
    widget_model: Any,  # noqa: ANN401
) -> None:
    """When use_pyarrow is False, make_page_router yields list[dict] for valid, rejected, and preprocessor-rejected items."""
    validator = make_router(widget_model, use_pyarrow=False)
    items = [
        make_item(record={"widget_id": "a", "count": 1}, raw_record='{"widget_id": "a", "count": 1}', line_no=1),
        make_item(record=None, raw_record="{bad", parse_error="boom", line_no=2),
    ]

    results = list(validator(items))
    by_table = {r.meta.table_name: r.data for r in results}

    assert isinstance(by_table["widget"], list)
    assert by_table["widget"] == [{"widget_id": "a", "count": 1}]
    assert isinstance(by_table["_rejected"], list)
    assert by_table["_rejected"][0]["line_no"] == 2  # noqa: PLR2004

    preprocess_rejected = {
        "table_name": "widget",
        "source_file": "widget/corrupt.jsonl",
        "line_no": None,
        "raw_record": None,
        "error_detail": "corrupt header",
    }
    pre_results = list(validator(preprocess_rejected))  # type: ignore[arg-type]
    assert len(pre_results) == 1
    assert pre_results[0].meta.table_name == "_rejected"
    assert isinstance(pre_results[0].data, list)
    assert pre_results[0].data == [preprocess_rejected]
