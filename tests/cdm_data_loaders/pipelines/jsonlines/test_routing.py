"""Unit tests for ``route_jsonl_lines`` and ``make_page_router`` in ``cdm_data_loaders.pipelines.jsonlines.routing``."""

import gzip
import json
from collections.abc import Callable
from pathlib import Path
from typing import Any, Final

import pyarrow as pa
import pytest
from dlt.extract.items import DataItemWithMeta, TableNameMeta
from dlt.sources.filesystem import filesystem
from pydantic import BaseModel, ValidationError

from cdm_data_loaders.core.buffer import REJECTED_PYARROW_SCHEMA
from cdm_data_loaders.pipelines.jsonlines.routing import (
    make_page_router,
    route_jsonl_lines,
)
from cdm_data_loaders.pipelines.jsonlines.settings import JsonlExtractSettings
from tests.helpers.cli_assertions import make_cts_settings, register_temp_module
from tests.helpers.fixtures_jsonlines import make_item
from tests.helpers.fs_backends import FsBackend

BACKEND_FILES: Final[dict[str, bytes]] = {
    "a.jsonl": b'{"widget_id": "caf\xc3\xa9"}\r\n\r\n{broken\n',
    "sub/b.jsonl.gz": gzip.compress(b'{"widget_id": "b"}\n'),
    "sub/deep/nested/dir/c.jsonl": b'{"widget_id": "c"}\n',
}
EXPECTED_REJECTED_LINE_NO: Final[int] = 2


def make_extract_settings(base_dir: Path, **overrides: object) -> JsonlExtractSettings:
    """Build a ``JsonlExtractSettings`` rooted at ``base_dir / "_settings"`` with ``table_name="widget"``.

    :param base_dir: Base directory for staging settings subpaths.
    :type base_dir: Path
    :param overrides: Field overrides passed to ``JsonlExtractSettings``.
    :type overrides: object
    :return: Instantiated ``JsonlExtractSettings``.
    :rtype: JsonlExtractSettings
    """
    defaults: dict[str, object] = {
        "dataset_name": "test_dataset",
        "table_name": "widget",
        "buffer_size": 10,
    }
    defaults.update(overrides)
    return make_cts_settings(JsonlExtractSettings, base_dir / "_settings", **defaults)


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


def run_validator(validator: Callable[[list[dict[str, Any]]], Any], item: dict[str, Any]) -> list[DataItemWithMeta]:
    """Run ``validator`` on a single-item page and return the tagged results.

    :param validator: Page router callable.
    :type validator: Callable[[list[dict[str, Any]]], Any]
    :param item: Line record dictionary.
    :type item: dict[str, Any]
    :return: Yielded ``DataItemWithMeta`` pages.
    :rtype: list[DataItemWithMeta]
    """
    return list(validator([item]))


def make_router(
    model: type[BaseModel],
    *,
    buffer_size: int = 10,
    use_pyarrow: bool = True,
) -> Callable[[list[dict[str, Any]]], Any]:
    """Build a page router for ``widget`` backed by ``model.model_validate``.

    :param model: Pydantic model class validating each record.
    :type model: type[BaseModel]
    :param buffer_size: Maximum rows per flushed page.
    :type buffer_size: int
    :param use_pyarrow: Whether ``make_page_router`` emits Arrow tables for ``_rejected``.
    :type use_pyarrow: bool
    :return: Configured page router callable.
    :rtype: Callable[[list[dict[str, Any]]], Any]
    """

    def _resolve_error(record: dict[str, Any]) -> str | None:
        try:
            model.model_validate(record)
        except ValidationError as error:
            return json.dumps(error.errors(), default=str)
        return None

    return make_page_router(
        "widget",
        buffer_size,
        _resolve_error,
        use_pyarrow=use_pyarrow,
    )


def tagged_pages(results: list[DataItemWithMeta]) -> list[tuple[str, int]]:
    """Return ``(table_name, row_count)`` pairs for tagged pages.

    :param results: Yielded ``DataItemWithMeta`` pages.
    :type results: list[DataItemWithMeta]
    :return: List of ``(table_name, row_count)`` pairs.
    :rtype: list[tuple[str, int]]
    """
    return [(r.meta.table_name, len(r.data)) for r in results]


def _split_widget_tags(row: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    """Filter zero-count rows and fan out ``tags`` into ``widget_tag`` rows.

    :param row: Input JSON record.
    :type row: dict[str, Any]
    :return: Table-keyed output rows.
    :rtype: dict[str, list[dict[str, Any]]]
    """
    if row["count"] == 0:
        return {}
    return {
        "widget": [{"widget_id": row["widget_id"], "count": row["count"]}],
        "widget_tag": [{"widget_id": row["widget_id"], "tag": tag} for tag in row["tags"]],
    }


def test_route_jsonl_lines_pass_routes_valid_and_invalid_rows(tmp_path: Path) -> None:
    """Valid lines route to the table and parse failures route to _rejected."""
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
    assert rejected[0]["line_no"] == EXPECTED_REJECTED_LINE_NO
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


def test_route_jsonl_lines_pass_file_items_any_backend(fs_backend: FsBackend, tmp_path: Path) -> None:
    """Routing works across all backends and decompresses .gz files."""
    fs_backend.put_all(BACKEND_FILES)

    pages = list(route_jsonl_lines(iter(fs_backend.file_items()), make_extract_settings(tmp_path)))

    by_table = {page.meta.table_name: page.data for page in pages}
    assert by_table["widget"] == [{"widget_id": "caf\u00e9"}, {"widget_id": "b"}, {"widget_id": "c"}]
    assert isinstance(by_table["_rejected"], pa.Table)
    assert [(r["table_name"], r["source_file"], r["line_no"]) for r in by_table["_rejected"].to_pylist()] == [
        ("widget", "a.jsonl", 3),
    ]


def test_route_jsonl_lines_pass_applies_table_transform_and_schema_map(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
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
    mod_name = register_temp_module(
        monkeypatch,
        "_test_route_jsonl_lines_transforms",
        TABLE_TRANSFORMS={"widget": _split_widget_tags},
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


def test_make_page_router_pass_valid_record_routed_to_main_table(widget_model: type[BaseModel]) -> None:
    """A record that passes validation routes to the main table."""
    validator = make_router(widget_model)
    item = make_item(record={"widget_id": "a", "count": 5}, raw_record='{"widget_id": "a", "count": 5}')

    results = run_validator(validator, item)

    assert len(results) == 1
    result = results[0]
    assert isinstance(result.meta, TableNameMeta)
    assert result.meta.table_name == "widget"
    assert result.data == [{"widget_id": "a", "count": 5}]


def test_make_page_router_fail_json_parse_error_routed_to_rejected_without_model_validation(
    widget_model: type[BaseModel],
) -> None:
    """A record with a parse_error routes to _rejected without calling the model."""
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
        pytest.param({"count": 5}, id="missing-required-widget-id"),
        pytest.param({"widget_id": "a", "count": -1}, id="count-below-minimum-boundary"),
        pytest.param({"widget_id": "a", "count": "not-a-number"}, id="count-not-coercible"),
        pytest.param({"widget_id": "", "count": 1}, id="widget-id-below-min-length-boundary"),
    ],
)
def test_make_page_router_fail_pydantic_validation_error_routed_to_rejected(
    widget_model: type[BaseModel],
    record: dict[str, Any],
) -> None:
    """A record failing model validation routes to _rejected with structured error detail."""
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
    widget_model: type[BaseModel],
) -> None:
    """A mixed batch routes valid and invalid records to their respective tables."""
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
    widget_model: type[BaseModel],
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
    widget_model: type[BaseModel],
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
    widget_model: type[BaseModel],
) -> None:
    """When use_pyarrow is False, make_page_router yields list[dict] for valid, rejected, and preprocessor-rejected items."""
    validator = make_router(widget_model, use_pyarrow=False)
    items = [
        make_item(record={"widget_id": "a", "count": 1}, raw_record='{"widget_id": "a", "count": 1}', line_no=1),
        make_item(record=None, raw_record="{bad", parse_error="boom", line_no=EXPECTED_REJECTED_LINE_NO),
    ]

    results = list(validator(items))
    by_table = {r.meta.table_name: r.data for r in results}

    assert isinstance(by_table["widget"], list)
    assert by_table["widget"] == [{"widget_id": "a", "count": 1}]
    assert isinstance(by_table["_rejected"], list)
    assert by_table["_rejected"][0]["line_no"] == EXPECTED_REJECTED_LINE_NO

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
