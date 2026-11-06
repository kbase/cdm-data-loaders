"""Tests for the page buffer in cdm_data_loaders.utils.buffer."""

from collections.abc import Iterator
from typing import Any, Final

import dlt
import pyarrow as pa
import pytest
from pydantic import ValidationError

import cdm_data_loaders.utils.buffer as buffer_module
from cdm_data_loaders.core.fields import REJECTED_TABLE_NAME
from cdm_data_loaders.core.registry import ArrowSchemaRegistry
from cdm_data_loaders.utils.buffer import (
    DEFAULT_BUFFER_MAX_ITEMS,
    REJECTED_PYARROW_SCHEMA,
    DictBuffer,
)

PEOPLE = "people"
EMAILS = "emails"
PEOPLE_SCHEMA: Final[pa.Schema] = pa.schema([pa.field("name", pa.string())])
EMAILS_SCHEMA: Final[pa.Schema] = pa.schema([pa.field("n", pa.int64())])

SCHEMA_MAP: Final[ArrowSchemaRegistry] = ArrowSchemaRegistry(
    registry={
        PEOPLE: PEOPLE_SCHEMA,
        EMAILS: EMAILS_SCHEMA,
        "widget": pa.schema([pa.field("n", pa.int64())]),
    }
)
PEOPLE_SCHEMA_MAP: Final[ArrowSchemaRegistry] = ArrowSchemaRegistry(registry={PEOPLE: PEOPLE_SCHEMA})


def name_row(n: int) -> dict[str, Any]:
    """Build a row dict matching the people schema."""
    return {"name": str(n)}


def number_row(n: int) -> dict[str, Any]:
    """Build a row dict matching the emails schema."""
    return {"n": n}


def tagged(pages: list[Any]) -> list[tuple[str, list[Any]]]:
    """Unwrap yielded pages into (table_name, rows) pairs."""
    expected_schemas = {REJECTED_TABLE_NAME: REJECTED_PYARROW_SCHEMA, **SCHEMA_MAP.registry}
    for page in pages:
        if isinstance(page.data, pa.Table):
            assert page.data.schema == expected_schemas[page.meta.table_name]

    return [
        (page.meta.table_name, page.data.to_pylist() if isinstance(page.data, pa.Table) else page.data)
        for page in pages
    ]


def arrow_tagged(pages: list[Any]) -> list[tuple[str, pa.Table]]:
    """Unwrap yielded pages into (table_name, pa.Table) pairs."""
    return [(page.meta.table_name, page.data) for page in pages]


def _make_buffer(
    *,
    max_items: int = DEFAULT_BUFFER_MAX_ITEMS,
    schema_map: ArrowSchemaRegistry | None = None,
) -> DictBuffer:
    """Instantiate DictBuffer with an optional custom schema_map."""
    if schema_map is None:
        return DictBuffer(max_items=max_items)
    return DictBuffer(max_items=max_items, schema_map=schema_map)


# public API and object instantiation


def test_buffer_module_pass_exposes_single_dict_buffer_api() -> None:
    """cdm_data_loaders.utils.buffer exposes DictBuffer and removes separate Arrow* buffer classes/factories."""
    assert hasattr(buffer_module, "DictBuffer")
    assert not hasattr(buffer_module, "ArrowDictBuffer")
    assert not hasattr(buffer_module, "ArrowBufferCore")
    assert not hasattr(buffer_module, "create_dict_buffer")


def test_dict_buffer_pass_defaults_include_rejected_schema() -> None:
    """A basic DictBuffer requires no arguments, defaults max_items, and defaults schema_map to None."""
    buffer = DictBuffer()
    assert buffer.max_items == DEFAULT_BUFFER_MAX_ITEMS
    assert buffer.schema_map is None


def test_dict_buffer_pass_custom_schema_map_adds_arrow_schemas() -> None:
    """DictBuffer stores an ArrowSchemaRegistry in schema_map."""
    max_items = 5
    registry = ArrowSchemaRegistry(registry={PEOPLE: PEOPLE_SCHEMA})
    buffer = DictBuffer(
        max_items=max_items,
        schema_map=registry,
    )
    assert buffer.max_items == max_items
    assert buffer.schema_map == registry


@pytest.mark.parametrize(
    "schema_map",
    [None, PEOPLE_SCHEMA_MAP],
    ids=["default", "custom_schema_map"],
)
@pytest.mark.parametrize("max_items", [0, -1], ids=["zero", "negative"])
def test_dict_buffer_fail_non_positive_max_items_raises_error(
    max_items: int,
    schema_map: ArrowSchemaRegistry | None,
) -> None:
    """A non-positive max_items raises a pydantic ValidationError."""
    with pytest.raises(ValidationError, match="Input should be greater than 0"):
        _make_buffer(max_items=max_items, schema_map=schema_map)


def test_dict_buffer_fail_unrecognised_args_throw_error() -> None:
    """Unrecognised arguments raise a pydantic ValidationError."""
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        DictBuffer(_buffers="some buffery thing")  # pyright: ignore[reportCallIssue]


@pytest.mark.parametrize(
    ("args", "err_msg"),
    [
        ({"": PEOPLE_SCHEMA}, "String should have at least 1 character"),
        ({"foo": "bar"}, "Input should be an instance of Schema"),
        ({"pop": PEOPLE_SCHEMA, "pip": {"this": "that"}}, "Input should be an instance of Schema"),
    ],
)
def test_dict_buffer_fail_invalid_schema_map(args: Any, err_msg: str) -> None:  # noqa: ANN401
    """DictBuffer raises a ValidationError on invalid schema_map."""
    with pytest.raises(ValidationError, match=err_msg):
        DictBuffer(schema_map=ArrowSchemaRegistry(registry=args))


def test_dict_buffer_pass_accepts_empty_schemas() -> None:
    """DictBuffer accepts an empty pyarrow.Schema in schema_map."""
    registry = ArrowSchemaRegistry(registry={"pip": pa.schema([])})
    buffer = DictBuffer(schema_map=registry)
    assert isinstance(buffer, DictBuffer)
    assert buffer.schema_map == registry
    assert buffer.schema_map.registry["pip"] == pa.schema([])


# buffer functions


@pytest.mark.parametrize("schema_map", [None, SCHEMA_MAP], ids=["unschematized", "with_schema_map"])
def test_dict_buffer_add_item_pass_yields_page_at_max_items(
    schema_map: ArrowSchemaRegistry | None,
) -> None:
    """A single item addition yields a page when max_items is reached."""
    buffer = _make_buffer(max_items=3, schema_map=schema_map)

    assert list(buffer.add_item(PEOPLE, name_row(1))) == []
    assert list(buffer.add_item(PEOPLE, name_row(2))) == []

    pages = list(buffer.add_item(PEOPLE, name_row(3)))
    assert len(pages) == 1
    assert tagged(pages) == [(PEOPLE, [name_row(1), name_row(2), name_row(3)])]

    assert list(buffer.flush()) == []


@pytest.mark.parametrize("schema_map", [None, SCHEMA_MAP], ids=["unschematized", "with_schema_map"])
def test_dict_buffer_core_item_pass_routes_to_different_tables(
    schema_map: ArrowSchemaRegistry | None,
) -> None:
    """add_item correctly separates rows into different table buffers."""
    buffer = _make_buffer(max_items=2, schema_map=schema_map)

    assert list(buffer.add_item(PEOPLE, name_row(1))) == []
    assert list(buffer.add_item(EMAILS, number_row(1))) == []

    pages_people = list(buffer.add_item(PEOPLE, name_row(2)))
    assert tagged(pages_people) == [(PEOPLE, [name_row(1), name_row(2)])]

    pages_emails = list(buffer.add_item(EMAILS, number_row(2)))
    assert tagged(pages_emails) == [(EMAILS, [number_row(1), number_row(2)])]


def test_dict_buffer_pass_flush_yields_arrow_tables_for_schematized_tables() -> None:
    """flush() yields pa.Tables for tables with Arrow schemas and empties the buffers."""
    buffer = _make_buffer(max_items=10, schema_map=SCHEMA_MAP)
    list(buffer.add_items({PEOPLE: [name_row(1)], EMAILS: [number_row(2)]}))

    people_page, email_page = arrow_tagged(list(buffer.flush()))
    assert people_page == (
        PEOPLE,
        pa.Table.from_pylist([name_row(1)], schema=SCHEMA_MAP.registry[PEOPLE]),
    )
    assert email_page == (
        EMAILS,
        pa.Table.from_pylist([number_row(2)], schema=SCHEMA_MAP.registry[EMAILS]),
    )
    assert list(buffer.flush()) == []


@pytest.mark.parametrize("schema_map", [None, SCHEMA_MAP], ids=["unschematized", "with_schema_map"])
def test_dict_buffer_pass_rows_accumulate_until_max_items(
    schema_map: ArrowSchemaRegistry | None,
) -> None:
    """No pages are yielded while every table's buffer is below max_items."""
    buffer = _make_buffer(max_items=5, schema_map=schema_map)

    assert list(buffer.add_items({PEOPLE: [name_row(1), name_row(2)], EMAILS: [number_row(3)]})) == []


@pytest.mark.parametrize("schema_map", [None, SCHEMA_MAP], ids=["unschematized", "with_schema_map"])
def test_dict_buffer_pass_full_table_yields_page_and_empties(
    schema_map: ArrowSchemaRegistry | None,
) -> None:
    """A table reaching max_items yields its rows as one page and the buffer resets."""
    buffer = _make_buffer(max_items=2, schema_map=schema_map)

    first = tagged(list(buffer.add_items({PEOPLE: [name_row(1), name_row(2)]})))
    assert first == [(PEOPLE, [name_row(1), name_row(2)])]

    assert list(buffer.add_items({PEOPLE: [name_row(3)]})) == []

    second = tagged(list(buffer.add_items({PEOPLE: [name_row(4)]})))
    assert second == [(PEOPLE, [name_row(3), name_row(4)])]
    assert list(buffer.add_items({PEOPLE: [name_row(5)]})) == []


@pytest.mark.parametrize("schema_map", [None, SCHEMA_MAP], ids=["unschematized", "with_schema_map"])
def test_dict_buffer_pass_flush_yields_remaining_rows(
    schema_map: ArrowSchemaRegistry | None,
) -> None:
    """flush() yields the rows still held per table and empties the buffers."""
    buffer = _make_buffer(max_items=10, schema_map=schema_map)
    list(buffer.add_items({PEOPLE: [name_row(1)], EMAILS: [number_row(2)]}))

    assert tagged(list(buffer.flush())) == [(PEOPLE, [name_row(1)]), (EMAILS, [number_row(2)])]
    assert list(buffer.flush()) == []


@pytest.mark.parametrize("schema_map", [None, SCHEMA_MAP], ids=["unschematized", "with_schema_map"])
def test_dict_buffer_pass_empty_table_rows_are_ignored(
    schema_map: ArrowSchemaRegistry | None,
) -> None:
    """Empty row lists contribute nothing and yield no pages."""
    buffer = _make_buffer(max_items=1, schema_map=schema_map)

    assert list(buffer.add_items({PEOPLE: []})) == []
    assert list(buffer.flush()) == []


def test_dict_buffer_pass_unknown_table_is_emitted_as_is() -> None:
    """A table without an Arrow schema (whether or not other tables have schemas) is emitted as a list of dicts."""
    buffer = _make_buffer(max_items=10, schema_map=PEOPLE_SCHEMA_MAP)

    assert list(buffer.add_items({PEOPLE: [name_row(1)], "surprise": [{"wtf?": "boo!"}]})) == []
    pages = {page.meta.table_name: page.data for page in buffer.flush()}

    assert isinstance(pages[PEOPLE], pa.Table)
    assert pages[PEOPLE] == pa.Table.from_pylist([name_row(1)], schema=PEOPLE_SCHEMA)
    assert pages["surprise"] == [{"wtf?": "boo!"}]


def test_dict_buffer_pass_rejected_table_always_uses_pyarrow_schema() -> None:
    """Even a basic DictBuffer without custom schemas emits _rejected pages as pa.Table with REJECTED_PYARROW_SCHEMA."""
    buffer = DictBuffer(max_items=10)
    rejected_row = {
        "table_name": PEOPLE,
        "source_file": "people.csv",
        "line_no": None,
        "raw_record": None,
        "error_detail": "failed validation",
    }

    assert list(buffer.add_item(PEOPLE, name_row(1))) == []
    assert list(buffer.add_item(REJECTED_TABLE_NAME, rejected_row)) == []
    pages = {page.meta.table_name: page.data for page in buffer.flush()}

    assert pages[PEOPLE] == [name_row(1)]
    assert isinstance(pages[REJECTED_TABLE_NAME], pa.Table)
    assert pages[REJECTED_TABLE_NAME].schema == REJECTED_PYARROW_SCHEMA
    assert pages[REJECTED_TABLE_NAME].to_pylist() == [rejected_row]


@pytest.mark.parametrize("schema_map", [None, SCHEMA_MAP], ids=["unschematized", "with_schema_map"])
def test_dict_buffer_pass_multiple_tables_page_independently(
    schema_map: ArrowSchemaRegistry | None,
) -> None:
    """Each table pages on its own count; a full table does not flush other tables."""
    buffer = _make_buffer(max_items=2, schema_map=schema_map)
    first = tagged(list(buffer.add_items({PEOPLE: [name_row(1), name_row(2)], EMAILS: [number_row(3)]})))
    assert first == [(PEOPLE, [name_row(1), name_row(2)])]

    second = tagged(list(buffer.add_items({PEOPLE: [name_row(4)], EMAILS: [number_row(5)]})))
    assert second == [(EMAILS, [number_row(3), number_row(5)])]

    assert tagged(list(buffer.flush())) == [(PEOPLE, [name_row(4)])]
    assert list(buffer.flush()) == []


@pytest.mark.parametrize("schema_map", [None, PEOPLE_SCHEMA_MAP], ids=["unschematized", "with_schema_map"])
def test_dict_buffer_pass_buffer_scope_is_call_local(
    schema_map: ArrowSchemaRegistry | None,
) -> None:
    """A dict buffer created inside one transformer call yields every row added to it."""
    max_items = 3
    buffer = _make_buffer(max_items=max_items, schema_map=schema_map)
    emitted: list[Any] = []
    for n in range(10):
        emitted.extend(buffer.add_item(PEOPLE, name_row(n)))
    emitted.extend(buffer.flush())

    values = [page.data for page in emitted]
    if schema_map is not None:
        assert [value for table in values for value in table.column("name").to_pylist()] == [str(n) for n in range(10)]
        assert [table.num_rows for table in values] == [3, 3, 3, 1]
    else:
        assert [value for table in values for value in table] == [{"name": str(n)} for n in range(10)]
        assert [len(row) for row in values] == [3, 3, 3, 1]


class _NaiveSharedBuffer:
    """Recreates the unsafe pattern rejected during research.

    One mutable buffer instance is shared across every transformer call, with the
    check-then-append-then-replace sequence split across call boundaries. When dlt
    runs a parallelized transformer, calls execute concurrently in worker threads:
    two calls interleave their read of `self.rows` and the later append replaces the
    list captured by the earlier call, silently dropping that call's row.
    """

    def __init__(self, max_items: int) -> None:
        self.rows: list[dict[str, int]] = []
        self.max_items = max_items

    def add(self, item: dict[str, int]) -> list[dict[str, int]]:
        if len(self.rows) >= self.max_items:
            page = self.rows
            self.rows = []
            return page
        self.rows.append(item)
        return []


def _run_naive_shared_buffer_transformer(total: int, max_items: int) -> list[int]:
    """Run a parallelized transformer over one shared _NaiveSharedBuffer. Return emitted values."""
    buffer = _NaiveSharedBuffer(max_items)

    @dlt.transformer(parallelized=True)
    def naive(item: int) -> Iterator[int]:
        return iter(row["v"] for row in buffer.add({"v": item}))

    @dlt.resource
    def source() -> Iterator[int]:
        yield from range(total)

    return [o["v"] for o in (source() | naive) if isinstance(o, dict)]


def _run_page_per_call_transformer(total: int, max_items: int, chunk_size: int) -> list[int]:
    """Run the shipped page-per-call pattern under a parallelized transformer. Return emitted values."""

    @dlt.transformer(parallelized=True)
    def safe(page: list[int]) -> Iterator[dict[str, int]]:
        buffer = DictBuffer(max_items=max_items)
        for item in page:
            yield from buffer.add_item("safe_table", {"v": item})  # pyright: ignore[reportReturnType]
        yield from buffer.flush()  # pyright: ignore[reportReturnType]

    @dlt.resource
    def source() -> Iterator[list[int]]:
        for start in range(0, total, chunk_size):
            yield list(range(start, min(start + chunk_size, total)))

    return [o["v"] for o in (source() | safe) if isinstance(o, dict)]


def test_naive_shared_buffer_fail_loses_rows_under_parallelized_transformer() -> None:
    """Recreates the research race: a buffer shared across parallelized transformer calls loses rows.

    dlt submits each transformer call to a thread pool; the calls interleave their
    check-then-append-then-replace sequences on the shared buffer, so rows vanish.
    """
    total = 50
    values = _run_naive_shared_buffer_transformer(total=total, max_items=5)

    assert len(values) < total, "race did not trigger; increase total or retry"
    assert sorted(values) != list(range(total))


def test_dict_buffer_pass_loses_nothing_under_parallelized_transformer() -> None:
    """The shipped pattern buffers inside one transformer call, so no rows are lost.

    The buffer is created per call and dies with the call: no state is shared across
    the worker threads dlt uses for a parallelized transformer.
    """
    total = 50
    values = _run_page_per_call_transformer(total=total, max_items=5, chunk_size=7)

    assert sorted(values) == list(range(total))
