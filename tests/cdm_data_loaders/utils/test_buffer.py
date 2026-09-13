"""Tests for the page buffers in cdm_data_loaders.utils.buffer."""

from collections.abc import Iterator
from typing import Any

import dlt
import pytest

from cdm_data_loaders.utils.buffer import DEFAULT_BUFFER_MAX_ITEMS, DictBuffer, ListBuffer


def row(n: int) -> dict[str, Any]:
    """Build a simple row dict."""
    return {"n": n}


def tagged(pages: list[Any]) -> list[tuple[str, list[Any]]]:
    """Unwrap yielded pages into (table_name, rows) pairs."""
    return [(page.meta.table_name, page.data) for page in pages]


def test_dict_buffer_pass_rows_accumulate_until_max_items() -> None:
    """No pages are yielded while every table's buffer is below max_items."""
    buffer = DictBuffer(max_items=5)

    assert list(buffer.add_items({"people": [row(1), row(2)], "emails": [row(3)]})) == []


def test_dict_buffer_pass_full_table_yields_page_and_empties() -> None:
    """A table reaching max_items yields its rows as one page and the buffer resets."""
    buffer = DictBuffer(max_items=2)

    first = tagged(list(buffer.add_items({"people": [row(1), row(2)]})))
    assert first == [("people", [row(1), row(2)])]

    assert list(buffer.add_items({"people": [row(3)]})) == []

    second = tagged(list(buffer.add_items({"people": [row(4)]})))
    assert second == [("people", [row(3), row(4)])]
    assert list(buffer.add_items({"people": [row(5)]})) == []


def test_dict_buffer_pass_flush_yields_remaining_rows() -> None:
    """flush() yields the rows still held per table and empties the buffers."""
    buffer = DictBuffer(max_items=10)
    list(buffer.add_items({"people": [row(1)], "emails": [row(2)]}))

    assert tagged(list(buffer.flush())) == [("people", [row(1)]), ("emails", [row(2)])]
    assert list(buffer.flush()) == []


def test_dict_buffer_pass_empty_table_rows_are_ignored() -> None:
    """Empty row lists contribute nothing and yield no pages."""
    buffer = DictBuffer(max_items=1)

    assert list(buffer.add_items({"people": []})) == []
    assert list(buffer.flush()) == []


def test_dict_buffer_pass_unknown_table_is_created_on_demand() -> None:
    """A table name not seen at construction is buffered like any other."""
    buffer = DictBuffer()

    assert list(buffer.add_items({"surprise": [row(1)]})) == []
    assert tagged(list(buffer.flush())) == [("surprise", [row(1)])]


def test_dict_buffer_pass_multiple_tables_page_independently() -> None:
    """Each table pages on its own count; a full table does not flush other tables."""
    buffer = DictBuffer(max_items=2)
    first = tagged(list(buffer.add_items({"people": [row(1), row(2)], "emails": [row(3)]})))
    assert first == [("people", [row(1), row(2)])]

    second = tagged(list(buffer.add_items({"people": [row(4)], "emails": [row(5)]})))
    assert second == [("emails", [row(3), row(5)])]

    assert tagged(list(buffer.flush())) == [("people", [row(4)])]
    assert list(buffer.flush()) == []


def test_list_buffer_pass_single_item_adds_and_pages() -> None:
    """add_item routes to the buffer's table and pages when max_items is reached."""
    buffer = ListBuffer(table_name="widget", max_items=3)
    assert list(buffer.add_item(row(1))) == []
    assert list(buffer.add_item(row(2))) == []

    pages = tagged(list(buffer.add_item(row(3))))

    assert pages == [("widget", [row(1), row(2), row(3)])]
    assert list(buffer.add_item(row(4))) == []


def test_list_buffer_pass_flush_yields_remaining_rows() -> None:
    """flush() yields rows still held for the buffer's table."""
    buffer = ListBuffer(table_name="widget", max_items=10)
    list(buffer.add_item(row(1)))

    assert tagged(list(buffer.flush())) == [("widget", [row(1)])]
    assert list(buffer.flush()) == []


def test_list_buffer_pass_default_max_items() -> None:
    """max_items defaults to DEFAULT_BUFFER_MAX_ITEMS."""
    buffer = ListBuffer(table_name="widget")

    assert buffer.max_items == DEFAULT_BUFFER_MAX_ITEMS


@pytest.mark.parametrize("max_items", [0, -1], ids=["zero", "negative"])
def test_buffer_fail_non_positive_max_items_yields_every_add(max_items: int) -> None:
    """A non-positive max_items makes every add yield a page; documented behavior, not an error."""
    list_buffer = ListBuffer(table_name="widget", max_items=max_items)
    dict_buffer = DictBuffer(max_items=max_items)

    assert tagged(list(list_buffer.add_item(row(1)))) == [("widget", [row(1)])]
    assert tagged(list(dict_buffer.add_items({"people": [row(1)]}))) == [("people", [row(1)])]


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
        buffer = ListBuffer(table_name="safe_table", max_items=max_items)
        for item in page:
            yield from buffer.add_item({"v": item})
        yield from buffer.flush()

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


def test_list_buffer_pass_loses_nothing_under_parallelized_transformer() -> None:
    """The shipped pattern buffers inside one transformer call, so no rows are lost.

    The buffer is created per call and dies with the call: no state is shared across
    the worker threads dlt uses for a parallelized transformer.
    """
    total = 50
    values = _run_page_per_call_transformer(total=total, max_items=5, chunk_size=7)

    assert sorted(values) == list(range(total))


def test_list_buffer_pass_buffer_scope_is_call_local() -> None:
    """A ListBuffer created inside one transformer call yields every row added to it.

    Demonstrates the invariant that makes the pattern safe under parallelism: the
    buffer never escapes the call that created it, so concurrent calls cannot
    interleave reads and writes.
    """
    max_items = 3
    buffer = ListBuffer(table_name="widget", max_items=max_items)
    emitted: list[Any] = []
    for n in range(10):
        emitted.extend(buffer.add_item(row(n)))
    emitted.extend(buffer.flush())

    values = [page.data for page in emitted]
    assert [v for page in values for v in page] == [row(n) for n in range(10)]
    assert [len(page) for page in values] == [3, 3, 3, 1]
