"""Buffer classes for accumulating rows per table prior to yielding them to dlt.

dlt loads most efficiently when transformers and resources yield pages of rows
rather than individual items. These buffers accumulate parsed rows per table and
hand back full pages when a table's buffer reaches its size limit.
"""

from collections import defaultdict
from collections.abc import Generator
from logging import Logger, getLogger
from typing import Any

import dlt
from dlt.extract.items import DataItemWithMeta

logger: Logger = getLogger(__name__)

DEFAULT_BUFFER_MAX_ITEMS = 1000


class BufferCore:
    """Accumulates rows per table and yields them as pages to dlt."""

    def __init__(self, table_names: list[str] | None = None, max_items: int = DEFAULT_BUFFER_MAX_ITEMS) -> None:
        """Initialize the buffer with optional table names and a per-table page size."""
        self.table_names = table_names
        self.max_items = max_items
        if table_names:
            self.buffers: dict[str, list[Any]] = {name: [] for name in table_names}
        else:
            self.buffers = defaultdict(list)

    def _add_items(self, items_to_add: dict[str, list[Any]]) -> Generator[DataItemWithMeta, Any]:
        """Add rows to per-table buffers. Yield a page for any table that reaches max_items."""
        for table_name, contents in items_to_add.items():
            if not contents:
                continue
            self.buffers[table_name].extend(contents)
            if len(self.buffers[table_name]) >= self.max_items:
                yield dlt.mark.with_table_name(self.buffers[table_name], table_name)
                self.buffers[table_name] = []

    def flush(self) -> Generator[DataItemWithMeta, Any]:
        """Yield any remaining buffered rows as final pages and empty the buffers."""
        for table_name, rows in self.buffers.items():
            if rows:
                yield dlt.mark.with_table_name(rows, table_name)
                self.buffers[table_name] = []


class DictBuffer(BufferCore):
    """A buffer for parsed results keyed by table name, e.g. from a parse_fn returning dict[str, rows]."""

    def add_items(self, items_to_add: dict[str, list[Any]]) -> Generator[DataItemWithMeta, Any]:
        """Add rows for one or more tables. Yield a page per table that reaches max_items."""
        yield from super()._add_items(items_to_add)


class ListBuffer(BufferCore):
    """A single-table buffer for routing one row at a time, e.g. validation results."""

    def __init__(self, table_name: str, max_items: int = DEFAULT_BUFFER_MAX_ITEMS) -> None:
        """Initialize the buffer for a single table."""
        super().__init__(table_names=[table_name], max_items=max_items)
        self.table_name = table_name

    def add_item(self, item_to_add: Any) -> Generator[DataItemWithMeta, Any]:  # noqa: ANN401
        """Add one row. Yield a page when the buffer reaches max_items."""
        yield from super()._add_items({self.table_name: [item_to_add]})
