"""Buffer classes for accumulating rows per table prior to yielding them to dlt.

dlt loads most efficiently when transformers and resources yield pages of rows
rather than individual items. These buffers accumulate parsed rows per table and
hand back full pages when a table's buffer reaches its size limit.
"""

from collections import defaultdict
from collections.abc import Generator
from typing import Annotated, Any, Self

import dlt
import pandas as pd  # noqa: F401 - import on main thread before dlt worker threads yield arrow tables
import pyarrow as pa
from dlt.extract.items import DataItemWithMeta
from pydantic import BaseModel, ConfigDict, Field, PositiveInt, PrivateAttr, model_validator

from cdm_data_loaders.core.fields import NonEmptyStr

DEFAULT_BUFFER_MAX_ITEMS = 1000


type Buffers = Annotated[dict[NonEmptyStr, list[Any]], Field(description="Buffer implementation")]

type MaxItems = Annotated[PositiveInt, Field(description="Maximum number of items per buffer")]

type TableNames = Annotated[list[NonEmptyStr], Field(description="Names of the tables to be stored in the buffer")]

type TableToSchemaMap = Annotated[
    dict[NonEmptyStr, pa.Schema], Field(min_length=1, description="Mapping of table names to schemas")
]


class BufferCore(BaseModel):
    """Accumulates rows per table and yields them as pages to dlt."""

    model_config = ConfigDict(extra="forbid")

    _table_names: TableNames = PrivateAttr(default_factory=list)
    _buffers: Buffers = PrivateAttr(default_factory=dict)

    max_items: MaxItems = Field(default=DEFAULT_BUFFER_MAX_ITEMS)

    @model_validator(mode="after")
    def set_up_buffers(self) -> Self:
        """Set up the buffers after successfully instantiating the Buffer instance."""
        self._init_buffers()
        return self

    def _init_buffers(self) -> None:
        """Initialise the buffers."""
        if self._table_names:
            self._buffers: dict[str, list[Any]] = {name: [] for name in self._table_names}
        else:
            self._buffers = defaultdict(list)

    def _validate_table_name(self, table_name: str) -> None:
        """Validate ``table_name`` before adding items to its buffer."""

    def _make_page(self, table_name: str, rows: list[Any]) -> Any:  # noqa: ANN401, ARG002
        """Construct the page payload for ``table_name`` from buffered ``rows``."""
        return rows

    def _flush_if_full(self, table_name: str) -> Generator[DataItemWithMeta, Any]:
        """Yield and clear the buffer for ``table_name`` if it has reached ``max_items``."""
        if len(self._buffers[table_name]) >= self.max_items:
            yield dlt.mark.with_table_name(self._make_page(table_name, self._buffers[table_name]), table_name)
            self._buffers[table_name] = []

    def _add_item(self, table_name: str, item_to_add: Any) -> Generator[DataItemWithMeta, Any]:  # noqa: ANN401
        """Add an item to a table. Yield a page if the table reaches max_items.

        :param table_name: name of the table to add the item to
        :type  table_name: str
        :param item_to_add: the item
        :type  item_to_add: Any
        :yield: generator that yields collected rows to the appropriate dlt table
        :rtype: Generator[DataItemWithMeta, Any]
        """
        self._validate_table_name(table_name)
        self._buffers[table_name].append(item_to_add)
        yield from self._flush_if_full(table_name)

    def _add_items(self, items_to_add: dict[str, list[Any]]) -> Generator[DataItemWithMeta, Any]:
        """Add items to per-table buffers. Yield a page for any table that reaches max_items.

        :param items_to_add: dictionary of lists of items to add to per-table buffers, indexed by table name
        :type  items_to_add: dict[str, list[Any]]
        :yield: generator that yields collected rows to the appropriate dlt table(s)
        :rtype: Generator[DataItemWithMeta, Any]
        """
        for table_name, contents in items_to_add.items():
            if not contents:
                continue
            self._validate_table_name(table_name)
            self._buffers[table_name].extend(contents)
            yield from self._flush_if_full(table_name)

    def flush(self) -> Generator[DataItemWithMeta, Any]:
        """Yield any remaining buffered rows as final pages and empty the buffers."""
        for table_name, rows in self._buffers.items():
            if rows:
                yield dlt.mark.with_table_name(self._make_page(table_name, rows), table_name)
                self._buffers[table_name] = []


class DictBuffer(BufferCore):
    """A buffer for parsed results keyed by table name, e.g. from a parse_fn returning dict[str, rows]."""

    def add_item(self, table_name: str, item_to_add: Any) -> Generator[DataItemWithMeta, Any]:  # noqa: ANN401
        """Add an item to a table. Yield a page if the table reaches max_items.

        :param table_name: name of the table to add the item to
        :type  table_name: str
        :param item_to_add: the item
        :type  item_to_add: Any
        :yield: generator that yields collected rows to the appropriate dlt table
        :rtype: Generator[DataItemWithMeta, Any]
        """
        yield from super()._add_item(table_name, item_to_add)

    def add_items(self, items_to_add: dict[str, list[Any]]) -> Generator[DataItemWithMeta, Any]:
        """Add items to per-table buffers. Yield a page for any table that reaches max_items.

        :param items_to_add: dictionary of lists of items to add to per-table buffers, indexed by table name
        :type  items_to_add: dict[str, list[Any]]
        :yield: generator that yields collected rows to the appropriate dlt table(s)
        :rtype: Generator[DataItemWithMeta, Any]
        """
        yield from super()._add_items(items_to_add)


class SingleTableBufferMixin:
    """Shared single-table `add_item` implementation for `ListBuffer` and `ArrowListBuffer`."""

    @property
    def _single_table_name(self) -> str:
        """Return the single target table name for this buffer."""
        raise NotImplementedError

    def add_item(self, item_to_add: Any) -> Generator[DataItemWithMeta, Any]:  # noqa: ANN401
        """Add an item to the table. Yield a page if the table reaches max_items.

        :param item_to_add: the item
        :type  item_to_add: Any
        :yield: generator that yields collected rows to the appropriate dlt table
        :rtype: Generator[DataItemWithMeta, Any]
        """
        yield from self._add_item(self._single_table_name, item_to_add)  # type: ignore[attr-defined]


class ListBuffer(SingleTableBufferMixin, BufferCore):
    """A single-table buffer for routing one row at a time, e.g. validation results."""

    table_name: Annotated[NonEmptyStr, Field(..., description="Table to save the data to")]

    @property
    def _single_table_name(self) -> str:
        return self.table_name


class ArrowBufferCore(BufferCore):
    """Accumulates rows per table and yields them as pyarrow tables to dlt."""

    model_config = ConfigDict(arbitrary_types_allowed=True, extra="forbid")

    table_to_schema_map: TableToSchemaMap

    def _init_buffers(self) -> None:
        """Initialise the buffers."""
        if not self.table_to_schema_map:
            err_msg = "Must supply table-to-schema mappings to use ArrowBuffers"
            raise ValueError(err_msg)

        self._buffers: dict[str, list[Any]] = {name: [] for name in self.table_to_schema_map}

    def _validate_table_name(self, table_name: str) -> None:
        """Ensure that a schema was supplied for ``table_name``."""
        if table_name not in self._buffers:
            err_msg = f"Cannot add data for {table_name} table: no schema supplied"
            raise ValueError(err_msg)

    def _make_page(self, table_name: str, rows: list[Any]) -> pa.Table:
        """Convert buffered rows for ``table_name`` into a PyArrow table."""
        return pa.Table.from_pylist(rows, schema=self.table_to_schema_map[table_name])


class ArrowDictBuffer(ArrowBufferCore, DictBuffer):
    """A pyarrow buffer for parsed results keyed by table name. Results are yielded as pyarrow tables."""


class ArrowListBuffer(SingleTableBufferMixin, ArrowBufferCore):
    """A single-table pyarrow buffer for routing one row at a time. Results are yielded as pyarrow tables."""

    _table_name: NonEmptyStr = PrivateAttr()
    table_to_schema_map: TableToSchemaMap = Field(min_length=1, max_length=1)

    @model_validator(mode="after")
    def set_up_table_name(self) -> Self:
        """Set up the buffers after successfully instantiating the Buffer instance."""
        if len(self.table_to_schema_map) != 1:
            err_msg = "ArrowListBuffers can only be used for a single table and schema"
            raise ValueError(err_msg)
        self._table_name = next(iter(self.table_to_schema_map.keys()))
        return self

    @property
    def _single_table_name(self) -> str:
        return self._table_name
