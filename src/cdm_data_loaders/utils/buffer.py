"""Buffer class for accumulating rows per table prior to yielding them to dlt.

dlt loads most efficiently when transformers and resources yield pages of rows
rather than individual items. ``DictBuffer`` accumulates parsed rows per table and
hands back full pages when a table's buffer reaches its size limit, converting pages
to ``pyarrow.Table`` instances for any table with a registered PyArrow schema
(including ``_rejected``, which is registered by default).
"""

import json
from collections import defaultdict
from collections.abc import Callable, Generator, Mapping
from typing import Annotated, Any, Final, Self

import dlt
import pandas as pd  # noqa: F401 - import on main thread before dlt worker threads yield arrow tables
import pyarrow as pa
from dlt.extract.items import DataItemWithMeta
from pydantic import BaseModel, ConfigDict, Field, PositiveInt, PrivateAttr

from cdm_data_loaders.core.fields import REJECTED_TABLE_NAME, NonEmptyStr
from cdm_data_loaders.core.registry import ArrowSchemaRegistry

DEFAULT_BUFFER_MAX_ITEMS = 1000
REJECTED_PYARROW_SCHEMA: Final[pa.Schema] = pa.schema(
    [
        pa.field("table_name", pa.string()),
        pa.field("source_file", pa.string()),
        pa.field("line_no", pa.int64()),
        pa.field("raw_record", pa.string()),
        pa.field("error_detail", pa.string()),
    ]
)

type Buffers = Annotated[dict[NonEmptyStr, list[Any]], Field(description="Buffer implementation")]

type MaxItems = Annotated[PositiveInt, Field(description="Maximum number of items per buffer")]


def make_rejected_row(
    table_name: str | None,
    error_detail: str,
    *,
    source_file: str | None = None,
    line_no: int | None = None,
    raw_record: str | None = None,
) -> dict[str, Any]:
    """Build a standardized row dictionary for the ``_rejected`` table.

    :param table_name: target table name associated with the rejected record
    :type table_name: str | None
    :param error_detail: human-readable error message
    :type error_detail: str
    :param source_file: relative path or identifier of the source file
    :type source_file: str | None
    :param line_no: 1-based line number within the source file, if applicable
    :type line_no: int | None
    :param raw_record: serialized raw record text, if applicable
    :type raw_record: str | None
    :return: dictionary matching ``REJECTED_PYARROW_SCHEMA``
    :rtype: dict[str, Any]
    """
    return {
        "table_name": table_name,
        "source_file": source_file,
        "line_no": line_no,
        "raw_record": raw_record,
        "error_detail": error_detail,
    }


def make_rejected_table_result(
    table_name: str,
    raw_record_obj: Mapping[str, Any],
    error_detail: str,
    *,
    source_file: str | None = None,
    line_no: int | None = None,
) -> dict[str, list[dict[str, Any]]]:
    """Build a ``{_rejected: [row]}`` result mapping for a record transform failure.

    :param table_name: source table name of the record that failed transformation
    :type table_name: str
    :param raw_record_obj: input record mapping to serialize into ``raw_record``
    :type raw_record_obj: Mapping[str, Any]
    :param error_detail: human-readable error message
    :type error_detail: str
    :param source_file: relative path or identifier of the source file
    :type source_file: str | None
    :param line_no: 1-based line number within the source file, if applicable
    :type line_no: int | None
    :return: single-row ``_rejected`` table result mapping
    :rtype: dict[str, list[dict[str, Any]]]
    """
    return {
        REJECTED_TABLE_NAME: [
            make_rejected_row(
                table_name,
                error_detail,
                source_file=source_file,
                line_no=line_no,
                raw_record=json.dumps(dict(raw_record_obj), default=str),
            )
        ]
    }


class DictBuffer(BaseModel):
    """Accumulates rows per table and yields them as pages to dlt.

    When ``use_pyarrow`` is ``True`` (the default), any table with a ``pyarrow.Schema`` in
    ``schema_map`` (as well as ``_rejected``, which uses ``REJECTED_PYARROW_SCHEMA``) is yielded
    as a ``pyarrow.Table``; otherwise, pages are yielded as ``list[dict]`` as-is.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True, extra="forbid")

    _buffers: Buffers = PrivateAttr(default_factory=lambda: defaultdict(list))

    max_items: MaxItems = Field(default=DEFAULT_BUFFER_MAX_ITEMS)
    schema_map: ArrowSchemaRegistry | None = Field(default=None)
    use_pyarrow: bool = Field(default=True)

    @classmethod
    def from_settings(cls, settings: Any) -> Self:  # noqa: ANN401
        """Construct a ``DictBuffer`` from pipeline settings.

        :param settings: pipeline settings with ``buffer_size`` and optional ``schema_map`` / ``use_pyarrow``
        :type settings: Any
        :return: configured ``DictBuffer`` instance
        :rtype: Self
        """
        return make_dict_buffer(settings, cls)  # type: ignore[return-value]

    def _make_page(self, table_name: str, rows: list[Any]) -> Any:  # noqa: ANN401
        """Construct the page payload for ``table_name`` from buffered ``rows``."""
        if not self.use_pyarrow:
            return rows
        if table_name == REJECTED_TABLE_NAME:
            return pa.Table.from_pylist(rows, schema=REJECTED_PYARROW_SCHEMA)
        if self.schema_map and table_name in self.schema_map.registry:
            return pa.Table.from_pylist(rows, schema=self.schema_map.registry[table_name])
        return rows

    def _flush_if_full(self, table_name: str) -> Generator[DataItemWithMeta, Any]:
        """Yield and clear the buffer for ``table_name`` if it has reached ``max_items``."""
        if len(self._buffers[table_name]) >= self.max_items:
            yield dlt.mark.with_table_name(self._make_page(table_name, self._buffers[table_name]), table_name)
            self._buffers[table_name] = []

    def flush(self) -> Generator[DataItemWithMeta, Any]:
        """Yield any remaining buffered rows as final pages and empty the buffers."""
        for table_name, rows in self._buffers.items():
            if rows:
                yield dlt.mark.with_table_name(self._make_page(table_name, rows), table_name)
                self._buffers[table_name] = []

    def add_item(self, table_name: str, item_to_add: Any) -> Generator[DataItemWithMeta, Any]:  # noqa: ANN401
        """Add an item to a table. Yield a page if the table reaches max_items.

        :param table_name: name of the table to add the item to
        :type  table_name: str
        :param item_to_add: the item
        :type  item_to_add: Any
        :yield: generator that yields collected rows to the appropriate dlt table
        :rtype: Generator[DataItemWithMeta, Any]
        """
        self._buffers[table_name].append(item_to_add)
        yield from self._flush_if_full(table_name)

    def add_items(self, items_to_add: dict[str, list[Any]]) -> Generator[DataItemWithMeta, Any]:
        """Add items to per-table buffers. Yield a page for any table that reaches max_items.

        :param items_to_add: dictionary of lists of items to add to per-table buffers, indexed by table name
        :type  items_to_add: dict[str, list[Any]]
        :yield: generator that yields collected rows to the appropriate dlt table(s)
        :rtype: Generator[DataItemWithMeta, Any]
        """
        for table_name, contents in items_to_add.items():
            if not contents:
                continue
            self._buffers[table_name].extend(contents)
            yield from self._flush_if_full(table_name)


def make_dict_buffer(
    settings: Any,  # noqa: ANN401
    buffer_cls: Callable[..., DictBuffer] = DictBuffer,
) -> DictBuffer:
    """Construct a ``DictBuffer`` from pipeline settings using ``buffer_cls``.

    :param settings: pipeline settings with ``buffer_size`` and optional ``schema_map`` / ``use_pyarrow``
    :type settings: Any
    :param buffer_cls: ``DictBuffer`` constructor or compatible factory
    :type buffer_cls: Callable[..., DictBuffer]
    :return: configured ``DictBuffer`` instance
    :rtype: DictBuffer
    """
    return buffer_cls(
        max_items=settings.buffer_size,
        schema_map=getattr(settings, "schema_map", None),
        use_pyarrow=getattr(settings, "use_pyarrow", True),
    )
