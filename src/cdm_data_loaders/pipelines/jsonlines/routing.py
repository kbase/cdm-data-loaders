"""Page validation and table routing for JSONLines ingestion pipelines."""

from collections.abc import Callable, Generator, Iterable, Iterator
from dataclasses import dataclass
from typing import Any, Final, TypedDict, Unpack

import dlt
import pyarrow as pa
from dlt.common.storages.fsspec_filesystem import FileItemDict
from dlt.common.typing import TDataItems
from pydantic import BaseModel

from cdm_data_loaders.coercion.json_to_arrow import coerce_json_record_for_pyarrow
from cdm_data_loaders.core.buffer import DictBuffer, make_rejected_row
from cdm_data_loaders.core.fields import REJECTED_TABLE_NAME
from cdm_data_loaders.core.registry import ArrowSchemaRegistry, TableTransformFn
from cdm_data_loaders.core.transforms.routing import RecordCoercerFn, make_record_router
from cdm_data_loaders.pipelines.jsonlines.settings import JsonlExtractSettings
from cdm_data_loaders.readers.jsonl import ParseErrorResolver, iter_file_jsonl_records

REJECTED_COLUMNS: Final[tuple[str, ...]] = ("source_file", "line_no", "raw_record")


class PageRouterOptions(TypedDict, total=False):
    """Optional keyword arguments configuring :func:`make_page_router`."""

    transform_valid: Callable[[Any], Any] | None
    table_transform: TableTransformFn | None
    schema_map: ArrowSchemaRegistry | None
    use_pyarrow: bool
    record_coercer: RecordCoercerFn


@dataclass(frozen=True, slots=True)
class _LineRouter:
    """Per-table JSONL line validator and record router."""

    table_name: str
    route_record: Callable[[BaseModel | dict[str, Any], str | None], dict[str, list[dict[str, Any]]]]
    resolve_error: ParseErrorResolver | None = None
    transform_valid: Callable[[Any], Any] | None = None

    def route_into(self, item: dict[str, Any], buffer: DictBuffer) -> Generator[Any, Any, Any]:
        """Validate and route a single parsed line record into ``buffer``.

        :param item: Parsed line record dictionary from :func:`iter_file_jsonl_records`.
        :type item: dict[str, Any]
        :param buffer: Target page buffer accumulating valid and rejected rows.
        :type buffer: DictBuffer
        :yield: Flushed pages from ``buffer`` when ``buffer.max_items`` is reached.
        :rtype: Generator[Any, Any, Any]
        """
        error_detail = item["parse_error"]
        if error_detail is None and self.resolve_error is not None:
            error_detail = self.resolve_error(item["record"])
        if error_detail is not None:
            rejected = make_rejected_row(
                self.table_name,
                error_detail,
                **{column: item[column] for column in REJECTED_COLUMNS},
            )
            yield from buffer.add_item(REJECTED_TABLE_NAME, rejected)
        else:
            record = item["record"]
            if self.transform_valid is not None:
                record = self.transform_valid(record)
            yield from buffer.add_items(self.route_record(record, item.get("source_file")))


def make_page_router(
    table_name: str,
    buffer_size: int,
    resolve_error: ParseErrorResolver | None = None,
    **options: Unpack[PageRouterOptions],
) -> Callable[[Iterable[dict[str, Any]]], Generator[Any, Any, Any]]:
    """Build a function that routes pages of line records to valid and rejected tables.

    For each item, ``resolve_error`` (when provided) inspects the record and returns ``None`` for a valid record or the
    error detail string otherwise. Parse errors bypass ``resolve_error``. Valid records pass through
    ``transform_valid`` (identity when ``None``) and ``table_transform`` into target table(s); failed records yield a
    provenance row (``table_name``, ``source_file``, ``line_no``, ``raw_record``, ``error_detail``) into ``_rejected``.

    :param table_name: Default table name for valid records.
    :type table_name: str
    :param buffer_size: Number of rows to buffer per table before yielding a page.
    :type buffer_size: int
    :param resolve_error: Optional function returning ``None`` for valid records or error detail.
    :type resolve_error: ParseErrorResolver | None
    :param options: Optional transform, schema map, PyArrow output, and record coercion configuration.
    :type options: Unpack[PageRouterOptions]
    :return: Function taking one page (or iterable) of line records and yielding routed pages.
    :rtype: Callable[[Iterable[dict[str, Any]]], Generator[Any, Any, Any]]
    """
    transform_valid = options.get("transform_valid")
    table_transform = options.get("table_transform")
    schema_map = options.get("schema_map")
    use_pyarrow = options.get("use_pyarrow", True)
    record_coercer = options.get("record_coercer", coerce_json_record_for_pyarrow)

    line_router = _LineRouter(
        table_name=table_name,
        route_record=make_record_router(
            table_name,
            transform=table_transform,
            schema_map=schema_map,
            record_coercer=record_coercer,
        ),
        resolve_error=resolve_error,
        transform_valid=transform_valid,
    )

    def _route(page: Iterable[dict[str, Any]] | dict[str, Any] | pa.Table) -> Generator[Any, Any, Any]:
        if isinstance(page, pa.Table):
            yield dlt.mark.with_table_name(page, REJECTED_TABLE_NAME)
            return
        if isinstance(page, dict):
            page = [page]
        buffer = DictBuffer(max_items=buffer_size, schema_map=schema_map, use_pyarrow=use_pyarrow)
        for item in page:
            if "parse_error" not in item and "error_detail" in item:
                yield from buffer.add_item(REJECTED_TABLE_NAME, item)
                continue
            yield from line_router.route_into(item, buffer)
        yield from buffer.flush()

    return _route


def route_jsonl_lines(
    items: Iterator[FileItemDict],
    settings: JsonlExtractSettings,
) -> Generator[TDataItems, Any, Any]:
    """Read each file in ``items`` and route each line by ``settings.table_name``.

    Valid records yield into the target table (or transformed tables when ``table_transform`` is supplied). Records
    that fail JSON parsing yield into ``_rejected`` with ``table_name``, ``source_file``, ``line_no``, ``raw_record``,
    and ``error_detail``.

    :param items: File items to read.
    :type items: Iterator[FileItemDict]
    :param settings: Pipeline configuration.
    :type settings: JsonlExtractSettings
    :yield: Table-tagged rows or PyArrow tables.
    :rtype: Generator[TDataItems, Any, Any]
    """
    line_router = _LineRouter(
        table_name=settings.table_name,
        route_record=make_record_router(
            settings.table_name,
            transform=settings.get_table_transform(settings.table_name),
            schema_map=settings.schema_map,
        ),
    )
    buffer = DictBuffer.from_settings(settings)
    for file_item in items:
        for item in iter_file_jsonl_records(file_item):
            yield from line_router.route_into(item, buffer)
    yield from buffer.flush()
