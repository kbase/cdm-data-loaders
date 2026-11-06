"""JSONLines file reading utilities."""

import json
from collections.abc import Callable, Generator, Iterable, Iterator
from typing import Any, Final

from dlt.common.storages.fsspec_filesystem import FileItemDict
from dlt.common.typing import TDataItems

from cdm_data_loaders.core.fields import REJECTED_TABLE_NAME
from cdm_data_loaders.core.registry import ArrowSchemaRegistry, RecordTransformFn
from cdm_data_loaders.core.transforms import route_and_coerce_record
from cdm_data_loaders.pipelines.jsonlines.settings import JsonlExtractSettings
from cdm_data_loaders.utils.buffer import DictBuffer

LineRecord = dict[str, Any]
ParseErrorResolver = Callable[[dict[str, Any]], str | None]

REJECTED_COLUMNS: Final[tuple[str, ...]] = ("source_file", "line_no", "raw_record")


def iter_file_jsonl_records(file_item: FileItemDict) -> Generator[LineRecord]:
    """Open ``file_item`` and yield parsed ``LineRecord`` dicts for each non-blank line.

    dlt's default ``compression="auto"`` decompresses files that the listing marked as gzip, which it
    does from the file name (``*.gz``) for every filesystem.
    """
    with file_item.open(mode="rt", encoding="utf-8") as fh:
        for line_no, line in enumerate(fh, start=1):
            stripped = line.strip()
            if not stripped:
                continue
            parse_error: str | None = None
            record: dict[str, Any] | None = None
            try:
                record = json.loads(stripped)
            except json.JSONDecodeError as e:
                parse_error = str(e)
            yield {
                "record": record,
                "raw_record": stripped,
                "parse_error": parse_error,
                "source_file": file_item["relative_path"],
                "line_no": line_no,
            }


def stream_jsonl_lines(items: Iterator[FileItemDict], buffer_size: int) -> Generator[TDataItems, Any, Any]:
    """Read each file in items. Yield pages of line records.

    Each record has: record (parsed JSON, or None on parse failure),
    raw_record, parse_error (None on success), source_file, line_no.
    Records are accumulated into pages of ``buffer_size`` before being
    yielded, so downstream transformers receive pages rather than
    individual rows.

    :param items: file items to read
    :type items: Iterator[FileItemDict]
    :param buffer_size: number of line records per yielded page
    :type buffer_size: int
    :yield: pages of line records
    :rtype: Generator[TDataItems, Any, Any]
    """
    page: list[LineRecord] = []
    for file_item in items:
        for line_record in iter_file_jsonl_records(file_item):
            page.append(line_record)
            if len(page) >= buffer_size:
                yield page
                page = []
    if page:
        yield page


def _route_line_record(  # noqa: PLR0913
    item: dict[str, Any],
    table_name: str,
    buffer: DictBuffer,
    *,
    resolve_error: ParseErrorResolver | None = None,
    transform_valid: Callable[[Any], Any] | None = None,
    table_transform: RecordTransformFn | None = None,
    schema_map: ArrowSchemaRegistry | None = None,
) -> Generator[Any, Any, Any]:
    """Validate and route a single parsed line record into ``buffer``."""
    error_detail = item["parse_error"]
    if error_detail is None and resolve_error is not None:
        error_detail = resolve_error(item["record"])
    if error_detail is not None:
        rejected = {
            "table_name": table_name,
            **{column: item[column] for column in REJECTED_COLUMNS},
            "error_detail": error_detail,
        }
        yield from buffer.add_item(REJECTED_TABLE_NAME, rejected)
    else:
        record = item["record"]
        if transform_valid is not None:
            record = transform_valid(record)
        yield from buffer.add_items(
            route_and_coerce_record(
                record,
                table_name,
                transform=table_transform,
                schema_map=schema_map,
            )
        )


def make_page_router(  # noqa: PLR0913
    table_name: str,
    buffer_size: int,
    resolve_error: ParseErrorResolver | None = None,
    *,
    transform_valid: Callable[[Any], Any] | None = None,
    table_transform: RecordTransformFn | None = None,
    schema_map: ArrowSchemaRegistry | None = None,
) -> Callable[[Iterable[dict[str, Any]]], Generator[Any, Any, Any]]:
    """Build a function that routes pages of line records to valid and rejected tables.

    For each item, ``resolve_error`` (when provided) inspects the record and
    returns None for a valid record, or the error detail string otherwise.
    Parse errors bypass ``resolve_error`` entirely. Valid records yield
    ``item["record"]`` passed through ``transform_valid`` (identity when None) and
    ``table_transform`` into target table(s); failed records yield a provenance
    row (``table_name``, ``source_file``, ``line_no``, ``raw_record``,
    ``error_detail``) into ``_rejected``. Records are accumulated in a
    ``DictBuffer``, which always emits ``_rejected`` as ``pyarrow.Table`` pages and
    emits target tables as ``pyarrow.Table`` pages whenever ``schema_map``
    is supplied.

    :param table_name: default table name for valid records
    :type  table_name: str
    :param buffer_size: number of rows to buffer per table before yielding a page
    :type  buffer_size: int
    :param resolve_error: optional function returning None for valid records or error detail
    :type  resolve_error: ParseErrorResolver | None
    :param transform_valid: function applied to each valid record before routing
    :type  transform_valid: Callable[[Any], Any] | None
    :param table_transform: optional transform function for filtering, transforming, or splitting records
    :type  table_transform: RecordTransformFn | None
    :param schema_map: optional per-table PyArrow schema registry
    :type  schema_map: ArrowSchemaRegistry | None
    :return: function taking one page (or iterable) of line records and yielding routed pages
    :rtype : Callable[[Iterable[dict[str, Any]]], Generator[Any, Any, Any]]
    """

    def _route(page: Iterable[dict[str, Any]]) -> Generator[Any, Any, Any]:
        buffer = DictBuffer(max_items=buffer_size, schema_map=schema_map)
        for item in page:
            yield from _route_line_record(
                item,
                table_name,
                buffer,
                resolve_error=resolve_error,
                transform_valid=transform_valid,
                table_transform=table_transform,
                schema_map=schema_map,
            )
        yield from buffer.flush()

    return _route


def route_jsonl_lines(
    items: Iterator[FileItemDict],
    settings: JsonlExtractSettings,
) -> Generator[TDataItems, Any, Any]:
    """Read each file in items and route each line by its file's table name.

    Valid records yield into the file's table with the given name (or transformed
    tables when ``table_transform`` is supplied).
    Records that fail JSON parsing yield into ``_rejected`` with
    ``table_name``, ``source_file``, ``line_no``, ``raw_record``, and ``error_detail``.

    :param items: file items to read
    :type items: Iterator[FileItemDict]
    :param settings: pipeline configuration
    :type  settings: JsonlExtractSettings
    :yield: table-tagged rows
    :rtype: Generator[TDataItems, Any, Any]
    """
    buffer = DictBuffer(max_items=settings.buffer_size, schema_map=settings.schema_map)
    for file_item in items:
        for item in iter_file_jsonl_records(file_item):
            yield from _route_line_record(
                item,
                settings.table_name,
                buffer,
                table_transform=settings.get_table_transform(settings.table_name),
                schema_map=settings.schema_map,
            )
    yield from buffer.flush()
