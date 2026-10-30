"""JSONLines file reading utilities."""

import json
from collections.abc import Callable, Generator, Iterable, Iterator
from typing import Any, Final, Literal

import pyarrow as pa
from dlt.common.storages.fsspec_filesystem import FileItemDict
from dlt.common.typing import TDataItems

from cdm_data_loaders.core.fields import GZIP_SUFFIX
from cdm_data_loaders.utils.buffer import ArrowListBuffer, ListBuffer

LineRecord = dict[str, Any]
ParseErrorResolver = Callable[[dict[str, Any]], str | None]

DEFAULT_REJECTED_SUFFIX: Final[str] = "_rejected"
REJECTED_COLUMNS: Final[tuple[str, ...]] = ("source_file", "line_no", "raw_record")


def compression_mode(file_name: str) -> Literal["enable", "disable"]:
    """Return the dlt compression mode for a file name.

    Returns "enable" for names ending in .gz and "disable" otherwise.
    Auto-detection is not used, since it depends on remote file metadata
    that local files do not carry.
    """
    return "enable" if file_name.endswith(GZIP_SUFFIX) else "disable"


def parse_jsonl_line(stripped: str) -> LineRecord:
    """Parse one stripped JSONL line into the shared line-record shape.

    The returned dict has: record (parsed JSON, or None on parse
    failure), raw_record, parse_error (None on success).
    """
    parse_error: str | None = None
    record: dict[str, Any] | None = None
    try:
        record = json.loads(stripped)
    except json.JSONDecodeError as e:
        parse_error = str(e)
    return {"record": record, "raw_record": stripped, "parse_error": parse_error}


def _iter_file_jsonl_records(file_item: FileItemDict) -> Generator[LineRecord]:
    """Open ``file_item`` and yield parsed ``LineRecord`` dicts for each non-blank line."""
    with file_item.open(mode="rt", compression=compression_mode(file_item["file_name"]), encoding="utf-8") as fh:
        for line_no, line in enumerate(fh, start=1):
            stripped = line.strip()
            if not stripped:
                continue
            yield {
                **parse_jsonl_line(stripped),
                "source_file": file_item["relative_path"],
                "line_no": line_no,
            }


def stream_jsonl_lines(items: Iterator[FileItemDict], buffer_size: int) -> Generator[TDataItems, Any, Any]:
    """Read each file in items. Yield pages of line records.

    Each record has: record (parsed JSON, or None on parse failure),
    raw_record, parse_error (None on success), source_file, line_no.
    Records are accumulated into pages of `buffer_size` before being
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
        for line_record in _iter_file_jsonl_records(file_item):
            page.append(line_record)
            if len(page) >= buffer_size:
                yield page
                page = []
    if page:
        yield page


def make_page_router(  # noqa: PLR0913
    table_name: str,
    buffer_size: int,
    resolve_error: ParseErrorResolver | None = None,
    *,
    rejected_suffix: str = DEFAULT_REJECTED_SUFFIX,
    include_record: bool = False,
    error_column: str = "error_detail",
    transform_valid: Callable[[Any], Any] | None = None,
    pyarrow_schema: pa.Schema | None = None,
) -> Callable[[Iterable[dict[str, Any]]], Generator[Any, Any, Any]]:
    """Build a function that routes pages of line records to valid and rejected tables.

    For each item, `resolve_error` (when provided) inspects the record and
    returns None for a valid record, or the error detail string otherwise.
    Parse errors bypass `resolve_error` entirely. Valid records yield
    `item["record"]` passed through `transform_valid` (identity when None) into
    `table_name`; failed records yield a provenance row into
    ``{table_name}{rejected_suffix}``. With `include_record`, the rejected row
    also carries the parsed `record` value. When `pyarrow_schema` is supplied,
    valid records are accumulated in an `ArrowListBuffer` and yielded as
    `pyarrow.Table` pages.

    :param table_name: table name for valid records
    :type  table_name: str
    :param buffer_size: number of rows to buffer per table before yielding a page
    :type  buffer_size: int
    :param resolve_error: optional function returning None for valid records or error detail
    :type  resolve_error: ParseErrorResolver | None
    :param rejected_suffix: suffix for the rejected table name
    :type  rejected_suffix: str
    :param include_record: whether rejected rows carry the parsed record value
    :type  include_record: bool
    :param error_column: column name used to store the error detail in rejected rows
    :type  error_column: str
    :param transform_valid: function applied to each valid record before yielding
    :type  transform_valid: Callable[[Any], Any] | None
    :param pyarrow_schema: optional PyArrow schema for buffering valid records as Arrow tables
    :type  pyarrow_schema: pa.Schema | None
    :return: function taking one page (or iterable) of line records and yielding routed pages
    :rtype : Callable[[Iterable[dict[str, Any]]], Generator[Any, Any, Any]]
    """
    rejected_table = f"{table_name}{rejected_suffix}"

    def _route(page: Iterable[dict[str, Any]]) -> Generator[Any, Any, Any]:
        valid_buffer: ListBuffer | ArrowListBuffer = (
            ArrowListBuffer(table_to_schema_map={table_name: pyarrow_schema}, max_items=buffer_size)
            if pyarrow_schema is not None
            else ListBuffer(table_name=table_name, max_items=buffer_size)
        )
        rejected_buffer = ListBuffer(table_name=rejected_table, max_items=buffer_size)
        for item in page:
            error_detail = item["parse_error"]
            if error_detail is None and resolve_error is not None:
                error_detail = resolve_error(item["record"])
            if error_detail is not None:
                rejected = {column: item[column] for column in REJECTED_COLUMNS}
                rejected[error_column] = error_detail
                if include_record:
                    rejected["record"] = item["record"]
                yield from rejected_buffer.add_item(rejected)
            else:
                record = item["record"]
                if transform_valid is not None:
                    record = transform_valid(record)
                yield from valid_buffer.add_item(record)
        yield from valid_buffer.flush()
        yield from rejected_buffer.flush()

    return _route


def route_jsonl_lines(
    items: Iterator[FileItemDict],
    buffer_size: int,
    table_name_of: Callable[[FileItemDict], str],
) -> Generator[TDataItems, Any, Any]:
    """Read each file in items and route each line by its file's table name.

    Valid records yield into the file's table with the given name.
    Records that fail JSON parsing yield into ``{table}_invalid`` with
    raw_record, record (None), source_file, line_no, and parse_error.

    :param items: file items to read
    :type items: Iterator[FileItemDict]
    :param buffer_size: number of rows to buffer per table before yielding
    :type buffer_size: int
    :param table_name_of: function returning the table name for a file item
    :type table_name_of: Callable[[FileItemDict], str]
    :yield: table-tagged rows
    :rtype: Generator[TDataItems, Any, Any]
    """
    for file_item in items:
        router = make_page_router(
            table_name_of(file_item),
            buffer_size,
            rejected_suffix="_invalid",
            include_record=True,
            error_column="parse_error",
        )
        yield from router(_iter_file_jsonl_records(file_item))
