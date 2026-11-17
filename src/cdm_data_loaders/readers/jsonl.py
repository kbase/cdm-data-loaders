"""Streaming JSONLines file reading utilities."""

import json
from collections.abc import Callable, Generator, Iterator
from typing import Any

from dlt.common.storages.fsspec_filesystem import FileItemDict
from dlt.common.typing import TDataItems

type LineRecord = dict[str, Any]
type ParseErrorResolver = Callable[[dict[str, Any]], str | None]


def iter_file_jsonl_records(file_item: FileItemDict) -> Generator[LineRecord]:
    """Open ``file_item`` and yield parsed ``LineRecord`` dicts for each non-blank line.

    ``dlt``'s default ``compression="auto"`` decompresses files marked as gzip from ``*.gz`` filenames across all
    filesystems.

    :param file_item: Input JSONL file item.
    :type file_item: FileItemDict
    :yield: Parsed line record dictionaries carrying ``record``, ``raw_record``, ``parse_error``, ``source_file``, and ``line_no``.
    :rtype: Generator[LineRecord]
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
    """Read each file in ``items`` and yield pages of ``LineRecord`` dictionaries.

    Each record contains ``record`` (parsed JSON or ``None`` on parse failure), ``raw_record``, ``parse_error``
    (``None`` on success), ``source_file``, and ``line_no``. Records accumulate into pages of ``buffer_size`` before
    yielding.

    :param items: File items to read.
    :type items: Iterator[FileItemDict]
    :param buffer_size: Number of line records per yielded page.
    :type buffer_size: int
    :yield: Pages of line records.
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
