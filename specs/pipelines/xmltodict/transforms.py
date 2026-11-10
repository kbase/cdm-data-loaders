"""File pre-processors, table transforms, and PyArrow output schemas for XML to Dict specification cases."""

from typing import Any, Final

import pyarrow as pa

from cdm_data_loaders.core.preprocessors import validate_md5_sidecar
from cdm_data_loaders.core.registry import FilePreprocessFn, RecordTransformFn

FILE_PREPROCESSORS: tuple[FilePreprocessFn, ...] = (validate_md5_sidecar,)

CATALOG_BOOKS_SCHEMA: Final[pa.Schema] = pa.schema(
    [
        pa.field("book_id", pa.string(), nullable=False),
        pa.field("title", pa.string(), nullable=False),
        pa.field("year", pa.int64(), nullable=False),
    ]
)

BOOK_AUTHORS_SCHEMA: Final[pa.Schema] = pa.schema(
    [
        pa.field("book_id", pa.string(), nullable=False),
        pa.field("author", pa.string(), nullable=False),
    ]
)


def transform_book(row: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    """Unwrap ``<book>`` dictionaries, drop withdrawn entries, and fan out authors to ``book_authors``.

    :param row: Parsed ``xmltodict`` dictionary of the form ``{"book": {...}}``.
    :type row: dict[str, Any]
    :return: Mapping of target table names to lists of output row dictionaries.
    :rtype: dict[str, list[dict[str, Any]]]
    """
    book = row["book"]
    if book.get("_status") == "withdrawn":
        return {}
    book_id = str(book["_id"]).upper()
    title = str(book["title"])
    year = int(book["year"])
    routed: dict[str, list[dict[str, Any]]] = {
        "books": [
            {
                "book_id": book_id,
                "title": title,
                "year": year,
            }
        ]
    }
    raw_authors = book.get("author") or []
    authors = raw_authors if isinstance(raw_authors, list) else [raw_authors]
    if authors:
        routed["book_authors"] = [{"book_id": book_id, "author": str(a)} for a in authors]
    return routed


TABLE_TRANSFORMS: Final[dict[str, RecordTransformFn]] = {
    "books": transform_book,
}

OUTPUT_SCHEMAS: Final[dict[str, pa.Schema]] = {
    "books": CATALOG_BOOKS_SCHEMA,
    "book_authors": BOOK_AUTHORS_SCHEMA,
}
