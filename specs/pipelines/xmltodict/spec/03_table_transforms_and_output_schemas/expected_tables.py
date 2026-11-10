"""Expected PyArrow tables for xmltodict 03_table_transforms_and_output_schemas."""

import pyarrow as pa

from specs.pipelines.xmltodict.transforms import (
    BOOK_AUTHORS_SCHEMA,
    CATALOG_BOOKS_SCHEMA,
)

EXPECTED_TABLES: dict[str, pa.Table] = {
    "books": pa.Table.from_pylist(
        [
            {"book_id": "B-1", "title": "The Left Hand of Darkness", "year": 1969},
            {"book_id": "B-2", "title": "Good Omens", "year": 1990},
        ],
        schema=CATALOG_BOOKS_SCHEMA,
    ),
    "book_authors": pa.Table.from_pylist(
        [
            {"book_id": "B-1", "author": "Ursula K. Le Guin"},
            {"book_id": "B-2", "author": "Terry Pratchett"},
            {"book_id": "B-2", "author": "Neil Gaiman"},
        ],
        schema=BOOK_AUTHORS_SCHEMA,
    ),
}
