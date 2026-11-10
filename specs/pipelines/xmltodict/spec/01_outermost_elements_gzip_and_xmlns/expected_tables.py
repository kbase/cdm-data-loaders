"""Expected PyArrow tables for xmltodict 01_outermost_elements_gzip_and_xmlns."""

import pyarrow as pa

BOOKS_SCHEMA = pa.schema(
    [
        pa.field("book___id", pa.string(), nullable=False),
        pa.field("book__title", pa.string(), nullable=False),
        pa.field("book__related__book___id", pa.string(), nullable=True),
        pa.field("book__related__book__title", pa.string(), nullable=True),
    ]
)

EXPECTED_TABLES: dict[str, pa.Table] = {
    "books": pa.Table.from_pylist(
        [
            {
                "book___id": "b-1",
                "book__title": "Dune",
                "book__related__book___id": "b-1a",
                "book__related__book__title": "Dune Messiah",
            },
            {
                "book___id": "b-2",
                "book__title": "Foundation",
                "book__related__book___id": None,
                "book__related__book__title": None,
            },
        ],
        schema=BOOKS_SCHEMA,
    ),
}
