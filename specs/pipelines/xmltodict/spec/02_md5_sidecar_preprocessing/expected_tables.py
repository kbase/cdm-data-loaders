"""Expected PyArrow tables for xmltodict 02_md5_sidecar_preprocessing."""

import pyarrow as pa

from cdm_data_loaders.utils.buffer import REJECTED_PYARROW_SCHEMA

BOOKS_SCHEMA = pa.schema(
    [
        pa.field("book___id", pa.string(), nullable=False),
        pa.field("book__title", pa.string(), nullable=False),
    ]
)

EXPECTED_TABLES: dict[str, pa.Table] = {
    "books": pa.Table.from_pylist(
        [
            {"book___id": "b-10", "book__title": "Hyperion"},
        ],
        schema=BOOKS_SCHEMA,
    ),
    "_rejected": pa.Table.from_pylist(
        [
            {
                "table_name": "books",
                "source_file": "bad.xml",
                "line_no": None,
                "raw_record": None,
                "error_detail": (
                    "MD5 mismatch for bad.xml: "
                    "expected 00000000000000000000000000000000, got 5a46030ee1e4775d1dd0418280d8a0d7"
                ),
            }
        ],
        schema=REJECTED_PYARROW_SCHEMA,
    ),
}
