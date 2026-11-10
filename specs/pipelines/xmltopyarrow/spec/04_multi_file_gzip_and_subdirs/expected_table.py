"""Expected PyArrow table for 04_multi_file_gzip_and_subdirs."""

import pyarrow as pa
from expected_schema import EXPECTED_SCHEMA

EXPECTED_TABLE = pa.Table.from_pylist(
    [
        {"_id": "e-1", "name": "Alpha", "score": 10.5},
        {"_id": "e-2", "name": "Beta", "score": 20.25},
    ],
    schema=EXPECTED_SCHEMA,
)
