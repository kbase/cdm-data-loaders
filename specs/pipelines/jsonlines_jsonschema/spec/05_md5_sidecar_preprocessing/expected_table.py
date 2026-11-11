"""Expected PyArrow table for 05_md5_sidecar_preprocessing."""

import pyarrow as pa
from expected_schema import EXPECTED_SCHEMA

EXPECTED_TABLE = pa.Table.from_pylist(
    [
        {"pmid": 2001, "title": "Validated JSONL Article"},
    ],
    schema=EXPECTED_SCHEMA,
)
