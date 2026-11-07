"""Expected PyArrow table for minimal real-world example ncbi_annotation_report."""

import pyarrow as pa
from expected_schema import EXPECTED_SCHEMA

EXPECTED_TABLE = pa.Table.from_pylist(
    [
        {"total_count": 1},
    ],
    schema=EXPECTED_SCHEMA,
)
