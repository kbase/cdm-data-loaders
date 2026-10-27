"""Expected PyArrow table for 07_multi_root_shared_type (xml_tag='entry')."""

# ruff: noqa: INP001

import pyarrow as pa
from expected_schema import EXPECTED_SCHEMA

EXPECTED_TABLE = pa.Table.from_pylist(
    [
        {"_id": "C1", "name": "First Cluster"},
        {"_id": "C2", "name": "Second Cluster"},
    ],
    schema=EXPECTED_SCHEMA,
)
