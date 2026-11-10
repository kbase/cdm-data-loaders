"""Expected PyArrow tables for 05_partial_and_total_validation_rejections."""

import datetime as dt

import pyarrow as pa
from expected_schema import EXPECTED_SCHEMA

EXPECTED_PARTIAL_TABLE = pa.Table.from_pylist(
    [
        {"record_id": "R1", "amount": 100, "event_date": dt.date(2025, 3, 1)},
        {"record_id": "R3", "amount": 250, "event_date": dt.date(2025, 3, 3)},
    ],
    schema=EXPECTED_SCHEMA,
)

EXPECTED_EMPTY_TABLE = pa.Table.from_pylist([], schema=EXPECTED_SCHEMA)
