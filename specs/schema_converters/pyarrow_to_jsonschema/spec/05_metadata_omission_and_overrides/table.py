"""Sample PyArrow table for 05_metadata_omission_and_overrides."""

import datetime as dt
from decimal import Decimal

import pyarrow as pa
from schema import SCHEMA

TABLE = pa.Table.from_pylist(
    [
        {
            "id": "S-01",
            "count": 1024,
            "gc_ratio": Decimal("0.5125"),
            "digest": b"01234567",
            "collected_on": dt.date(2026, 3, 15),
        },
        {
            "id": "S-02",
            "count": 0,
            "gc_ratio": None,
            "digest": b"\x00\x01\x02\x03\x04\x05\x06\x07",
            "collected_on": None,
        },
    ],
    schema=SCHEMA,
)
