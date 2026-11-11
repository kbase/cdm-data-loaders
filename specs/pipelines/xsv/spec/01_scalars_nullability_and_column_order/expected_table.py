"""Expected PyArrow table for 01_scalars_nullability_and_column_order."""

import datetime as dt

import pyarrow as pa
from expected_schema import EXPECTED_SCHEMA

EXPECTED_TABLE = pa.Table.from_pylist(
    [
        {
            "id": "rec-1",
            "count": 42,
            "rank32": 10,
            "score": 3.5,
            "price": 19.99,
            "active": True,
            "created_date": dt.date(2025, 1, 15),
            "updated_at": "2025-01-15T08:30:00Z",
            "status": "active",
            "opt_note": "first note",
            "unlisted_tag": "alpha",
            "extra_col": None,
        },
        {
            "id": "rec-2",
            "count": -100,
            "rank32": 0,
            "score": -0.25,
            "price": 0.5,
            "active": False,
            "created_date": dt.date(2025, 2, 20),
            "updated_at": "2025-02-20T14:00:00Z",
            "status": "inactive",
            "opt_note": None,
            "unlisted_tag": None,
            "extra_col": None,
        },
    ],
    schema=EXPECTED_SCHEMA,
)
