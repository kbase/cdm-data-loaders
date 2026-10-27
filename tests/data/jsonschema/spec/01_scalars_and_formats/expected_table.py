"""Expected PyArrow table for 01_scalars_and_formats."""

# ruff: noqa: INP001

import datetime as dt

import pyarrow as pa
from expected_schema import EXPECTED_SCHEMA

EXPECTED_TABLE = pa.Table.from_pylist(
    [
        {
            "id": "rec1",
            "status": "active",
            "version": 2,
            "flag": True,
            "bounded_int": -42,
            "big_int": 999999999999,
            "float_val": 2.718281828,
            "decimal_multiple": 3.14,
            "created_date": dt.date(2025, 6, 15),
            "updated_at": "2025-06-15T12:30:00Z",
            "event_time": "12:30:00Z",
            "elapsed": "P3DT4H",
            "record_uuid": "550e8400-e29b-41d4-a716-446655440000",
            "homepage": "https://example.org/rec1",
            "const_tag": "v1",
            "untyped_int_enum": 20,
            "untyped_mixed_enum": "1",
            "numeric_union": 42.0,
            "scalar_union": "100",
            "date_or_year_union": "2025-06-15",
            "raw_b64": b"Hello",
            "raw_hex": b"Hello",
        },
        {"id": "rec2", "status": "archived", "version": 1, "flag": False},
        {
            "id": "rec3",
            "status": "pending",
            "version": 32767,
            "flag": True,
            "bounded_int": 999,
            "big_int": -999999999999,
            "float_val": -0.5,
            "decimal_multiple": 42.0,
            "created_date": dt.date(1999, 12, 31),
            "updated_at": "2025-06-15T12:30:00.123+02:00",
            "event_time": "23:59:59+02:00",
            "elapsed": "PT15M",
            "record_uuid": "123e4567-e89b-12d3-a456-426614174000",
            "homepage": "https://example.org/rec3",
            "const_tag": "v1",
            "untyped_int_enum": 30,
            "untyped_mixed_enum": "true",
            "numeric_union": 3.5,
            "scalar_union": "hundred",
            "date_or_year_union": "1999",
            "raw_b64": b"World",
            "raw_hex": b"World",
        },
    ],
    schema=EXPECTED_SCHEMA,
)
