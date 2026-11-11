"""Sample PyArrow table for 03_temporal_types."""

import datetime as dt

import pyarrow as pa
from schema import SCHEMA

TABLE = pa.table(
    {
        "d32": pa.array([dt.date(2026, 4, 10), dt.date(1970, 1, 1)], type=pa.date32()),
        "d64": pa.array([dt.date(2026, 4, 11), dt.date(2000, 2, 29)], type=pa.date64()),
        "t32_s": pa.array([dt.time(8, 30, 0), dt.time(23, 59, 59)], type=pa.time32("s")),
        "t32_ms": pa.array([dt.time(8, 30, 0, 250000), dt.time(0, 0, 0)], type=pa.time32("ms")),
        "t64_us": pa.array([dt.time(14, 15, 30, 123456), dt.time(12, 0, 0)], type=pa.time64("us")),
        "t64_ns": pa.array([52_200_123_456_000, 0], type=pa.time64("ns")),
        "ts_s_utc": pa.array(
            [
                dt.datetime(2026, 4, 10, 14, 30, 0, tzinfo=dt.UTC),
                dt.datetime(2025, 1, 1, 0, 0, 0, tzinfo=dt.UTC),
            ],
            type=pa.timestamp("s", tz="UTC"),
        ),
        "ts_us_la": pa.array(
            [
                dt.datetime(2026, 4, 10, 14, 30, 0, 125000, tzinfo=dt.UTC),
                dt.datetime(2025, 1, 15, 18, 0, 0, tzinfo=dt.UTC),
            ],
            type=pa.timestamp("us", tz="America/Los_Angeles"),
        ),
        "ts_ns_utc": pa.array(
            [
                dt.datetime(2026, 4, 10, 14, 30, 0, 750000, tzinfo=dt.UTC),
                dt.datetime(2025, 6, 1, 6, 0, 0, tzinfo=dt.UTC),
            ],
            type=pa.timestamp("ns", tz="UTC"),
        ),
        "ts_naive_us": pa.array(
            [
                dt.datetime(2026, 4, 10, 9, 45, 0, tzinfo=dt.UTC).replace(tzinfo=None),
                dt.datetime(2025, 12, 31, 23, 59, 59, tzinfo=dt.UTC).replace(tzinfo=None),
            ],
            type=pa.timestamp("us"),
        ),
        "ts_naive_ns_opt": pa.array(
            [
                dt.datetime(2026, 4, 10, 9, 45, 0, 500000, tzinfo=dt.UTC).replace(tzinfo=None),
                None,
            ],
            type=pa.timestamp("ns"),
        ),
    },
    schema=SCHEMA,
)
