"""Expected PyArrow tables for 06_table_transforms_and_output_schemas."""

import datetime as dt
from decimal import Decimal

import pyarrow as pa
from expected_schema import SAMPLE_MEASUREMENT_SCHEMA, SAMPLE_SUMMARY_SCHEMA

EXPECTED_SAMPLES_TABLE = pa.Table.from_pylist(
    [
        {
            "sample_id": "SMP-1",
            "ph": Decimal("6.46"),
            "ph_bucket": "acidic",
            "collected_date": dt.date(2025, 6, 15),
            "collected_at": dt.datetime(2025, 6, 15, 12, 30, tzinfo=dt.UTC),
            "sample_time": dt.time(14, 30),
            "checksum": b"Hello",
        },
        {
            "sample_id": "SMP-3",
            "ph": Decimal("7.80"),
            "ph_bucket": "neutral_or_basic",
            "collected_date": dt.date(2025, 6, 16),
            "collected_at": dt.datetime(2025, 6, 16, 9, 0, tzinfo=dt.UTC),
            "sample_time": dt.time(9, 0),
            "checksum": None,
        },
    ],
    schema=SAMPLE_SUMMARY_SCHEMA,
)

EXPECTED_SAMPLE_MEASUREMENTS_TABLE = pa.Table.from_pylist(
    [
        {"sample_id": "SMP-1", "metric": "moisture"},
        {"sample_id": "SMP-1", "metric": "salinity"},
    ],
    schema=SAMPLE_MEASUREMENT_SCHEMA,
)

EXPECTED_TABLE = EXPECTED_SAMPLES_TABLE
EXPECTED_TABLES = {
    "samples": EXPECTED_SAMPLES_TABLE,
    "sample_measurements": EXPECTED_SAMPLE_MEASUREMENTS_TABLE,
}
