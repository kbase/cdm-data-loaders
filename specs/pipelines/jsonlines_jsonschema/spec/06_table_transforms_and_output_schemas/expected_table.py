"""Expected PyArrow tables for 06_table_transforms_and_output_schemas."""

import pyarrow as pa
from expected_schema import SAMPLE_MEASUREMENT_SCHEMA, SAMPLE_SUMMARY_SCHEMA

EXPECTED_SAMPLES_TABLE = pa.Table.from_pylist(
    [
        {"sample_id": "SMP-1", "ph": 6.4, "ph_bucket": "acidic"},
        {"sample_id": "SMP-3", "ph": 7.8, "ph_bucket": "neutral_or_basic"},
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
