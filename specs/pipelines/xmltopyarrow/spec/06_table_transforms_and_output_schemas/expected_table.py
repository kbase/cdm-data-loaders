"""Expected PyArrow tables for 06_table_transforms_and_output_schemas."""

import pyarrow as pa
from expected_schema import MEASUREMENT_SCHEMA, MEASUREMENT_TAG_SCHEMA

EXPECTED_MEASUREMENTS_TABLE = pa.Table.from_pylist(
    [
        {"sensor_id": "S1", "value": 1.25, "scaled_value": 12.5},
        {"sensor_id": "S3", "value": 3.5, "scaled_value": 35.0},
    ],
    schema=MEASUREMENT_SCHEMA,
)

EXPECTED_MEASUREMENT_TAGS_TABLE = pa.Table.from_pylist(
    [
        {"sensor_id": "S1", "tag": "alpha"},
        {"sensor_id": "S1", "tag": "beta"},
    ],
    schema=MEASUREMENT_TAG_SCHEMA,
)

EXPECTED_TABLE = EXPECTED_MEASUREMENTS_TABLE
EXPECTED_TABLES = {
    "measurements": EXPECTED_MEASUREMENTS_TABLE,
    "measurement_tags": EXPECTED_MEASUREMENT_TAGS_TABLE,
}
