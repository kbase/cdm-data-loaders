"""Expected PyArrow schemas for 06_table_transforms_and_output_schemas."""

import pyarrow as pa

MEASUREMENT_SCHEMA = pa.schema(
    [
        pa.field("sensor_id", pa.string(), nullable=False),
        pa.field("value", pa.float64(), nullable=False),
        pa.field("scaled_value", pa.float64(), nullable=False),
    ]
)

MEASUREMENT_TAG_SCHEMA = pa.schema(
    [
        pa.field("sensor_id", pa.string(), nullable=False),
        pa.field("tag", pa.string(), nullable=False),
    ]
)

EXPECTED_SCHEMA = MEASUREMENT_SCHEMA
EXPECTED_SCHEMAS = {
    "measurements": MEASUREMENT_SCHEMA,
    "measurement_tags": MEASUREMENT_TAG_SCHEMA,
}
