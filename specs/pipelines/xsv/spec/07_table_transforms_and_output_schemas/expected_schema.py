"""Expected default PyArrow schema (from schema.json before output_schemas override) for 07_table_transforms_and_output_schemas."""

import pyarrow as pa

EXPECTED_SCHEMA = pa.schema(
    [
        pa.field("sensor_id", pa.string(), nullable=False, metadata={"jsonschema_type": "string"}),
        pa.field("celsius", pa.float64(), nullable=False, metadata={"jsonschema_type": "number"}),
        pa.field("status", pa.string(), nullable=False, metadata={"jsonschema_type": "string"}),
    ]
)
