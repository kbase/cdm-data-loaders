"""Expected PyArrow schema for 05_partial_and_total_validation_rejections."""

import pyarrow as pa

EXPECTED_SCHEMA = pa.schema(
    [
        pa.field("record_id", pa.string(), nullable=False, metadata={"jsonschema_type": "string"}),
        pa.field("amount", pa.int64(), nullable=False, metadata={"jsonschema_type": "integer"}),
        pa.field("event_date", pa.date32(), nullable=False, metadata={"jsonschema_type": "string (date)"}),
    ]
)
