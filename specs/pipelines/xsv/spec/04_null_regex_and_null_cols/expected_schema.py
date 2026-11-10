"""Expected PyArrow schema for 04_null_regex_and_null_cols."""

import pyarrow as pa

EXPECTED_SCHEMA = pa.schema(
    [
        pa.field("site", pa.string(), nullable=False, metadata={"jsonschema_type": "string"}),
        pa.field("depth", pa.int64(), nullable=True, metadata={"jsonschema_type": "integer"}),
        pa.field("ph", pa.float64(), nullable=True, metadata={"jsonschema_type": "number"}),
        pa.field("raw_code", pa.string(), nullable=False, metadata={"jsonschema_type": "string"}),
    ]
)
