"""Expected PyArrow schema for 02_delimiters_comments_quotes_and_escapes."""

import pyarrow as pa

EXPECTED_SCHEMA = pa.schema(
    [
        pa.field("sample_id", pa.string(), nullable=False, metadata={"jsonschema_type": "string"}),
        pa.field("measurement", pa.float64(), nullable=False, metadata={"jsonschema_type": "number"}),
        pa.field("label", pa.string(), nullable=False, metadata={"jsonschema_type": "string"}),
    ]
)
