"""Expected PyArrow schema for 03_blank_lines_parse_errors_and_format_rejections."""

import pyarrow as pa

EXPECTED_SCHEMA = pa.schema(
    [
        pa.field("id", pa.string(), nullable=False, metadata={"jsonschema_type": "string"}),
        pa.field("count", pa.int64(), nullable=False, metadata={"jsonschema_type": "integer"}),
        pa.field("contact_email", pa.string(), nullable=False, metadata={"jsonschema_type": "string (email)"}),
    ]
)
