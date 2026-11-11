"""E3 fixture: duplicate sibling field name in root pa.Schema."""

import pyarrow as pa

SCHEMA = pa.schema(
    [
        pa.field("id", pa.string(), nullable=False),
        pa.field("id", pa.int64(), nullable=False),
    ]
)
