"""E7 fixture: pa.sparse_union column."""

import pyarrow as pa

SCHEMA = pa.schema(
    [
        pa.field(
            "value_union",
            pa.sparse_union([pa.field("as_int", pa.int64()), pa.field("as_str", pa.string())]),
            nullable=False,
        )
    ]
)
