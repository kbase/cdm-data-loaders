"""E10 fixture: list containing map elements pa.list_(pa.map_(...))."""

import pyarrow as pa

SCHEMA = pa.schema(
    [
        pa.field(
            "map_batches",
            pa.list_(pa.map_(pa.string(), pa.int64())),
            nullable=False,
        )
    ]
)
