"""E3 fixture: duplicate sibling field name in nested pa.struct."""

import pyarrow as pa

SCHEMA = pa.schema(
    [
        pa.field(
            "meta",
            pa.struct(
                [
                    pa.field("tag", pa.string(), nullable=False),
                    pa.field("tag", pa.string(), nullable=True),
                ]
            ),
            nullable=False,
        )
    ]
)
