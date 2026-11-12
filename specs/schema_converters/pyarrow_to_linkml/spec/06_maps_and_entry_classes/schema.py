"""PyArrow schema for 06_maps_and_entry_classes."""

import pyarrow as pa

HIT_DETAIL_STRUCT = pa.struct(
    [
        pa.field("score", pa.float64(), nullable=False),
        pa.field("note", pa.string(), nullable=True),
    ]
)

SCHEMA = pa.schema(
    [
        pa.field("sample_id", pa.string(), nullable=False),
        pa.field(
            "read_counts",
            pa.map_(pa.string(), pa.field("value", pa.int64(), nullable=False)),
            nullable=False,
        ),
        pa.field(
            "taxid_hits",
            pa.map_(pa.int32(), pa.field("value", HIT_DETAIL_STRUCT, nullable=True), keys_sorted=True),
            nullable=True,
        ),
    ],
    metadata={"title": "SampleReadMap"},
)
