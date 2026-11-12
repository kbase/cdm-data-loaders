"""PyArrow schema for 07_maps_and_dictionaries."""

import pyarrow as pa

ANNOTATION_VALUE_STRUCT = pa.struct(
    [
        pa.field("db", pa.string(), nullable=False),
        pa.field("evalue", pa.float64(), nullable=True),
    ]
)

SCHEMA = pa.schema(
    [
        pa.field("entry_id", pa.string(), nullable=False),
        pa.field(
            "biotype",
            pa.dictionary(pa.int16(), pa.string(), ordered=False),
            nullable=False,
            metadata={"comment": "Controlled biotype term."},
        ),
        pa.field(
            "priority_tier",
            pa.dictionary(pa.int8(), pa.int64(), ordered=True),
            nullable=True,
        ),
        pa.field(
            "rate_bucket",
            pa.dictionary(pa.int8(), pa.decimal128(8, 2), ordered=False),
            nullable=False,
        ),
        pa.field(
            "xref_hits",
            pa.map_(
                pa.field("key", pa.string(), nullable=False, metadata={"comment": "External database key."}),
                pa.field("value", ANNOTATION_VALUE_STRUCT, nullable=True),
            ),
            nullable=False,
        ),
        pa.field(
            "coverage_by_bin",
            pa.map_(
                pa.int32(),
                pa.field("value", pa.float64(), nullable=False),
                keys_sorted=True,
            ),
            nullable=True,
        ),
    ]
)
