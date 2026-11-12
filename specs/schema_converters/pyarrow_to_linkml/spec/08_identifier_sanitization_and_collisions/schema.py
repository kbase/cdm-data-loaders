"""PyArrow schema for 08_identifier_sanitization_and_collisions."""

import pyarrow as pa

SCHEMA = pa.schema(
    [
        pa.field(
            "meta_info",
            pa.struct([pa.field("source", pa.string(), nullable=False)]),
            nullable=False,
        ),
        pa.field(
            "meta",
            pa.struct(
                [
                    pa.field(
                        "info",
                        pa.struct([pa.field("version", pa.int32(), nullable=False)]),
                        nullable=False,
                    )
                ]
            ),
            nullable=False,
        ),
        pa.field(
            "labels",
            pa.map_(pa.string(), pa.field("value", pa.string(), nullable=False)),
            nullable=False,
        ),
        pa.field(
            "labels_map",
            pa.struct([pa.field("note", pa.string(), nullable=True)]),
            nullable=True,
        ),
    ],
    metadata={
        "title": "OverriddenTitle",
        "$id": "urn:overridden",
    },
)
