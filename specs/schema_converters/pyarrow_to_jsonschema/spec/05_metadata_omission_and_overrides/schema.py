"""PyArrow schema for 05_metadata_omission_and_overrides."""

import pyarrow as pa

SCHEMA = pa.schema(
    [
        pa.field("id", pa.string(), nullable=False, metadata={"comment": "Record identifier.", "xsd_type": "xs:ID"}),
        pa.field("count", pa.uint16(), nullable=False, metadata={"unit": "reads"}),
        pa.field("gc_ratio", pa.decimal128(5, 4), nullable=True, metadata={"comment": "GC fraction."}),
        pa.field("digest", pa.binary(8), nullable=False),
        pa.field("collected_on", pa.date32(), nullable=True),
    ],
    metadata={
        "title": "OverriddenTitle",
        "$id": "urn:overridden:sample",
        "comment": "Pure Draft 2020-12 schema with x-pyarrow suppressed.",
        "internal_tag": "drop-when-omitted",
    },
)
