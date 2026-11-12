"""PyArrow schema for 04_nullability_and_descriptions."""

import pyarrow as pa

SCHEMA = pa.schema(
    [
        pa.field("assay_id", pa.string(), nullable=False, metadata={"comment": "Primary assay identifier."}),
        pa.field("od600", pa.float64(), nullable=True, metadata={"comment": "Optical density at 600 nm."}),
        pa.field("replicate_num", pa.int16(), nullable=False),
        pa.field("notes", pa.string(), nullable=True),
        pa.field(
            "reagents",
            pa.list_(pa.field("item", pa.string(), nullable=False, metadata={"comment": "Reagent catalog code."})),
            nullable=True,
        ),
    ],
    metadata={
        "title": "AssayMeasurement",
        "$id": "https://kbase.us/linkml/assay_measurement",
        "comment": "High-throughput growth assay measurement records.",
    },
)
