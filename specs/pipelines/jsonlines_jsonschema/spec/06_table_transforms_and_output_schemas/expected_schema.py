"""Expected PyArrow schemas for 06_table_transforms_and_output_schemas."""

import pyarrow as pa

SAMPLE_SUMMARY_SCHEMA = pa.schema(
    [
        pa.field("sample_id", pa.string(), nullable=False),
        pa.field("ph", pa.decimal128(4, 2), nullable=False),
        pa.field("ph_bucket", pa.string(), nullable=False),
        pa.field("collected_date", pa.date32(), nullable=False),
        pa.field("collected_at", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("sample_time", pa.time64("us"), nullable=False),
        pa.field("checksum", pa.binary(), nullable=True, metadata={"xsd_type": "xs:hexBinary"}),
    ]
)

SAMPLE_MEASUREMENT_SCHEMA = pa.schema(
    [
        pa.field("sample_id", pa.string(), nullable=False),
        pa.field("metric", pa.string(), nullable=False),
    ]
)

EXPECTED_SCHEMA = SAMPLE_SUMMARY_SCHEMA
EXPECTED_SCHEMAS = {
    "samples": SAMPLE_SUMMARY_SCHEMA,
    "sample_measurements": SAMPLE_MEASUREMENT_SCHEMA,
}
