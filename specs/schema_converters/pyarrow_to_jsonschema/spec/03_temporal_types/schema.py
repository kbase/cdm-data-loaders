"""PyArrow schema for 03_temporal_types."""

import pyarrow as pa

SCHEMA = pa.schema(
    [
        pa.field("d32", pa.date32(), nullable=False),
        pa.field("d64", pa.date64(), nullable=False),
        pa.field("t32_s", pa.time32("s"), nullable=False),
        pa.field("t32_ms", pa.time32("ms"), nullable=False),
        pa.field("t64_us", pa.time64("us"), nullable=False),
        pa.field("t64_ns", pa.time64("ns"), nullable=False),
        pa.field("ts_s_utc", pa.timestamp("s", tz="UTC"), nullable=False),
        pa.field("ts_ms_utc", pa.timestamp("ms", tz="UTC"), nullable=False),
        pa.field("ts_us_la", pa.timestamp("us", tz="America/Los_Angeles"), nullable=False),
        pa.field("ts_ns_utc", pa.timestamp("ns", tz="UTC"), nullable=False),
        pa.field("ts_naive_us", pa.timestamp("us"), nullable=False),
        pa.field("ts_naive_ns_opt", pa.timestamp("ns"), nullable=True),
    ]
)
