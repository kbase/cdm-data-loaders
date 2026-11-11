"""E6 fixture: pa.duration column."""

import pyarrow as pa

SCHEMA = pa.schema([pa.field("elapsed", pa.duration("us"), nullable=False)])
