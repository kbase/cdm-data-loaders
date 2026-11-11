"""E8 fixture: pa.run_end_encoded column."""

import pyarrow as pa

SCHEMA = pa.schema([pa.field("rle_status", pa.run_end_encoded(pa.int32(), pa.string()), nullable=False)])
