"""E5 fixture: decimal128 with negative scale."""

import pyarrow as pa

SCHEMA = pa.schema([pa.field("scaled_amount", pa.decimal128(6, -2), nullable=False)])
