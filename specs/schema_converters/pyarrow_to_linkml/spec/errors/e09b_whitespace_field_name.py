"""E9 fixture: whitespace-only field name in pa.Schema."""

import pyarrow as pa

SCHEMA = pa.schema([pa.field("   ", pa.int64(), nullable=False)])
