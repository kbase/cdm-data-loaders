"""E9 fixture: empty field name in pa.Schema."""

import pyarrow as pa

SCHEMA = pa.schema([pa.field("", pa.string(), nullable=False)])
