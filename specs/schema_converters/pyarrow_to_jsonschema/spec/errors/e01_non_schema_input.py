"""E1 fixture: input is a pa.StructType rather than a pa.Schema."""

import pyarrow as pa

SCHEMA = pa.struct([pa.field("id", pa.string(), nullable=False)])
