"""E1 fixture: input is a pa.Field rather than a pa.Schema."""

import pyarrow as pa

SCHEMA = pa.field("id", pa.string(), nullable=False)
