"""E10 fixture: directly nested 2D list pa.list_(pa.list_(...))."""

import pyarrow as pa

SCHEMA = pa.schema([pa.field("matrix", pa.list_(pa.list_(pa.int64())), nullable=False)])
