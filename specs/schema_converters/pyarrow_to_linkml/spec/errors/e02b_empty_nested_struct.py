"""E2 fixture: nested pa.struct has zero child fields."""

import pyarrow as pa

SCHEMA = pa.schema([pa.field("empty_meta", pa.struct([]), nullable=False)])
