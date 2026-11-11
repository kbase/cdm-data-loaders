"""E4 fixture: standalone pa.null() column."""

import pyarrow as pa

SCHEMA = pa.schema([pa.field("nothing", pa.null(), nullable=True)])
