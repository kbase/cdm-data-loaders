"""E8 fixture: Arrow extension type column (pa.uuid())."""

import pyarrow as pa

SCHEMA = pa.schema([pa.field("record_uuid", pa.uuid(), nullable=False)])
