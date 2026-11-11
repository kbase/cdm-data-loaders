"""E6 fixture: pa.month_day_nano_interval column."""

import pyarrow as pa

SCHEMA = pa.schema([pa.field("window_span", pa.month_day_nano_interval(), nullable=False)])
