"""Expected PyArrow table for 03_blank_lines_parse_errors_and_format_rejections."""

import pyarrow as pa
from expected_schema import EXPECTED_SCHEMA

EXPECTED_TABLE = pa.Table.from_pylist(
    [
        {"id": "rec-1", "count": 5, "contact_email": "alice@example.org"},
        {"id": "rec-4", "count": 12, "contact_email": "carol@example.org"},
    ],
    schema=EXPECTED_SCHEMA,
)
