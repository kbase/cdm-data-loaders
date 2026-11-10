"""Expected PyArrow table for 02_delimiters_comments_quotes_and_escapes."""

import pyarrow as pa
from expected_schema import EXPECTED_SCHEMA

EXPECTED_TABLE = pa.Table.from_pylist(
    [
        {"sample_id": "S-1", "measurement": 12.5, "label": "quoted label"},
        {"sample_id": "S#2", "measurement": 8.75, "label": "escaped 'quote' inside"},
    ],
    schema=EXPECTED_SCHEMA,
)
