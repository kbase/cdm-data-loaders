"""Expected PyArrow table for 08_aliases_and_field_names."""

import pyarrow as pa
from expected_schema import EXPECTED_SCHEMA, EXPECTED_SCHEMA_BY_ALIAS

EXPECTED_TABLE = pa.Table.from_pylist(
    [
        {
            "record_id": "rec-a",
            "display_name": "Alpha",
            "alt_input": 100,
            "child": {"internal_code": "c-1", "score_val": 9.5},
        },
        {
            "record_id": "rec-b",
            "display_name": "Beta",
            "alt_input": 200,
            "child": {"internal_code": "c-2", "score_val": 4.25},
        },
    ],
    schema=EXPECTED_SCHEMA,
)

EXPECTED_TABLE_BY_ALIAS = pa.Table.from_pylist(
    [
        {
            "recordId": "rec-a",
            "displayName": "Alpha",
            "alt_input": 100,
            "child": {"code": "c-1", "serialized_score": 9.5},
        },
        {
            "recordId": "rec-b",
            "displayName": "Beta",
            "alt_input": 200,
            "child": {"code": "c-2", "serialized_score": 4.25},
        },
    ],
    schema=EXPECTED_SCHEMA_BY_ALIAS,
)
