"""Expected PyArrow table for 02_defs_ref_allof_and_draft_dialects."""

import datetime as dt

import pyarrow as pa
from expected_schema import EXPECTED_SCHEMA

EXPECTED_TABLE = pa.Table.from_pylist(
    [
        {
            "submission_id": "SUB-01",
            "submitted_date": dt.date(2025, 3, 15),
            "contact": {"email": "pi@university.edu", "orcid": "0000-0001-2345-6789"},
        },
        {
            "submission_id": "SUB-02",
            "submitted_date": dt.date(2025, 3, 16),
            "contact": {"email": "curator@institute.org", "orcid": None},
        },
    ],
    schema=EXPECTED_SCHEMA,
)
