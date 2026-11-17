"""Expected PyArrow tables for 02_defs_ref_allof_and_draft_dialects."""

import datetime as dt

import pyarrow as pa
from expected_schema import SUBMISSIONS_SCHEMA, XSV_DIALECT_META_SCHEMA

EXPECTED_SUBMISSIONS_TABLE = pa.Table.from_pylist(
    [
        {
            "submission_id": "SUB-01",
            "priority": 2,
            "submitted_date": dt.date(2025, 3, 15),
            "contact": {
                "email": "pi@university.edu",
                "orcid": "0000-0001-2345-6789",
                "department": "Genomics",
            },
        },
        {
            "submission_id": "SUB-02",
            "priority": 8,
            "submitted_date": dt.date(2025, 3, 16),
            "contact": {
                "email": "curator@institute.org",
                "orcid": None,
                "department": "Curation",
            },
        },
    ],
    schema=SUBMISSIONS_SCHEMA,
)

EXPECTED_XSV_META_TABLE = pa.Table.from_pylist(
    [
        {
            "batch_id": "B-100",
            "created_date": dt.date(2025, 4, 1),
        },
    ],
    schema=XSV_DIALECT_META_SCHEMA,
)

EXPECTED_TABLE = EXPECTED_SUBMISSIONS_TABLE
EXPECTED_TABLES = {
    "submissions": EXPECTED_SUBMISSIONS_TABLE,
    "xsv_dialect_meta": EXPECTED_XSV_META_TABLE,
}
