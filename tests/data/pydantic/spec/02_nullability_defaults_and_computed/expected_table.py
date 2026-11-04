"""Expected PyArrow table for 02_nullability_defaults_and_computed."""

# ruff: noqa: INP001

import pyarrow as pa
from expected_schema import EXPECTED_SCHEMA

EXPECTED_TABLE = pa.Table.from_pylist(
    [
        {
            "id": "job-10",
            "required_score": 95,
            "defaulted_retries": 5,
            "default_tags": ["urgent", "batch"],
            "optional_alias": "primary-job",
            "explicit_nullable": 12,
            "summary_label": "job-10:r5",
        },
        {
            "id": "job-20",
            "required_score": 60,
            "defaulted_retries": 3,
            "default_tags": ["general"],
            "explicit_nullable": None,
            "summary_label": "job-20:r3",
        },
    ],
    schema=EXPECTED_SCHEMA,
)
