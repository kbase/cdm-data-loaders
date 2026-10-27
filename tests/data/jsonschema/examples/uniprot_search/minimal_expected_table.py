"""Expected PyArrow table for minimal real-world example uniprot_search."""

# ruff: noqa: INP001

import pyarrow as pa
from expected_schema import EXPECTED_SCHEMA

EXPECTED_TABLE = pa.Table.from_pylist(
    [
        {"results": [{"entryType": "UniProtKB unreviewed (TrEMBL)", "primaryAccession": "X5IFX5"}]},
    ],
    schema=EXPECTED_SCHEMA,
)
