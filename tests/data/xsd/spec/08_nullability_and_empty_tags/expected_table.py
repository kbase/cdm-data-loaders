"""Expected PyArrow table for 08_nullability_and_empty_tags (xml_tag='entry')."""

# ruff: noqa: INP001

import pyarrow as pa
from expected_schema import EXPECTED_SCHEMA

EXPECTED_TABLE = pa.Table.from_pylist(
    [
        {"_id": "e1", "name": "BRCA1", "summary": "", "geneId": 672, "comment": {"_type": "function"}},
    ],
    schema=EXPECTED_SCHEMA,
)
