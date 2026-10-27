"""Expected PyArrow table for 06_context_dependent_paths (xml_tag='root')."""

# ruff: noqa: INP001

import pyarrow as pa
from expected_schema import EXPECTED_SCHEMA

EXPECTED_TABLE = pa.Table.from_pylist(
    [
        {
            "left": {"node": {"leaf": "l1"}},
            "right": {"node": {"leaf": [{"#text": "r1"}, {"_lang": "en", "#text": "r2"}]}},
        },
    ],
    schema=EXPECTED_SCHEMA,
)
