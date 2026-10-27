"""Expected PyArrow table for 09_empty_marker_elements (xml_tag='book')."""

# ruff: noqa: INP001

import pyarrow as pa
from expected_schema import EXPECTED_SCHEMA

EXPECTED_TABLE = pa.Table.from_pylist(
    [
        {"title": "In-Process Book", "BookInProcess": "", "flag": ["", ""]},
        {"title": "Completed Book"},
    ],
    schema=EXPECTED_SCHEMA,
)
