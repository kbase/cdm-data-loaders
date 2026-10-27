"""Expected PyArrow table for 05_groups_and_compositors (xml_tag='item')."""

# ruff: noqa: INP001

import pyarrow as pa
from expected_schema import EXPECTED_SCHEMA

EXPECTED_TABLE = pa.Table.from_pylist(
    [
        {
            "_createdBy": "alice",
            "_version": 1,
            "pagination": {"StartPage": "10", "EndPage": "20", "MedlinePgn": "10-20"},
            "interaction": {"interactant": ["P12345", "Q67890"]},
            "meta": {"author": "Bob", "year": 2025},
            "pairs": {"k": ["a", "b"], "v": [1, 2]},
        },
        {"pagination": {"MedlinePgn": "e12345"}},
    ],
    schema=EXPECTED_SCHEMA,
)
