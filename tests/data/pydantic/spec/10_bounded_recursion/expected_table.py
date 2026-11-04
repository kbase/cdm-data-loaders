"""Expected PyArrow table for 10_bounded_recursion."""

# ruff: noqa: INP001

import pyarrow as pa
from expected_schema import EXPECTED_SCHEMA, EXPECTED_SCHEMA_DEPTH_1

EXPECTED_TABLE = pa.Table.from_pylist(
    [
        {
            "root_taxon": {
                "rank": "domain",
                "name": "Bacteria",
                "children": [
                    {
                        "rank": "phylum",
                        "name": "Pseudomonadota",
                        "children": ['{"rank": "class", "name": "Gammaproteobacteria", "children": []}'],
                    },
                ],
            },
            "entry_step": {
                "step_id": "step-1",
                "reaction": {
                    "ec_number": "2.7.1.1",
                    "next_step": {"step_id": "step-2", "reaction": {"ec_number": "5.3.1.9", "next_step": None}},
                },
            },
        },
    ],
    schema=EXPECTED_SCHEMA,
)

EXPECTED_TABLE_DEPTH_1 = pa.Table.from_pylist(
    [
        {
            "root_taxon": {
                "rank": "domain",
                "name": "Bacteria",
                "children": [
                    '{"rank": "phylum", "name": "Pseudomonadota", "children": [{"rank": "class", "name": "Gammaproteobacteria", "children": []}]}',
                ],
            },
            "entry_step": {
                "step_id": "step-1",
                "reaction": {
                    "ec_number": "2.7.1.1",
                    "next_step": '{"step_id": "step-2", "reaction": {"ec_number": "5.3.1.9", "next_step": null}}',
                },
            },
        },
    ],
    schema=EXPECTED_SCHEMA_DEPTH_1,
)
