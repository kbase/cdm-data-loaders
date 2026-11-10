"""Expected PyArrow tables for 02_self_nested_tags_and_recursion_depth."""

import pyarrow as pa
from expected_schema import EXPECTED_SCHEMA, EXPECTED_SCHEMA_DEPTH_1

EXPECTED_TABLE = pa.Table.from_pylist(
    [
        {
            "_id": "cat-1",
            "title": "Bacteria",
            "category": [
                {
                    "_id": "cat-1-1",
                    "title": "Proteobacteria",
                    "category": ['{"_id": "cat-1-1-1", "title": "Gammaproteobacteria"}'],
                }
            ],
        },
        {
            "_id": "cat-2",
            "title": "Archaea",
            "category": None,
        },
    ],
    schema=EXPECTED_SCHEMA,
)

EXPECTED_TABLE_DEPTH_1 = pa.Table.from_pylist(
    [
        {
            "_id": "cat-1",
            "title": "Bacteria",
            "category": [
                '{"_id": "cat-1-1", "title": "Proteobacteria", "category": [{"_id": "cat-1-1-1", "title": "Gammaproteobacteria"}]}'
            ],
        },
        {
            "_id": "cat-2",
            "title": "Archaea",
            "category": None,
        },
    ],
    schema=EXPECTED_SCHEMA_DEPTH_1,
)
