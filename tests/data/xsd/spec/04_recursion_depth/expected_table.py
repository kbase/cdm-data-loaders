"""Expected PyArrow table for 04_recursion_depth (xml_tag='record')."""

# ruff: noqa: INP001

import pyarrow as pa
from expected_schema import EXPECTED_SCHEMA, EXPECTED_SCHEMA_DEPTH_1

EXPECTED_TABLE = pa.Table.from_pylist(
    [
        {
            "_id": "r1",
            "tree": {
                "_label": "L1",
                "_weight": 10,
                "branch": [
                    {
                        "_label": "L2",
                        "branch": [
                            '{"_label": "L3", "_weight": 30, "branch": [{"_label": "L4"}]}',
                            '{"_label": "L3_sibling"}',
                        ],
                    }
                ],
            },
            "Section": {
                "SectionTitle": "Ch 1",
                "Section": [{"SectionTitle": "Sec 1.1", "Section": ['{"SectionTitle": "Subsec 1.1.1"}']}],
            },
        },
    ],
    schema=EXPECTED_SCHEMA,
)


EXPECTED_TABLE_DEPTH_1 = pa.Table.from_pylist(
    [
        {
            "_id": "r1",
            "tree": {
                "_label": "L1",
                "_weight": 10,
                "branch": [
                    '{"_label": "L2", "branch": [{"_label": "L3", "_weight": 30, "branch": [{"_label": "L4"}]}, {"_label": "L3_sibling"}]}'
                ],
            },
            "Section": {
                "SectionTitle": "Ch 1",
                "Section": ['{"SectionTitle": "Sec 1.1", "Section": [{"SectionTitle": "Subsec 1.1.1"}]}'],
            },
        },
    ],
    schema=EXPECTED_SCHEMA_DEPTH_1,
)
