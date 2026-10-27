"""Expected PyArrow table for 06_recursion_depth."""

# ruff: noqa: INP001

import pyarrow as pa
from expected_schema import EXPECTED_SCHEMA, EXPECTED_SCHEMA_DEPTH_1

EXPECTED_TABLE = pa.Table.from_pylist(
    [
        {
            "id": "r1",
            "tree": {
                "label": "L1",
                "weight": 10,
                "branch": [
                    {
                        "label": "L2",
                        "branch": [
                            '{"label": "L3", "weight": 30, "branch": [{"label": "L4"}]}',
                            '{"label": "L3_sibling"}',
                        ],
                    }
                ],
            },
            "section": {
                "section_title": "Ch 1",
                "subsection": {
                    "subsection_title": "Sub 1.a",
                    "nested_sections": [
                        {
                            "section_title": "Sec 1.1",
                            "subsection": {
                                "subsection_title": "Sub 1.1.a",
                                "nested_sections": ['{"section_title": "Subsec 1.1.1"}'],
                            },
                        }
                    ],
                },
            },
        },
    ],
    schema=EXPECTED_SCHEMA,
)

EXPECTED_TABLE_DEPTH_1 = pa.Table.from_pylist(
    [
        {
            "id": "r1",
            "tree": {
                "label": "L1",
                "weight": 10,
                "branch": [
                    '{"label": "L2", "branch": [{"label": "L3", "weight": 30, "branch": [{"label": "L4"}]}, {"label": "L3_sibling"}]}'
                ],
            },
            "section": {
                "section_title": "Ch 1",
                "subsection": {
                    "subsection_title": "Sub 1.a",
                    "nested_sections": [
                        '{"section_title": "Sec 1.1", "subsection": {"subsection_title": "Sub 1.1.a", "nested_sections": [{"section_title": "Subsec 1.1.1"}]}}'
                    ],
                },
            },
        },
    ],
    schema=EXPECTED_SCHEMA_DEPTH_1,
)
