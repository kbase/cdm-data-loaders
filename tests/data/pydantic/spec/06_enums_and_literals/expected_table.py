"""Expected PyArrow table for 06_enums_and_literals."""

import pyarrow as pa
from expected_schema import EXPECTED_SCHEMA

EXPECTED_TABLE = pa.Table.from_pylist(
    [
        {
            "id": "enum-1",
            "strand": "+",
            "priority": 3,
            "mixed_code": "200",
            "string_literal": "complete",
            "int_literal": 20,
            "bool_literal": True,
            "num_widened_literal": 1.0,
            "heterogeneous_literal": "two",
            "nullable_literal": "active",
        },
        {
            "id": "enum-2",
            "strand": "-",
            "priority": 1,
            "mixed_code": "custom-ok",
            "string_literal": "draft",
            "int_literal": 10,
            "bool_literal": True,
            "num_widened_literal": 2.5,
            "heterogeneous_literal": "true",
            "nullable_literal": None,
        },
    ],
    schema=EXPECTED_SCHEMA,
)
