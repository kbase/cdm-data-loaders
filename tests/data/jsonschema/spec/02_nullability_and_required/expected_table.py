"""Expected PyArrow table for 02_nullability_and_required."""

# ruff: noqa: INP001

import pyarrow as pa
from expected_schema import EXPECTED_SCHEMA

EXPECTED_TABLE = pa.Table.from_pylist(
    [
        {
            "required_str": "alpha",
            "required_int": 10,
            "required_nullable_str": "present",
            "required_anyof_nullable": 99,
            "required_enum_nullable": "high",
            "optional_str": "opt_value",
            "optional_with_default": "explicit",
            "optional_nullable_number": 1.25,
            "strict_int_list": [1, 2, 3],
            "nullable_int_list": [10, None, 30],
            "optional_str_list": ["a", "b"],
        },
        {
            "required_str": "beta",
            "required_int": 0,
            "required_nullable_str": None,
            "required_anyof_nullable": None,
            "required_enum_nullable": None,
            "optional_nullable_number": None,
            "strict_int_list": [],
            "nullable_int_list": [None],
        },
    ],
    schema=EXPECTED_SCHEMA,
)
