"""Expected PyArrow table for 04_null_regex_and_null_cols."""

import pyarrow as pa
from expected_schema import EXPECTED_SCHEMA

EXPECTED_TABLE = pa.Table.from_pylist(
    [
        {"site": "station-1", "depth": 10, "ph": 7.2, "raw_code": "NA"},
        {"site": "station-2", "depth": None, "ph": None, "raw_code": "-"},
        {"site": "station-3", "depth": None, "ph": 6.8, "raw_code": "null"},
        {"site": "station-4", "depth": 25, "ph": None, "raw_code": "OK"},
    ],
    schema=EXPECTED_SCHEMA,
)
