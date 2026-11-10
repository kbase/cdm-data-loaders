"""Expected PyArrow table for 06_multi_file_glob_and_subdirs."""

import pyarrow as pa
from expected_schema import EXPECTED_SCHEMA

EXPECTED_TABLE = pa.Table.from_pylist(
    [
        {"sample_num": 1, "sample_name": "alpha"},
        {"sample_num": 2, "sample_name": "beta"},
        {"sample_num": 4, "sample_name": "delta"},
    ],
    schema=EXPECTED_SCHEMA,
)
