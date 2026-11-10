"""Expected PyArrow table for 05_md5_sidecar_preprocessing_and_rejected."""

import pyarrow as pa
from expected_schema import EXPECTED_SCHEMA

EXPECTED_TABLE = pa.Table.from_pylist(
    [
        {"pmid": 1001, "title": "Validated Article"},
    ],
    schema=EXPECTED_SCHEMA,
)
