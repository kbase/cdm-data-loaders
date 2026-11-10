"""Expected PyArrow table for 03_missing_header_and_defs_ref."""

import pyarrow as pa
from expected_schema import EXPECTED_SCHEMA

EXPECTED_TABLE = pa.Table.from_pylist(
    [
        {"gene_id": "G0001", "reads": 1500, "organism": "Escherichia coli"},
        {"gene_id": "G0002", "reads": None, "organism": "Bacillus subtilis"},
        {"gene_id": "G0003", "reads": 0, "organism": "Saccharomyces cerevisiae"},
    ],
    schema=EXPECTED_SCHEMA,
)
