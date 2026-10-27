"""Expected PyArrow table for 04_objects_and_maps."""

# ruff: noqa: INP001

import pyarrow as pa
from expected_schema import EXPECTED_SCHEMA

EXPECTED_TABLE = pa.Table.from_pylist(
    [
        {
            "id": "obj1",
            "closed_meta": {"owner": "alice", "priority": 1},
            "open_meta": {"source": "ncbi"},
            "string_to_int_map": {"reads": 1500, "contigs": 12},
            "nullable_score_map": {"completeness": 99.4, "contamination": None},
            "pattern_metric_map": {"metric_1": 0.95, "metric_2": 0.88},
            "struct_map": {"kmer_31": {"count": 42000, "note": "canonical"}, "kmer_21": {"count": 38000}},
        },
        {"id": "obj2", "closed_meta": {"owner": "bob"}},
    ],
    schema=EXPECTED_SCHEMA,
)
