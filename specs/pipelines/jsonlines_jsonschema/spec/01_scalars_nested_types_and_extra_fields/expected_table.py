"""Expected PyArrow table for 01_scalars_nested_types_and_extra_fields."""

import pyarrow as pa
from expected_schema import EXPECTED_SCHEMA

EXPECTED_TABLE = pa.Table.from_pylist(
    [
        {
            "genome_id": "GCF_0001",
            "contig_count": 1,
            "gc_content": 50.8,
            "is_complete": True,
            "strain": "K-12",
            "tags": ["reference", "gut"],
            "assembly_stats": {"n50": 4641652, "source_lab": "NCBI"},
        },
        {
            "genome_id": "GCF_0002",
            "contig_count": 42,
            "gc_content": 38.2,
            "is_complete": False,
            "strain": None,
            "tags": None,
            "assembly_stats": None,
        },
    ],
    schema=EXPECTED_SCHEMA,
)
