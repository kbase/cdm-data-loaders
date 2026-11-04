"""Expected PyArrow table for 04_nested_models_and_maps."""

# ruff: noqa: INP001

import pyarrow as pa
from expected_schema import EXPECTED_SCHEMA

EXPECTED_TABLE = pa.Table.from_pylist(
    [
        {
            "id": "asm-100",
            "organism": {"taxon_id": 562, "scientific_name": "Escherichia coli", "strain": "K-12"},
            "stats": {"contigs": 1, "gc_percent": 50.8},
            "submitter": {"institution": "Lawrence Berkeley National Laboratory", "department": "Biosciences"},
            "feature_counts": {"cds": 4300, "trna": 86},
            "optional_metrics": {"completeness": 99.9, "contamination": None},
            "extra_organisms": {"host": {"taxon_id": 9606, "scientific_name": "Homo sapiens"}},
        },
        {
            "id": "asm-200",
            "organism": {"taxon_id": 1423, "scientific_name": "Bacillus subtilis"},
            "stats": {"contigs": 4, "gc_percent": 43.5},
            "submitter": None,
            "feature_counts": {"cds": 4100},
            "optional_metrics": {"completeness": 98.2},
            "extra_organisms": {},
        },
    ],
    schema=EXPECTED_SCHEMA,
)
