"""Expected PyArrow table for 07_unions_and_discriminators."""

# ruff: noqa: INP001

import pyarrow as pa
from expected_schema import EXPECTED_SCHEMA

EXPECTED_TABLE = pa.Table.from_pylist(
    [
        {
            "id": "u-1",
            "numeric_union": 10.0,
            "scalar_fallback_union": "42",
            "source": {"shared_id": "src-illumina", "platform": "Illumina NovaSeq", "read_length": 150, "metric": 99.0},
            "top_hit": {"hit_type": "blast", "accession": "NP_414542.1", "evalue": 1e-50},
            "citation": {"pmid": 12345678, "title": "Genome sequence of E. coli"},
        },
        {
            "id": "u-2",
            "numeric_union": 3.5,
            "scalar_fallback_union": "true",
            "source": {"shared_id": "src-nanopore", "metric": 14.25, "chemistry": "R10.4.1"},
            "top_hit": {"hit_type": "hmm", "accession": "PF00001.24", "bit_score": 210.4},
            "citation": {"_value": "Unpublished observation"},
        },
    ],
    schema=EXPECTED_SCHEMA,
)
