"""Expected PyArrow table for 07_combinators_allof_anyof_oneof."""

# ruff: noqa: INP001

import pyarrow as pa
from expected_schema import EXPECTED_SCHEMA

EXPECTED_TABLE = pa.Table.from_pylist(
    [
        {
            "entity_id": "ent1",
            "created_by": "pipeline",
            "sample_name": "E. coli K-12",
            "location": {"storage_kind": "s3", "bucket": "cdm-lake", "key": "genomes/ecoli.fna.gz"},
            "pagination": {"start_page": "101", "end_page": "115"},
            "identifier_union": "12345",
            "note": {"text": "Structured note", "author": "curator"},
        },
        {
            "entity_id": "ent2",
            "sample_name": "B. subtilis",
            "location": {"storage_kind": "http", "url": "https://example.org/bsub.fna.gz", "checksum": "sha256:abc123"},
            "pagination": {"medline_pgn": "e10042"},
            "identifier_union": "GCF_000009045.1",
            "note": {"_value": "Plain text note"},
        },
    ],
    schema=EXPECTED_SCHEMA,
)
