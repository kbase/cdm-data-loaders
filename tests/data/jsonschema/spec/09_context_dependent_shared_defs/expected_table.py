"""Expected PyArrow table for 09_context_dependent_shared_defs."""

# ruff: noqa: INP001

import pyarrow as pa
from expected_schema import EXPECTED_SCHEMA

EXPECTED_TABLE = pa.Table.from_pylist(
    [
        {
            "id": "ctx1",
            "primary_tag": {"key": "domain", "value": "Bacteria"},
            "secondary_tag": {"key": "phylum", "value": "Proteobacteria"},
            "fallback_tag": {"key": "status", "value": "curated"},
            "tag_list": [{"key": "gram", "value": "negative"}, {"key": "motile"}],
            "tag_lookup": {"ncbi": {"key": "taxid", "value": "562"}},
        },
        {"id": "ctx2", "primary_tag": {"key": "domain", "value": "Archaea"}, "fallback_tag": None},
    ],
    schema=EXPECTED_SCHEMA,
)
