"""Expected PyArrow table for 05_defs_and_refs."""

# ruff: noqa: INP001

import datetime as dt

import pyarrow as pa
from expected_schema import EXPECTED_SCHEMA

EXPECTED_TABLE = pa.Table.from_pylist(
    [
        {
            "doc_id": "doc1",
            "published_on": dt.date(2025, 7, 1),
            "lead_author": {"name": "Ada Lovelace", "orcid": "0000-0001-2345-6789"},
            "reviewer": {"name": "Grace Hopper"},
            "co_authors": [{"name": "Alan Turing", "orcid": "0000-0002-3456-7890"}],
        },
        {"doc_id": "doc2", "lead_author": {"name": "Barbara Liskov"}},
    ],
    schema=EXPECTED_SCHEMA,
)
