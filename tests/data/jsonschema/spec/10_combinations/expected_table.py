"""Expected PyArrow table for 10_combinations."""

# ruff: noqa: INP001

import datetime as dt

import pyarrow as pa
from expected_schema import EXPECTED_SCHEMA

EXPECTED_TABLE = pa.Table.from_pylist(
    [
        {
            "schema_version": 1,
            "record_id": "comb1",
            "active": True,
            "count": 42,
            "ratio": 0.875,
            "release_date": dt.date(2025, 8, 15),
            "timestamp_str": "2025-08-15T09:00:00Z",
            "payload_b64": b"CDM",
            "payload_sha256": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
            "score_or_label": "98.5",
            "range_tuple": {"item_0": 100, "item_1": 500, "item_2": "+"},
            "tags": ["complete", "reference"],
            "annotations": {"bioproject": "PRJNA225", "biosample": "SAMN02604091"},
            "source_choice": {"kind": "genbank", "accession": "GCF_000005845.2"},
            "lineage_tree": {
                "taxon_name": "Bacteria",
                "rank": "domain",
                "children": [
                    {
                        "taxon_name": "Proteobacteria",
                        "rank": "phylum",
                        "children": ['{"taxon_name": "Gammaproteobacteria", "rank": "class"}'],
                    }
                ],
            },
        },
        {
            "schema_version": 1,
            "record_id": "comb2",
            "active": False,
            "count": 0,
            "ratio": 0.0,
            "release_date": dt.date(2025, 1, 1),
            "score_or_label": None,
            "range_tuple": {"item_0": 1, "item_1": 10},
            "source_choice": {"kind": "publication", "doi": "10.1038/s41586-025-00001-x"},
        },
    ],
    schema=EXPECTED_SCHEMA,
)
