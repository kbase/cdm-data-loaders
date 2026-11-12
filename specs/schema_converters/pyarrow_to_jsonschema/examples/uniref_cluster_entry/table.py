"""Sample PyArrow table for UniRef cluster inventory."""

import datetime as dt

import pyarrow as pa
from schema import SCHEMA

TABLE = pa.Table.from_pylist(
    [
        {
            "cluster_id": "UniRef90_P0A7B8",
            "cluster_name": "Cluster: ATP-dependent Clp protease proteolytic subunit",
            "identity_threshold": 0.9,
            "member_count": 3,
            "updated_at": dt.datetime(2026, 2, 18, 8, 0, 0, tzinfo=dt.UTC).replace(tzinfo=None),
            "representative": {
                "uniprot_acc": "P0A7B8",
                "length_aa": 207,
                "crc64": b"\x12\x34\x56\x78\x9a\xbc\xde\xf0",
            },
            "embedding_preview": [0.125, -0.5, 0.75, 0.0],
            "member_accessions": ["P0A7B8", "P0A7B9", "Q8X685"],
            "taxon_counts": [(562, 2), (83334, 1)],
        },
        {
            "cluster_id": "UniRef50_Q9Y261",
            "cluster_name": "Cluster: Forkhead box protein A2",
            "identity_threshold": 0.5,
            "member_count": 1,
            "updated_at": dt.datetime(2026, 2, 18, 8, 30, 0, tzinfo=dt.UTC).replace(tzinfo=None),
            "representative": {
                "uniprot_acc": "Q9Y261",
                "length_aa": 457,
                "crc64": b"\xfe\xdc\xba\x98\x76\x54\x32\x10",
            },
            "embedding_preview": None,
            "member_accessions": ["Q9Y261"],
            "taxon_counts": [(9606, 1)],
        },
    ],
    schema=SCHEMA,
)
