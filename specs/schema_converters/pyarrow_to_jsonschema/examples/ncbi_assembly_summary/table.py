"""Sample PyArrow table for NCBI Datasets assembly summary."""

import datetime as dt
from decimal import Decimal

import pyarrow as pa
from schema import SCHEMA

TABLE = pa.Table.from_pylist(
    [
        {
            "assembly_accession": "GCF_000005845.2",
            "bioproject": "PRJNA57779",
            "tax_id": 511145,
            "is_reference": True,
            "assembly_level": "Complete Genome",
            "release_date": dt.date(2013, 9, 26),
            "ingested_at": dt.datetime(2026, 4, 10, 16, 0, 0, tzinfo=dt.UTC),
            "gc_percent": Decimal("50.79"),
            "fasta_md5": b"\x01\x23\x45\x67\x89\xab\xcd\xef\xfe\xdc\xba\x98\x76\x54\x32\x10",
            "replicons": [
                {"accession": "NC_000913.3", "molecule_type": "Chromosome", "length_bp": 4641652},
            ],
        },
        {
            "assembly_accession": "GCA_009914755.4",
            "bioproject": None,
            "tax_id": 9606,
            "is_reference": False,
            "assembly_level": "Scaffold",
            "release_date": dt.date(2022, 3, 31),
            "ingested_at": dt.datetime(2026, 4, 10, 16, 5, 30, tzinfo=dt.UTC),
            "gc_percent": None,
            "fasta_md5": b"\xaa\xbb\xcc\xdd\xee\xff\x00\x11\x22\x33\x44\x55\x66\x77\x88\x99",
            "replicons": [],
        },
    ],
    schema=SCHEMA,
)
