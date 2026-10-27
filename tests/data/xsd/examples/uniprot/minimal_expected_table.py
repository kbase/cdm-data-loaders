"""Expected PyArrow table for uniprot minimal.xml (xml_tag='entry')."""

# ruff: noqa: INP001

import datetime as dt

import pyarrow as pa
from expected_schema import EXPECTED_SCHEMA

EXPECTED_TABLE = pa.Table.from_pylist(
    [
        {
            "_dataset": "Swiss-Prot",
            "_created": dt.date(1986, 8, 13),
            "_modified": dt.date(2024, 5, 29),
            "_version": 285,
            "accession": ["P04637"],
            "name": ["P53_HUMAN"],
            "protein": {"recommendedName": {"fullName": {"#text": "Cellular tumor antigen p53"}}},
            "organism": {
                "name": [{"_type": "synonym", "#text": "Homo sapiens"}],
                "dbReference": [{"_type": "UniProtKB ID", "_id": "P53_HUMAN"}],
            },
            "reference": [
                {
                    "_key": "1",
                    "citation": {"_type": "journal article"},
                    "scope": ["NUCLEOTIDE SEQUENCE [LARGE SCALE GENOMIC DNA]"],
                }
            ],
            "proteinExistence": {"_type": "evidence at protein level"},
            "sequence": {
                "_length": 393,
                "_mass": 43653,
                "_checksum": "AD5C149FD8106131",
                "_modified": dt.date(2009, 11, 24),
                "_version": 1,
                "#text": "MEEPQSDPSVEPPLSQETFSDLWKLLPENNVLSPLPSQAMDDLMLSPDDIEQWFTEDPGP",
            },
        },
    ],
    schema=EXPECTED_SCHEMA,
)
