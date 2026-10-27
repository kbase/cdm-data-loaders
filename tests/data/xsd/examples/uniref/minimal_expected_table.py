"""Expected PyArrow table for uniref minimal.xml (xml_tag='entry')."""

# ruff: noqa: INP001

import datetime as dt

import pyarrow as pa
from expected_schema import EXPECTED_SCHEMA

EXPECTED_TABLE = pa.Table.from_pylist(
    [
        {
            "_id": "UniRef100_P04637",
            "_updated": dt.date(2025, 2, 5),
            "name": "Cluster: Glycogen synthase",
            "representativeMember": {"dbReference": {"_type": "UniProtKB ID", "_id": "P53_HUMAN"}},
        },
    ],
    schema=EXPECTED_SCHEMA,
)
