"""Sample PyArrow table for 04_nullability_and_descriptions."""

import pyarrow as pa
from schema import SCHEMA

TABLE = pa.Table.from_pylist(
    [
        {
            "assay_id": "ASY-1001",
            "od600": 0.825,
            "replicate_num": 1,
            "notes": "Exponential phase",
            "reagents": ["R-10", "R-22"],
        },
        {
            "assay_id": "ASY-1002",
            "od600": None,
            "replicate_num": 2,
            "notes": None,
            "reagents": None,
        },
    ],
    schema=SCHEMA,
)
