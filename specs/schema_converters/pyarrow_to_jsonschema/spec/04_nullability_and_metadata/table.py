"""Sample PyArrow table for 04_nullability_and_metadata."""

import pyarrow as pa
from schema import SCHEMA

TABLE = pa.Table.from_pylist(
    [
        {
            "sample_id": "SAMN00010001",
            "depth_m": 125.5,
            "quality_score": 98,
            "optional_details": {"collector": "A. Smith", "ph": 7.25},
        },
        {
            "sample_id": "SAMN00010002",
            "depth_m": None,
            "quality_score": None,
            "optional_details": None,
        },
    ],
    schema=SCHEMA,
)
