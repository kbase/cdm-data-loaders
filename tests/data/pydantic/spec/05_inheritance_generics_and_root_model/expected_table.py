"""Expected PyArrow table for 05_inheritance_generics_and_root_model."""

# ruff: noqa: INP001

import pyarrow as pa
from expected_schema import EXPECTED_SCHEMA

EXPECTED_TABLE = pa.Table.from_pylist(
    [
        {
            "id": "env-01",
            "version": 2,
            "updated_by": "curator_a",
            "payload": {"sample_name": "surface_water", "depth_m": 5.0},
            "secondary_payload": {"sample_name": "deep_water", "depth_m": 120.5},
        },
        {
            "id": "env-02",
            "version": 1,
            "updated_by": "curator_b",
            "payload": {"sample_name": "sediment", "depth_m": 250.0},
            "secondary_payload": None,
        },
    ],
    schema=EXPECTED_SCHEMA,
)
