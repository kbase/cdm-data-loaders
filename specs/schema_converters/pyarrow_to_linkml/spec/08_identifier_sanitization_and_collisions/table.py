"""Sample PyArrow table for 08_identifier_sanitization_and_collisions."""

import pyarrow as pa
from schema import SCHEMA

TABLE = pa.Table.from_pylist(
    [
        {
            "meta_info": {"source": "IMG/M"},
            "meta": {"info": {"version": 4}},
            "labels": [("env", "soil")],
            "labels_map": {"note": "collides with labels map class"},
        },
        {
            "meta_info": {"source": "GTDB"},
            "meta": {"info": {"version": 220}},
            "labels": [],
            "labels_map": None,
        },
    ],
    schema=SCHEMA,
)
