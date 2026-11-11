"""Sample PyArrow table for 06_maps_and_entry_classes."""

import pyarrow as pa
from schema import SCHEMA

TABLE = pa.Table.from_pylist(
    [
        {
            "sample_id": "SMP-01",
            "read_counts": [("raw", 150000), ("trimmed", 142500)],
            "taxid_hits": [
                (562, {"score": 99.5, "note": "Escherichia coli"}),
                (1280, None),
            ],
        },
        {
            "sample_id": "SMP-02",
            "read_counts": [],
            "taxid_hits": None,
        },
    ],
    schema=SCHEMA,
)
