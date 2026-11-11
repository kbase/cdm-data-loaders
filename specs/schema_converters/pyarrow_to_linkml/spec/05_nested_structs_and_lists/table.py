"""Sample PyArrow table for 05_nested_structs_and_lists."""

import pyarrow as pa
from schema import SCHEMA

TABLE = pa.Table.from_pylist(
    [
        {
            "protein_id": "WP_000001.1",
            "source_locus": {"contig": "NC_000913.3", "strand": "+"},
            "ec_numbers": ["2.7.7.7", "3.1.11.2"],
            "quality_flags": [1, 4, 255],
            "coords_3d": [1.25, -0.5, 4.0],
            "domain_hits": [
                {
                    "domain_acc": "PF00476",
                    "start_aa": 12,
                    "end_aa": 298,
                    "evidence_codes": ["ECO:0000259", "ECO:0000313"],
                }
            ],
        },
        {
            "protein_id": "WP_000002.1",
            "source_locus": {"contig": "NC_000913.3", "strand": None},
            "ec_numbers": [],
            "quality_flags": None,
            "coords_3d": [0.0, 0.0, 0.0],
            "domain_hits": [],
        },
    ],
    schema=SCHEMA,
)
