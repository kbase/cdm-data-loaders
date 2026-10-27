"""Expected PyArrow table for 08_conditional_and_dependent_schemas."""

# ruff: noqa: INP001

import pyarrow as pa
from expected_schema import EXPECTED_SCHEMA

EXPECTED_TABLE = pa.Table.from_pylist(
    [
        {
            "id": "cond1",
            "specimen_type": "isolate",
            "sequencing_depth": 120.5,
            "depth_unit": "x",
            "platform": "Illumina NovaSeq",
            "strain_name": "K-12 MG1655",
        },
        {"id": "cond2", "specimen_type": "metagenome", "biome": "marine pelagic"},
    ],
    schema=EXPECTED_SCHEMA,
)
