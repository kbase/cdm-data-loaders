"""Expected PyArrow schema for 03_missing_header_and_defs_ref."""

import pyarrow as pa

EXPECTED_SCHEMA = pa.schema(
    [
        pa.field(
            "gene_id",
            pa.string(),
            nullable=False,
            metadata={"comment": "Standard gene identifier.", "jsonschema_type": "string"},
        ),
        pa.field(
            "reads",
            pa.int64(),
            nullable=True,
            metadata={"comment": "Read count or null when unmeasured.", "jsonschema_type": "integer"},
        ),
        pa.field(
            "organism",
            pa.string(),
            nullable=True,
            metadata={"comment": "Organism name or null.", "jsonschema_type": "string"},
        ),
    ]
)
