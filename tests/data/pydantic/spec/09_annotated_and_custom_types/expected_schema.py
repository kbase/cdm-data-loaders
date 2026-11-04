"""Expected PyArrow schema for 09_annotated_and_custom_types."""

# ruff: noqa: INP001

import pyarrow as pa

EXPECTED_SCHEMA = pa.schema(
    [
        pa.field(
            "accession",
            pa.string(),
            nullable=False,
            metadata={"comment": "Base accession string", "pydantic_type": "string"},
        ),
        pa.field(
            "overridden_accession",
            pa.string(),
            nullable=False,
            metadata={"comment": "Overridden accession description", "pydantic_type": "string"},
        ),
        pa.field(
            "coverage",
            pa.decimal128(8, 3),
            nullable=False,
            metadata={"comment": "Sequencing depth coverage", "pydantic_type": "decimal"},
        ),
        pa.field(
            "quality_phred",
            pa.int64(),
            nullable=False,
            metadata={"comment": "Phred quality score", "pydantic_type": "integer"},
        ),
    ]
)
