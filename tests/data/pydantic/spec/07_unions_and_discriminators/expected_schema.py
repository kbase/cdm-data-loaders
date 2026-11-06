"""Expected PyArrow schema for 07_unions_and_discriminators."""

import pyarrow as pa

EXPECTED_SCHEMA = pa.schema(
    [
        pa.field("id", pa.string(), nullable=False, metadata={"pydantic_type": "string"}),
        pa.field("numeric_union", pa.float64(), nullable=False, metadata={"pydantic_type": "integer | number"}),
        pa.field(
            "scalar_fallback_union",
            pa.string(),
            nullable=False,
            metadata={"pydantic_type": "integer | string | boolean"},
        ),
        pa.field(
            "source",
            pa.struct(
                [
                    pa.field(
                        "shared_id",
                        pa.string(),
                        nullable=False,
                        metadata={"comment": "Shared source identifier", "pydantic_type": "string"},
                    ),
                    pa.field("platform", pa.string(), nullable=True, metadata={"pydantic_type": "string"}),
                    pa.field("read_length", pa.int64(), nullable=True, metadata={"pydantic_type": "integer"}),
                    pa.field("metric", pa.float64(), nullable=False, metadata={"pydantic_type": "integer | number"}),
                    pa.field("chemistry", pa.string(), nullable=True, metadata={"pydantic_type": "string"}),
                ]
            ),
            nullable=False,
        ),
        pa.field(
            "top_hit",
            pa.struct(
                [
                    pa.field("hit_type", pa.string(), nullable=False, metadata={"pydantic_type": "string"}),
                    pa.field("accession", pa.string(), nullable=False, metadata={"pydantic_type": "string"}),
                    pa.field("evalue", pa.float64(), nullable=True, metadata={"pydantic_type": "number"}),
                    pa.field("bit_score", pa.float64(), nullable=True, metadata={"pydantic_type": "number"}),
                ]
            ),
            nullable=False,
        ),
        pa.field(
            "citation",
            pa.struct(
                [
                    pa.field("pmid", pa.int64(), nullable=True, metadata={"pydantic_type": "integer"}),
                    pa.field("title", pa.string(), nullable=True, metadata={"pydantic_type": "string"}),
                    pa.field("_value", pa.string(), nullable=True, metadata={"pydantic_type": "string"}),
                ]
            ),
            nullable=False,
        ),
    ]
)
