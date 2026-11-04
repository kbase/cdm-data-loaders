"""Expected PyArrow schema for 02_nullability_defaults_and_computed."""

# ruff: noqa: INP001

import pyarrow as pa

EXPECTED_SCHEMA = pa.schema(
    [
        pa.field("id", pa.string(), nullable=False, metadata={"pydantic_type": "string"}),
        pa.field("required_score", pa.int64(), nullable=False, metadata={"pydantic_type": "integer"}),
        pa.field("defaulted_retries", pa.int64(), nullable=False, metadata={"pydantic_type": "integer"}),
        pa.field(
            "default_tags",
            pa.list_(
                pa.field("item", pa.string(), nullable=False, metadata={"pydantic_type": "string"}),
            ),
            nullable=False,
        ),
        pa.field("optional_alias", pa.string(), nullable=True, metadata={"pydantic_type": "string"}),
        pa.field("explicit_nullable", pa.int64(), nullable=True, metadata={"pydantic_type": "integer"}),
        pa.field(
            "summary_label",
            pa.string(),
            nullable=False,
            metadata={"comment": "Synthesized identifier and retry label", "pydantic_type": "string"},
        ),
    ]
)
