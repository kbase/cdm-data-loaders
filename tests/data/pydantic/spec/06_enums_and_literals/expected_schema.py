"""Expected PyArrow schema for 06_enums_and_literals."""

# ruff: noqa: INP001

import pyarrow as pa

EXPECTED_SCHEMA = pa.schema(
    [
        pa.field("id", pa.string(), nullable=False, metadata={"pydantic_type": "string"}),
        pa.field(
            "strand",
            pa.string(),
            nullable=False,
            metadata={"comment": "Genomic strand orientation.", "pydantic_type": "string"},
        ),
        pa.field("priority", pa.int64(), nullable=False, metadata={"pydantic_type": "integer"}),
        pa.field(
            "mixed_code",
            pa.string(),
            nullable=False,
            metadata={"comment": "Mixed integer and string status codes.", "pydantic_type": "integer | string"},
        ),
        pa.field("string_literal", pa.string(), nullable=False, metadata={"pydantic_type": "string"}),
        pa.field("int_literal", pa.int64(), nullable=False, metadata={"pydantic_type": "integer"}),
        pa.field("bool_literal", pa.bool_(), nullable=False, metadata={"pydantic_type": "boolean"}),
        pa.field("num_widened_literal", pa.float64(), nullable=False, metadata={"pydantic_type": "integer | number"}),
        pa.field(
            "heterogeneous_literal",
            pa.string(),
            nullable=False,
            metadata={"pydantic_type": "integer | string | boolean"},
        ),
        pa.field("nullable_literal", pa.string(), nullable=True, metadata={"pydantic_type": "string"}),
    ]
)
