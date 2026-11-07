"""Expected PyArrow schema for 05_inheritance_generics_and_root_model."""

import pyarrow as pa

EXPECTED_SCHEMA = pa.schema(
    [
        pa.field("id", pa.string(), nullable=False, metadata={"pydantic_type": "string"}),
        pa.field(
            "version",
            pa.int64(),
            nullable=False,
            metadata={"comment": "Overridden integer version", "pydantic_type": "integer"},
        ),
        pa.field("updated_by", pa.string(), nullable=False, metadata={"pydantic_type": "string"}),
        pa.field(
            "payload",
            pa.struct(
                [
                    pa.field("sample_name", pa.string(), nullable=False, metadata={"pydantic_type": "string"}),
                    pa.field("depth_m", pa.float64(), nullable=False, metadata={"pydantic_type": "number"}),
                ]
            ),
            nullable=False,
        ),
        pa.field(
            "secondary_payload",
            pa.struct(
                [
                    pa.field("sample_name", pa.string(), nullable=False, metadata={"pydantic_type": "string"}),
                    pa.field("depth_m", pa.float64(), nullable=False, metadata={"pydantic_type": "number"}),
                ]
            ),
            nullable=True,
        ),
    ]
)
