"""Expected PyArrow schema for 08_aliases_and_field_names."""

import pyarrow as pa

EXPECTED_SCHEMA = pa.schema(
    [
        pa.field(
            "record_id",
            pa.string(),
            nullable=False,
            metadata={"comment": "Record identifier", "pydantic_type": "string"},
        ),
        pa.field("display_name", pa.string(), nullable=False, metadata={"pydantic_type": "string"}),
        pa.field("alt_input", pa.int64(), nullable=False, metadata={"pydantic_type": "integer"}),
        pa.field(
            "child",
            pa.struct(
                [
                    pa.field("internal_code", pa.string(), nullable=False, metadata={"pydantic_type": "string"}),
                    pa.field("score_val", pa.float64(), nullable=False, metadata={"pydantic_type": "number"}),
                ]
            ),
            nullable=False,
        ),
    ]
)

EXPECTED_SCHEMA_BY_ALIAS = pa.schema(
    [
        pa.field(
            "recordId",
            pa.string(),
            nullable=False,
            metadata={"comment": "Record identifier", "pydantic_type": "string"},
        ),
        pa.field("displayName", pa.string(), nullable=False, metadata={"pydantic_type": "string"}),
        pa.field("alt_input", pa.int64(), nullable=False, metadata={"pydantic_type": "integer"}),
        pa.field(
            "child",
            pa.struct(
                [
                    pa.field("code", pa.string(), nullable=False, metadata={"pydantic_type": "string"}),
                    pa.field("serialized_score", pa.float64(), nullable=False, metadata={"pydantic_type": "number"}),
                ]
            ),
            nullable=False,
        ),
    ]
)
