"""Expected PyArrow schema for 03_sequences_sets_and_tuples."""

import pyarrow as pa

EXPECTED_SCHEMA = pa.schema(
    [
        pa.field("id", pa.string(), nullable=False, metadata={"pydantic_type": "string"}),
        pa.field(
            "tags",
            pa.list_(
                pa.field("item", pa.string(), nullable=False, metadata={"pydantic_type": "string"}),
            ),
            nullable=False,
        ),
        pa.field(
            "nullable_scores",
            pa.list_(
                pa.field("item", pa.int64(), nullable=True, metadata={"pydantic_type": "integer"}),
            ),
            nullable=False,
        ),
        pa.field(
            "matrix",
            pa.list_(
                pa.field(
                    "item",
                    pa.list_(
                        pa.field("item", pa.float64(), nullable=False, metadata={"pydantic_type": "number"}),
                    ),
                    nullable=False,
                ),
            ),
            nullable=False,
        ),
        pa.field(
            "unique_codes",
            pa.list_(
                pa.field("item", pa.string(), nullable=False, metadata={"pydantic_type": "string"}),
            ),
            nullable=False,
        ),
        pa.field(
            "frozen_flags",
            pa.list_(
                pa.field("item", pa.int64(), nullable=False, metadata={"pydantic_type": "integer"}),
            ),
            nullable=False,
        ),
        pa.field(
            "variadic_labels",
            pa.list_(
                pa.field("item", pa.string(), nullable=False, metadata={"pydantic_type": "string"}),
            ),
            nullable=False,
        ),
        pa.field(
            "point_3d",
            pa.struct(
                [
                    pa.field("item_0", pa.string(), nullable=False, metadata={"pydantic_type": "string"}),
                    pa.field("item_1", pa.int64(), nullable=False, metadata={"pydantic_type": "integer"}),
                    pa.field("item_2", pa.float64(), nullable=True, metadata={"pydantic_type": "number"}),
                ]
            ),
            nullable=False,
        ),
        pa.field(
            "optional_pair",
            pa.struct(
                [
                    pa.field("item_0", pa.int64(), nullable=False, metadata={"pydantic_type": "integer"}),
                    pa.field("item_1", pa.string(), nullable=False, metadata={"pydantic_type": "string"}),
                ]
            ),
            nullable=True,
        ),
    ]
)
