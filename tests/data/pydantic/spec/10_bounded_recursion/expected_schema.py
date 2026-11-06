"""Expected PyArrow schema for 10_bounded_recursion."""

import pyarrow as pa

EXPECTED_SCHEMA = pa.schema(
    [
        pa.field(
            "root_taxon",
            pa.struct(
                [
                    pa.field("rank", pa.string(), nullable=False, metadata={"pydantic_type": "string"}),
                    pa.field("name", pa.string(), nullable=False, metadata={"pydantic_type": "string"}),
                    pa.field(
                        "children",
                        pa.list_(
                            pa.field(
                                "item",
                                pa.struct(
                                    [
                                        pa.field(
                                            "rank",
                                            pa.string(),
                                            nullable=False,
                                            metadata={"pydantic_type": "string"},
                                        ),
                                        pa.field(
                                            "name",
                                            pa.string(),
                                            nullable=False,
                                            metadata={"pydantic_type": "string"},
                                        ),
                                        pa.field(
                                            "children",
                                            pa.list_(
                                                pa.field("item", pa.string(), nullable=False),
                                            ),
                                            nullable=False,
                                        ),
                                    ]
                                ),
                                nullable=False,
                            ),
                        ),
                        nullable=False,
                    ),
                ]
            ),
            nullable=False,
        ),
        pa.field(
            "entry_step",
            pa.struct(
                [
                    pa.field("step_id", pa.string(), nullable=False, metadata={"pydantic_type": "string"}),
                    pa.field(
                        "reaction",
                        pa.struct(
                            [
                                pa.field(
                                    "ec_number",
                                    pa.string(),
                                    nullable=False,
                                    metadata={"pydantic_type": "string"},
                                ),
                                pa.field(
                                    "next_step",
                                    pa.struct(
                                        [
                                            pa.field(
                                                "step_id",
                                                pa.string(),
                                                nullable=False,
                                                metadata={"pydantic_type": "string"},
                                            ),
                                            pa.field(
                                                "reaction",
                                                pa.struct(
                                                    [
                                                        pa.field(
                                                            "ec_number",
                                                            pa.string(),
                                                            nullable=False,
                                                            metadata={"pydantic_type": "string"},
                                                        ),
                                                        pa.field("next_step", pa.string(), nullable=True),
                                                    ]
                                                ),
                                                nullable=True,
                                            ),
                                        ]
                                    ),
                                    nullable=True,
                                ),
                            ]
                        ),
                        nullable=True,
                    ),
                ]
            ),
            nullable=False,
        ),
    ]
)

EXPECTED_SCHEMA_DEPTH_1 = pa.schema(
    [
        pa.field(
            "root_taxon",
            pa.struct(
                [
                    pa.field("rank", pa.string(), nullable=False, metadata={"pydantic_type": "string"}),
                    pa.field("name", pa.string(), nullable=False, metadata={"pydantic_type": "string"}),
                    pa.field(
                        "children",
                        pa.list_(
                            pa.field("item", pa.string(), nullable=False),
                        ),
                        nullable=False,
                    ),
                ]
            ),
            nullable=False,
        ),
        pa.field(
            "entry_step",
            pa.struct(
                [
                    pa.field("step_id", pa.string(), nullable=False, metadata={"pydantic_type": "string"}),
                    pa.field(
                        "reaction",
                        pa.struct(
                            [
                                pa.field(
                                    "ec_number",
                                    pa.string(),
                                    nullable=False,
                                    metadata={"pydantic_type": "string"},
                                ),
                                pa.field("next_step", pa.string(), nullable=True),
                            ]
                        ),
                        nullable=True,
                    ),
                ]
            ),
            nullable=False,
        ),
    ]
)
