"""Expected PyArrow schema for 06_recursion_depth."""

# ruff: noqa: INP001

import pyarrow as pa

EXPECTED_SCHEMA = pa.schema(
    [
        pa.field("id", pa.string(), nullable=False, metadata={"jsonschema_type": "string"}),
        pa.field(
            "tree",
            pa.struct(
                [
                    pa.field("label", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
                    pa.field("weight", pa.int64(), nullable=True, metadata={"jsonschema_type": "integer"}),
                    pa.field(
                        "branch",
                        pa.list_(
                            pa.field(
                                "item",
                                pa.struct(
                                    [
                                        pa.field(
                                            "label",
                                            pa.string(),
                                            nullable=True,
                                            metadata={"jsonschema_type": "string"},
                                        ),
                                        pa.field(
                                            "weight",
                                            pa.int64(),
                                            nullable=True,
                                            metadata={"jsonschema_type": "integer"},
                                        ),
                                        pa.field(
                                            "branch",
                                            pa.list_(
                                                pa.field("item", pa.string(), nullable=False),
                                            ),
                                            nullable=True,
                                        ),
                                    ]
                                ),
                                nullable=False,
                            ),
                        ),
                        nullable=True,
                    ),
                ]
            ),
            nullable=True,
        ),
        pa.field(
            "section",
            pa.struct(
                [
                    pa.field("section_title", pa.string(), nullable=False, metadata={"jsonschema_type": "string"}),
                    pa.field(
                        "subsection",
                        pa.struct(
                            [
                                pa.field(
                                    "subsection_title",
                                    pa.string(),
                                    nullable=False,
                                    metadata={"jsonschema_type": "string"},
                                ),
                                pa.field(
                                    "nested_sections",
                                    pa.list_(
                                        pa.field(
                                            "item",
                                            pa.struct(
                                                [
                                                    pa.field(
                                                        "section_title",
                                                        pa.string(),
                                                        nullable=False,
                                                        metadata={"jsonschema_type": "string"},
                                                    ),
                                                    pa.field(
                                                        "subsection",
                                                        pa.struct(
                                                            [
                                                                pa.field(
                                                                    "subsection_title",
                                                                    pa.string(),
                                                                    nullable=False,
                                                                    metadata={"jsonschema_type": "string"},
                                                                ),
                                                                pa.field(
                                                                    "nested_sections",
                                                                    pa.list_(
                                                                        pa.field("item", pa.string(), nullable=False),
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
)

EXPECTED_SCHEMA_DEPTH_1 = pa.schema(
    [
        pa.field("id", pa.string(), nullable=False, metadata={"jsonschema_type": "string"}),
        pa.field(
            "tree",
            pa.struct(
                [
                    pa.field("label", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
                    pa.field("weight", pa.int64(), nullable=True, metadata={"jsonschema_type": "integer"}),
                    pa.field(
                        "branch",
                        pa.list_(
                            pa.field("item", pa.string(), nullable=False),
                        ),
                        nullable=True,
                    ),
                ]
            ),
            nullable=True,
        ),
        pa.field(
            "section",
            pa.struct(
                [
                    pa.field("section_title", pa.string(), nullable=False, metadata={"jsonschema_type": "string"}),
                    pa.field(
                        "subsection",
                        pa.struct(
                            [
                                pa.field(
                                    "subsection_title",
                                    pa.string(),
                                    nullable=False,
                                    metadata={"jsonschema_type": "string"},
                                ),
                                pa.field(
                                    "nested_sections",
                                    pa.list_(
                                        pa.field("item", pa.string(), nullable=False),
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
)
