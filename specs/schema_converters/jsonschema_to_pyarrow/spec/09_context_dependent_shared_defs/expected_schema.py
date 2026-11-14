"""Expected PyArrow schema for 09_context_dependent_shared_defs."""

import pyarrow as pa

TAG_STRUCT = pa.struct(
    [
        pa.field("key", pa.string(), nullable=False, metadata={"jsonschema_type": "string"}),
        pa.field("value", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
    ]
)

EXPECTED_SCHEMA = pa.schema(
    [
        pa.field("id", pa.string(), nullable=False, metadata={"jsonschema_type": "string"}),
        pa.field(
            "primary_tag",
            TAG_STRUCT,
            nullable=False,
            metadata={"comment": "Reusable key-value tag structure."},
        ),
        pa.field(
            "secondary_tag",
            TAG_STRUCT,
            nullable=True,
            metadata={"comment": "Reusable key-value tag structure."},
        ),
        pa.field(
            "fallback_tag",
            TAG_STRUCT,
            nullable=True,
            metadata={"comment": "Reusable key-value tag structure."},
        ),
        pa.field(
            "tag_list",
            pa.list_(
                pa.field(
                    "item",
                    TAG_STRUCT,
                    nullable=False,
                    metadata={"comment": "Reusable key-value tag structure."},
                ),
            ),
            nullable=True,
        ),
        pa.field(
            "tag_lookup",
            pa.map_(
                pa.string(),
                pa.field(
                    "value",
                    TAG_STRUCT,
                    nullable=False,
                    metadata={"comment": "Reusable key-value tag structure."},
                ),
            ),
            nullable=True,
        ),
    ]
)
