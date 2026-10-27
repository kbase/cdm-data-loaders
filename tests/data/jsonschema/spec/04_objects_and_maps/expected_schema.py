"""Expected PyArrow schema for 04_objects_and_maps."""

# ruff: noqa: INP001

import pyarrow as pa

EXPECTED_SCHEMA = pa.schema(
    [
        pa.field("id", pa.string(), nullable=False, metadata={"jsonschema_type": "string"}),
        pa.field(
            "closed_meta",
            pa.struct(
                [
                    pa.field("owner", pa.string(), nullable=False, metadata={"jsonschema_type": "string"}),
                    pa.field("priority", pa.int64(), nullable=True, metadata={"jsonschema_type": "integer"}),
                ]
            ),
            nullable=False,
            metadata={"comment": "Strict object with additionalProperties set to false."},
        ),
        pa.field(
            "open_meta",
            pa.struct(
                [
                    pa.field("source", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
                ]
            ),
            nullable=True,
            metadata={"comment": "Fixed-property object with boolean additionalProperties true."},
        ),
        pa.field(
            "string_to_int_map",
            pa.map_(
                pa.string(),
                pa.field("value", pa.int64(), nullable=False, metadata={"jsonschema_type": "integer"}),
            ),
            nullable=True,
            metadata={"comment": "Dynamic map from string keys to non-null integer counts."},
        ),
        pa.field(
            "nullable_score_map",
            pa.map_(
                pa.string(),
                pa.field("value", pa.float64(), nullable=True, metadata={"jsonschema_type": "number"}),
            ),
            nullable=True,
        ),
        pa.field(
            "pattern_metric_map",
            pa.map_(
                pa.string(),
                pa.field("value", pa.float64(), nullable=False, metadata={"jsonschema_type": "number"}),
            ),
            nullable=True,
        ),
        pa.field(
            "struct_map",
            pa.map_(
                pa.string(),
                pa.field(
                    "value",
                    pa.struct(
                        [
                            pa.field("count", pa.int64(), nullable=False, metadata={"jsonschema_type": "integer"}),
                            pa.field("note", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
                        ]
                    ),
                    nullable=False,
                ),
            ),
            nullable=True,
        ),
    ]
)
