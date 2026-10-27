"""Expected PyArrow schema for 03_arrays_and_tuples."""

# ruff: noqa: INP001

import pyarrow as pa

EXPECTED_SCHEMA = pa.schema(
    [
        pa.field("id", pa.string(), nullable=False, metadata={"jsonschema_type": "string"}),
        pa.field(
            "tags",
            pa.list_(
                pa.field("item", pa.string(), nullable=False, metadata={"jsonschema_type": "string"}),
            ),
            nullable=False,
            metadata={"comment": "Ordered list of string tags."},
        ),
        pa.field(
            "measurements",
            pa.list_(
                pa.field(
                    "item",
                    pa.struct(
                        [
                            pa.field("label", pa.string(), nullable=False, metadata={"jsonschema_type": "string"}),
                            pa.field("value", pa.float64(), nullable=False, metadata={"jsonschema_type": "number"}),
                            pa.field("unit", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
                        ]
                    ),
                    nullable=False,
                ),
            ),
            nullable=True,
        ),
        pa.field(
            "matrix_2d",
            pa.list_(
                pa.field(
                    "item",
                    pa.list_(
                        pa.field("item", pa.float64(), nullable=False, metadata={"jsonschema_type": "number"}),
                    ),
                    nullable=False,
                ),
            ),
            nullable=True,
            metadata={"comment": "Two-dimensional numeric matrix."},
        ),
        pa.field(
            "coord_3d",
            pa.struct(
                [
                    pa.field(
                        "item_0",
                        pa.float64(),
                        nullable=False,
                        metadata={"comment": "X coordinate.", "jsonschema_type": "number"},
                    ),
                    pa.field(
                        "item_1",
                        pa.float64(),
                        nullable=False,
                        metadata={"comment": "Y coordinate.", "jsonschema_type": "number"},
                    ),
                    pa.field(
                        "item_2",
                        pa.float64(),
                        nullable=True,
                        metadata={"comment": "Optional Z coordinate.", "jsonschema_type": "number"},
                    ),
                ]
            ),
            nullable=False,
            metadata={"comment": "Closed 3D coordinate tuple (x, y, optional z)."},
        ),
        pa.field(
            "pair_tuple_list",
            pa.list_(
                pa.field(
                    "item",
                    pa.struct(
                        [
                            pa.field("item_0", pa.string(), nullable=False, metadata={"jsonschema_type": "string"}),
                            pa.field("item_1", pa.int64(), nullable=False, metadata={"jsonschema_type": "integer"}),
                        ]
                    ),
                    nullable=False,
                ),
            ),
            nullable=True,
        ),
    ]
)
