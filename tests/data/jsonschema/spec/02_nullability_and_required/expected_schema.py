"""Expected PyArrow schema for 02_nullability_and_required."""

# ruff: noqa: INP001

import pyarrow as pa

EXPECTED_SCHEMA = pa.schema(
    [
        pa.field(
            "required_str",
            pa.string(),
            nullable=False,
            metadata={"comment": "Strictly required non-null string.", "jsonschema_type": "string"},
        ),
        pa.field("required_int", pa.int64(), nullable=False, metadata={"jsonschema_type": "integer"}),
        pa.field(
            "required_nullable_str",
            pa.string(),
            nullable=True,
            metadata={"comment": "Required key whose value may be null.", "jsonschema_type": "string"},
        ),
        pa.field("required_anyof_nullable", pa.int64(), nullable=True, metadata={"jsonschema_type": "integer"}),
        pa.field("required_enum_nullable", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("optional_str", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("optional_with_default", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("optional_nullable_number", pa.float64(), nullable=True, metadata={"jsonschema_type": "number"}),
        pa.field(
            "strict_int_list",
            pa.list_(
                pa.field("item", pa.int64(), nullable=False, metadata={"jsonschema_type": "integer"}),
            ),
            nullable=False,
        ),
        pa.field(
            "nullable_int_list",
            pa.list_(
                pa.field("item", pa.int64(), nullable=True, metadata={"jsonschema_type": "integer"}),
            ),
            nullable=False,
        ),
        pa.field(
            "optional_str_list",
            pa.list_(
                pa.field("item", pa.string(), nullable=False, metadata={"jsonschema_type": "string"}),
            ),
            nullable=True,
        ),
    ]
)
