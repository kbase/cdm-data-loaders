"""Expected PyArrow schema for 01_scalars_nullability_and_column_order."""

import pyarrow as pa

EXPECTED_SCHEMA = pa.schema(
    [
        pa.field(
            "id",
            pa.string(),
            nullable=False,
            metadata={"comment": "Primary record identifier.", "jsonschema_type": "string"},
        ),
        pa.field("count", pa.int64(), nullable=False, metadata={"jsonschema_type": "integer"}),
        pa.field("rank32", pa.int64(), nullable=False, metadata={"jsonschema_type": "integer"}),
        pa.field(
            "score",
            pa.float64(),
            nullable=False,
            metadata={"comment": "Floating-point score.", "jsonschema_type": "number"},
        ),
        pa.field("price", pa.float64(), nullable=False, metadata={"jsonschema_type": "number"}),
        pa.field("active", pa.bool_(), nullable=False, metadata={"jsonschema_type": "boolean"}),
        pa.field("created_date", pa.date32(), nullable=False, metadata={"jsonschema_type": "string (date)"}),
        pa.field("updated_at", pa.string(), nullable=False, metadata={"jsonschema_type": "string (date-time)"}),
        pa.field("status", pa.string(), nullable=False, metadata={"jsonschema_type": "string"}),
        pa.field("opt_note", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("unlisted_tag", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field(
            "extra_col",
            pa.int64(),
            nullable=True,
            metadata={"comment": "Extra property omitted from required.", "jsonschema_type": "integer"},
        ),
    ]
)
