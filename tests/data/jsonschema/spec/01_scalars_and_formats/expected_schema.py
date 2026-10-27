"""Expected PyArrow schema for 01_scalars_and_formats."""

# ruff: noqa: INP001

import pyarrow as pa

EXPECTED_SCHEMA = pa.schema(
    [
        pa.field(
            "id",
            pa.string(),
            nullable=False,
            metadata={"comment": "Unique record identifier.", "jsonschema_type": "string"},
        ),
        pa.field(
            "status",
            pa.string(),
            nullable=False,
            metadata={"comment": "Record lifecycle status.", "jsonschema_type": "string"},
        ),
        pa.field("version", pa.int64(), nullable=False, metadata={"jsonschema_type": "integer"}),
        pa.field(
            "flag",
            pa.bool_(),
            nullable=False,
            metadata={"comment": "Primary boolean flag.", "jsonschema_type": "boolean"},
        ),
        pa.field("bounded_int", pa.int64(), nullable=True, metadata={"jsonschema_type": "integer"}),
        pa.field("big_int", pa.int64(), nullable=True, metadata={"jsonschema_type": "integer"}),
        pa.field("float_val", pa.float64(), nullable=True, metadata={"jsonschema_type": "number"}),
        pa.field("decimal_multiple", pa.float64(), nullable=True, metadata={"jsonschema_type": "number"}),
        pa.field("created_date", pa.date32(), nullable=True, metadata={"jsonschema_type": "string (date)"}),
        pa.field("updated_at", pa.string(), nullable=True, metadata={"jsonschema_type": "string (date-time)"}),
        pa.field("event_time", pa.string(), nullable=True, metadata={"jsonschema_type": "string (time)"}),
        pa.field("elapsed", pa.string(), nullable=True, metadata={"jsonschema_type": "string (duration)"}),
        pa.field("record_uuid", pa.string(), nullable=True, metadata={"jsonschema_type": "string (uuid)"}),
        pa.field("homepage", pa.string(), nullable=True, metadata={"jsonschema_type": "string (uri)"}),
        pa.field("const_tag", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("untyped_int_enum", pa.int64(), nullable=True, metadata={"jsonschema_type": "integer"}),
        pa.field(
            "untyped_mixed_enum",
            pa.string(),
            nullable=True,
            metadata={"jsonschema_type": "integer | string | boolean"},
        ),
        pa.field("numeric_union", pa.float64(), nullable=True, metadata={"jsonschema_type": "integer | number"}),
        pa.field("scalar_union", pa.string(), nullable=True, metadata={"jsonschema_type": "integer | string"}),
        pa.field(
            "date_or_year_union",
            pa.string(),
            nullable=True,
            metadata={"jsonschema_type": "string (date) | string"},
        ),
        pa.field("raw_b64", pa.binary(), nullable=True, metadata={"jsonschema_type": "string (base64)"}),
        pa.field("raw_hex", pa.binary(), nullable=True, metadata={"jsonschema_type": "string (base16)"}),
    ]
)
