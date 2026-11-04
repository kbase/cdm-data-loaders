"""Expected PyArrow schema for 01_scalars_and_special_types."""

# ruff: noqa: INP001

import pyarrow as pa

EXPECTED_SCHEMA = pa.schema(
    [
        pa.field(
            "id",
            pa.string(),
            nullable=False,
            metadata={"comment": "Primary record identifier", "pydantic_type": "string"},
        ),
        pa.field("active", pa.bool_(), nullable=False, metadata={"pydantic_type": "boolean"}),
        pa.field("count", pa.int64(), nullable=False, metadata={"pydantic_type": "integer"}),
        pa.field("ratio", pa.float64(), nullable=False, metadata={"pydantic_type": "number"}),
        pa.field(
            "price",
            pa.decimal128(10, 2),
            nullable=False,
            metadata={"comment": "Fixed-scale price", "pydantic_type": "decimal"},
        ),
        pa.field("unconstrained_dec", pa.float64(), nullable=False, metadata={"pydantic_type": "number"}),
        pa.field("release_date", pa.date32(), nullable=False, metadata={"pydantic_type": "date"}),
        pa.field("created_at", pa.timestamp("us", tz="UTC"), nullable=False, metadata={"pydantic_type": "date-time"}),
        pa.field("local_ts", pa.timestamp("us"), nullable=False, metadata={"pydantic_type": "date-time-naive"}),
        pa.field("alarm_time", pa.time64("us"), nullable=False, metadata={"pydantic_type": "time"}),
        pa.field("elapsed", pa.string(), nullable=False, metadata={"pydantic_type": "duration"}),
        pa.field("item_uuid", pa.string(), nullable=False, metadata={"pydantic_type": "uuid"}),
        pa.field("config_path", pa.string(), nullable=False, metadata={"pydantic_type": "path"}),
        pa.field("host_ip", pa.string(), nullable=False, metadata={"pydantic_type": "ip"}),
        pa.field("homepage", pa.string(), nullable=False, metadata={"pydantic_type": "string"}),
        pa.field("contact_email", pa.string(), nullable=False, metadata={"pydantic_type": "string"}),
        pa.field("api_token", pa.string(), nullable=False, metadata={"pydantic_type": "string"}),
        pa.field("raw_bytes", pa.binary(), nullable=False, metadata={"pydantic_type": "binary"}),
        pa.field("secret_blob", pa.binary(), nullable=False, metadata={"pydantic_type": "binary"}),
        pa.field("encoded_payload", pa.binary(), nullable=False, metadata={"pydantic_type": "binary (base64)"}),
        pa.field("encoded_text", pa.string(), nullable=False, metadata={"pydantic_type": "string"}),
        pa.field(
            "optional_note",
            pa.string(),
            nullable=True,
            metadata={"comment": "Optional free-text note", "pydantic_type": "string"},
        ),
    ]
)
