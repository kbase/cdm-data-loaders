"""Expected PyArrow schema for 07_combinators_allof_anyof_oneof."""

# ruff: noqa: INP001

import pyarrow as pa

EXPECTED_SCHEMA = pa.schema(
    [
        pa.field(
            "entity_id",
            pa.string(),
            nullable=False,
            metadata={"comment": "Inherited base entity identifier.", "jsonschema_type": "string"},
        ),
        pa.field("created_by", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("sample_name", pa.string(), nullable=False, metadata={"jsonschema_type": "string"}),
        pa.field(
            "location",
            pa.struct(
                [
                    pa.field("storage_kind", pa.string(), nullable=False, metadata={"jsonschema_type": "string"}),
                    pa.field("bucket", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
                    pa.field("key", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
                    pa.field("url", pa.string(), nullable=True, metadata={"jsonschema_type": "string (uri)"}),
                    pa.field("checksum", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
                ]
            ),
            nullable=False,
            metadata={"comment": "Storage location choice across S3 or HTTP."},
        ),
        pa.field(
            "pagination",
            pa.struct(
                [
                    pa.field("start_page", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
                    pa.field("end_page", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
                    pa.field("medline_pgn", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
                ]
            ),
            nullable=True,
        ),
        pa.field("identifier_union", pa.string(), nullable=True, metadata={"jsonschema_type": "integer | string"}),
        pa.field(
            "note",
            pa.struct(
                [
                    pa.field("text", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
                    pa.field("author", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
                    pa.field("_value", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
                ]
            ),
            nullable=True,
        ),
    ]
)
