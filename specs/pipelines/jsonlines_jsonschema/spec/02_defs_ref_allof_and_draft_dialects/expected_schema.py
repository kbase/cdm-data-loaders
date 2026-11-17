"""Expected PyArrow schemas for 02_defs_ref_allof_and_draft_dialects."""

import pyarrow as pa

SUBMISSIONS_SCHEMA = pa.schema(
    [
        pa.field("submission_id", pa.string(), nullable=False, metadata={"jsonschema_type": "string"}),
        pa.field("priority", pa.int64(), nullable=False, metadata={"jsonschema_type": "integer"}),
        pa.field("submitted_date", pa.date32(), nullable=False, metadata={"jsonschema_type": "string (date)"}),
        pa.field(
            "contact",
            pa.struct(
                [
                    pa.field("email", pa.string(), nullable=False, metadata={"jsonschema_type": "string (email)"}),
                    pa.field("orcid", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
                    pa.field("department", pa.string(), nullable=False, metadata={"jsonschema_type": "string"}),
                ]
            ),
            nullable=False,
        ),
    ]
)

XSV_DIALECT_META_SCHEMA = pa.schema(
    [
        pa.field("batch_id", pa.string(), nullable=False, metadata={"jsonschema_type": "string"}),
        pa.field("created_date", pa.date32(), nullable=False, metadata={"jsonschema_type": "string (date)"}),
    ]
)

EXPECTED_SCHEMA = SUBMISSIONS_SCHEMA
EXPECTED_SCHEMAS = {
    "submissions": SUBMISSIONS_SCHEMA,
    "xsv_dialect_meta": XSV_DIALECT_META_SCHEMA,
}
