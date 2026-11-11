"""Expected PyArrow schema for 02_defs_ref_allof_and_draft_dialects."""

import pyarrow as pa

EXPECTED_SCHEMA = pa.schema(
    [
        pa.field("submission_id", pa.string(), nullable=False, metadata={"jsonschema_type": "string"}),
        pa.field("submitted_date", pa.date32(), nullable=False, metadata={"jsonschema_type": "string (date)"}),
        pa.field(
            "contact",
            pa.struct(
                [
                    pa.field("email", pa.string(), nullable=False, metadata={"jsonschema_type": "string (email)"}),
                    pa.field("orcid", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
                ]
            ),
            nullable=False,
        ),
    ]
)
