"""Expected PyArrow schema for 05_defs_and_refs."""

# ruff: noqa: INP001

import pyarrow as pa

_AUTHOR_STRUCT = pa.struct(
    [
        pa.field("name", pa.string(), nullable=False, metadata={"jsonschema_type": "string"}),
        pa.field("orcid", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
    ]
)

EXPECTED_SCHEMA = pa.schema(
    [
        pa.field("doc_id", pa.string(), nullable=False, metadata={"jsonschema_type": "string"}),
        pa.field(
            "published_on",
            pa.date32(),
            nullable=True,
            metadata={"comment": "ISO-8601 calendar date.", "jsonschema_type": "string (date)"},
        ),
        pa.field(
            "lead_author",
            _AUTHOR_STRUCT,
            nullable=False,
            metadata={"comment": "Primary corresponding author."},
        ),
        pa.field(
            "reviewer",
            _AUTHOR_STRUCT,
            nullable=True,
            metadata={"comment": "Author contact record."},
        ),
        pa.field(
            "co_authors",
            pa.list_(
                pa.field(
                    "item",
                    _AUTHOR_STRUCT,
                    nullable=False,
                    metadata={"comment": "Author contact record."},
                ),
            ),
            nullable=True,
        ),
    ]
)
