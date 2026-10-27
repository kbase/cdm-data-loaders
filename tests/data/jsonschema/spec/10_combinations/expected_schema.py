"""Expected PyArrow schema for 10_combinations."""

# ruff: noqa: INP001

import pyarrow as pa

EXPECTED_SCHEMA = pa.schema(
    [
        pa.field("schema_version", pa.int64(), nullable=False, metadata={"jsonschema_type": "integer"}),
        pa.field(
            "record_id",
            pa.string(),
            nullable=False,
            metadata={"comment": "Primary record identifier.", "jsonschema_type": "string"},
        ),
        pa.field("active", pa.bool_(), nullable=False, metadata={"jsonschema_type": "boolean"}),
        pa.field("count", pa.int64(), nullable=False, metadata={"jsonschema_type": "integer"}),
        pa.field("ratio", pa.float64(), nullable=False, metadata={"jsonschema_type": "number"}),
        pa.field("release_date", pa.date32(), nullable=False, metadata={"jsonschema_type": "string (date)"}),
        pa.field("timestamp_str", pa.string(), nullable=True, metadata={"jsonschema_type": "string (date-time)"}),
        pa.field("payload_b64", pa.binary(), nullable=True, metadata={"jsonschema_type": "string (base64)"}),
        pa.field("score_or_label", pa.string(), nullable=True, metadata={"jsonschema_type": "number | string"}),
        pa.field(
            "range_tuple",
            pa.struct(
                [
                    pa.field("item_0", pa.int64(), nullable=False, metadata={"jsonschema_type": "integer"}),
                    pa.field("item_1", pa.int64(), nullable=False, metadata={"jsonschema_type": "integer"}),
                    pa.field("item_2", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
                ]
            ),
            nullable=False,
        ),
        pa.field(
            "tags",
            pa.list_(
                pa.field("item", pa.string(), nullable=False, metadata={"jsonschema_type": "string"}),
            ),
            nullable=True,
        ),
        pa.field(
            "annotations",
            pa.map_(
                pa.string(),
                pa.field("value", pa.string(), nullable=False, metadata={"jsonschema_type": "string"}),
            ),
            nullable=True,
        ),
        pa.field(
            "source_choice",
            pa.struct(
                [
                    pa.field("kind", pa.string(), nullable=False, metadata={"jsonschema_type": "string"}),
                    pa.field("accession", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
                    pa.field("doi", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
                ]
            ),
            nullable=True,
        ),
        pa.field(
            "lineage_tree",
            pa.struct(
                [
                    pa.field("taxon_name", pa.string(), nullable=False, metadata={"jsonschema_type": "string"}),
                    pa.field("rank", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
                    pa.field(
                        "children",
                        pa.list_(
                            pa.field(
                                "item",
                                pa.struct(
                                    [
                                        pa.field(
                                            "taxon_name",
                                            pa.string(),
                                            nullable=False,
                                            metadata={"jsonschema_type": "string"},
                                        ),
                                        pa.field(
                                            "rank",
                                            pa.string(),
                                            nullable=True,
                                            metadata={"jsonschema_type": "string"},
                                        ),
                                        pa.field(
                                            "children",
                                            pa.list_(
                                                pa.field("item", pa.string(), nullable=False),
                                            ),
                                            nullable=True,
                                        ),
                                    ]
                                ),
                                nullable=False,
                            ),
                        ),
                        nullable=True,
                    ),
                ]
            ),
            nullable=True,
        ),
        pa.field("payload_sha256", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
    ]
)
