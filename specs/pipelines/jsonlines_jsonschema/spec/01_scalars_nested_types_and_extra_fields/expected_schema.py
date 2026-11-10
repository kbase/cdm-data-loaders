"""Expected PyArrow schema for 01_scalars_nested_types_and_extra_fields."""

import pyarrow as pa

EXPECTED_SCHEMA = pa.schema(
    [
        pa.field("genome_id", pa.string(), nullable=False, metadata={"jsonschema_type": "string"}),
        pa.field("contig_count", pa.int64(), nullable=False, metadata={"jsonschema_type": "integer"}),
        pa.field("gc_content", pa.float64(), nullable=False, metadata={"jsonschema_type": "number"}),
        pa.field("is_complete", pa.bool_(), nullable=False, metadata={"jsonschema_type": "boolean"}),
        pa.field("strain", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field(
            "tags",
            pa.list_(pa.field("item", pa.string(), nullable=False, metadata={"jsonschema_type": "string"})),
            nullable=True,
        ),
        pa.field(
            "assembly_stats",
            pa.struct(
                [
                    pa.field("n50", pa.int64(), nullable=False, metadata={"jsonschema_type": "integer"}),
                    pa.field("source_lab", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
                ]
            ),
            nullable=True,
        ),
    ]
)
