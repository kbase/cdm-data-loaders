"""PyArrow schema for 05_nested_structs_and_lists."""

import pyarrow as pa

DOMAIN_HIT_STRUCT = pa.struct(
    [
        pa.field("domain_acc", pa.string(), nullable=False),
        pa.field("start_aa", pa.uint32(), nullable=False),
        pa.field("end_aa", pa.uint32(), nullable=False),
        pa.field("evidence_codes", pa.list_(pa.field("item", pa.string(), nullable=False)), nullable=False),
    ]
)

SCHEMA = pa.schema(
    [
        pa.field("protein_id", pa.string(), nullable=False),
        pa.field(
            "source_locus",
            pa.struct(
                [
                    pa.field("contig", pa.string(), nullable=False),
                    pa.field("strand", pa.string(), nullable=True),
                ]
            ),
            nullable=False,
        ),
        pa.field("ec_numbers", pa.list_(pa.field("item", pa.string(), nullable=False)), nullable=False),
        pa.field("quality_flags", pa.large_list(pa.field("item", pa.uint8(), nullable=False)), nullable=True),
        pa.field("coords_3d", pa.list_(pa.field("item", pa.float32(), nullable=False), 3), nullable=False),
        pa.field("domain_hits", pa.list_(pa.field("item", DOMAIN_HIT_STRUCT, nullable=False)), nullable=False),
    ],
    metadata={"title": "ProteinAnnotation"},
)
