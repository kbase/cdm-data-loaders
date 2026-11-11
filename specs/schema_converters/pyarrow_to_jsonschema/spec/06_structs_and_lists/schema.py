"""PyArrow schema for 06_structs_and_lists."""

import pyarrow as pa

SCHEMA = pa.schema(
    [
        pa.field("feature_id", pa.string(), nullable=False),
        pa.field(
            "location",
            pa.struct(
                [
                    pa.field("contig", pa.string(), nullable=False),
                    pa.field("start", pa.int64(), nullable=False),
                    pa.field("end", pa.int64(), nullable=False),
                    pa.field("strand", pa.string(), nullable=True),
                ]
            ),
            nullable=False,
        ),
        pa.field(
            "go_terms",
            pa.list_(
                pa.field(
                    "item",
                    pa.string(),
                    nullable=False,
                    metadata={"comment": "Gene Ontology term accession.", "ontology": "GO"},
                )
            ),
            nullable=False,
        ),
        pa.field(
            "nullable_scores",
            pa.large_list(pa.field("item", pa.int32(), nullable=True)),
            nullable=True,
        ),
        pa.field(
            "aliases_view",
            pa.list_view(pa.field("item", pa.string(), nullable=False)),
            nullable=False,
        ),
        pa.field(
            "notes_large_view",
            pa.large_list_view(pa.field("item", pa.string(), nullable=True)),
            nullable=True,
        ),
        pa.field(
            "rgb_triplet",
            pa.list_(pa.field("item", pa.uint8(), nullable=False), 3),
            nullable=False,
        ),
        pa.field(
            "score_matrix",
            pa.list_(
                pa.field(
                    "row",
                    pa.list_(pa.field("cell", pa.int32(), nullable=False)),
                    nullable=False,
                )
            ),
            nullable=False,
        ),
    ]
)
