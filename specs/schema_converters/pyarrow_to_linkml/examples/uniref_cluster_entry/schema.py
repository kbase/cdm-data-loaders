"""UniRef sequence cluster PyArrow schema."""

import pyarrow as pa

SCHEMA = pa.schema(
    [
        pa.field("cluster_id", pa.string(), nullable=False, metadata={"comment": "UniRef cluster identifier."}),
        pa.field("cluster_name", pa.large_string(), nullable=False),
        pa.field("identity_threshold", pa.float32(), nullable=False),
        pa.field("member_count", pa.uint32(), nullable=False),
        pa.field("updated_at", pa.timestamp("s"), nullable=False),
        pa.field(
            "representative",
            pa.struct(
                [
                    pa.field("uniprot_acc", pa.string(), nullable=False),
                    pa.field("length_aa", pa.uint32(), nullable=False),
                    pa.field("crc64", pa.binary(8), nullable=False),
                ]
            ),
            nullable=False,
        ),
        pa.field(
            "embedding_preview",
            pa.list_(pa.field("item", pa.float32(), nullable=False), 4),
            nullable=True,
        ),
        pa.field(
            "member_accessions",
            pa.large_list(pa.field("item", pa.string(), nullable=False)),
            nullable=False,
        ),
        pa.field(
            "taxon_counts",
            pa.map_(pa.int32(), pa.field("value", pa.uint32(), nullable=False)),
            nullable=False,
        ),
    ],
    metadata={
        "title": "UniRefClusterEntry",
        "$id": "https://kbase.us/linkml/cdm/uniref_cluster_entry",
        "comment": "UniRef cluster representative and member summary.",
    },
)
