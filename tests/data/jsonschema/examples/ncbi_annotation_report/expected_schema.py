"""Expected PyArrow schema for real-world example ncbi_annotation_report."""

# ruff: noqa: INP001

import pyarrow as pa

EXPECTED_SCHEMA = pa.schema(
    [
        pa.field(
            "reports",
            pa.list_(
                pa.field(
                    "item",
                    pa.struct(
                        [
                            pa.field(
                                "annotation",
                                pa.struct(
                                    [
                                        pa.field(
                                            "gene_id",
                                            pa.string(),
                                            nullable=True,
                                            metadata={
                                                "comment": "NCBI GeneID",
                                                "jsonschema_type": "string (uint64)",
                                            },
                                        ),
                                        pa.field(
                                            "symbol",
                                            pa.string(),
                                            nullable=True,
                                            metadata={
                                                "comment": "Gene symbol",
                                                "jsonschema_type": "string",
                                            },
                                        ),
                                        pa.field(
                                            "description",
                                            pa.string(),
                                            nullable=True,
                                            metadata={
                                                "comment": "Gene name",
                                                "jsonschema_type": "string",
                                            },
                                        ),
                                        pa.field(
                                            "tax_id",
                                            pa.string(),
                                            nullable=True,
                                            metadata={
                                                "comment": "NCBI Taxonomy ID for the organism",
                                                "jsonschema_type": "string (uint64)",
                                            },
                                        ),
                                        pa.field(
                                            "taxname",
                                            pa.string(),
                                            nullable=True,
                                            metadata={
                                                "comment": "Taxonomic name of the organism",
                                                "jsonschema_type": "string",
                                            },
                                        ),
                                        pa.field(
                                            "gene_type",
                                            pa.string(),
                                            nullable=True,
                                            metadata={
                                                "comment": "Gene locus type",
                                                "jsonschema_type": "string",
                                            },
                                        ),
                                        pa.field(
                                            "orientation",
                                            pa.string(),
                                            nullable=True,
                                            metadata={"jsonschema_type": "string"},
                                        ),
                                        pa.field(
                                            "locus_tag",
                                            pa.string(),
                                            nullable=True,
                                            metadata={"jsonschema_type": "string"},
                                        ),
                                        pa.field(
                                            "annotations",
                                            pa.list_(
                                                pa.field(
                                                    "item",
                                                    pa.struct(
                                                        [
                                                            pa.field(
                                                                "assembly_accession",
                                                                pa.string(),
                                                                nullable=True,
                                                                metadata={"jsonschema_type": "string"},
                                                            ),
                                                            pa.field(
                                                                "assembly_name",
                                                                pa.string(),
                                                                nullable=True,
                                                                metadata={"jsonschema_type": "string"},
                                                            ),
                                                            pa.field(
                                                                "annotation_name",
                                                                pa.string(),
                                                                nullable=True,
                                                                metadata={"jsonschema_type": "string"},
                                                            ),
                                                            pa.field(
                                                                "annotation_release_date",
                                                                pa.date32(),
                                                                nullable=True,
                                                                metadata={"jsonschema_type": "string (date)"},
                                                            ),
                                                            pa.field(
                                                                "genomic_locations",
                                                                pa.list_(
                                                                    pa.field(
                                                                        "item",
                                                                        pa.struct(
                                                                            [
                                                                                pa.field(
                                                                                    "genomic_accession_version",
                                                                                    pa.string(),
                                                                                    nullable=True,
                                                                                    metadata={
                                                                                        "jsonschema_type": "string"
                                                                                    },
                                                                                ),
                                                                                pa.field(
                                                                                    "sequence_name",
                                                                                    pa.string(),
                                                                                    nullable=True,
                                                                                    metadata={
                                                                                        "jsonschema_type": "string"
                                                                                    },
                                                                                ),
                                                                                pa.field(
                                                                                    "genomic_range",
                                                                                    pa.struct(
                                                                                        [
                                                                                            pa.field(
                                                                                                "begin",
                                                                                                pa.string(),
                                                                                                nullable=True,
                                                                                                metadata={
                                                                                                    "jsonschema_type": "string (int64)"
                                                                                                },
                                                                                            ),
                                                                                            pa.field(
                                                                                                "end",
                                                                                                pa.string(),
                                                                                                nullable=True,
                                                                                                metadata={
                                                                                                    "jsonschema_type": "string (int64)"
                                                                                                },
                                                                                            ),
                                                                                            pa.field(
                                                                                                "orientation",
                                                                                                pa.string(),
                                                                                                nullable=True,
                                                                                                metadata={
                                                                                                    "jsonschema_type": "string"
                                                                                                },
                                                                                            ),
                                                                                            pa.field(
                                                                                                "order",
                                                                                                pa.int64(),
                                                                                                nullable=True,
                                                                                                metadata={
                                                                                                    "jsonschema_type": "integer"
                                                                                                },
                                                                                            ),
                                                                                        ]
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
                                                    nullable=False,
                                                ),
                                            ),
                                            nullable=True,
                                        ),
                                    ]
                                ),
                                nullable=True,
                            ),
                            pa.field(
                                "query",
                                pa.list_(
                                    pa.field(
                                        "item", pa.string(), nullable=False, metadata={"jsonschema_type": "string"}
                                    ),
                                ),
                                nullable=True,
                            ),
                            pa.field("row_id", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
                        ]
                    ),
                    nullable=False,
                ),
            ),
            nullable=True,
        ),
        pa.field(
            "total_count",
            pa.int64(),
            nullable=True,
            metadata={
                "comment": "The total count of available genes (ignoring the page_size parameter).",
                "jsonschema_type": "integer",
            },
        ),
        pa.field(
            "next_page_token",
            pa.string(),
            nullable=True,
            metadata={
                "comment": "A token that can be sent as page_token to retrieve the next page.",
                "jsonschema_type": "string",
            },
        ),
    ]
)
