"""Expected PyArrow schema for ncbi_annotation_report."""

import pyarrow as pa

EXPECTED_SCHEMA = pa.schema(
    [
        pa.field(
            "gene_id",
            pa.int64(),
            nullable=False,
            metadata={"comment": "NCBI Entrez Gene identifier", "pydantic_type": "integer"},
        ),
        pa.field(
            "symbol",
            pa.string(),
            nullable=False,
            metadata={"comment": "Official gene symbol", "pydantic_type": "string"},
        ),
        pa.field(
            "description",
            pa.string(),
            nullable=True,
            metadata={"comment": "Gene product description", "pydantic_type": "string"},
        ),
        pa.field("gene_type", pa.string(), nullable=False, metadata={"pydantic_type": "string"}),
        pa.field("tax_id", pa.int64(), nullable=False, metadata={"pydantic_type": "integer"}),
        pa.field(
            "synonyms",
            pa.list_(
                pa.field("item", pa.string(), nullable=False, metadata={"pydantic_type": "string"}),
            ),
            nullable=False,
        ),
        pa.field(
            "genomic_regions",
            pa.list_(
                pa.field(
                    "item",
                    pa.struct(
                        [
                            pa.field(
                                "accession_version",
                                pa.string(),
                                nullable=False,
                                metadata={"pydantic_type": "string"},
                            ),
                            pa.field("begin", pa.int64(), nullable=False, metadata={"pydantic_type": "integer"}),
                            pa.field("end", pa.int64(), nullable=False, metadata={"pydantic_type": "integer"}),
                            pa.field("orientation", pa.string(), nullable=False, metadata={"pydantic_type": "string"}),
                        ]
                    ),
                    nullable=False,
                    metadata={"comment": "Genomic interval coordinates on a sequence accession."},
                ),
            ),
            nullable=False,
        ),
        pa.field(
            "transcripts",
            pa.list_(
                pa.field(
                    "item",
                    pa.struct(
                        [
                            pa.field(
                                "accession_version",
                                pa.string(),
                                nullable=False,
                                metadata={"pydantic_type": "string"},
                            ),
                            pa.field("name", pa.string(), nullable=True, metadata={"pydantic_type": "string"}),
                            pa.field("length", pa.int64(), nullable=False, metadata={"pydantic_type": "integer"}),
                            pa.field(
                                "cds_range",
                                pa.struct(
                                    [
                                        pa.field(
                                            "accession_version",
                                            pa.string(),
                                            nullable=False,
                                            metadata={"pydantic_type": "string"},
                                        ),
                                        pa.field(
                                            "begin",
                                            pa.int64(),
                                            nullable=False,
                                            metadata={"pydantic_type": "integer"},
                                        ),
                                        pa.field(
                                            "end",
                                            pa.int64(),
                                            nullable=False,
                                            metadata={"pydantic_type": "integer"},
                                        ),
                                        pa.field(
                                            "orientation",
                                            pa.string(),
                                            nullable=False,
                                            metadata={"pydantic_type": "string"},
                                        ),
                                    ]
                                ),
                                nullable=True,
                                metadata={"comment": "Genomic interval coordinates on a sequence accession."},
                            ),
                        ]
                    ),
                    nullable=False,
                ),
            ),
            nullable=False,
        ),
    ]
)
