"""Expected PyArrow schema for ncbi_assembly_report."""

import pyarrow as pa

EXPECTED_SCHEMA = pa.schema(
    [
        pa.field(
            "accession",
            pa.string(),
            nullable=False,
            metadata={"comment": "Assembly accession (GCF_ or GCA_)", "pydantic_type": "string"},
        ),
        pa.field("paired_accession", pa.string(), nullable=True, metadata={"pydantic_type": "string"}),
        pa.field("source_database", pa.string(), nullable=False, metadata={"pydantic_type": "string"}),
        pa.field("assembly_level", pa.string(), nullable=False, metadata={"pydantic_type": "string"}),
        pa.field("release_date", pa.date32(), nullable=False, metadata={"pydantic_type": "date"}),
        pa.field("updated_at", pa.timestamp("us", tz="UTC"), nullable=False, metadata={"pydantic_type": "date-time"}),
        pa.field(
            "organism",
            pa.struct(
                [
                    pa.field("tax_id", pa.int64(), nullable=False, metadata={"pydantic_type": "integer"}),
                    pa.field("organism_name", pa.string(), nullable=False, metadata={"pydantic_type": "string"}),
                    pa.field("strain", pa.string(), nullable=True, metadata={"pydantic_type": "string"}),
                ]
            ),
            nullable=False,
            metadata={"comment": "Organism taxonomy and infraspecific metadata."},
        ),
        pa.field(
            "assembly_stats",
            pa.struct(
                [
                    pa.field(
                        "total_sequence_length",
                        pa.int64(),
                        nullable=False,
                        metadata={"pydantic_type": "integer"},
                    ),
                    pa.field("number_of_contigs", pa.int64(), nullable=False, metadata={"pydantic_type": "integer"}),
                    pa.field("contig_n50", pa.int64(), nullable=False, metadata={"pydantic_type": "integer"}),
                    pa.field("gc_percent", pa.float64(), nullable=False, metadata={"pydantic_type": "number"}),
                ]
            ),
            nullable=False,
        ),
        pa.field(
            "annotation_info",
            pa.struct(
                [
                    pa.field("name", pa.string(), nullable=False, metadata={"pydantic_type": "string"}),
                    pa.field("provider", pa.string(), nullable=False, metadata={"pydantic_type": "string"}),
                    pa.field("release_date", pa.date32(), nullable=False, metadata={"pydantic_type": "date"}),
                    pa.field("total_gene_count", pa.int64(), nullable=False, metadata={"pydantic_type": "integer"}),
                ]
            ),
            nullable=True,
        ),
        pa.field(
            "bioproject_accessions",
            pa.list_(
                pa.field("item", pa.string(), nullable=False, metadata={"pydantic_type": "string"}),
            ),
            nullable=False,
        ),
    ]
)
