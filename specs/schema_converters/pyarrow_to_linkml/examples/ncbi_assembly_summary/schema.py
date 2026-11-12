"""NCBI Datasets assembly summary PyArrow schema."""

import pyarrow as pa

REPLICON_STRUCT = pa.struct(
    [
        pa.field("accession", pa.string(), nullable=False),
        pa.field("molecule_type", pa.string(), nullable=False),
        pa.field("length_bp", pa.uint64(), nullable=False),
    ]
)

SCHEMA = pa.schema(
    [
        pa.field("assembly_accession", pa.string(), nullable=False, metadata={"comment": "RefSeq/GenBank accession."}),
        pa.field("bioproject", pa.string(), nullable=True),
        pa.field("tax_id", pa.int32(), nullable=False),
        pa.field("is_reference", pa.bool_(), nullable=False),
        pa.field("assembly_level", pa.dictionary(pa.int8(), pa.string()), nullable=False),
        pa.field("release_date", pa.date32(), nullable=False),
        pa.field("ingested_at", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("gc_percent", pa.decimal128(5, 2), nullable=True),
        pa.field("fasta_md5", pa.binary(16), nullable=False, metadata={"comment": "16-byte raw MD5 digest."}),
        pa.field("replicons", pa.list_(pa.field("item", REPLICON_STRUCT, nullable=False)), nullable=False),
    ],
    metadata={
        "title": "NcbiAssemblySummary",
        "$id": "https://kbase.us/linkml/cdm/ncbi_assembly_summary",
        "comment": "Curated NCBI assembly metadata and replicon inventory.",
    },
)
