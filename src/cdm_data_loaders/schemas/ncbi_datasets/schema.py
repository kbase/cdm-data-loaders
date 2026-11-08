"""PyArrow schemas and reusable struct components for NCBI Datasets JSONL."""

from typing import Final

import pyarrow as pa

from cdm_data_loaders.converters.emitters.pyarrow import assemble_pyarrow_schema

GENE_RANGE_RANGE_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("begin", pa.string(), nullable=True, metadata={"jsonschema_type": "string (integer)"}),
        pa.field("end", pa.string(), nullable=True, metadata={"jsonschema_type": "string (integer)"}),
        pa.field("orientation", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("order", pa.int64(), nullable=True, metadata={"jsonschema_type": "integer"}),
        pa.field("ribosomal_slippage", pa.int64(), nullable=True, metadata={"jsonschema_type": "integer"}),
    ]
)

REFERENCE_STANDARDS_GENE_RANGE_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("accession_version", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("range", pa.list_(pa.field("item", GENE_RANGE_RANGE_STRUCT, nullable=False)), nullable=True),
    ]
)

REFERENCE_STANDARDS_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("gene_range", REFERENCE_STANDARDS_GENE_RANGE_STRUCT, nullable=True),
        pa.field("type", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
    ]
)

GENE_RANGE_GENE_RANGE_RANGE_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("begin", pa.string(), nullable=True, metadata={"jsonschema_type": "string (integer)"}),
        pa.field("end", pa.string(), nullable=True, metadata={"jsonschema_type": "string (integer)"}),
        pa.field("orientation", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("order", pa.int64(), nullable=True, metadata={"jsonschema_type": "integer"}),
        pa.field("ribosomal_slippage", pa.int64(), nullable=True, metadata={"jsonschema_type": "integer"}),
    ]
)

GENOMIC_REGIONS_GENE_RANGE_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("accession_version", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field(
            "range", pa.list_(pa.field("item", GENE_RANGE_GENE_RANGE_RANGE_STRUCT, nullable=False)), nullable=True
        ),
    ]
)

GENOMIC_REGIONS_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("gene_range", GENOMIC_REGIONS_GENE_RANGE_STRUCT, nullable=True),
        pa.field("type", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
    ]
)

CDS_RANGE_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("begin", pa.string(), nullable=True, metadata={"jsonschema_type": "string (integer)"}),
        pa.field("end", pa.string(), nullable=True, metadata={"jsonschema_type": "string (integer)"}),
        pa.field("orientation", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("order", pa.int64(), nullable=True, metadata={"jsonschema_type": "integer"}),
        pa.field("ribosomal_slippage", pa.int64(), nullable=True, metadata={"jsonschema_type": "integer"}),
    ]
)

CDS_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("accession_version", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("range", pa.list_(pa.field("item", CDS_RANGE_STRUCT, nullable=False)), nullable=True),
    ]
)

GENOMIC_LOCATIONS_GENOMIC_RANGE_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("begin", pa.string(), nullable=True, metadata={"jsonschema_type": "string (integer)"}),
        pa.field("end", pa.string(), nullable=True, metadata={"jsonschema_type": "string (integer)"}),
        pa.field("orientation", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("order", pa.int64(), nullable=True, metadata={"jsonschema_type": "integer"}),
        pa.field("ribosomal_slippage", pa.int64(), nullable=True, metadata={"jsonschema_type": "integer"}),
    ]
)

GENOMIC_LOCATIONS_EXONS_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("begin", pa.string(), nullable=True, metadata={"jsonschema_type": "string (integer)"}),
        pa.field("end", pa.string(), nullable=True, metadata={"jsonschema_type": "string (integer)"}),
        pa.field("orientation", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("order", pa.int64(), nullable=True, metadata={"jsonschema_type": "integer"}),
        pa.field("ribosomal_slippage", pa.int64(), nullable=True, metadata={"jsonschema_type": "integer"}),
    ]
)

TRANSCRIPTS_GENOMIC_LOCATIONS_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("genomic_accession_version", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("sequence_name", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("genomic_range", GENOMIC_LOCATIONS_GENOMIC_RANGE_STRUCT, nullable=True),
        pa.field("exons", pa.list_(pa.field("item", GENOMIC_LOCATIONS_EXONS_STRUCT, nullable=False)), nullable=True),
    ]
)

PROTEIN_MATURE_PEPTIDES_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("accession_version", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("name", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("length", pa.int64(), nullable=True, metadata={"jsonschema_type": "integer"}),
    ]
)

PROTEIN_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("accession_version", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("name", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("length", pa.int64(), nullable=True, metadata={"jsonschema_type": "integer"}),
        pa.field("isoform_name", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("ensembl_protein", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field(
            "mature_peptides", pa.list_(pa.field("item", PROTEIN_MATURE_PEPTIDES_STRUCT, nullable=False)), nullable=True
        ),
    ]
)

TRANSCRIPTS_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("accession_version", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("name", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("length", pa.int64(), nullable=True, metadata={"jsonschema_type": "integer"}),
        pa.field("cds", CDS_STRUCT, nullable=True),
        pa.field(
            "genomic_locations",
            pa.list_(pa.field("item", TRANSCRIPTS_GENOMIC_LOCATIONS_STRUCT, nullable=False)),
            nullable=True,
        ),
        pa.field("ensembl_transcript", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("protein", PROTEIN_STRUCT, nullable=True),
        pa.field("type", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("select_category", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
    ]
)

PROTEINS_MATURE_PEPTIDES_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("accession_version", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("name", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("length", pa.int64(), nullable=True, metadata={"jsonschema_type": "integer"}),
    ]
)

PROTEINS_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("accession_version", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("name", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("length", pa.int64(), nullable=True, metadata={"jsonschema_type": "integer"}),
        pa.field("isoform_name", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("ensembl_protein", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field(
            "mature_peptides",
            pa.list_(pa.field("item", PROTEINS_MATURE_PEPTIDES_STRUCT, nullable=False)),
            nullable=True,
        ),
    ]
)

GENOMIC_LOCATIONS_GENOMIC_LOCATIONS_GENOMIC_RANGE_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("begin", pa.string(), nullable=True, metadata={"jsonschema_type": "string (integer)"}),
        pa.field("end", pa.string(), nullable=True, metadata={"jsonschema_type": "string (integer)"}),
        pa.field("orientation", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("order", pa.int64(), nullable=True, metadata={"jsonschema_type": "integer"}),
        pa.field("ribosomal_slippage", pa.int64(), nullable=True, metadata={"jsonschema_type": "integer"}),
    ]
)

GENOMIC_LOCATIONS_GENOMIC_LOCATIONS_EXONS_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("begin", pa.string(), nullable=True, metadata={"jsonschema_type": "string (integer)"}),
        pa.field("end", pa.string(), nullable=True, metadata={"jsonschema_type": "string (integer)"}),
        pa.field("orientation", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("order", pa.int64(), nullable=True, metadata={"jsonschema_type": "integer"}),
        pa.field("ribosomal_slippage", pa.int64(), nullable=True, metadata={"jsonschema_type": "integer"}),
    ]
)

ANNOTATIONS_GENOMIC_LOCATIONS_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("genomic_accession_version", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("sequence_name", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("genomic_range", GENOMIC_LOCATIONS_GENOMIC_LOCATIONS_GENOMIC_RANGE_STRUCT, nullable=True),
        pa.field(
            "exons",
            pa.list_(pa.field("item", GENOMIC_LOCATIONS_GENOMIC_LOCATIONS_EXONS_STRUCT, nullable=False)),
            nullable=True,
        ),
    ]
)

ANNOTATIONS_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("assembly_accession", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("assembly_name", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("annotation_name", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("annotation_release_date", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field(
            "genomic_locations",
            pa.list_(pa.field("item", ANNOTATIONS_GENOMIC_LOCATIONS_STRUCT, nullable=False)),
            nullable=True,
        ),
    ]
)

ANNOTATION_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("gene_id", pa.string(), nullable=True, metadata={"jsonschema_type": "string (integer)"}),
        pa.field("symbol", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("description", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("name", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("tax_id", pa.string(), nullable=True, metadata={"jsonschema_type": "string (integer)"}),
        pa.field("taxname", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("common_name", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("type", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("gene_type", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("rna_type", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("orientation", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("locus_tag", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field(
            "reference_standards", pa.list_(pa.field("item", REFERENCE_STANDARDS_STRUCT, nullable=False)), nullable=True
        ),
        pa.field("genomic_regions", pa.list_(pa.field("item", GENOMIC_REGIONS_STRUCT, nullable=False)), nullable=True),
        pa.field("transcripts", pa.list_(pa.field("item", TRANSCRIPTS_STRUCT, nullable=False)), nullable=True),
        pa.field("proteins", pa.list_(pa.field("item", PROTEINS_STRUCT, nullable=False)), nullable=True),
        pa.field(
            "chromosomes",
            pa.list_(pa.field("item", pa.string(), nullable=False, metadata={"jsonschema_type": "string"})),
            nullable=True,
        ),
        pa.field(
            "swiss_prot_accessions",
            pa.list_(pa.field("item", pa.string(), nullable=False, metadata={"jsonschema_type": "string"})),
            nullable=True,
        ),
        pa.field(
            "ensembl_gene_ids",
            pa.list_(pa.field("item", pa.string(), nullable=False, metadata={"jsonschema_type": "string"})),
            nullable=True,
        ),
        pa.field(
            "omim_ids",
            pa.list_(pa.field("item", pa.string(), nullable=False, metadata={"jsonschema_type": "string"})),
            nullable=True,
        ),
        pa.field(
            "synonyms",
            pa.list_(pa.field("item", pa.string(), nullable=False, metadata={"jsonschema_type": "string"})),
            nullable=True,
        ),
        pa.field("annotations", pa.list_(pa.field("item", ANNOTATIONS_STRUCT, nullable=False)), nullable=True),
    ]
)

WARNING_REPLACED_ID_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("requested", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("returned", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
    ]
)

REPORTS_WARNING_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("gene_warning_code", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("reason", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("message", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("replaced_id", WARNING_REPLACED_ID_STRUCT, nullable=True),
        pa.field("unrecognized_identifier", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
    ]
)

ERRORS_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("assembly_error_code", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("gene_error_code", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("organelle_error_code", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("virus_error_code", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("taxonomy_error_code", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("sequence_error_code", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("reason", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("message", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field(
            "invalid_identifiers",
            pa.list_(pa.field("item", pa.string(), nullable=False, metadata={"jsonschema_type": "string"})),
            nullable=True,
        ),
    ]
)

REPORTS_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("annotation", ANNOTATION_STRUCT, nullable=True),
        pa.field(
            "query",
            pa.list_(pa.field("item", pa.string(), nullable=False, metadata={"jsonschema_type": "string"})),
            nullable=True,
        ),
        pa.field("warning", REPORTS_WARNING_STRUCT, nullable=True),
        pa.field("errors", pa.list_(pa.field("item", ERRORS_STRUCT, nullable=False)), nullable=True),
        pa.field("row_id", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
    ]
)

ERROR_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("assembly_error_code", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("gene_error_code", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("organelle_error_code", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("virus_error_code", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("taxonomy_error_code", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("sequence_error_code", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("reason", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("message", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field(
            "invalid_identifiers",
            pa.list_(pa.field("item", pa.string(), nullable=False, metadata={"jsonschema_type": "string"})),
            nullable=True,
        ),
    ]
)

WARNING_WARNING_REPLACED_ID_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("requested", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("returned", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
    ]
)

MESSAGES_WARNING_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("gene_warning_code", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("reason", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("message", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("replaced_id", WARNING_WARNING_REPLACED_ID_STRUCT, nullable=True),
        pa.field("unrecognized_identifier", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
    ]
)

MESSAGES_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("error", ERROR_STRUCT, nullable=True),
        pa.field("warning", MESSAGES_WARNING_STRUCT, nullable=True),
    ]
)

ANNOTATION_REPORT_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("reports", pa.list_(pa.field("item", REPORTS_STRUCT, nullable=False)), nullable=True),
        pa.field("messages", pa.list_(pa.field("item", MESSAGES_STRUCT, nullable=False)), nullable=True),
        pa.field("total_count", pa.int64(), nullable=True, metadata={"jsonschema_type": "integer"}),
        pa.field("next_page_token", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
    ]
)

ORGANISM_LINEAGE_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("tax_id", pa.int64(), nullable=True, metadata={"jsonschema_type": "integer"}),
        pa.field("name", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
    ]
)

ORGANISM_INFRASPECIFIC_NAMES_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("breed", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("cultivar", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("ecotype", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("isolate", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("sex", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("strain", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
    ]
)

DATASET_REPORT_ORGANISM_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("tax_id", pa.int64(), nullable=True, metadata={"jsonschema_type": "integer"}),
        pa.field("sci_name", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("organism_name", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("common_name", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("lineage", pa.list_(pa.field("item", ORGANISM_LINEAGE_STRUCT, nullable=False)), nullable=True),
        pa.field("strain", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("pangolin_classification", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("infraspecific_names", ORGANISM_INFRASPECIFIC_NAMES_STRUCT, nullable=True),
    ]
)

PAIRED_ASSEMBLY_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("accession", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("status", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("annotation_name", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("only_genbank", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("only_refseq", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("changed", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("manual_diff", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("refseq_genbank_are_different", pa.bool_(), nullable=True, metadata={"jsonschema_type": "boolean"}),
        pa.field("differences", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
    ]
)

BIOPROJECT_LINEAGE_BIOPROJECTS_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("accession", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("title", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("parent_accession", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field(
            "parent_accessions",
            pa.list_(pa.field("item", pa.string(), nullable=False, metadata={"jsonschema_type": "string"})),
            nullable=True,
        ),
    ]
)

BIOPROJECT_LINEAGE_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field(
            "bioprojects",
            pa.list_(pa.field("item", BIOPROJECT_LINEAGE_BIOPROJECTS_STRUCT, nullable=False)),
            nullable=True,
        )
    ]
)

LINKED_ASSEMBLIES_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("linked_assembly", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("assembly_type", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
    ]
)

ATYPICAL_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("is_atypical", pa.bool_(), nullable=True, metadata={"jsonschema_type": "boolean"}),
        pa.field(
            "warnings",
            pa.list_(pa.field("item", pa.string(), nullable=False, metadata={"jsonschema_type": "string"})),
            nullable=True,
        ),
    ]
)

SAMPLE_IDS_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("db", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("label", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("value", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
    ]
)

ORGANISM_ORGANISM_LINEAGE_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("tax_id", pa.int64(), nullable=True, metadata={"jsonschema_type": "integer"}),
        pa.field("name", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
    ]
)

ORGANISM_ORGANISM_INFRASPECIFIC_NAMES_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("breed", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("cultivar", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("ecotype", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("isolate", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("sex", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("strain", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
    ]
)

DESCRIPTION_ORGANISM_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("tax_id", pa.int64(), nullable=True, metadata={"jsonschema_type": "integer"}),
        pa.field("sci_name", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("organism_name", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("common_name", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field(
            "lineage", pa.list_(pa.field("item", ORGANISM_ORGANISM_LINEAGE_STRUCT, nullable=False)), nullable=True
        ),
        pa.field("strain", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("pangolin_classification", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("infraspecific_names", ORGANISM_ORGANISM_INFRASPECIFIC_NAMES_STRUCT, nullable=True),
    ]
)

DESCRIPTION_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("title", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("organism", DESCRIPTION_ORGANISM_STRUCT, nullable=True),
        pa.field("comment", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
    ]
)

CONTACTS_STRUCT: Final[pa.StructType] = pa.struct(
    [pa.field("lab", pa.string(), nullable=True, metadata={"jsonschema_type": "string"})]
)

OWNER_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("name", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("contacts", pa.list_(pa.field("item", CONTACTS_STRUCT, nullable=False)), nullable=True),
    ]
)

BIOSAMPLE_BIOPROJECTS_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("accession", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("title", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("parent_accession", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field(
            "parent_accessions",
            pa.list_(pa.field("item", pa.string(), nullable=False, metadata={"jsonschema_type": "string"})),
            nullable=True,
        ),
    ]
)

ATTRIBUTES_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("name", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("value", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
    ]
)

STATUS_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("status", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("when", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
    ]
)

BIOSAMPLE_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("accession", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("last_updated", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("publication_date", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("submission_date", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("sample_ids", pa.list_(pa.field("item", SAMPLE_IDS_STRUCT, nullable=False)), nullable=True),
        pa.field("description", DESCRIPTION_STRUCT, nullable=True),
        pa.field("owner", OWNER_STRUCT, nullable=True),
        pa.field(
            "models",
            pa.list_(pa.field("item", pa.string(), nullable=False, metadata={"jsonschema_type": "string"})),
            nullable=True,
        ),
        pa.field(
            "bioprojects", pa.list_(pa.field("item", BIOSAMPLE_BIOPROJECTS_STRUCT, nullable=False)), nullable=True
        ),
        pa.field("package", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("attributes", pa.list_(pa.field("item", ATTRIBUTES_STRUCT, nullable=False)), nullable=True),
        pa.field("status", STATUS_STRUCT, nullable=True),
        pa.field("age", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("biomaterial_provider", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("breed", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("collected_by", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("collection_date", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("cultivar", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("dev_stage", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("ecotype", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("geo_loc_name", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("host", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("host_disease", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("identified_by", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("ifsac_category", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("isolate", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("isolate_name_alias", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("isolation_source", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("lat_lon", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("project_name", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("sample_name", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("serovar", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("sex", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("source_type", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("strain", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("sub_species", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("tissue", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("serotype", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
    ]
)

ASSEMBLY_INFO_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("assembly_level", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("assembly_status", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("paired_assembly", PAIRED_ASSEMBLY_STRUCT, nullable=True),
        pa.field("assembly_name", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("assembly_long_name", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("assembly_type", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field(
            "bioproject_lineage", pa.list_(pa.field("item", BIOPROJECT_LINEAGE_STRUCT, nullable=False)), nullable=True
        ),
        pa.field("bioproject_accession", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("submission_date", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("release_date", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("description", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("submitter", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("refseq_category", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("synonym", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("linked_assembly", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field(
            "linked_assemblies", pa.list_(pa.field("item", LINKED_ASSEMBLIES_STRUCT, nullable=False)), nullable=True
        ),
        pa.field("atypical", ATYPICAL_STRUCT, nullable=True),
        pa.field(
            "genome_notes",
            pa.list_(pa.field("item", pa.string(), nullable=False, metadata={"jsonschema_type": "string"})),
            nullable=True,
        ),
        pa.field("sequencing_tech", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("assembly_method", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("grouping_method", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("biosample", BIOSAMPLE_STRUCT, nullable=True),
        pa.field("blast_url", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("comments", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("suppression_reason", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("diploid_role", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
    ]
)

ASSEMBLY_STATS_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("total_number_of_chromosomes", pa.int64(), nullable=True, metadata={"jsonschema_type": "integer"}),
        pa.field("total_sequence_length", pa.string(), nullable=True, metadata={"jsonschema_type": "string (integer)"}),
        pa.field("total_ungapped_length", pa.string(), nullable=True, metadata={"jsonschema_type": "string (integer)"}),
        pa.field("number_of_contigs", pa.int64(), nullable=True, metadata={"jsonschema_type": "integer"}),
        pa.field("contig_n50", pa.int64(), nullable=True, metadata={"jsonschema_type": "integer"}),
        pa.field("contig_l50", pa.int64(), nullable=True, metadata={"jsonschema_type": "integer"}),
        pa.field("number_of_scaffolds", pa.int64(), nullable=True, metadata={"jsonschema_type": "integer"}),
        pa.field("scaffold_n50", pa.int64(), nullable=True, metadata={"jsonschema_type": "integer"}),
        pa.field("scaffold_l50", pa.int64(), nullable=True, metadata={"jsonschema_type": "integer"}),
        pa.field("gaps_between_scaffolds_count", pa.int64(), nullable=True, metadata={"jsonschema_type": "integer"}),
        pa.field("number_of_component_sequences", pa.int64(), nullable=True, metadata={"jsonschema_type": "integer"}),
        pa.field("atgc_count", pa.string(), nullable=True, metadata={"jsonschema_type": "string (integer)"}),
        pa.field("gc_count", pa.string(), nullable=True, metadata={"jsonschema_type": "string (integer)"}),
        pa.field("gc_percent", pa.float64(), nullable=True, metadata={"jsonschema_type": "number"}),
        pa.field("genome_coverage", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("number_of_organelles", pa.int64(), nullable=True, metadata={"jsonschema_type": "integer"}),
    ]
)

ORGANELLE_INFO_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("assembly_name", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("infraspecific_name", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field(
            "bioproject",
            pa.list_(pa.field("item", pa.string(), nullable=False, metadata={"jsonschema_type": "string"})),
            nullable=True,
        ),
        pa.field("description", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("total_seq_length", pa.string(), nullable=True, metadata={"jsonschema_type": "string (integer)"}),
        pa.field("submitter", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
    ]
)

ADDITIONAL_SUBMITTERS_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("genbank_accession", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("refseq_accession", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("chr_name", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("molecule_type", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("submitter", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("bioproject_accession", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
    ]
)

GENE_COUNTS_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("total", pa.int64(), nullable=True, metadata={"jsonschema_type": "integer"}),
        pa.field("protein_coding", pa.int64(), nullable=True, metadata={"jsonschema_type": "integer"}),
        pa.field("non_coding", pa.int64(), nullable=True, metadata={"jsonschema_type": "integer"}),
        pa.field("pseudogene", pa.int64(), nullable=True, metadata={"jsonschema_type": "integer"}),
        pa.field("other", pa.int64(), nullable=True, metadata={"jsonschema_type": "integer"}),
    ]
)

STATS_STRUCT: Final[pa.StructType] = pa.struct([pa.field("gene_counts", GENE_COUNTS_STRUCT, nullable=True)])

BUSCO_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("busco_lineage", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("busco_ver", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("complete", pa.float64(), nullable=True, metadata={"jsonschema_type": "number"}),
        pa.field("single_copy", pa.float64(), nullable=True, metadata={"jsonschema_type": "number"}),
        pa.field("duplicated", pa.float64(), nullable=True, metadata={"jsonschema_type": "number"}),
        pa.field("fragmented", pa.float64(), nullable=True, metadata={"jsonschema_type": "number"}),
        pa.field("missing", pa.float64(), nullable=True, metadata={"jsonschema_type": "number"}),
        pa.field("total_count", pa.string(), nullable=True, metadata={"jsonschema_type": "string (integer)"}),
    ]
)

ANNOTATION_INFO_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("name", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("provider", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("release_date", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("report_url", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("stats", STATS_STRUCT, nullable=True),
        pa.field("busco", BUSCO_STRUCT, nullable=True),
        pa.field("method", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("pipeline", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("software_version", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("status", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("release_version", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
    ]
)

WGS_INFO_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("wgs_project_accession", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("master_wgs_url", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("wgs_contigs_url", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
    ]
)

TYPE_MATERIAL_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("type_label", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("type_display_text", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
    ]
)

CHECKM_INFO_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("checkm_marker_set", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("checkm_species_tax_id", pa.int64(), nullable=True, metadata={"jsonschema_type": "integer"}),
        pa.field("checkm_marker_set_rank", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("checkm_version", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("completeness", pa.float64(), nullable=True, metadata={"jsonschema_type": "number"}),
        pa.field("contamination", pa.float64(), nullable=True, metadata={"jsonschema_type": "number"}),
        pa.field("completeness_percentile", pa.float64(), nullable=True, metadata={"jsonschema_type": "number"}),
    ]
)

SUBMITTED_ANI_MATCH_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("assembly", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("organism_name", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("category", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("ani", pa.float64(), nullable=True, metadata={"jsonschema_type": "number"}),
        pa.field("assembly_coverage", pa.float64(), nullable=True, metadata={"jsonschema_type": "number"}),
        pa.field("type_assembly_coverage", pa.float64(), nullable=True, metadata={"jsonschema_type": "number"}),
    ]
)

BEST_ANI_MATCH_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("assembly", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("organism_name", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("category", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("ani", pa.float64(), nullable=True, metadata={"jsonschema_type": "number"}),
        pa.field("assembly_coverage", pa.float64(), nullable=True, metadata={"jsonschema_type": "number"}),
        pa.field("type_assembly_coverage", pa.float64(), nullable=True, metadata={"jsonschema_type": "number"}),
    ]
)

AVERAGE_NUCLEOTIDE_IDENTITY_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("taxonomy_check_status", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("match_status", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("submitted_organism", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("submitted_species", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("category", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("submitted_ani_match", SUBMITTED_ANI_MATCH_STRUCT, nullable=True),
        pa.field("best_ani_match", BEST_ANI_MATCH_STRUCT, nullable=True),
        pa.field("comment", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
    ]
)

DATASET_REPORT_STRUCT: Final[pa.StructType] = pa.struct(
    [
        pa.field("accession", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("current_accession", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("paired_accession", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("source_database", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
        pa.field("organism", DATASET_REPORT_ORGANISM_STRUCT, nullable=True),
        pa.field("assembly_info", ASSEMBLY_INFO_STRUCT, nullable=True),
        pa.field("assembly_stats", ASSEMBLY_STATS_STRUCT, nullable=True),
        pa.field("organelle_info", pa.list_(pa.field("item", ORGANELLE_INFO_STRUCT, nullable=False)), nullable=True),
        pa.field(
            "additional_submitters",
            pa.list_(pa.field("item", ADDITIONAL_SUBMITTERS_STRUCT, nullable=False)),
            nullable=True,
        ),
        pa.field("annotation_info", ANNOTATION_INFO_STRUCT, nullable=True),
        pa.field("wgs_info", WGS_INFO_STRUCT, nullable=True),
        pa.field("type_material", TYPE_MATERIAL_STRUCT, nullable=True),
        pa.field("checkm_info", CHECKM_INFO_STRUCT, nullable=True),
        pa.field("average_nucleotide_identity", AVERAGE_NUCLEOTIDE_IDENTITY_STRUCT, nullable=True),
    ]
)

ANNOTATION_REPORT_SCHEMA: Final[pa.Schema] = assemble_pyarrow_schema(ANNOTATION_REPORT_STRUCT)
DATASET_REPORT_SCHEMA: Final[pa.Schema] = assemble_pyarrow_schema(DATASET_REPORT_STRUCT)
SCHEMAS: Final[dict[str, pa.Schema]] = {
    "annotation_report": ANNOTATION_REPORT_SCHEMA,
    "dataset_report": DATASET_REPORT_SCHEMA,
}
