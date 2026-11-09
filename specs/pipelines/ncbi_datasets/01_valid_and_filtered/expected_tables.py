"""Expected PyArrow tables for ncbi_datasets 01_valid_and_filtered."""

import pyarrow as pa

from cdm_data_loaders.schemas.ncbi_datasets.schema import (
    ANNOTATION_REPORT_SCHEMA,
    DATASET_REPORT_SCHEMA,
)

EXPECTED_TABLES: dict[str, pa.Table] = {
    "annotation_report": pa.Table.from_pylist(
        [
            {
                "reports": [
                    {
                        "annotation": {
                            "annotations": [{"assembly_accession": "GCA_000234645.4"}],
                            "gene_type": "protein-coding",
                            "genomic_regions": [
                                {
                                    "gene_range": {
                                        "accession_version": "JF734911.1",
                                        "range": [{"begin": "562", "end": "1713", "orientation": "plus"}],
                                    }
                                }
                            ],
                            "name": "integrase/recombinase",
                            "proteins": [
                                {
                                    "accession_version": "AET85156.1",
                                    "length": 383,
                                    "name": "integrase/recombinase",
                                }
                            ],
                            "symbol": "int",
                        },
                        "row_id": "1",
                    }
                ],
                "total_count": 1,
            }
        ],
        schema=ANNOTATION_REPORT_SCHEMA,
    ),
    "dataset_report": pa.Table.from_pylist(
        [
            {
                "accession": "GCA_018350175.1",
                "current_accession": "GCA_018350175.1",
                "paired_accession": "GCF_018350175.1",
                "source_database": "SOURCE_DATABASE_GENBANK",
                "organism": {
                    "tax_id": 9685,
                    "organism_name": "Felis catus",
                    "common_name": "domestic cat",
                    "infraspecific_names": {"isolate": "Fca126", "sex": "female"},
                },
                "assembly_info": {
                    "assembly_level": "Chromosome",
                    "assembly_status": "current",
                    "assembly_name": "F.catus_Fca126_mat1.0",
                    "assembly_type": "haploid",
                    "bioproject_accession": "PRJNA684600",
                    "release_date": "2021-05-13",
                    "submitter": "Texas A&M University",
                    "refseq_category": "reference genome",
                    "sequencing_tech": "PacBio Sequel 2",
                    "assembly_method": "NextDenovo v. 2.2-beta.0",
                },
                "assembly_stats": {
                    "total_number_of_chromosomes": 19,
                    "total_sequence_length": "2425730029",
                    "number_of_contigs": 109,
                    "contig_n50": 90731473,
                    "contig_l50": 10,
                    "gc_percent": 42.0,
                },
                "wgs_info": {"wgs_project_accession": "JAFEKA01"},
            }
        ],
        schema=DATASET_REPORT_SCHEMA,
    ),
}
