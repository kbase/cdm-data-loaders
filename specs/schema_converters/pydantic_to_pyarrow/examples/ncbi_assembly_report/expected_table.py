"""Expected PyArrow table for ncbi_assembly_report."""

import datetime as dt

import pyarrow as pa
from expected_schema import EXPECTED_SCHEMA

EXPECTED_TABLE = pa.Table.from_pylist(
    [
        {
            "accession": "GCF_000005845.2",
            "paired_accession": "GCA_000005845.2",
            "source_database": "SOURCE_DATABASE_REFSEQ",
            "assembly_level": "Complete Genome",
            "release_date": dt.date(2013, 9, 26),
            "updated_at": dt.datetime(2024, 3, 15, 10, 0, 0, tzinfo=dt.UTC),
            "organism": {
                "tax_id": 511145,
                "organism_name": "Escherichia coli str. K-12 substr. MG1655",
                "strain": "K-12 substr. MG1655",
            },
            "assembly_stats": {
                "total_sequence_length": 4641652,
                "number_of_contigs": 1,
                "contig_n50": 4641652,
                "gc_percent": 50.8,
            },
            "annotation_info": {
                "name": "NCBI E. coli K-12 Annotation",
                "provider": "NCBI RefSeq",
                "release_date": dt.date(2023, 5, 1),
                "total_gene_count": 4494,
            },
            "bioproject_accessions": ["PRJNA57779", "PRJNA225"],
        },
        {
            "accession": "GCA_000009045.1",
            "paired_accession": None,
            "source_database": "SOURCE_DATABASE_GENBANK",
            "assembly_level": "Complete Genome",
            "release_date": dt.date(2009, 3, 17),
            "updated_at": dt.datetime(2023, 11, 20, 8, 30, 0, tzinfo=dt.UTC),
            "organism": {"tax_id": 224308, "organism_name": "Bacillus subtilis subsp. subtilis str. 168"},
            "assembly_stats": {
                "total_sequence_length": 4215606,
                "number_of_contigs": 1,
                "contig_n50": 4215606,
                "gc_percent": 43.5,
            },
            "annotation_info": None,
            "bioproject_accessions": ["PRJNA27"],
        },
    ],
    schema=EXPECTED_SCHEMA,
)
