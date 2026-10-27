"""Expected PyArrow table for maximal real-world example ncbi_annotation_report."""

# ruff: noqa: INP001

import datetime as dt

import pyarrow as pa
from expected_schema import EXPECTED_SCHEMA

EXPECTED_TABLE = pa.Table.from_pylist(
    [
        {
            "reports": [
                {
                    "annotation": {
                        "gene_id": "944742",
                        "symbol": "thrL",
                        "description": "thr operon leader peptide",
                        "tax_id": "511145",
                        "taxname": "Escherichia coli str. K-12 substr. MG1655",
                        "gene_type": "PROTEIN_CODING",
                        "orientation": "plus",
                        "locus_tag": "b0001",
                        "annotations": [
                            {
                                "assembly_accession": "GCF_000005845.2",
                                "assembly_name": "ASM584v2",
                                "annotation_name": "NCBI Escherichia coli str. K-12 substr. MG1655 Annotation Release 102",
                                "annotation_release_date": dt.date(2013, 9, 26),
                                "genomic_locations": [
                                    {
                                        "genomic_accession_version": "NC_000913.3",
                                        "sequence_name": "Chromosome",
                                        "genomic_range": {
                                            "begin": "190",
                                            "end": "255",
                                            "orientation": "plus",
                                            "order": 1,
                                        },
                                    }
                                ],
                            }
                        ],
                    },
                    "query": ["b0001"],
                    "row_id": "1",
                }
            ],
            "total_count": 4639,
            "next_page_token": "tok_next_page_002",
        },
    ],
    schema=EXPECTED_SCHEMA,
)
