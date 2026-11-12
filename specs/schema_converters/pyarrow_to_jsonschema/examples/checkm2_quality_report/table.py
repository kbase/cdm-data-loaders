"""Sample PyArrow table for CheckM2 quality report."""

from decimal import Decimal

import pyarrow as pa
from schema import SCHEMA

TABLE = pa.Table.from_pylist(
    [
        {
            "bin_id": "MAG_00042",
            "completeness": Decimal("98.45"),
            "contamination": Decimal("1.12"),
            "coding_density": 0.8925,
            "contig_n50": 185400,
            "total_contigs": 34,
            "translation_table": 11,
            "assembly_stats": {
                "genome_size_bp": 4125890,
                "max_contig_bp": 612300,
                "gc_fraction": 0.625,
            },
            "kmer_metrics": [("tetranucleotide_z", 1.125), ("coverage_cv", 0.085)],
            "qc_warnings": [],
        },
        {
            "bin_id": "MAG_00043",
            "completeness": Decimal("72.10"),
            "contamination": Decimal("4.80"),
            "coding_density": 0.845,
            "contig_n50": 19200,
            "total_contigs": 290,
            "translation_table": 11,
            "assembly_stats": {
                "genome_size_bp": 2890100,
                "max_contig_bp": 74500,
                "gc_fraction": 0.4375,
            },
            "kmer_metrics": None,
            "qc_warnings": ["fragmented_assembly"],
        },
    ],
    schema=SCHEMA,
)
