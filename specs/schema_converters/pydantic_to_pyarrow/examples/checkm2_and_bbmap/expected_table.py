"""Expected PyArrow table for checkm2_and_bbmap."""

from decimal import Decimal

import pyarrow as pa
from expected_schema import EXPECTED_SCHEMA

EXPECTED_TABLE = pa.Table.from_pylist(
    [
        {
            "sample_id": "SAMN10000001",
            "checkm2": {
                "completeness": Decimal("99.45"),
                "contamination": Decimal("0.32"),
                "coding_density": 0.892,
                "translation_table": 11,
            },
            "bbmap": {
                "filename": "/data/assemblies/SAMN10000001.fna.gz",
                "scaffolds": 12,
                "contigs": 14,
                "scaf_bp": 4521000,
                "contig_bp": 4520500,
                "gc_avg": 0.508,
                "gc_std": 0.032,
                "scaf_n50": 850000,
                "scaf_l50": 2,
                "ctg_n50": 720000,
                "ctg_l50": 3,
                "scaf_max": 1650000,
                "ctg_max": 1400000,
            },
            "extra_scores": {"gunc_clade_separation": 0.02, "busco_complete": 98.8},
            "mimag_tier": "high",
        },
        {
            "sample_id": "SAMN10000002",
            "checkm2": {
                "completeness": Decimal("74.10"),
                "contamination": Decimal("3.85"),
                "coding_density": None,
                "translation_table": 11,
            },
            "bbmap": {
                "filename": "/data/assemblies/SAMN10000002.fna.gz",
                "scaffolds": 145,
                "contigs": 160,
                "scaf_bp": 2910000,
                "contig_bp": 2905000,
                "gc_avg": 0.412,
                "gc_std": 0.045,
                "scaf_n50": 42000,
                "scaf_l50": 22,
                "ctg_n50": 38000,
                "ctg_l50": 25,
                "scaf_max": 180000,
                "ctg_max": 160000,
            },
            "extra_scores": {},
            "mimag_tier": "medium",
        },
    ],
    schema=EXPECTED_SCHEMA,
)
