"""Shared expected-result constants for standalone file parser tests."""

from typing import Any, Final

STATS_OUTPUT: Final[dict[str, dict[str, Any]]] = {
    "FW305-3-2-15-C-TSA1_scaffolds.fna": {
        "scaffolds": 59,
        "contigs": 61,
        "gc_avg": 0.65985,
        "gc_std": 0.02052,
        "filename": "/home/runner/work/cdm-data-loaders/cdm-data-loaders/tests/data/FW305-3-2-15-C-TSA1/FW305-3-2-15-C-TSA1_scaffolds.fna",
    },
    "FW305-C-112.1_scaffolds.fna": {
        "scaffolds": 28,
        "contigs": 31,
        "gc_avg": 0.69368,
        "gc_std": 0.03501,
        "filename": "/home/runner/work/cdm-data-loaders/cdm-data-loaders/tests/data/FW305-C-112.1/FW305-C-112.1_scaffolds.fna",
    },
}

CHECKM2_OUTPUT: Final[dict[str, dict[str, float | None]]] = {
    "FW305-3-2-15-C-TSA1_scaffolds": {
        "checkm2_completeness": 99.99,
        "checkm2_contamination": 1.09,
    },
    "FW305-C-112.1_scaffolds": {"checkm2_completeness": None, "checkm2_contamination": 12.27},
}

RESULTS: Final[dict[str, dict[str, Any]]] = {
    "single": {
        "stats": {"FW305-3-2-15-C-TSA1_scaffolds.fna": STATS_OUTPUT["FW305-3-2-15-C-TSA1_scaffolds.fna"]},
        "checkm2": {"FW305-3-2-15-C-TSA1_scaffolds": CHECKM2_OUTPUT["FW305-3-2-15-C-TSA1_scaffolds"]},
    },
    "multi": {"stats": STATS_OUTPUT, "checkm2": CHECKM2_OUTPUT},
}
