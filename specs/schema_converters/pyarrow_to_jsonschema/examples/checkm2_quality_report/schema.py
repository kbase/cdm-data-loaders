"""CheckM2 genome quality assessment PyArrow schema."""

import pyarrow as pa

SCHEMA = pa.schema(
    [
        pa.field("bin_id", pa.string(), nullable=False, metadata={"comment": "MAG or isolate bin identifier."}),
        pa.field(
            "completeness", pa.decimal128(5, 2), nullable=False, metadata={"comment": "Estimated completeness %."}
        ),
        pa.field(
            "contamination",
            pa.decimal128(5, 2),
            nullable=False,
            metadata={"comment": "Estimated contamination %."},
        ),
        pa.field("coding_density", pa.float64(), nullable=False),
        pa.field("contig_n50", pa.uint64(), nullable=False),
        pa.field("total_contigs", pa.uint32(), nullable=False),
        pa.field("translation_table", pa.dictionary(pa.int8(), pa.int32()), nullable=False),
        pa.field(
            "assembly_stats",
            pa.struct(
                [
                    pa.field("genome_size_bp", pa.uint64(), nullable=False),
                    pa.field("max_contig_bp", pa.uint64(), nullable=False),
                    pa.field("gc_fraction", pa.float32(), nullable=False),
                ]
            ),
            nullable=False,
        ),
        pa.field(
            "kmer_metrics",
            pa.map_(pa.string(), pa.field("value", pa.float64(), nullable=False)),
            nullable=True,
        ),
        pa.field(
            "qc_warnings",
            pa.list_(pa.field("item", pa.string(), nullable=False)),
            nullable=True,
        ),
    ],
    metadata={
        "title": "CheckM2QualityReport",
        "$id": "https://kbase.us/schemas/cdm/checkm2_quality_report.json",
        "comment": "CheckM2 genome completeness, contamination, and assembly metrics.",
    },
)
