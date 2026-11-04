"""Expected PyArrow schema for checkm2_and_bbmap."""

# ruff: noqa: INP001

import pyarrow as pa

EXPECTED_SCHEMA = pa.schema(
    [
        pa.field(
            "sample_id",
            pa.string(),
            nullable=False,
            metadata={"comment": "Sample / bin identifier", "pydantic_type": "string"},
        ),
        pa.field(
            "checkm2",
            pa.struct(
                [
                    pa.field(
                        "completeness",
                        pa.decimal128(5, 2),
                        nullable=False,
                        metadata={"comment": "Estimated completeness (%)", "pydantic_type": "decimal"},
                    ),
                    pa.field(
                        "contamination",
                        pa.decimal128(5, 2),
                        nullable=False,
                        metadata={"comment": "Estimated contamination (%)", "pydantic_type": "decimal"},
                    ),
                    pa.field("coding_density", pa.float64(), nullable=True, metadata={"pydantic_type": "number"}),
                    pa.field("translation_table", pa.int64(), nullable=False, metadata={"pydantic_type": "integer"}),
                ]
            ),
            nullable=False,
            metadata={"comment": "CheckM2 completeness and contamination quality metrics."},
        ),
        pa.field(
            "bbmap",
            pa.struct(
                [
                    pa.field("filename", pa.string(), nullable=False, metadata={"pydantic_type": "path"}),
                    pa.field("scaffolds", pa.int64(), nullable=False, metadata={"pydantic_type": "integer"}),
                    pa.field("contigs", pa.int64(), nullable=False, metadata={"pydantic_type": "integer"}),
                    pa.field("scaf_bp", pa.int64(), nullable=False, metadata={"pydantic_type": "integer"}),
                    pa.field("contig_bp", pa.int64(), nullable=False, metadata={"pydantic_type": "integer"}),
                    pa.field("gc_avg", pa.float64(), nullable=False, metadata={"pydantic_type": "number"}),
                    pa.field("gc_std", pa.float64(), nullable=False, metadata={"pydantic_type": "number"}),
                    pa.field("scaf_n50", pa.int64(), nullable=False, metadata={"pydantic_type": "integer"}),
                    pa.field("scaf_l50", pa.int64(), nullable=False, metadata={"pydantic_type": "integer"}),
                    pa.field("ctg_n50", pa.int64(), nullable=False, metadata={"pydantic_type": "integer"}),
                    pa.field("ctg_l50", pa.int64(), nullable=False, metadata={"pydantic_type": "integer"}),
                    pa.field("scaf_max", pa.int64(), nullable=False, metadata={"pydantic_type": "integer"}),
                    pa.field("ctg_max", pa.int64(), nullable=False, metadata={"pydantic_type": "integer"}),
                ]
            ),
            nullable=False,
            metadata={"comment": "BBMap stats.sh assembly summary metrics."},
        ),
        pa.field(
            "extra_scores",
            pa.map_(
                pa.field("key", pa.string(), nullable=False),
                pa.field("value", pa.float64(), nullable=False, metadata={"pydantic_type": "number"}),
            ),
            nullable=False,
        ),
        pa.field(
            "mimag_tier",
            pa.string(),
            nullable=False,
            metadata={"comment": "MIMAG quality tier (high, medium, or low)", "pydantic_type": "string"},
        ),
    ]
)
