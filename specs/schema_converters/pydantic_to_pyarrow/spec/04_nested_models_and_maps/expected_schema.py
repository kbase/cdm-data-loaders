"""Expected PyArrow schema for 04_nested_models_and_maps."""

import pyarrow as pa

EXPECTED_SCHEMA = pa.schema(
    [
        pa.field("id", pa.string(), nullable=False, metadata={"pydantic_type": "string"}),
        pa.field(
            "organism",
            pa.struct(
                [
                    pa.field("taxon_id", pa.int64(), nullable=False, metadata={"pydantic_type": "integer"}),
                    pa.field("scientific_name", pa.string(), nullable=False, metadata={"pydantic_type": "string"}),
                    pa.field("strain", pa.string(), nullable=True, metadata={"pydantic_type": "string"}),
                ]
            ),
            nullable=False,
            metadata={"comment": "Taxonomic organism metadata."},
        ),
        pa.field(
            "stats",
            pa.struct(
                [
                    pa.field("contigs", pa.int64(), nullable=False, metadata={"pydantic_type": "integer"}),
                    pa.field("gc_percent", pa.float64(), nullable=False, metadata={"pydantic_type": "number"}),
                ]
            ),
            nullable=False,
        ),
        pa.field(
            "submitter",
            pa.struct(
                [
                    pa.field("institution", pa.string(), nullable=False, metadata={"pydantic_type": "string"}),
                    pa.field("department", pa.string(), nullable=True, metadata={"pydantic_type": "string"}),
                ]
            ),
            nullable=True,
            metadata={"comment": "Submitting institution contact."},
        ),
        pa.field(
            "feature_counts",
            pa.map_(
                pa.field("key", pa.string(), nullable=False),
                pa.field("value", pa.int64(), nullable=False, metadata={"pydantic_type": "integer"}),
            ),
            nullable=False,
        ),
        pa.field(
            "optional_metrics",
            pa.map_(
                pa.field("key", pa.string(), nullable=False),
                pa.field("value", pa.float64(), nullable=True, metadata={"pydantic_type": "number"}),
            ),
            nullable=False,
        ),
        pa.field(
            "extra_organisms",
            pa.map_(
                pa.field("key", pa.string(), nullable=False),
                pa.field(
                    "value",
                    pa.struct(
                        [
                            pa.field("taxon_id", pa.int64(), nullable=False, metadata={"pydantic_type": "integer"}),
                            pa.field(
                                "scientific_name",
                                pa.string(),
                                nullable=False,
                                metadata={"pydantic_type": "string"},
                            ),
                            pa.field("strain", pa.string(), nullable=True, metadata={"pydantic_type": "string"}),
                        ]
                    ),
                    nullable=False,
                    metadata={"comment": "Taxonomic organism metadata."},
                ),
            ),
            nullable=False,
        ),
    ]
)
