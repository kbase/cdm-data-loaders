"""PyArrow schema for 04_nullability_and_metadata."""

import pyarrow as pa

SCHEMA = pa.schema(
    [
        pa.field(
            "sample_id",
            pa.string(),
            nullable=False,
            metadata={
                "comment": "Unique biosample identifier.",
                "pydantic_type": "string",
                "source_column": "biosample_acc",
            },
        ),
        pa.field(
            "depth_m",
            pa.float64(),
            nullable=True,
            metadata={
                "comment": "Sampling depth in meters below surface.",
                "unit": "m",
            },
        ),
        pa.field(
            "quality_score",
            pa.int32(),
            nullable=True,
        ),
        pa.field(
            "optional_details",
            pa.struct(
                [
                    pa.field("collector", pa.string(), nullable=True, metadata={"comment": "Field collector name."}),
                    pa.field("ph", pa.float32(), nullable=True),
                ]
            ),
            nullable=True,
            metadata={"comment": "Optional environmental annotations where every child field is nullable."},
        ),
    ],
    metadata={
        "title": "EnvironmentalSample",
        "$id": "https://kbase.us/schemas/environmental_sample.json",
        "comment": "Environmental sample record with rich Arrow metadata.",
        "dataset_version": "2026.04",
        "curator": "cdm-ingest",
    },
)
