"""Expected PyArrow tables for jsonlines_pydantic 03_table_transforms_and_output_schemas."""

import pyarrow as pa

from specs.pipelines.jsonlines_pydantic.transforms import (
    ASSAY_METRIC_SCHEMA,
    ASSAY_SUMMARY_SCHEMA,
)

EXPECTED_TABLES: dict[str, pa.Table] = {
    "assays": pa.Table.from_pylist(
        [
            {"assay_id": "A-1", "ph": 5.4, "ph_bucket": "acidic"},
            {"assay_id": "A-2", "ph": 7.4, "ph_bucket": "neutral_or_basic"},
        ],
        schema=ASSAY_SUMMARY_SCHEMA,
    ),
    "assay_metrics": pa.Table.from_pylist(
        [
            {"assay_id": "A-1", "metric": "od600"},
            {"assay_id": "A-1", "metric": "temp"},
            {"assay_id": "A-2", "metric": "dissolved_o2"},
        ],
        schema=ASSAY_METRIC_SCHEMA,
    ),
}
