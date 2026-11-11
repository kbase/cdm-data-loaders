"""Entity schema registries, file pre-processors, table transforms, and output schemas for JSONLines spec cases."""

import json
from pathlib import Path
from typing import Any, Final

import pyarrow as pa

from cdm_data_loaders.core.preprocessors import validate_md5_sidecar
from cdm_data_loaders.core.registry import FilePreprocessFn, RecordTransformFn

SPEC_DIR = Path(__file__).parent / "spec"
NEUTRAL_PH: Final[float] = 7.0

FILE_PREPROCESSORS: tuple[FilePreprocessFn, ...] = (validate_md5_sidecar,)

SAMPLE_SUMMARY_SCHEMA = pa.schema(
    [
        pa.field("sample_id", pa.string(), nullable=False),
        pa.field("ph", pa.float64(), nullable=False),
        pa.field("ph_bucket", pa.string(), nullable=False),
    ]
)

SAMPLE_MEASUREMENT_SCHEMA = pa.schema(
    [
        pa.field("sample_id", pa.string(), nullable=False),
        pa.field("metric", pa.string(), nullable=False),
    ]
)


def transform_sample(row: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    """Drop excluded samples, bucket pH values, and fan out metrics to ``sample_measurements``."""
    if row.get("exclude") is True:
        return {}
    sample_id = str(row["sample_id"]).upper()
    ph = float(row["ph"])
    ph_bucket = "acidic" if ph < NEUTRAL_PH else "neutral_or_basic"
    routed: dict[str, list[dict[str, Any]]] = {
        "samples": [
            {
                "sample_id": sample_id,
                "ph": ph,
                "ph_bucket": ph_bucket,
            }
        ]
    }
    metrics = row.get("metrics") or []
    if metrics:
        routed["sample_measurements"] = [{"sample_id": sample_id, "metric": str(m)} for m in metrics]
    return routed


ENTITY_SCHEMAS: dict[str, dict[str, Any]] = {
    "samples": json.loads((SPEC_DIR / "06_table_transforms_and_output_schemas" / "schema.json").read_text("utf-8")),
}

TABLE_TRANSFORMS: dict[str, RecordTransformFn] = {
    "samples": transform_sample,
}

OUTPUT_SCHEMAS: dict[str, pa.Schema] = {
    "samples": SAMPLE_SUMMARY_SCHEMA,
    "sample_measurements": SAMPLE_MEASUREMENT_SCHEMA,
}
