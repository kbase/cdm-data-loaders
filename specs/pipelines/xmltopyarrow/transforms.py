"""Pre-processors, table transforms, and PyArrow output schemas for XML-to-PyArrow specification cases."""

from typing import Any

import pyarrow as pa

from cdm_data_loaders.core.preprocessors import validate_md5_sidecar
from cdm_data_loaders.core.registry import FilePreprocessFn, RecordTransformFn

FILE_PREPROCESSORS: tuple[FilePreprocessFn, ...] = (validate_md5_sidecar,)

MEASUREMENT_SCHEMA = pa.schema(
    [
        pa.field("sensor_id", pa.string(), nullable=False),
        pa.field("value", pa.float64(), nullable=False),
        pa.field("scaled_value", pa.float64(), nullable=False),
    ]
)

MEASUREMENT_TAG_SCHEMA = pa.schema(
    [
        pa.field("sensor_id", pa.string(), nullable=False),
        pa.field("tag", pa.string(), nullable=False),
    ]
)


def transform_measurement(row: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    """Drop draft measurements, compute ``scaled_value``, and fan out tags to ``measurement_tags``."""
    if row.get("_status") == "drop":
        return {}
    sensor_id = str(row["_id"]).upper()
    value = float(row["value"])
    routed: dict[str, list[dict[str, Any]]] = {
        "measurements": [
            {
                "sensor_id": sensor_id,
                "value": value,
                "scaled_value": round(value * 10.0, 2),
            }
        ]
    }
    tags = row.get("tag") or []
    if tags:
        routed["measurement_tags"] = [{"sensor_id": sensor_id, "tag": str(t)} for t in tags]
    return routed


TABLE_TRANSFORMS: dict[str, RecordTransformFn] = {
    "measurements": transform_measurement,
}

OUTPUT_SCHEMAS: dict[str, pa.Schema] = {
    "measurements": MEASUREMENT_SCHEMA,
    "measurement_tags": MEASUREMENT_TAG_SCHEMA,
}
