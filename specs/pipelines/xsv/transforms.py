"""Table transforms and PyArrow output schemas for XSV specification case 07."""

from typing import Any

import pyarrow as pa

READINGS_SCHEMA = pa.schema(
    [
        pa.field("sensor_id", pa.string(), nullable=False),
        pa.field("celsius", pa.float64(), nullable=False),
        pa.field("kelvin", pa.float64(), nullable=False),
    ]
)

ALERTS_SCHEMA = pa.schema(
    [
        pa.field("sensor_id", pa.string(), nullable=False),
        pa.field("celsius", pa.float64(), nullable=False),
    ]
)


def transform_readings(row: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    """Filter dropped sensor rows, enrich with Kelvin temperature, and fan out high-temperature alerts."""
    if row["status"] == "drop":
        return {}
    sensor_id = str(row["sensor_id"]).upper()
    celsius = float(row["celsius"])
    routed: dict[str, list[dict[str, Any]]] = {
        "readings": [
            {
                "sensor_id": sensor_id,
                "celsius": celsius,
                "kelvin": round(celsius + 273.15, 2),
            }
        ]
    }
    if celsius >= 100.0:  # noqa: PLR2004
        routed["alerts"] = [{"sensor_id": sensor_id, "celsius": celsius}]
    return routed


TABLE_TRANSFORMS = {"readings": transform_readings}
OUTPUT_SCHEMAS = {"readings": READINGS_SCHEMA, "alerts": ALERTS_SCHEMA}
