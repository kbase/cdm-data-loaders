"""Expected PyArrow tables for 07_table_transforms_and_output_schemas."""

import pyarrow as pa

from specs.pipelines.xsv.transforms import ALERTS_SCHEMA, READINGS_SCHEMA

EXPECTED_TABLE = pa.Table.from_pylist(
    [
        {"sensor_id": "S-1", "celsius": 25.0, "kelvin": 298.15},
        {"sensor_id": "S-3", "celsius": 120.5, "kelvin": 393.65},
    ],
    schema=READINGS_SCHEMA,
)

EXPECTED_ALERTS_TABLE = pa.Table.from_pylist(
    [
        {"sensor_id": "S-3", "celsius": 120.5},
    ],
    schema=ALERTS_SCHEMA,
)

EXPECTED_TABLES = {
    "readings": EXPECTED_TABLE,
    "alerts": EXPECTED_ALERTS_TABLE,
}
