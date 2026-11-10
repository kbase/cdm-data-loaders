"""Expected PyArrow tables for jsonlines_extract 03_table_transforms_and_output_schemas."""

import pyarrow as pa

from specs.pipelines.jsonlines_extract.transforms import (
    EVENT_TAGS_SCHEMA,
    EVENTS_SUMMARY_SCHEMA,
)

EXPECTED_TABLES: dict[str, pa.Table] = {
    "events": pa.Table.from_pylist(
        [
            {"event_id": "EVT-1", "service": "auth", "latency_ms": 25, "is_slow": False},
            {"event_id": "EVT-2", "service": "storage", "latency_ms": 140, "is_slow": True},
        ],
        schema=EVENTS_SUMMARY_SCHEMA,
    ),
    "event_tags": pa.Table.from_pylist(
        [
            {"event_id": "EVT-1", "tag": "prod"},
            {"event_id": "EVT-1", "tag": "edge"},
            {"event_id": "EVT-2", "tag": "batch"},
        ],
        schema=EVENT_TAGS_SCHEMA,
    ),
}
