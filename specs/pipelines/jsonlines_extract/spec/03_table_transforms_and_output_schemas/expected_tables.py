"""Expected PyArrow tables for jsonlines_extract 03_table_transforms_and_output_schemas."""

import datetime as dt
from decimal import Decimal

import pyarrow as pa

from specs.pipelines.jsonlines_extract.transforms import (
    EVENT_TAGS_SCHEMA,
    EVENTS_SUMMARY_SCHEMA,
)

EXPECTED_TABLES: dict[str, pa.Table] = {
    "events": pa.Table.from_pylist(
        [
            {
                "event_id": "EVT-1",
                "service": "auth",
                "latency_ms": 25,
                "cost_usd": Decimal("0.13"),
                "occurred_at": dt.datetime(2025, 6, 15, 12, 30, tzinfo=dt.UTC),
                "payload": b"abc",
                "is_slow": False,
            },
            {
                "event_id": "EVT-2",
                "service": "storage",
                "latency_ms": 140,
                "cost_usd": Decimal("1.50"),
                "occurred_at": dt.datetime(2025, 6, 15, 13, 0, tzinfo=dt.UTC),
                "payload": None,
                "is_slow": True,
            },
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
