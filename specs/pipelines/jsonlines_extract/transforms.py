"""File pre-processors, table transforms, and PyArrow output schemas for JSONLines Extract specification cases."""

from typing import Any, Final

import pyarrow as pa

from cdm_data_loaders.core.md5_preprocessor import validate_md5_sidecar
from cdm_data_loaders.core.registry import FilePreprocessFn, RecordTransformFn

HIGH_LATENCY_MS: Final[int] = 100

FILE_PREPROCESSORS: tuple[FilePreprocessFn, ...] = (validate_md5_sidecar,)

EVENTS_SUMMARY_SCHEMA: Final[pa.Schema] = pa.schema(
    [
        pa.field("event_id", pa.string(), nullable=False),
        pa.field("service", pa.string(), nullable=False),
        pa.field("latency_ms", pa.int64(), nullable=False),
        pa.field("cost_usd", pa.decimal128(6, 2), nullable=False),
        pa.field("occurred_at", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("payload", pa.large_binary(), nullable=True),
        pa.field("is_slow", pa.bool_(), nullable=False),
    ]
)

EVENT_TAGS_SCHEMA: Final[pa.Schema] = pa.schema(
    [
        pa.field("event_id", pa.string(), nullable=False),
        pa.field("tag", pa.string(), nullable=False),
    ]
)


def transform_event(row: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    """Drop ignored events, normalize service names, compute ``is_slow``, and fan out tags to ``event_tags``.

    :param row: Raw parsed JSONL event dictionary.
    :type row: dict[str, Any]
    :return: Mapping of target table names to lists of output row dictionaries.
    :rtype: dict[str, list[dict[str, Any]]]
    """
    if row.get("drop") is True:
        return {}
    event_id = str(row["event_id"]).upper()
    service = str(row["service"]).lower()
    summary_row: dict[str, Any] = {
        "event_id": event_id,
        "service": service,
        "latency_ms": row["latency_ms"],
        "cost_usd": row["cost_usd"],
        "occurred_at": row["occurred_at"],
        "is_slow": int(row["latency_ms"]) >= HIGH_LATENCY_MS,
    }
    if row.get("payload_b64") is not None:
        summary_row["payload"] = row["payload_b64"]
    routed: dict[str, list[dict[str, Any]]] = {"events": [summary_row]}
    tags = row.get("tags") or []
    if tags:
        routed["event_tags"] = [{"event_id": event_id, "tag": str(tag)} for tag in tags]
    return routed


TABLE_TRANSFORMS: Final[dict[str, RecordTransformFn]] = {
    "events": transform_event,
}

OUTPUT_SCHEMAS: Final[dict[str, pa.Schema]] = {
    "events": EVENTS_SUMMARY_SCHEMA,
    "event_tags": EVENT_TAGS_SCHEMA,
}
