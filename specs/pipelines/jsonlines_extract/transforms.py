"""File pre-processors, table transforms, and PyArrow output schemas for JSONLines Extract specification cases."""

from typing import Any, Final

import pyarrow as pa

from cdm_data_loaders.core.preprocessors import validate_md5_sidecar
from cdm_data_loaders.core.registry import FilePreprocessFn, RecordTransformFn

HIGH_LATENCY_MS: Final[int] = 100

FILE_PREPROCESSORS: tuple[FilePreprocessFn, ...] = (validate_md5_sidecar,)

EVENTS_SUMMARY_SCHEMA: Final[pa.Schema] = pa.schema(
    [
        pa.field("event_id", pa.string(), nullable=False),
        pa.field("service", pa.string(), nullable=False),
        pa.field("latency_ms", pa.int64(), nullable=False),
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
    latency_ms = int(row["latency_ms"])
    routed: dict[str, list[dict[str, Any]]] = {
        "events": [
            {
                "event_id": event_id,
                "service": service,
                "latency_ms": latency_ms,
                "is_slow": latency_ms >= HIGH_LATENCY_MS,
            }
        ]
    }
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
