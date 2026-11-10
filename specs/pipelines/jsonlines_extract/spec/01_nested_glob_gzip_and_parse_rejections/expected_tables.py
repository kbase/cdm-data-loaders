"""Expected PyArrow tables for jsonlines_extract 01_nested_glob_gzip_and_parse_rejections."""

import pyarrow as pa

from cdm_data_loaders.utils.buffer import REJECTED_PYARROW_SCHEMA

EVENTS_SCHEMA = pa.schema(
    [
        pa.field("event_id", pa.string(), nullable=False),
        pa.field("kind", pa.string(), nullable=False),
        pa.field("latency_ms", pa.int64(), nullable=False),
    ]
)

EXPECTED_TABLES: dict[str, pa.Table] = {
    "events": pa.Table.from_pylist(
        [
            {"event_id": "evt-1", "kind": "login", "latency_ms": 12},
            {"event_id": "evt-2", "kind": "query", "latency_ms": 45},
            {"event_id": "evt-3", "kind": "logout", "latency_ms": 8},
        ],
        schema=EVENTS_SCHEMA,
    ),
    "_rejected": pa.Table.from_pylist(
        [
            {
                "table_name": "events",
                "source_file": "batch_a/part1.jsonl",
                "line_no": 3,
                "raw_record": '{"event_id": "evt-bad", "kind": }',
                "error_detail": "Expecting value: line 1 column 33 (char 32)",
            }
        ],
        schema=REJECTED_PYARROW_SCHEMA,
    ),
}
