"""Expected PyArrow tables for jsonlines_extract 02_md5_sidecar_preprocessing."""

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
            {"event_id": "evt-10", "kind": "ingest", "latency_ms": 21},
        ],
        schema=EVENTS_SCHEMA,
    ),
    "_rejected": pa.Table.from_pylist(
        [
            {
                "table_name": "events",
                "source_file": "bad.jsonl",
                "line_no": None,
                "raw_record": None,
                "error_detail": (
                    "MD5 mismatch for bad.jsonl: "
                    "expected 00000000000000000000000000000000, got f23fdd271c6003eac5a5e788c4e86b84"
                ),
            }
        ],
        schema=REJECTED_PYARROW_SCHEMA,
    ),
}
