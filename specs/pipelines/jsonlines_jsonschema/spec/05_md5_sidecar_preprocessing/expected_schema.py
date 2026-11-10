"""Expected PyArrow schema for 05_md5_sidecar_preprocessing."""

import pyarrow as pa

EXPECTED_SCHEMA = pa.schema(
    [
        pa.field("pmid", pa.int64(), nullable=False, metadata={"jsonschema_type": "integer"}),
        pa.field("title", pa.string(), nullable=False, metadata={"jsonschema_type": "string"}),
    ]
)
