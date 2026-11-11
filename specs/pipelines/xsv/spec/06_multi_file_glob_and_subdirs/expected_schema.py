"""Expected PyArrow schema for 06_multi_file_glob_and_subdirs."""

import pyarrow as pa

EXPECTED_SCHEMA = pa.schema(
    [
        pa.field("sample_num", pa.int64(), nullable=False, metadata={"jsonschema_type": "integer"}),
        pa.field("sample_name", pa.string(), nullable=False, metadata={"jsonschema_type": "string"}),
    ]
)
