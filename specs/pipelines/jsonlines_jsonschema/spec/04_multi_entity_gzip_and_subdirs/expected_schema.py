"""Expected PyArrow schemas for 04_multi_entity_gzip_and_subdirs."""

import pyarrow as pa

PROJECTS_SCHEMA = pa.schema(
    [
        pa.field("project_id", pa.string(), nullable=False, metadata={"jsonschema_type": "string"}),
        pa.field("name", pa.string(), nullable=False, metadata={"jsonschema_type": "string"}),
    ]
)

EXPERIMENTS_SCHEMA = pa.schema(
    [
        pa.field("exp_id", pa.string(), nullable=False, metadata={"jsonschema_type": "string"}),
        pa.field("project_id", pa.string(), nullable=False, metadata={"jsonschema_type": "string"}),
        pa.field("reads", pa.int64(), nullable=False, metadata={"jsonschema_type": "integer"}),
    ]
)

EXPECTED_SCHEMA = PROJECTS_SCHEMA
EXPECTED_SCHEMAS = {
    "projects": PROJECTS_SCHEMA,
    "experiments": EXPERIMENTS_SCHEMA,
    "missing_entity": PROJECTS_SCHEMA,
}
