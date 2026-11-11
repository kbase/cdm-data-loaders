"""Expected PyArrow tables for 04_multi_entity_gzip_and_subdirs."""

import pyarrow as pa
from expected_schema import EXPERIMENTS_SCHEMA, PROJECTS_SCHEMA

EXPECTED_PROJECTS_TABLE = pa.Table.from_pylist(
    [
        {"project_id": "PRJ-2", "name": "Marine Virome"},
        {"project_id": "PRJ-1", "name": "Soil Metagenome"},
    ],
    schema=PROJECTS_SCHEMA,
)

EXPECTED_EXPERIMENTS_TABLE = pa.Table.from_pylist(
    [
        {"exp_id": "EXP-10", "project_id": "PRJ-1", "reads": 250000},
    ],
    schema=EXPERIMENTS_SCHEMA,
)

EXPECTED_TABLE = EXPECTED_PROJECTS_TABLE
EXPECTED_TABLES = {
    "projects": EXPECTED_PROJECTS_TABLE,
    "experiments": EXPECTED_EXPERIMENTS_TABLE,
}
