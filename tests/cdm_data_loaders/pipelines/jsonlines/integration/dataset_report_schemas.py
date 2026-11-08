"""JSON Schema registry for individual assembly reports in the reference JSONL."""

from cdm_data_loaders.schemas.ncbi_datasets.ncbi_openapi import DATASET_REPORT_JSONSCHEMA

ENTITY_SCHEMAS = {
    "dataset": DATASET_REPORT_JSONSCHEMA,
}
