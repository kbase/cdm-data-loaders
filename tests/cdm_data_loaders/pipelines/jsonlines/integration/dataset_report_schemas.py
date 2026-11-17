"""JSON Schema registry for individual assembly reports in the reference JSONL."""

from cdm_data_loaders.datasets.ncbi_datasets.hooks import DATASET_REPORT_JSONSCHEMA

ENTITY_SCHEMAS = {
    "dataset": DATASET_REPORT_JSONSCHEMA,
}
