"""Bundled JSON Schema registry, record transforms, and PyArrow schemas for NCBI Datasets."""

import json
from importlib.resources import files
from typing import Any, Final

import pyarrow as pa

from cdm_data_loaders.core.registry import RecordTransformFn
from cdm_data_loaders.schemas.ncbi_datasets.schema import SCHEMAS

ANNOTATION_REPORT_TABLE: Final[str] = "annotation_report"
DATASET_REPORT_TABLE: Final[str] = "dataset_report"

_PACKAGE_FILES = files("cdm_data_loaders.schemas.ncbi_datasets")

ANNOTATION_REPORT_JSONSCHEMA: Final[dict[str, Any]] = json.loads(
    _PACKAGE_FILES.joinpath("annotationReport.schema.json").read_text(encoding="utf-8")
)
DATASET_REPORT_JSONSCHEMA: Final[dict[str, Any]] = json.loads(
    _PACKAGE_FILES.joinpath("datasetReport.schema.json").read_text(encoding="utf-8")
)

ENTITY_SCHEMAS: Final[dict[str, dict[str, Any]]] = {
    ANNOTATION_REPORT_TABLE: ANNOTATION_REPORT_JSONSCHEMA,
    DATASET_REPORT_TABLE: DATASET_REPORT_JSONSCHEMA,
}


def filter_annotation_report(record: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    """Discard annotation report records without a non-empty top-level ``reports`` list.

    :param record: validated annotation report record
    :type record: dict[str, Any]
    :return: mapping with ``annotation_report`` row when ``reports`` is non-empty, or empty dict
    :rtype: dict[str, list[dict[str, Any]]]
    """
    if not record.get("reports"):
        return {}
    return {ANNOTATION_REPORT_TABLE: [record]}


TABLE_TRANSFORMS: Final[dict[str, RecordTransformFn]] = {
    ANNOTATION_REPORT_TABLE: filter_annotation_report,
}

OUTPUT_SCHEMAS: Final[dict[str, pa.Schema]] = SCHEMAS
