"""Unit tests for ``cdm_data_loaders.datasets.ncbi_datasets.hooks``."""

from typing import Any

import pytest

from cdm_data_loaders.converters.facades.jsonschema_to_pyarrow import convert_schema_to_pyarrow
from cdm_data_loaders.datasets.ncbi_datasets.hooks import (
    ANNOTATION_REPORT_TABLE,
    DATASET_REPORT_TABLE,
    ENTITY_SCHEMAS,
    filter_annotation_report,
)
from cdm_data_loaders.schemas.ncbi_datasets.schema import SCHEMAS as NCBI_DATASETS_SCHEMAS
from cdm_data_loaders.validation.jsonschema.models import ValidatedSchema


@pytest.mark.parametrize(
    ("record", "expected_kept"),
    [
        pytest.param({"total_count": 0}, False, id="missing-reports"),
        pytest.param({"messages": [{"warning": {"message": "no annotations"}}]}, False, id="messages-only"),
        pytest.param({"reports": [], "total_count": 0}, False, id="empty-reports"),
        pytest.param(
            {"reports": [{"row_id": "1", "annotation": {"name": "int"}}], "total_count": 1},
            True,
            id="non-empty-reports",
        ),
    ],
)
def test_filter_annotation_report_pass_discards_missing_or_empty_reports(
    record: dict[str, Any],
    expected_kept: bool,
) -> None:
    """``filter_annotation_report`` drops records without a non-empty top-level ``reports`` list."""
    result = filter_annotation_report(record)
    if expected_kept:
        assert result == {ANNOTATION_REPORT_TABLE: [record]}
    else:
        assert result == {}


@pytest.mark.parametrize("table_name", [ANNOTATION_REPORT_TABLE, DATASET_REPORT_TABLE], ids=str)
def test_ncbi_datasets_precomputed_schemas_match_converter(table_name: str) -> None:
    """Precomputed NCBI Datasets PyArrow schemas match ``convert_schema_to_pyarrow`` output."""
    expected = convert_schema_to_pyarrow(ValidatedSchema(jsonschema=ENTITY_SCHEMAS[table_name]))
    assert NCBI_DATASETS_SCHEMAS[table_name] == expected
