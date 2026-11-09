"""Tests for the constrained field types in cdm_data_loaders.core.fields that the core settings classes do not use.

Fields used by the classes in cdm_data_loaders.core.settings are exercised via those classes in test_settings.py.
FileGlob is an unconstrained str field, and the module constants are plain values,
so none of those are tested here.
"""

import pytest
from pydantic import BaseModel, ValidationError

from cdm_data_loaders.core.fields import (
    REJECTED_TABLE_NAME,
    DatasetName,
    LoaderFileFormat,
    LoaderFileFormatEnum,
    TableName,
)

FORMAT_ERROR = "Input should be 'iceberg', 'jsonl' or 'parquet'"


class PipelineFieldsModel(BaseModel):
    """Model holding the constrained field types that pipeline-specific settings classes add."""

    dataset_name: DatasetName
    loader_file_format: LoaderFileFormat


@pytest.mark.parametrize(
    ("kwargs", "expected"),
    [
        pytest.param(
            {"dataset_name": "ds"},
            {"dataset_name": "ds", "loader_file_format": "parquet"},
            id="defaults",
        ),
        pytest.param(
            {"dataset_name": "ds", "loader_file_format": "jsonl"},
            {"dataset_name": "ds", "loader_file_format": "jsonl"},
            id="jsonl_string",
        ),
        pytest.param(
            {"dataset_name": "ds", "loader_file_format": LoaderFileFormatEnum.JSONL},
            {"dataset_name": "ds", "loader_file_format": "jsonl"},
            id="jsonl_enum_member",
        ),
        pytest.param(
            {"dataset_name": "ds", "loader_file_format": "iceberg"},
            {"dataset_name": "ds", "loader_file_format": "iceberg"},
            id="iceberg_string",
        ),
        pytest.param(
            {"dataset_name": "ds", "loader_file_format": LoaderFileFormatEnum.ICEBERG},
            {"dataset_name": "ds", "loader_file_format": "iceberg"},
            id="iceberg_enum_member",
        ),
    ],
)
def test_pipeline_fields_pass_valid_values(kwargs: dict[str, object], expected: dict[str, object]) -> None:
    """Valid values are accepted; the format is an enum member whose str() is the plain name passed to dlt."""
    model = PipelineFieldsModel.model_validate(kwargs)
    assert model.model_dump() == expected
    assert isinstance(model.loader_file_format, LoaderFileFormatEnum)
    assert str(model.loader_file_format) == expected["loader_file_format"]


@pytest.mark.parametrize(
    ("kwargs", "expected_errors"),
    [
        pytest.param(
            {"dataset_name": "ds", "loader_file_format": "csv"},
            [(("loader_file_format",), FORMAT_ERROR)],
            id="unknown_format",
        ),
        pytest.param(
            {"dataset_name": "ds", "loader_file_format": "PARQUET"},
            [(("loader_file_format",), FORMAT_ERROR)],
            id="format_wrong_case",
        ),
        pytest.param(
            {"dataset_name": ""},
            [(("dataset_name",), "String should have at least 1 character")],
            id="empty_dataset_name",
        ),
        pytest.param({}, [(("dataset_name",), "Field required")], id="missing_dataset_name"),
    ],
)
def test_pipeline_fields_fail_invalid_values(
    kwargs: dict[str, object], expected_errors: list[tuple[tuple[str, ...], str]]
) -> None:
    """Unknown formats and empty or missing dataset names are rejected."""
    with pytest.raises(ValidationError) as exc_info:
        PipelineFieldsModel.model_validate(kwargs)
    assert [(err["loc"], err["msg"]) for err in exc_info.value.errors()] == expected_errors


class TableNameModel(BaseModel):
    """Model holding the TableName field type."""

    table_name: TableName


def test_table_name_fail_reserved_rejected_table_name() -> None:
    """TableName rejects the reserved _rejected table name."""
    assert REJECTED_TABLE_NAME == "_rejected"
    with pytest.raises(ValidationError, match=REJECTED_TABLE_NAME):
        TableNameModel.model_validate({"table_name": REJECTED_TABLE_NAME})
