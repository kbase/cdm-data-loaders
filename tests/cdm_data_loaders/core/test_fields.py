"""Tests for the constrained field types and helpers in cdm_data_loaders.core.fields."""

import re
from typing import Final

import pytest
from pydantic import BaseModel, ValidationError

from cdm_data_loaders.core.fields import (
    CTS_BANG,
    CTS_GLOB_WILDCARDS,
    CTS_GLOB_WILDCARDS_DESCRIPTION,
    CTS_GLOBSTAR,
    CTS_LBRACK,
    CTS_QMARK,
    CTS_RBRACK,
    CTS_STAR,
    REJECTED_TABLE_NAME,
    DatasetName,
    FileGlob,
    LoaderFileFormat,
    LoaderFileFormatEnum,
    TableName,
    expand_cts_glob_wildcards,
    file_glob_description,
)

FORMAT_ERROR: Final[str] = "Input should be 'iceberg', 'jsonl' or 'parquet'"
CTS_PARAM_VALUE_PATTERN: Final[re.Pattern[str]] = re.compile(r"^[\w\./][\w\.\,+/-]*$")


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


class FileGlobModel(BaseModel):
    """Model holding the FileGlob field type."""

    file_glob: FileGlob


def test_file_glob_pass_default_value() -> None:
    """FileGlob defaults to '*' when omitted."""
    assert FileGlobModel.model_validate({}).file_glob == "*"


def test_cts_glob_wildcards_pass_match_cts_openapi_regex() -> None:
    """Every CTS wildcard constant matches the CTS POST /jobs parameter regex."""
    expected_wildcards = {
        CTS_GLOBSTAR: "**",
        CTS_STAR: "*",
        CTS_QMARK: "?",
        CTS_LBRACK: "[",
        CTS_RBRACK: "]",
        CTS_BANG: "!",
    }
    assert dict(CTS_GLOB_WILDCARDS) == expected_wildcards
    for wildcard in CTS_GLOB_WILDCARDS:
        assert CTS_PARAM_VALUE_PATTERN.fullmatch(wildcard) is not None


@pytest.mark.parametrize(
    ("raw", "expected", "is_cts_safe"),
    [
        pytest.param("*.ext.*", "*.ext.*", False, id="raw_star_glob_unchanged"),
        pytest.param("**/*.xml.gz", "**/*.xml.gz", False, id="raw_globstar_unchanged"),
        pytest.param("batch_?.csv", "batch_?.csv", False, id="raw_qmark_unchanged"),
        pytest.param("part_[0-9].tsv", "part_[0-9].tsv", False, id="raw_brackets_unchanged"),
        pytest.param("data_[!a].jsonl", "data_[!a].jsonl", False, id="raw_negated_brackets_unchanged"),
        pytest.param("__STAR__.ext.__STAR__", "*.ext.*", True, id="cts_star_tokens"),
        pytest.param("__GLOBSTAR__/__STAR__.ext.__STAR__", "**/*.ext.*", True, id="cts_globstar_and_star"),
        pytest.param("__STAR____STAR__/__STAR__.xml.gz", "**/*.xml.gz", True, id="cts_adjacent_star_tokens"),
        pytest.param("batch___QMARK__.csv", "batch_?.csv", True, id="cts_qmark_token"),
        pytest.param("part___LBRACK__0-9__RBRACK__.tsv", "part_[0-9].tsv", True, id="cts_bracket_range_tokens"),
        pytest.param(
            "data___LBRACK____BANG__a-z__RBRACK__.jsonl",
            "data_[!a-z].jsonl",
            True,
            id="cts_negated_bracket_tokens",
        ),
        pytest.param("subdir/plain_file-1.0,2+3.xml.gz", "subdir/plain_file-1.0,2+3.xml.gz", True, id="plain_cts_path"),
        pytest.param("__star__.xml", "__star__.xml", True, id="lowercase_pseudo_token_untouched"),
        pytest.param("__QMARK__STAR__", "?STAR__", True, id="left_to_right_shared_underscores"),
    ],
)
def test_expand_cts_glob_wildcards_and_file_glob_pass(
    raw: str,
    expected: str,
    is_cts_safe: bool,
) -> None:
    """expand_cts_glob_wildcards and FileGlob expand CTS wildcard tokens while preserving raw globs."""
    if is_cts_safe:
        assert CTS_PARAM_VALUE_PATTERN.fullmatch(raw) is not None
    assert expand_cts_glob_wildcards(raw) == expected
    assert FileGlobModel.model_validate({"file_glob": raw}).file_glob == expected


def test_file_glob_fail_empty_string() -> None:
    """FileGlob rejects an empty string."""
    with pytest.raises(ValidationError) as exc_info:
        FileGlobModel.model_validate({"file_glob": ""})
    assert [(err["loc"], err["msg"]) for err in exc_info.value.errors()] == [
        (("file_glob",), "String should have at least 1 character")
    ]


@pytest.mark.parametrize(
    ("summary", "expected"),
    [
        pytest.param(
            "Glob pattern for XML files",
            f"Glob pattern for XML files. {CTS_GLOB_WILDCARDS_DESCRIPTION}",
            id="without_trailing_period",
        ),
        pytest.param(
            "Glob pattern for XML files.",
            f"Glob pattern for XML files. {CTS_GLOB_WILDCARDS_DESCRIPTION}",
            id="with_trailing_period",
        ),
    ],
)
def test_file_glob_description_pass(summary: str, expected: str) -> None:
    """file_glob_description appends the CTS wildcard token reference without duplicating periods."""
    assert file_glob_description(summary) == expected
