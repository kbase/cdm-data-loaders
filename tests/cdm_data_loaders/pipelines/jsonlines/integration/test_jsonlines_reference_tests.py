"""Integration tests for all JSONL pipelines against the dataset_report reference dataset."""

import json
from collections.abc import Callable
from typing import Any

import duckdb
import pytest
from jsonschema import FormatChecker
from jsonschema.validators import validator_for

from cdm_data_loaders.core.fields import PARQUET
from cdm_data_loaders.pipelines.jsonlines.settings import (
    JSONSCHEMA_PIPELINE_NAME,
    PYDANTIC_PIPELINE_NAME,
)
from tests.cdm_data_loaders.pipelines.helpers import (
    LOADER_FILE_FORMATS,
    read_pipeline_tables,
    run_and_read,
)
from tests.cdm_data_loaders.pipelines.jsonlines.integration.dataset_report_models import ENTITY_MODELS
from tests.cdm_data_loaders.pipelines.jsonlines.integration.dataset_report_schemas import ENTITY_SCHEMAS
from tests.cdm_data_loaders.pipelines.jsonlines.integration.jsonlines_reference_helpers import (
    EXPECTED_ENTRY_COUNT,
    canonicalize_entry,
    reconstructed_entries,
    reference_entries_from_duckdb,
)


@pytest.mark.parametrize("loader_file_format", LOADER_FILE_FORMATS, ids=str)
@pytest.mark.parametrize("pipeline_kind", [PYDANTIC_PIPELINE_NAME, JSONSCHEMA_PIPELINE_NAME], ids=str)
def test_jsonlines_validation_pass_reference_records_load(
    run_reference_pipeline: Callable[..., Any], loader_file_format: str
) -> None:
    """All assembly reports load into the validated dataset without rejections."""
    load_info, output_dir = run_and_read(run_reference_pipeline, "chunk_52", loader_file_format=loader_file_format)
    tables = read_pipeline_tables(output_dir / load_info.dataset_name, loader_file_format)
    assert "dataset_rejected" not in tables
    assert len(tables["dataset"]) == EXPECTED_ENTRY_COUNT


def test_jsonlines_reference_pass_assembly_model_accepts_all_records(
    jsonl_reference_entities: list[dict[str, Any]],
) -> None:
    """The registered assembly model accepts every reference record without losing accessions."""
    model = ENTITY_MODELS["dataset"]
    validated = [model.model_validate(entry) for entry in jsonl_reference_entities]
    assert len(validated) == EXPECTED_ENTRY_COUNT
    assert [entry.model_dump()["accession"] for entry in validated] == [
        entry["accession"] for entry in jsonl_reference_entities
    ]
    assert [canonicalize_entry(entry.model_dump(mode="json", exclude_unset=True)) for entry in validated] == [
        canonicalize_entry(entry) for entry in jsonl_reference_entities
    ]


def test_jsonlines_reference_pass_assembly_schema_accepts_all_records(
    jsonl_reference_entities: list[dict[str, Any]],
) -> None:
    """The attached schema validates every individual report with all nested references resolved."""
    schema = ENTITY_SCHEMAS["dataset"]
    validator_class = validator_for(schema)
    validator_class.check_schema(schema)
    validator = validator_class(schema, format_checker=FormatChecker())
    assert len(jsonl_reference_entities) == EXPECTED_ENTRY_COUNT
    assert [list(validator.iter_errors(entry)) for entry in jsonl_reference_entities] == [
        [] for _ in jsonl_reference_entities
    ]


def test_jsonlines_pass_reconstructed_matches_duckdb_reference(
    run_reference_pipeline: Callable[..., Any],
    reference_dataset: duckdb.DuckDBPyConnection,
    prepare_reference: Callable[[dict[str, Any]], dict[str, Any]],
) -> None:
    """Reconstructed pipeline entries equal the entries loaded from the DuckDB reference."""
    (load_info, output_dir) = run_and_read(run_reference_pipeline, "chunk_52", loader_file_format=PARQUET)
    tables = read_pipeline_tables(output_dir / load_info.dataset_name, PARQUET)
    reconstructed = reconstructed_entries(tables)
    from_duckdb = reference_entries_from_duckdb(reference_dataset)
    assert len(from_duckdb) == EXPECTED_ENTRY_COUNT
    from_duckdb_canonical = sorted(
        json.dumps(canonicalize_entry(prepare_reference(entry)), sort_keys=True, default=str) for entry in from_duckdb
    )
    from_pipeline = sorted(
        json.dumps(canonicalize_entry(entry), sort_keys=True, default=str) for entry in reconstructed
    )
    assert from_pipeline == from_duckdb_canonical
