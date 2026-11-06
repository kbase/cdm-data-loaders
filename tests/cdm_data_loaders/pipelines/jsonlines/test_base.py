"""Unit tests for `cdm_data_loaders.pipelines.jsonlines.base`."""

from collections.abc import Callable
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import dlt
import pyarrow as pa
import pytest
from dlt.common.pipeline import LoadInfo
from dlt.extract.resource import DltResource
from pydantic import BaseModel

import cdm_data_loaders.pipelines.jsonlines.base as jsonlines_base_module
from cdm_data_loaders.core.fields import REJECTED_TABLE_NAME
from cdm_data_loaders.pipelines.core import DEFAULT_SCHEMA_CONTRACT
from cdm_data_loaders.pipelines.jsonlines.base import (
    build_entity_resource,
    run_registry_ingest,
)
from cdm_data_loaders.pipelines.jsonlines.settings import JsonlPydanticIngestSettings
from cdm_data_loaders.utils.buffer import REJECTED_PYARROW_SCHEMA
from tests.helpers import register_temp_module


def _resolve_negative_count_error(record: dict[str, Any]) -> str | None:
    """Reject records whose count is negative."""
    if record.get("count", 0) < 0:
        return "count must be non-negative"
    return None


@pytest.mark.parametrize(
    ("use_transform_valid", "use_table_transform", "use_output_schemas"),
    [
        pytest.param(False, False, False, id="default_arrow_schema_from_settings"),
        pytest.param(True, False, False, id="with_transform_valid_and_table_transform_unschematized"),
        pytest.param(True, True, True, id="with_transform_valid_table_transform_and_output_schema"),
    ],
)
def test_build_entity_resource_pass_routes_valid_and_rejected_records(
    settings_factory: Callable[..., JsonlPydanticIngestSettings],
    monkeypatch: pytest.MonkeyPatch,
    use_transform_valid: bool,
    use_table_transform: bool,
    use_output_schemas: bool,
) -> None:
    """build_entity_resource configures the raw/validated resource pair and routes valid and rejected pages."""
    custom_pa_schema = pa.schema(
        [
            pa.field("widget_id", pa.string(), nullable=False),
            pa.field("count", pa.int64(), nullable=False),
            pa.field("tagged", pa.bool_(), nullable=False),
        ]
    )
    transforms_mod: str | None = None
    output_schemas_mod: str | None = None
    if use_transform_valid or use_table_transform:
        transforms_mod = register_temp_module(
            monkeypatch,
            "_test_base_entity_transforms",
            TABLE_TRANSFORMS={"widget": lambda row: {"widget": [row]}},
            OUTPUT_SCHEMAS={"widget": custom_pa_schema},
        )
        if use_output_schemas:
            output_schemas_mod = transforms_mod

    settings = settings_factory(
        transforms_module=transforms_mod,
        output_schemas_module=output_schemas_mod,
    )
    entity_dir = Path(settings.input_dir) / "widget"
    entity_dir.mkdir(parents=True)
    (entity_dir / "part.jsonl").write_text(
        '{"widget_id": "w1", "count": 2}\n{"widget_id": "w2", "count": -1}\n{malformed\n',
        encoding="utf-8",
    )

    transform_valid = (lambda r: {**r, "tagged": True}) if use_transform_valid else None
    resource = build_entity_resource(
        "widget",
        settings,
        _resolve_negative_count_error,
        transform_valid=transform_valid,
    )

    assert resource.name == "widget_validated"
    assert resource.max_table_nesting == 0
    assert resource.schema_contract == DEFAULT_SCHEMA_CONTRACT

    items = list(resource)
    expected_page_count = 2
    assert len(items) == expected_page_count

    if use_output_schemas:
        valid_table = next(item for item in items if isinstance(item, pa.Table) and "tagged" in item.schema.names)
        assert valid_table.schema == custom_pa_schema
        assert valid_table.to_pylist() == [{"widget_id": "w1", "count": 2, "tagged": True}]
    elif use_transform_valid:
        valid_dict = next(item for item in items if isinstance(item, dict))
        assert valid_dict == {"widget_id": "w1", "count": 2, "tagged": True}
    else:
        valid_table = next(item for item in items if isinstance(item, pa.Table) and "widget_id" in item.schema.names)
        assert valid_table.to_pylist() == [{"widget_id": "w1", "count": 2}]

    rejected_table = next(
        item for item in items if isinstance(item, pa.Table) and item.schema == REJECTED_PYARROW_SCHEMA
    )
    rejected_rows = rejected_table.to_pylist()
    expected_rejected_rows = 2
    assert len(rejected_rows) == expected_rejected_rows
    assert [row["line_no"] for row in rejected_rows] == [2, 3]
    assert [row["table_name"] for row in rejected_rows] == ["widget", "widget"]
    assert rejected_rows[0]["error_detail"] == "count must be non-negative"
    assert rejected_rows[1]["raw_record"] == "{malformed"
    assert REJECTED_TABLE_NAME == "_rejected"


@pytest.mark.parametrize(
    "setup_mode",
    ["missing_subdir", "empty_subdir", "non_matching_files"],
    ids=["missing-subdir", "empty-subdir", "non-matching-files"],
)
def test_build_entity_resource_pass_no_matching_files(
    settings_factory: Callable[..., JsonlPydanticIngestSettings],
    setup_mode: str,
) -> None:
    """build_entity_resource yields no records when the entity directory is missing, empty, or has no matching files."""
    settings = settings_factory()
    entity_dir = Path(settings.input_dir) / "widget"
    if setup_mode in {"empty_subdir", "non_matching_files"}:
        entity_dir.mkdir(parents=True)
    if setup_mode == "non_matching_files":
        (entity_dir / "notes.txt").write_text('{"widget_id": "w1", "count": 1}\n', encoding="utf-8")

    resource = build_entity_resource("widget", settings, _resolve_negative_count_error)
    assert list(resource) == []


@pytest.mark.parametrize(
    ("pipeline_run_kwargs", "run_pipeline_return", "expected_run_kwargs"),
    [
        pytest.param(
            None,
            MagicMock(spec=LoadInfo),
            {"loader_file_format": "parquet"},
            id="default_pipeline_run_kwargs",
        ),
        pytest.param(
            {"loader_file_format": "jsonl", "schema_contract": "freeze"},
            MagicMock(spec=LoadInfo),
            {"loader_file_format": "jsonl", "schema_contract": "freeze"},
            id="merges_and_overrides_pipeline_run_kwargs",
        ),
        pytest.param(
            None,
            None,
            {"loader_file_format": "parquet"},
            id="propagates_none_on_pipeline_failure",
        ),
    ],
)
def test_run_registry_ingest_pass_builds_sorted_resources_and_delegates_to_run_pipeline(
    settings_factory: Callable[..., JsonlPydanticIngestSettings],
    widget_model: type[BaseModel],
    pipeline_run_kwargs: dict[str, Any] | None,
    run_pipeline_return: LoadInfo | None,
    expected_run_kwargs: dict[str, Any],
) -> None:
    """run_registry_ingest builds resources in sorted table order and delegates to core.run_pipeline."""
    settings = settings_factory(dataset_name="registry_ds", loader_file_format="parquet")
    registry: dict[str, type[BaseModel]] = {
        "zeta": widget_model,
        "alpha": widget_model,
    }
    built_calls: list[tuple[str, Any, Any]] = []
    built_resources: dict[str, DltResource] = {}

    def fake_build_resource(table_name: str, entry: Any, cfg: Any) -> DltResource:  # noqa: ANN401
        built_calls.append((table_name, entry, cfg))
        res = dlt.resource([{"table": table_name}], name=f"{table_name}_res")
        built_resources[table_name] = res
        return res

    with patch.object(jsonlines_base_module, "run_pipeline", return_value=run_pipeline_return) as mock_run_pipeline:
        result = run_registry_ingest(
            settings=settings,
            registry=registry,  # type: ignore[arg-type]
            build_resource=fake_build_resource,
            pipeline_name="test_registry_pipeline",
            pipeline_run_kwargs=pipeline_run_kwargs,
        )

    assert result is run_pipeline_return
    assert built_calls == [
        ("alpha", widget_model, settings),
        ("zeta", widget_model, settings),
    ]
    mock_run_pipeline.assert_called_once_with(
        settings=settings,
        resource=[built_resources["alpha"], built_resources["zeta"]],
        pipeline_kwargs={
            "pipeline_name": "test_registry_pipeline",
            "dataset_name": "registry_ds",
        },
        pipeline_run_kwargs=expected_run_kwargs,
    )
