"""Unit tests for `cdm_data_loaders.pipelines.jsonlines.base`."""

from collections.abc import Callable
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import dlt
import pyarrow as pa
import pytest
import yaml
from dlt.common.pipeline import LoadInfo
from dlt.extract.resource import DltResource
from pydantic import BaseModel

import cdm_data_loaders.pipelines.jsonlines.base as jsonlines_base_module
from cdm_data_loaders.core.buffer import REJECTED_PYARROW_SCHEMA
from cdm_data_loaders.core.fields import REJECTED_TABLE_NAME
from cdm_data_loaders.pipelines.jsonlines.base import (
    build_entity_resource,
    run_registry_ingest,
)
from cdm_data_loaders.pipelines.jsonlines.settings import JsonlPydanticIngestSettings
from cdm_data_loaders.pipelines.runner import DEFAULT_SCHEMA_CONTRACT
from tests.helpers.cli_assertions import register_temp_module


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


def test_build_entity_resource_pass_jsonl_format_yields_dicts_not_pyarrow(
    settings_factory: Callable[..., JsonlPydanticIngestSettings],
) -> None:
    """When loader_file_format is jsonl, build_entity_resource yields dict items instead of pa.Table for valid and _rejected."""
    settings = settings_factory(loader_file_format="jsonl")
    entity_dir = Path(settings.input_dir) / "widget"
    entity_dir.mkdir(parents=True)
    (entity_dir / "part.jsonl").write_text(
        '{"widget_id": "w1", "count": 2}\n{"widget_id": "w2", "count": -1}\n{malformed\n',
        encoding="utf-8",
    )

    resource = build_entity_resource("widget", settings, _resolve_negative_count_error)
    items = list(resource)

    assert all(isinstance(item, dict) for item in items)
    valid_items = [item for item in items if "widget_id" in item]
    rejected_items = [item for item in items if "error_detail" in item]
    assert valid_items == [{"widget_id": "w1", "count": 2}]
    assert [row["line_no"] for row in rejected_items] == [2, 3]


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
    ("cli_prog_name", "pipeline_run_kwargs", "run_pipeline_return", "expected_pipeline_name", "expected_run_kwargs"),
    [
        pytest.param(
            "jsonlines_pydantic",
            None,
            MagicMock(spec=LoadInfo),
            "jsonlines_pydantic",
            {"loader_file_format": "parquet"},
            id="uses_settings_cli_prog_name_with_default_run_kwargs",
        ),
        pytest.param(
            None,
            None,
            MagicMock(spec=LoadInfo),
            "test_registry_pipeline",
            {"loader_file_format": "parquet"},
            id="falls_back_to_pipeline_name_when_cli_prog_name_unset",
        ),
        pytest.param(
            "jsonlines_pydantic",
            {"loader_file_format": "jsonl", "schema_contract": "freeze"},
            MagicMock(spec=LoadInfo),
            "jsonlines_pydantic",
            {"loader_file_format": "jsonl", "schema_contract": "freeze"},
            id="merges_and_overrides_pipeline_run_kwargs",
        ),
        pytest.param(
            "jsonlines_pydantic",
            None,
            None,
            "jsonlines_pydantic",
            {"loader_file_format": "parquet"},
            id="propagates_none_on_pipeline_failure",
        ),
    ],
)
def test_run_registry_ingest_pass_builds_sorted_resources_and_delegates_to_run_pipeline(
    settings_factory: Callable[..., JsonlPydanticIngestSettings],
    widget_model: type[BaseModel],
    cli_prog_name: str | None,
    pipeline_run_kwargs: dict[str, Any] | None,
    run_pipeline_return: LoadInfo | None,
    expected_pipeline_name: str,
    expected_run_kwargs: dict[str, Any],
) -> None:
    """run_registry_ingest builds resources in sorted table order and delegates to core.run_pipeline."""

    class CustomProgSettings(JsonlPydanticIngestSettings, cli_prog_name=cli_prog_name):
        pass

    base_settings = settings_factory(dataset_name="registry_ds", loader_file_format="parquet")
    settings = CustomProgSettings.model_validate(
        base_settings.model_dump(exclude=set(JsonlPydanticIngestSettings.model_computed_fields))
    )
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
            "pipeline_name": expected_pipeline_name,
            "dataset_name": "registry_ds",
        },
        pipeline_run_kwargs=expected_run_kwargs,
    )


def test_run_registry_ingest_pass_file_listing_multi_entity_skips_validation_and_transforms(
    settings_factory: Callable[..., JsonlPydanticIngestSettings],
    widget_model: type[BaseModel],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """run_registry_ingest with loader_file_format='file_listing' lists all files and per-entity matches without parsing."""

    def fail_resolve_error(_record: dict[str, Any]) -> str | None:
        msg = "validator must not run during file_listing"
        raise AssertionError(msg)

    def fail_transform(_row: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
        msg = "TABLE_TRANSFORMS must not run during file_listing"
        raise AssertionError(msg)

    registry: dict[str, type[BaseModel]] = {
        "zeta": widget_model,
        "missing_entity": widget_model,
        "alpha": widget_model,
    }
    transforms_mod = register_temp_module(
        monkeypatch,
        "_test_registry_file_listing_transforms",
        ENTITY_MODELS=registry,
        TABLE_TRANSFORMS={"alpha": fail_transform, "zeta": fail_transform},
    )
    settings = settings_factory(
        dataset_name="registry_listing_ds",
        entity_models=transforms_mod,
        loader_file_format="file_listing",
        transforms_module=transforms_mod,
    )
    input_dir = Path(settings.input_dir)
    (input_dir / "alpha").mkdir()
    (input_dir / "zeta").mkdir()
    (input_dir / "alpha" / "part.jsonl").write_text("{corrupt\n", encoding="utf-8")
    (input_dir / "zeta" / "part.jsonl").write_text("{corrupt\n", encoding="utf-8")
    (input_dir / "alpha" / "skip.txt").write_text("ignored", encoding="utf-8")
    (input_dir / "root_manifest.txt").write_text("manifest", encoding="utf-8")

    result = run_registry_ingest(
        settings=settings,
        registry=registry,  # type: ignore[arg-type]
        build_resource=lambda table_name, _entry, cfg: build_entity_resource(table_name, cfg, fail_resolve_error),
        pipeline_name="test_registry_listing_pipeline",
    )

    assert result is None
    assert not settings.report_path.exists()
    assert yaml.safe_load(settings.file_listing_path.read_text(encoding="utf-8")) == {
        "input_dir_files": [
            "alpha/part.jsonl",
            "alpha/skip.txt",
            "root_manifest.txt",
            "zeta/part.jsonl",
        ],
        "matched_files": ["alpha/part.jsonl", "zeta/part.jsonl"],
    }


def test_build_entity_resource_wires_raw_and_validated_options(
    settings_factory: Callable[..., JsonlPydanticIngestSettings],
) -> None:
    """build_entity_resource forwards raw resource name, max_table_nesting=0, and parallelized=True."""
    settings = settings_factory()
    with (
        patch.object(
            jsonlines_base_module,
            "build_filesystem_resource",
            wraps=jsonlines_base_module.build_filesystem_resource,
        ) as mock_fs_resource,
        patch.object(
            jsonlines_base_module.dlt,
            "transformer",
            wraps=dlt.transformer,
        ) as mock_transformer,
    ):
        build_entity_resource("widget", settings, _resolve_negative_count_error)

    assert mock_fs_resource.call_args.kwargs["name"] == "widget_raw"
    assert mock_fs_resource.call_args.kwargs["max_table_nesting"] == 0
    assert mock_transformer.call_args.kwargs["parallelized"] is True


def test_run_registry_ingest_falls_back_when_dataset_name_none(
    settings_factory: Callable[..., JsonlPydanticIngestSettings],
    widget_model: type[BaseModel],
) -> None:
    """run_registry_ingest falls back to default dataset_name when settings.dataset_name is None."""
    settings = settings_factory().model_copy(update={"dataset_name": None})
    mock_load_info = MagicMock(spec=LoadInfo)
    with patch.object(jsonlines_base_module, "run_pipeline", return_value=mock_load_info) as mock_run_pipeline:
        result = run_registry_ingest(
            settings=settings,
            registry={"widget": widget_model},
            build_resource=lambda table_name, _entry, cfg: build_entity_resource(
                table_name, cfg, _resolve_negative_count_error
            ),
            pipeline_name="fallback_pipe",
        )

    assert result is mock_load_info
    assert mock_run_pipeline.call_args.kwargs["pipeline_kwargs"]["dataset_name"] == "jsonlines_pydantic"
