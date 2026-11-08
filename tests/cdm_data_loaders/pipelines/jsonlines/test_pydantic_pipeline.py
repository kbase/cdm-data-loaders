"""Integration tests for build_entity_resource: file reading, validation, and dlt wiring together."""

from collections.abc import Callable
from pathlib import Path
from typing import Any

import dlt
import pyarrow as pa
import pytest
from pydantic import BaseModel

from cdm_data_loaders.converters.facades.pydantic_to_pyarrow import PydanticToPyArrow
from cdm_data_loaders.core.preprocessors import FilePreprocessError
from cdm_data_loaders.pipelines.jsonlines.extract_pydantic_validate_pipeline import (
    build_entity_resource,
    run_jsonlines_with_validation_pipeline,
)
from cdm_data_loaders.pipelines.jsonlines.settings import JsonlPydanticIngestSettings
from cdm_data_loaders.utils.buffer import REJECTED_PYARROW_SCHEMA
from tests.cdm_data_loaders.pipelines.helpers import assert_run_ok
from tests.conftest import LOCAL_FS
from tests.helpers import register_temp_module


def run_pipeline(table_name: str, model: type[BaseModel], settings: JsonlPydanticIngestSettings) -> dlt.Dataset:
    """Run one entity resource through a local dlt pipeline. Return the resulting dataset."""
    resource = build_entity_resource(table_name, model, settings)

    pipeline = dlt.pipeline(
        pipeline_name=f"test_{table_name}",
        destination=dlt.destination(LOCAL_FS),
        dataset_name=f"test_{table_name}_dataset",
        dev_mode=True,
    )
    pipeline.run(resource, loader_file_format="parquet")
    return pipeline.dataset()


def test_build_entity_resource_pass_happy_path_all_valid(
    scenario_input_dir: Callable[[str], str],
    settings_factory: Callable[..., JsonlPydanticIngestSettings],
    widget_model: type[BaseModel],
    tmp_path: Path,
) -> None:
    """Every valid record lands in the main table. No rejected table is created."""
    settings = settings_factory(input_dir=scenario_input_dir("happy_path"), output_dir=str(tmp_path))

    dataset = run_pipeline("widget", widget_model, settings)

    rows = dataset.widget.df()
    assert sorted(rows["widget_id"].tolist()) == ["a", "b", "c"]
    assert "_rejected" not in dataset.tables


def test_build_entity_resource_pass_mixed_batch_split_correctly(
    scenario_input_dir: Callable[[str], str],
    settings_factory: Callable[..., JsonlPydanticIngestSettings],
    widget_model: type[BaseModel],
    tmp_path: Path,
) -> None:
    """A mixed batch puts valid rows in the main table and invalid rows in _rejected."""
    settings = settings_factory(input_dir=scenario_input_dir("mixed"), output_dir=str(tmp_path))

    dataset = run_pipeline("widget", widget_model, settings)

    valid_rows = dataset.widget.df()
    rejected_rows = dataset.table("_rejected").df()
    assert sorted(valid_rows["widget_id"].tolist()) == ["a", "c"]
    expected_rejected_count = 2
    assert len(rejected_rows) == expected_rejected_count
    assert rejected_rows["table_name"].tolist() == ["widget", "widget"]


def test_build_entity_resource_fail_all_invalid_main_table_absent_or_empty(
    scenario_input_dir: Callable[[str], str],
    settings_factory: Callable[..., JsonlPydanticIngestSettings],
    widget_model: type[BaseModel],
    tmp_path: Path,
) -> None:
    """When every record is invalid, the main table has zero rows or does not exist. Rejected has all rows."""
    settings = settings_factory(input_dir=scenario_input_dir("all_invalid"), output_dir=str(tmp_path))

    dataset = run_pipeline("widget", widget_model, settings)

    rejected_rows = dataset.table("_rejected").df()
    expected_rejected_count = 3
    assert len(rejected_rows) == expected_rejected_count
    assert rejected_rows["table_name"].tolist() == ["widget", "widget", "widget"]
    assert "widget" not in dataset.tables or len(dataset.widget.df()) == 0


def test_build_entity_resource_pass_empty_input_directory_yields_no_tables(
    tmp_path: Path, settings_factory: Callable[..., JsonlPydanticIngestSettings], widget_model: type[BaseModel]
) -> None:
    """An entity subdirectory with no matching files produces no tables."""
    input_dir = tmp_path / "empty_input"
    (input_dir / "widget").mkdir(parents=True)
    settings = settings_factory(input_dir=str(input_dir), output_dir=str(tmp_path))

    dataset = run_pipeline("widget", widget_model, settings)

    assert "widget" not in dataset.tables
    assert "_rejected" not in dataset.tables


def test_build_entity_resource_pass_missing_entity_directory_yields_no_tables(
    tmp_path: Path, settings_factory: Callable[..., JsonlPydanticIngestSettings], widget_model: type[BaseModel]
) -> None:
    """A missing entity subdirectory produces no tables and does not raise."""
    input_dir = tmp_path / "input_without_widget_dir"
    input_dir.mkdir()
    settings = settings_factory(input_dir=str(input_dir), output_dir=str(tmp_path))

    dataset = run_pipeline("widget", widget_model, settings)

    assert "widget" not in dataset.tables
    assert "_rejected" not in dataset.tables


@pytest.mark.parametrize(
    ("file_glob", "expected_ids", "expected_rejected"),
    [
        pytest.param("*.jsonl*", ["top"], [], id="non-recursive-glob"),
        pytest.param(
            "**/*.jsonl*",
            ["deep", "deeper", "top"],
            [("widget", "widget/a/b/c/deep.jsonl", 2)],
            id="recursive-glob",
        ),
    ],
)
def test_build_entity_resource_pass_deeply_nested_directory_tree(
    tmp_path: Path,
    settings_factory: Callable[..., JsonlPydanticIngestSettings],
    widget_model: type[BaseModel],
    write_gzip_file: Callable[[Path, str, str], Path],
    file_glob: str,
    expected_ids: list[str],
    expected_rejected: list[tuple[str, str, int]],
) -> None:
    """Recursive globs read deeply nested files under the entity directory while non-recursive globs read only top-level files."""
    input_dir = tmp_path / "nested_input"
    top_dir = input_dir / "widget"
    deep_dir = top_dir / "a" / "b" / "c"
    deeper_dir = deep_dir / "d"
    other_deep_dir = input_dir / "other" / "a" / "b" / "c"
    deeper_dir.mkdir(parents=True)
    other_deep_dir.mkdir(parents=True)

    (top_dir / "top.jsonl").write_text('{"widget_id": "top", "count": 1}\n', encoding="utf-8")
    (deep_dir / "deep.jsonl").write_text(
        '{"widget_id": "deep", "count": 2}\n{"widget_id": "bad", "count": -1}\n',
        encoding="utf-8",
    )
    write_gzip_file(deeper_dir, "deeper.jsonl.gz", '{"widget_id": "deeper", "count": 3}\n')
    (other_deep_dir / "other.jsonl").write_text('{"widget_id": "other", "count": 4}\n', encoding="utf-8")

    settings = settings_factory(
        input_dir=str(input_dir),
        output_dir=str(tmp_path / "output"),
        file_glob=file_glob,
    )

    dataset = run_pipeline("widget", widget_model, settings)

    assert sorted(dataset.widget.df()["widget_id"].tolist()) == expected_ids
    if not expected_rejected:
        assert "_rejected" not in dataset.tables
    else:
        rejected_df = dataset.table("_rejected").df()
        actual_rejected = [
            (row["table_name"], row["source_file"], int(row["line_no"])) for _, row in rejected_df.iterrows()
        ]
        assert actual_rejected == expected_rejected


def test_build_entity_resource_pass_filters_and_splits_records_with_transforms_module(
    scenario_input_dir: Callable[[str], str],
    settings_factory: Callable[..., JsonlPydanticIngestSettings],
    widget_model: type[BaseModel],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """build_entity_resource applies RecordTransformFn to filter and split validated Pydantic records."""

    def transform(row: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
        return (
            {}
            if row["widget_id"] == "b"
            else {
                "widget": [{"widget_id": row["widget_id"], "count": row["count"]}],
                "widget_summary": [{"widget_id": row["widget_id"], "doubled": row["count"] * 2}],
            }
        )

    transforms_mod = register_temp_module(
        monkeypatch,
        "_test_pydantic_transforms",
        TABLE_TRANSFORMS={"widget": transform},
    )
    settings = settings_factory(
        input_dir=scenario_input_dir("happy_path"),
        output_dir=str(tmp_path),
        transforms_module=transforms_mod,
    )

    dataset = run_pipeline("widget", widget_model, settings)

    assert sorted(dataset.widget.df()["widget_id"].tolist()) == ["a", "c"]
    assert sorted(dataset.widget_summary.df()["widget_id"].tolist()) == ["a", "c"]
    assert "_rejected" not in dataset.tables


def test_build_entity_resource_pass_validates_each_valid_record_once(
    scenario_input_dir: Callable[[str], str],
    settings_factory: Callable[..., JsonlPydanticIngestSettings],
    widget_model: type[BaseModel],
    tmp_path: Path,
) -> None:
    """build_entity_resource validates each valid record only once instead of re-validating during dump."""
    validate_calls = 0

    class CountingWidget(widget_model):  # type: ignore[misc, valid-type]
        @classmethod
        def model_validate(cls, *args: Any, **kwargs: Any) -> Any:  # noqa: ANN401
            nonlocal validate_calls
            validate_calls += 1
            return super().model_validate(*args, **kwargs)

    settings = settings_factory(input_dir=scenario_input_dir("happy_path"), output_dir=str(tmp_path))

    dataset = run_pipeline("widget", CountingWidget, settings)

    expected_valid_records = 3
    assert len(dataset.widget.df()) == expected_valid_records
    assert validate_calls == expected_valid_records


def _identity_widget_transform(row: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    """Return the input row routed to the widget table."""
    return {"widget": [row]}


def test_build_entity_resource_pass_emits_pyarrow_tables_with_converted_model_schema(
    scenario_input_dir: Callable[[str], str],
    settings_factory: Callable[..., JsonlPydanticIngestSettings],
    widget_model: type[BaseModel],
    tmp_path: Path,
) -> None:
    """build_entity_resource yields PyArrow tables matching PydanticToPyArrow for valid rows and REJECTED_PYARROW_SCHEMA for rejected rows."""
    settings = settings_factory(input_dir=scenario_input_dir("mixed"), output_dir=str(tmp_path))

    items = list(build_entity_resource("widget", widget_model, settings))

    expected_page_count = 2
    assert len(items) == expected_page_count
    assert all(isinstance(item, pa.Table) for item in items)
    expected_widget_schema = PydanticToPyArrow().convert(widget_model)
    valid_table = next(item for item in items if item.schema == expected_widget_schema)
    rejected_table = next(item for item in items if item.schema == REJECTED_PYARROW_SCHEMA)
    assert [row["widget_id"] for row in valid_table.to_pylist()] == ["a", "c"]
    expected_rejected_count = 2
    assert len(rejected_table) == expected_rejected_count


def test_run_jsonlines_with_validation_pipeline_pass_mixed_dict_and_arrow_transforms_rejected_table(
    settings_factory: Callable[..., JsonlPydanticIngestSettings],
    entity_models_factory: Callable[[dict[str, type[BaseModel]]], str],
    widget_model: type[BaseModel],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Entities with dict transforms and Arrow schemas emit expected valid pages and share a uniform _rejected table."""
    models_mod = entity_models_factory({"widget": widget_model, "gadget": widget_model})
    transforms_mod = register_temp_module(
        monkeypatch,
        "_test_mixed_pydantic_transforms",
        TABLE_TRANSFORMS={"widget": _identity_widget_transform},
    )
    input_dir = tmp_path / "mixed_entities_input"
    (input_dir / "widget").mkdir(parents=True)
    (input_dir / "gadget").mkdir(parents=True)
    (input_dir / "widget" / "data.jsonl").write_text(
        '{"widget_id": "w_ok", "count": 1}\n{"widget_id": "w_bad", "count": -1}\n',
        encoding="utf-8",
    )
    (input_dir / "gadget" / "data.jsonl").write_text(
        '{"widget_id": "g_ok", "count": 2}\n{"widget_id": "g_bad", "count": -1}\n',
        encoding="utf-8",
    )

    settings = settings_factory(
        input_dir=str(input_dir),
        output_dir=str(tmp_path / "output"),
        entity_models=models_mod,
        transforms_module=transforms_mod,
    )
    assert settings.schema_map is not None
    assert set(settings.schema_map.registry) == {"gadget"}

    widget_items = list(build_entity_resource("widget", widget_model, settings))
    gadget_items = list(build_entity_resource("gadget", widget_model, settings))

    expected_items_per_entity = 2
    assert len(widget_items) == expected_items_per_entity
    assert len(gadget_items) == expected_items_per_entity
    assert widget_items[0] == {"widget_id": "w_ok", "count": 1}
    assert isinstance(widget_items[1], pa.Table)
    assert widget_items[1].schema == REJECTED_PYARROW_SCHEMA
    assert isinstance(gadget_items[0], pa.Table)
    assert gadget_items[0].schema == PydanticToPyArrow().convert(widget_model)
    assert isinstance(gadget_items[1], pa.Table)
    assert gadget_items[1].schema == REJECTED_PYARROW_SCHEMA

    load_info = assert_run_ok(run_jsonlines_with_validation_pipeline(settings))
    dataset = load_info.pipeline.dataset()
    assert dataset.widget.df()["widget_id"].tolist() == ["w_ok"]
    assert dataset.gadget.df()["widget_id"].tolist() == ["g_ok"]
    rejected_df = dataset.table("_rejected").df()
    expected_rejected_rows = 2
    assert len(rejected_df) == expected_rejected_rows
    assert sorted(rejected_df["table_name"].tolist()) == ["gadget", "widget"]


def test_build_entity_resource_pass_per_table_preprocessors_route_failed_files_to_rejected(
    settings_factory: Callable[..., JsonlPydanticIngestSettings],
    entity_models_factory: Callable[[dict[str, type[BaseModel]]], str],
    widget_model: type[BaseModel],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Per-table FILE_PREPROCESSORS route failed entity files through make_page_router to _rejected."""
    models_mod = entity_models_factory({"widget": widget_model, "gadget": widget_model})

    def reject_corrupt_file(file_item: Any) -> Any:  # noqa: ANN401
        if file_item["relative_path"].endswith("corrupt.jsonl"):
            msg = "corrupt file header"
            raise FilePreprocessError(msg)
        return file_item

    preprocessors_mod = register_temp_module(
        monkeypatch,
        "_test_pydantic_per_table_preprocessors",
        FILE_PREPROCESSORS={"widget": [reject_corrupt_file]},
    )

    input_dir = tmp_path / "preprocess_entities_input"
    (input_dir / "widget").mkdir(parents=True)
    (input_dir / "gadget").mkdir(parents=True)
    (input_dir / "widget" / "ok.jsonl").write_text('{"widget_id": "w_ok", "count": 1}\n', encoding="utf-8")
    (input_dir / "widget" / "corrupt.jsonl").write_text('{"widget_id": "w_corrupt", "count": 2}\n', encoding="utf-8")
    (input_dir / "gadget" / "corrupt.jsonl").write_text('{"widget_id": "g_ok", "count": 3}\n', encoding="utf-8")

    settings = settings_factory(
        input_dir=str(input_dir),
        output_dir=str(tmp_path / "output"),
        entity_models=models_mod,
        preprocessors_module=preprocessors_mod,
    )

    load_info = assert_run_ok(run_jsonlines_with_validation_pipeline(settings))
    dataset = load_info.pipeline.dataset()
    assert dataset.widget.df()["widget_id"].tolist() == ["w_ok"]
    assert dataset.gadget.df()["widget_id"].tolist() == ["g_ok"]
    rejected_df = dataset.table("_rejected").df()
    assert len(rejected_df) == 1
    row = rejected_df.iloc[0]
    assert row["table_name"] == "widget"
    assert row["source_file"] == "widget/corrupt.jsonl"
    assert row["error_detail"] == "corrupt file header"
