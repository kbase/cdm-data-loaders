"""Unit tests for run_jsonlines_pipeline and cli wiring."""

import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

import dlt
import pyarrow as pa
import pytest

import cdm_data_loaders.pipelines.jsonlines.extract_pipeline as extract_pipeline_module
from cdm_data_loaders.core.fields import LoaderFileFormatEnum
from cdm_data_loaders.pipelines.core import LOAD_INFO_TABLE_NAME
from cdm_data_loaders.pipelines.jsonlines.extract_pipeline import cli, run_jsonlines_pipeline
from cdm_data_loaders.pipelines.jsonlines.settings import EXTRACT_PIPELINE_NAME, JsonlExtractSettings
from cdm_data_loaders.readers.jsonlines import route_jsonl_lines
from cdm_data_loaders.utils.buffer import REJECTED_PYARROW_SCHEMA
from tests.cdm_data_loaders.pipelines.conftest import DEFAULT_DLT_TABLES
from tests.cdm_data_loaders.pipelines.helpers import (
    assert_binds_filesystem_reader,
    assert_core_run_pipeline_args,
    assert_pipeline_report_written,
    assert_run_ok,
    make_file_item,
    run_with_duckdb_pipeline,
)
from tests.helpers import register_temp_module, settings_to_argv

SIMPLE_JSONL = """{"widget_id": "a", "count": 1}
{"widget_id": "b", "count": 2}
"""


@pytest.mark.parametrize(
    ("use_transforms", "use_output_schemas"),
    [
        pytest.param(False, False, id="untransformed_dicts"),
        pytest.param(True, True, id="transformed_and_coerced_to_pyarrow"),
    ],
)
def test_route_jsonl_lines_transformer_pass_routes_valid_and_rejected_items(
    extract_settings_factory: Callable[..., JsonlExtractSettings],
    monkeypatch: pytest.MonkeyPatch,
    use_transforms: bool,
    use_output_schemas: bool,
) -> None:
    """route_jsonl_lines transformer routes valid records to settings.table_name and parse errors to _rejected."""
    widget_schema = pa.schema(
        [pa.field("widget_id", pa.string(), nullable=False), pa.field("count", pa.int64(), nullable=False)]
    )
    transforms_mod: str | None = None
    if use_transforms:
        transforms_mod = register_temp_module(
            monkeypatch,
            "_test_jsonl_reader_transforms",
            TABLE_TRANSFORMS={
                "widget": lambda row: {} if row["count"] == 0 else {"widget": [row]},
            },
            OUTPUT_SCHEMAS={"widget": widget_schema},
        )
    settings = extract_settings_factory(
        buffer_size=10,
        transforms_module=transforms_mod,
        output_schemas_module=transforms_mod if use_output_schemas else None,
    )
    input_dir = Path(settings.input_dir)
    nested_dir = input_dir / "nested"
    nested_dir.mkdir(parents=True)
    file_1 = input_dir / "top.jsonl"
    file_2 = nested_dir / "sub.jsonl"
    file_1.write_text('{"widget_id": "a", "count": 0}\n{"widget_id": "b", "count": 2}\n', encoding="utf-8")
    file_2.write_text("{bad_json\n", encoding="utf-8")

    file_items = [make_file_item(file_1), make_file_item(file_2)]
    reader = dlt.transformer(route_jsonl_lines, name="jsonl_reader", parallelized=True)
    pages = list(dlt.resource([file_items], name="files") | reader(settings))

    if use_output_schemas:
        expected_table_pages = 2
        assert len(pages) == expected_table_pages
        valid_page = next(page for page in pages if isinstance(page, pa.Table) and "widget_id" in page.schema.names)
        assert valid_page.schema == widget_schema
        assert valid_page.to_pylist() == [{"widget_id": "b", "count": 2}]
    else:
        expected_untransformed_items = 3
        assert len(pages) == expected_untransformed_items
        valid_rows = [page for page in pages if isinstance(page, dict)]
        assert valid_rows == [{"widget_id": "a", "count": 0}, {"widget_id": "b", "count": 2}]

    rejected_page = next(
        page for page in pages if isinstance(page, pa.Table) and page.schema == REJECTED_PYARROW_SCHEMA
    )
    assert rejected_page.to_pylist() == [
        {
            "table_name": "widget",
            "source_file": "sub.jsonl",
            "line_no": 1,
            "raw_record": "{bad_json",
            "error_detail": "Expecting property name enclosed in double quotes: line 1 column 2 (char 1)",
        }
    ]


@pytest.mark.parametrize("loader_file_format", list(LoaderFileFormatEnum), ids=str)
def test_run_jsonlines_pipeline_pass_sets_core_run_pipeline_args_correctly(
    extract_settings_factory: Callable[..., JsonlExtractSettings],
    loader_file_format: str,
) -> None:
    """run_jsonlines_pipeline builds the reader and delegates to run_pipeline with the correct args."""
    settings = extract_settings_factory(loader_file_format=loader_file_format)
    assert_core_run_pipeline_args(extract_pipeline_module, run_jsonlines_pipeline, settings, EXTRACT_PIPELINE_NAME)


def test_run_jsonlines_pipeline_pass_resource_is_reader_piped_into_source(
    extract_settings_factory: Callable[..., JsonlExtractSettings],
) -> None:
    """The resource passed to run_pipeline combines the filesystem source with the bound reader."""
    settings = extract_settings_factory()
    assert_binds_filesystem_reader(
        extract_pipeline_module,
        run_jsonlines_pipeline,
        settings,
        expected_reader_name="jsonl_reader",
    )


def test_run_jsonlines_pipeline_pass_routes_lines_by_directory(
    extract_settings_factory: Callable[..., JsonlExtractSettings],
) -> None:
    """Top-level and deeply nested files all land in the table_name table regardless of directory."""
    settings = extract_settings_factory(file_glob="**/*.jsonl*")
    input_dir = Path(settings.input_dir)
    (input_dir / "top.jsonl").write_text('{"widget_id": "top", "count": 0}\n', encoding="utf-8")
    (input_dir / "widget").mkdir(parents=True)
    (input_dir / "widget" / "data.jsonl").write_text(SIMPLE_JSONL, encoding="utf-8")
    deep_dir = input_dir / "gadget" / "a" / "b" / "c"
    deep_dir.mkdir(parents=True)
    (deep_dir / "deep.jsonl").write_text('{"widget_id": "g", "count": 3}\n', encoding="utf-8")

    load_info = assert_run_ok(run_jsonlines_pipeline(settings))

    dataset = load_info.pipeline.dataset()
    assert sorted(dataset.widget.df()["widget_id"].tolist()) == ["a", "b", "g", "top"]
    assert_pipeline_report_written(
        Path(settings.output_dir),
        load_info,
        ingested_files={"top.jsonl", "widget/data.jsonl", "gadget/a/b/c/deep.jsonl"},
    )


def test_run_jsonlines_pipeline_pass_invalid_rows_routed_to_invalid_table(
    extract_settings_factory: Callable[..., JsonlExtractSettings],
) -> None:
    """Lines that are not valid JSON land in _rejected with table_name, relative source_file, and error detail."""
    settings = extract_settings_factory(file_glob="**/*.jsonl*")
    input_dir = Path(settings.input_dir)
    deep_dir = input_dir / "widget" / "a" / "b" / "c"
    deep_dir.mkdir(parents=True)
    (deep_dir / "data.jsonl").write_text('{"widget_id": "a"}\n{broken\n', encoding="utf-8")

    load_info = assert_run_ok(run_jsonlines_pipeline(settings))

    dataset = load_info.pipeline.dataset()
    assert sorted(dataset.widget.df()["widget_id"].tolist()) == ["a"]
    rejected_df = dataset.table("_rejected").df()
    assert len(rejected_df) == 1
    assert rejected_df.iloc[0]["table_name"] == "widget"
    assert rejected_df.iloc[0]["source_file"] == "widget/a/b/c/data.jsonl"
    assert rejected_df.iloc[0]["raw_record"] == "{broken"
    assert rejected_df.iloc[0]["error_detail"]
    assert_pipeline_report_written(Path(settings.output_dir), load_info, ingested_files=["widget/a/b/c/data.jsonl"])


def test_run_jsonlines_pipeline_pass_no_matching_files_yields_no_tables(
    extract_settings_factory: Callable[..., JsonlExtractSettings],
) -> None:
    """An input dir with no matching files does not fail the run and creates no data tables."""
    settings = extract_settings_factory()
    Path(settings.input_dir).mkdir(exist_ok=True)

    load_info = assert_run_ok(run_jsonlines_pipeline(settings))

    dataset = load_info.pipeline.dataset()
    assert set(dataset.tables) == DEFAULT_DLT_TABLES
    assert_pipeline_report_written(Path(settings.output_dir), load_info, ingested_files=[])


def test_cli_pass_runs_end_to_end_from_command_line_arguments(
    extract_settings_factory: Callable[..., JsonlExtractSettings],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """cli() reads command-line arguments and runs the pipeline, producing the expected output."""
    settings = extract_settings_factory(dataset_name="cli_dataset", dlt_dev_mode=False)
    (Path(settings.input_dir) / "data.jsonl").write_text(SIMPLE_JSONL, encoding="utf-8")
    monkeypatch.setattr(sys, "argv", [EXTRACT_PIPELINE_NAME, *settings_to_argv(settings)])

    load_info = run_with_duckdb_pipeline(
        tmp_path,
        extract_pipeline_module,
        cli.run,
        expected_pipeline_name=EXTRACT_PIPELINE_NAME,
        expected_dataset_name="cli_dataset",
        pipeline_name_prefix=EXTRACT_PIPELINE_NAME,
    )

    # n.b. no _load_info due to this being a monkeypatched pipeline
    assert set(load_info.pipeline.dataset().tables) == (DEFAULT_DLT_TABLES - {LOAD_INFO_TABLE_NAME}) | {"widget"}
    assert sorted(load_info.pipeline.dataset().widget.df()["widget_id"].tolist()) == ["a", "b"]


def test_run_jsonlines_pipeline_pass_filters_and_splits_with_transforms_module(
    extract_settings_factory: Callable[..., JsonlExtractSettings],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """run_jsonlines_pipeline filters and splits parsed records when transforms_module is configured."""

    def transform(row: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
        return (
            {}
            if row["count"] == 0
            else {
                "widget": [{"widget_id": row["widget_id"]}],
                "widget_counts": [{"widget_id": row["widget_id"], "count": row["count"]}],
            }
        )

    transforms_mod = register_temp_module(
        monkeypatch,
        "_test_extract_transforms",
        TABLE_TRANSFORMS={"widget": transform},
    )
    settings = extract_settings_factory(transforms_module=transforms_mod)
    (Path(settings.input_dir) / "data.jsonl").write_text(
        '{"widget_id": "skip", "count": 0}\n{"widget_id": "keep", "count": 5}\n',
        encoding="utf-8",
    )

    load_info = assert_run_ok(run_jsonlines_pipeline(settings))
    dataset = load_info.pipeline.dataset()
    assert dataset.widget.df()["widget_id"].tolist() == ["keep"]
    assert dataset.widget_counts.df()[["widget_id", "count"]].to_dict(orient="records") == [
        {"widget_id": "keep", "count": 5}
    ]
