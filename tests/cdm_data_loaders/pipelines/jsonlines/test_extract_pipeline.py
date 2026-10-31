"""Unit tests for run_jsonlines_pipeline and cli wiring."""

import sys
from collections.abc import Callable
from pathlib import Path

import pytest

import cdm_data_loaders.pipelines.jsonlines.extract_pipeline as extract_pipeline_module
from cdm_data_loaders.core.fields import LoaderFileFormatEnum
from cdm_data_loaders.pipelines.core import LOAD_INFO_TABLE_NAME
from cdm_data_loaders.pipelines.jsonlines.extract_pipeline import cli, run_jsonlines_pipeline
from cdm_data_loaders.pipelines.jsonlines.settings import EXTRACT_PIPELINE_NAME, JsonlExtractSettings
from tests.cdm_data_loaders.pipelines.conftest import DEFAULT_DLT_TABLES
from tests.cdm_data_loaders.pipelines.helpers import (
    assert_binds_filesystem_reader,
    assert_core_run_pipeline_args,
    assert_pipeline_report_written,
    assert_run_ok,
    run_with_duckdb_pipeline,
)
from tests.helpers import settings_to_argv

SIMPLE_JSONL = """{"widget_id": "a", "count": 1}
{"widget_id": "b", "count": 2}
"""


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
    assert_binds_filesystem_reader(extract_pipeline_module, run_jsonlines_pipeline, settings)


def test_run_jsonlines_pipeline_pass_routes_lines_by_directory(
    extract_settings_factory: Callable[..., JsonlExtractSettings],
) -> None:
    """Top-level and nested files all land in the table_name table regardless of directory."""
    settings = extract_settings_factory(file_glob="**/*.jsonl*")
    input_dir = Path(settings.input_dir)
    (input_dir / "widget").mkdir(parents=True)
    (input_dir / "widget" / "data.jsonl").write_text(SIMPLE_JSONL, encoding="utf-8")
    (input_dir / "gadget").mkdir()
    (input_dir / "gadget" / "data.jsonl").write_text('{"widget_id": "g", "count": 0}\n', encoding="utf-8")

    load_info = assert_run_ok(run_jsonlines_pipeline(settings))

    dataset = load_info.pipeline.dataset()
    assert sorted(dataset.widget.df()["widget_id"].tolist()) == ["a", "b", "g"]
    assert_pipeline_report_written(
        Path(settings.output_dir),
        load_info,
        ingested_files={"widget/data.jsonl", "gadget/data.jsonl"},
    )


def test_run_jsonlines_pipeline_pass_invalid_rows_routed_to_invalid_table(
    extract_settings_factory: Callable[..., JsonlExtractSettings],
) -> None:
    """Lines that are not valid JSON land in <table_name>_invalid with parse error detail."""
    settings = extract_settings_factory(file_glob="**/*.jsonl*")
    input_dir = Path(settings.input_dir)
    (input_dir / "widget").mkdir(parents=True)
    (input_dir / "widget" / "data.jsonl").write_text('{"widget_id": "a"}\n{broken\n', encoding="utf-8")

    load_info = assert_run_ok(run_jsonlines_pipeline(settings))

    dataset = load_info.pipeline.dataset()
    assert sorted(dataset.widget.df()["widget_id"].tolist()) == ["a"]
    invalid_df = dataset.widget_invalid.df()
    assert len(invalid_df) == 1
    assert invalid_df.iloc[0]["raw_record"] == "{broken"
    assert_pipeline_report_written(Path(settings.output_dir), load_info, ingested_files=["widget/data.jsonl"])


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
