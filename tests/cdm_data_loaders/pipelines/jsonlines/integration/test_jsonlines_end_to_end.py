"""End-to-end tests for the jsonlines pipeline."""

from collections.abc import Callable
from pathlib import Path

import pytest

from cdm_data_loaders.pipelines.jsonlines.settings import PYDANTIC_PIPELINE_NAME, JsonlPydanticIngestSettings
from tests.cdm_data_loaders.pipelines.framework import PipelineTestRunner
from tests.cdm_data_loaders.pipelines.helpers import (
    assert_pipeline_report_written,
    assert_run_ok,
)
from tests.cdm_data_loaders.pipelines.jsonlines.test_jsonlines_pydantic_spec import (
    ENTITY_TABLE_NAME,
    RUN_PARAMS,
    stage_jsonl_run,
)
from tests.converter_spec import Case, Run
from tests.helpers import settings_to_argv


def test_run_jsonlines_pipeline_pass_writes_expected_parquet(
    scenario_input_dir: Callable[[str], str],
    settings_factory: Callable[..., JsonlPydanticIngestSettings],
    pipeline_runner: PipelineTestRunner,
) -> None:
    """Running the pipeline on a mixed batch puts the correct rows in both output tables."""
    settings = settings_factory(input_dir=scenario_input_dir("mixed"))

    load_info = assert_run_ok(pipeline_runner.run(PYDANTIC_PIPELINE_NAME, settings=settings))
    dataset = load_info.pipeline.dataset()
    assert sorted(dataset.widget.df()["widget_id"].tolist()) == ["a", "c"]
    expected_rejected_count = 2
    assert len(dataset.table("_rejected").df()) == expected_rejected_count
    assert set(dataset.table("_rejected").df()["table_name"].tolist()) == {"widget"}
    assert_pipeline_report_written(Path(settings.output_dir), load_info)


def test_cli_pass_runs_end_to_end_from_command_line_arguments(
    scenario_input_dir: Callable[[str], str],
    settings_factory: Callable[..., JsonlPydanticIngestSettings],
    pipeline_runner: PipelineTestRunner,
) -> None:
    """cli() reads command-line arguments and runs the pipeline, producing the expected output."""
    settings = settings_factory(
        input_dir=scenario_input_dir("happy_path"),
        dataset_name="some_cool_dataset",
        dlt_dev_mode=False,
        use_output_dir_for_pipeline_metadata=True,
    )

    load_info = assert_run_ok(pipeline_runner.run(PYDANTIC_PIPELINE_NAME, cli_args=settings_to_argv(settings)))
    assert load_info.pipeline.pipeline_name == PYDANTIC_PIPELINE_NAME
    dataset = load_info.pipeline.dataset()
    assert sorted(dataset.widget.df()["widget_id"].tolist()) == ["a", "b", "c"]
    assert_pipeline_report_written(Path(settings.output_dir), load_info, metadata_moved=True)


@pytest.mark.parametrize(("case", "run"), RUN_PARAMS)
def test_run_jsonlines_with_validation_pipeline_pass_converter_spec_cases(
    case: Case,
    run: Run,
    tmp_path: Path,
    dlt_destination_config: str,
    monkeypatch: pytest.MonkeyPatch,
    pipeline_runner: PipelineTestRunner,
) -> None:
    """Running jsonlines_pydantic end-to-end loads every pydantic_to_pyarrow spec and example fixture without error."""
    _, settings = stage_jsonl_run(tmp_path, dlt_destination_config, monkeypatch, case, run)
    load_info = assert_run_ok(pipeline_runner.run(PYDANTIC_PIPELINE_NAME, settings=settings))
    dataset = load_info.pipeline.dataset()
    assert len(dataset.table(ENTITY_TABLE_NAME).df()) == run.expected_count
    assert "_rejected" not in dataset.tables
    assert_pipeline_report_written(Path(settings.output_dir), load_info)
