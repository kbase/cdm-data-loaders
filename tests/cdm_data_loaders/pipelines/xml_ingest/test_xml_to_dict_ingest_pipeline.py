"""Unit tests for run_xml_ingest_pipeline and cli wiring."""

import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any
from unittest.mock import patch
from uuid import uuid4

import dlt
import pytest

import cdm_data_loaders.pipelines.xml_to_dict_ingest as xml_to_dict_ingest_module
from cdm_data_loaders.pipelines.xml_to_dict_ingest import (
    PIPELINE_NAME,
    XmlToDictIngestSettings,
    cli,
    run_xml_ingest_pipeline,
)

SIMPLE_LIBRARY_XML = """<?xml version="1.0"?>
<library>
    <book id="1"><title>The Shining</title></book>
    <book id="2"><title>The Stand</title></book>
</library>
"""


def test_run_xml_ingest_pipeline_pass_sets_core_run_pipeline_args_correctly(
    settings_factory: Callable[..., XmlToDictIngestSettings],
    fresh_xml_to_dict_reader: Callable[[], Any],
) -> None:
    """run_xml_ingest_pipeline binds the reader and delegates to run_pipeline with the correct args."""
    settings = settings_factory()
    fresh_xml_to_dict_reader()

    with patch.object(xml_to_dict_ingest_module, "run_pipeline") as mock_run_pipeline:
        run_xml_ingest_pipeline(settings)

    assert mock_run_pipeline.call_count == 1
    _, kwargs = mock_run_pipeline.call_args
    assert kwargs.keys() == {"settings", "resource", "destination_kwargs", "pipeline_kwargs", "pipeline_run_kwargs"}
    assert kwargs["settings"] == settings
    assert kwargs["destination_kwargs"] == {"max_table_nesting": 0}
    assert kwargs["pipeline_kwargs"] == {
        "pipeline_name": PIPELINE_NAME,
        "dataset_name": settings.dataset_name,
    }
    assert kwargs["pipeline_run_kwargs"] == {"loader_file_format": "parquet"}
    assert isinstance(kwargs["resource"], object)


def test_run_xml_ingest_pipeline_pass_binds_reader_before_run_pipeline(
    settings_factory: Callable[..., XmlToDictIngestSettings],
    fresh_xml_to_dict_reader: Callable[[], Any],
) -> None:
    """The module-level transformer is bound to settings before run_pipeline is called."""
    settings = settings_factory()
    fresh_xml_to_dict_reader()

    with patch.object(xml_to_dict_ingest_module, "run_pipeline") as mock_run_pipeline:
        run_xml_ingest_pipeline(settings)

    mock_run_pipeline.assert_called_once()
    reader = xml_to_dict_ingest_module.xml_to_dict_reader
    assert reader.args_bound
    assert reader.explicit_args == {"items": settings}


def test_run_xml_ingest_pipeline_pass_resource_is_reader_piped_into_source(
    settings_factory: Callable[..., XmlToDictIngestSettings],
    fresh_xml_to_dict_reader: Callable[[], Any],
) -> None:
    """The resource passed to run_pipeline combines the filesystem source with the bound reader."""
    settings = settings_factory()
    fresh_xml_to_dict_reader()

    with patch.object(xml_to_dict_ingest_module, "run_pipeline") as mock_run_pipeline:
        run_xml_ingest_pipeline(settings)

    resource = mock_run_pipeline.call_args.kwargs["resource"]
    assert isinstance(resource, dlt.sources.DltResource)


def test_cli_pass_runs_end_to_end_from_command_line_arguments(
    fresh_xml_to_dict_reader: Callable[[], Any],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """cli() reads command-line arguments and runs the pipeline via run_xml_ingest_pipeline.

    The real XmlToDictIngestSettings parses argv (seeded by the autouse dlt
    config isolation fixture), and core.run_pipeline is redirected to a real
    DuckDB pipeline so the full flow is validated.
    """
    input_dir = tmp_path / "cli_input"
    input_dir.mkdir()
    (input_dir / "library.xml").write_text(SIMPLE_LIBRARY_XML, encoding="utf-8")
    output_dir = tmp_path / "cli_output"
    output_dir.mkdir()
    log_config_file = tmp_path / "logging.json"
    log_config_file.write_text('{"version": 1}')
    fresh_xml_to_dict_reader()

    argv = [
        "xml_to_dict_ingest",
        "--input-dir",
        str(input_dir),
        "--output-dir",
        str(output_dir),
        "--log-config-file",
        str(log_config_file),
        "--dataset-name",
        "cli_dataset",
        "--table-name",
        "book",
        "--xml-tag",
        "book",
        "--use-output-dir-for-pipeline-metadata",
        "false",
        "--dev-mode",
        "false",
    ]
    monkeypatch.setattr(sys, "argv", argv)

    # unique pipeline name: the duckdb database file is derived from it, so
    # repeated runs of the suite do not see rows from previous runs
    pipeline_name = f"test_xml_cli_pipeline_{uuid4().hex}"
    captured: dict[str, Any] = {}

    def fake_run_pipeline(
        *,
        resource: Any,  # noqa: ANN401
        pipeline_kwargs: dict[str, Any],
        **_: Any,  # noqa: ANN401
    ) -> None:
        assert pipeline_kwargs == {"pipeline_name": PIPELINE_NAME, "dataset_name": "cli_dataset"}
        pipeline = dlt.pipeline(
            pipeline_name=pipeline_name,
            destination="duckdb",
            dataset_name="cli_dataset",
            pipelines_dir=str(tmp_path / "pipelines"),
        )
        captured["load_info"] = pipeline.run(resource)

    with patch.object(xml_to_dict_ingest_module, "run_pipeline", fake_run_pipeline):
        cli()

    load_info = captured["load_info"]
    assert not load_info.has_failed_jobs

    pipeline = dlt.pipeline(
        pipeline_name=pipeline_name,
        destination="duckdb",
        dataset_name="cli_dataset",
        pipelines_dir=str(tmp_path / "pipelines"),
    )
    with (
        pipeline.sql_client() as client,
        client.execute_query("SELECT COUNT(*) FROM book") as cur,
    ):
        (row_count,) = cur.fetchone()
    assert row_count == 2  # noqa: PLR2004


def test_cli_pass_writes_rows_for_matching_xml_files(
    fresh_xml_to_dict_reader: Callable[[], Any],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """cli() with xml files in the input dir loads one row per matching element."""
    input_dir = tmp_path / "xml_input"
    input_dir.mkdir()
    (input_dir / "data.xml").write_text(SIMPLE_LIBRARY_XML, encoding="utf-8")

    output_dir = tmp_path / "cli_output"
    output_dir.mkdir()
    log_config_file = tmp_path / "logging.json"
    log_config_file.write_text('{"version": 1}')
    fresh_xml_to_dict_reader()

    argv = [
        "xml_to_dict_ingest",
        "--input-dir",
        str(input_dir),
        "--output-dir",
        str(output_dir),
        "--log-config-file",
        str(log_config_file),
        "--dataset-name",
        "cli_dataset",
        "--table-name",
        "book",
        "--xml-tag",
        "book",
        "--use-output-dir-for-pipeline-metadata",
        "false",
        "--dev-mode",
        "false",
    ]
    monkeypatch.setattr(sys, "argv", argv)

    pipeline_name = f"test_xml_cli_rows_pipeline_{uuid4().hex}"
    captured: dict[str, Any] = {}

    def fake_run_pipeline(
        *,
        resource: Any,  # noqa: ANN401
        **_: Any,  # noqa: ANN401
    ) -> None:
        pipeline = dlt.pipeline(
            pipeline_name=pipeline_name,
            destination="duckdb",
            dataset_name="cli_dataset",
            pipelines_dir=str(tmp_path / "pipelines"),
        )
        captured["load_info"] = pipeline.run(resource)
        captured["pipeline"] = pipeline

    with patch.object(xml_to_dict_ingest_module, "run_pipeline", fake_run_pipeline):
        cli()

    assert not captured["load_info"].has_failed_jobs
    pipeline = captured["pipeline"]
    with (
        pipeline.sql_client() as client,
        client.execute_query("SELECT COUNT(*) FROM book") as cur,
    ):
        (row_count,) = cur.fetchone()
    assert row_count == 2  # noqa: PLR2004


@pytest.mark.parametrize(
    ("missing_args", "expected_fragment"),
    [
        (["--table-name", "book", "--xml-tag", "book"], "dataset_name"),
        (["--dataset-name", "cli_dataset", "--xml-tag", "book"], "table_name"),
        (["--dataset-name", "cli_dataset", "--table-name", "book"], "xml_tag"),
    ],
)
def test_cli_fail_missing_required_args_raise(
    fresh_xml_to_dict_reader: Callable[[], Any],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    missing_args: list[str],
    expected_fragment: str,
) -> None:
    """cli() without required pipeline-specific args raises before running the pipeline."""
    log_config_file = tmp_path / "logging.json"
    log_config_file.write_text('{"version": 1}')
    fresh_xml_to_dict_reader()

    argv = [
        "xml_to_dict_ingest",
        "--input-dir",
        str(tmp_path / "input"),
        "--output-dir",
        str(tmp_path / "output"),
        "--log-config-file",
        str(log_config_file),
        *missing_args,
    ]
    monkeypatch.setattr(sys, "argv", argv)

    with (
        patch.object(xml_to_dict_ingest_module, "run_pipeline") as mock_run_pipeline,
        pytest.raises(Exception, match=expected_fragment),
    ):
        cli()

    mock_run_pipeline.assert_not_called()
