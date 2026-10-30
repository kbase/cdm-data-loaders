"""Unit tests for run_xmltodict_pipeline and cli wiring."""

import sys
from collections.abc import Callable
from pathlib import Path
from unittest.mock import patch

import pytest

import cdm_data_loaders.pipelines.xmltodict.pipeline as xmltodict_module
from cdm_data_loaders.core.fields import LoaderFileFormatEnum
from cdm_data_loaders.pipelines.xmltodict.pipeline import (
    cli,
    run_xmltodict_pipeline,
)
from cdm_data_loaders.pipelines.xmltodict.settings import PIPELINE_NAME, XmlToDictSettings
from tests.cdm_data_loaders.pipelines.helpers import (
    assert_binds_filesystem_reader,
    assert_core_run_pipeline_args,
    fetch_all_duckdb,
    run_with_duckdb_pipeline,
)
from tests.helpers import settings_to_argv
from tests.xml_samples import TWO_BOOK_LIBRARY_XML


@pytest.mark.parametrize("loader_file_format", list(LoaderFileFormatEnum), ids=str)
def test_run_xmltodict_pipeline_pass_sets_core_run_pipeline_args_correctly(
    settings_factory: Callable[..., XmlToDictSettings],
    loader_file_format: str,
) -> None:
    """run_xmltodict_pipeline builds the reader and delegates to run_pipeline with the correct args."""
    settings = settings_factory(loader_file_format=loader_file_format)
    assert_core_run_pipeline_args(xmltodict_module, run_xmltodict_pipeline, settings, PIPELINE_NAME)


def test_run_xmltodict_pipeline_pass_binds_reader_before_run_pipeline(
    settings_factory: Callable[..., XmlToDictSettings],
) -> None:
    """The resource passed to run_pipeline is the bound reader piped into the filesystem source."""
    settings = settings_factory()
    assert_binds_filesystem_reader(xmltodict_module, run_xmltodict_pipeline, settings, "xmltodict_reader")


def test_cli_pass_runs_end_to_end_from_command_line_arguments(
    tmp_path: Path,
    settings_factory: Callable[..., XmlToDictSettings],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """cli() reads command-line arguments and runs the pipeline via run_xmltodict_pipeline.

    The real XmlToDictSettings parses argv (seeded by the autouse dlt
    config isolation fixture), and core.run_pipeline is redirected to a real
    DuckDB pipeline so the full flow is validated.
    """
    settings = settings_factory(dataset_name="cli_dataset", dlt_dev_mode=False)
    (Path(settings.input_dir) / "library.xml").write_text(TWO_BOOK_LIBRARY_XML, encoding="utf-8")
    monkeypatch.setattr(sys, "argv", [PIPELINE_NAME, *settings_to_argv(settings)])

    load_info = run_with_duckdb_pipeline(
        tmp_path,
        xmltodict_module,
        cli.run,
        expected_pipeline_name=PIPELINE_NAME,
        expected_dataset_name="cli_dataset",
        pipeline_name_prefix="test_xml_cli_pipeline",
    )

    assert fetch_all_duckdb(load_info.pipeline, "SELECT COUNT(*) FROM book") == [(2,)]


@pytest.mark.parametrize("missing_field", ["dataset_name", "table_name", "xml_tag"])
def test_cli_fail_missing_required_args_raise(
    settings_factory: Callable[..., XmlToDictSettings],
    monkeypatch: pytest.MonkeyPatch,
    missing_field: str,
) -> None:
    """cli() without required pipeline-specific args raises before running the pipeline."""
    settings = settings_factory()
    monkeypatch.setattr(sys, "argv", [PIPELINE_NAME, *settings_to_argv(settings, exclude=[missing_field])])

    with (
        patch.object(xmltodict_module, "run_pipeline") as mock_run_pipeline,
        pytest.raises(Exception, match=missing_field),
    ):
        cli()

    mock_run_pipeline.assert_not_called()
