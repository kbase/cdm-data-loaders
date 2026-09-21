"""End-to-end tests for the xml_to_dict_ingest pipeline."""

import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

import dlt
from pandas import DataFrame
import pytest
from dlt.common.pipeline import LoadInfo
from cdm_data_loaders.core.fields import LoaderFileFormatEnum, JSONL, PARQUET
from cdm_data_loaders.pipelines.xml_to_dict.pipeline import (
    cli,
    run_xml_ingest_pipeline,
)
from cdm_data_loaders.pipelines.xml_to_dict.settings import (
    PIPELINE_NAME,
    XmlToDictSettings,
)

SIMPLE_LIBRARY_XML = """<?xml version="1.0"?>
<library>
    <book id="1"><title>The Shining</title></book>
    <book id="2"><title>The Stand</title></book>
    <book id="3"><title>The Tommyknockers</title></book>
</library>
"""
DEFAULT_DLT_TABLES = {"_dlt_version", "_dlt_loads", "_dlt_pipeline_state"}


def check_book_list_results(load_info: LoadInfo | None, table_name: str | None = None) -> DataFrame:
    """Check over the output of parsing SIMPLE_LIBRARY_XML."""
    assert load_info is not None
    assert load_info.has_failed_jobs is False
    dataset = load_info.pipeline.dataset()
    table_name = table_name or "book"
    assert set(dataset.tables) == {table_name, *DEFAULT_DLT_TABLES}
    book_df = dataset.table(table_name).df()
    assert set(book_df.columns.tolist()) >= {"book___id", "book__title", "_dlt_id", "_dlt_load_id"}
    return book_df


@pytest.mark.parametrize("loader_file_format", LoaderFileFormatEnum.__members__.values())
def test_run_xml_ingest_pipeline_pass_writes_expected_output(
    settings_factory: Callable[..., XmlToDictSettings],
    fresh_xml_to_dict_reader: Callable[[], Any],
    loader_file_format: str,
) -> None:
    """Running the pipeline on xml files loads one row per matching element into the configured table."""
    settings = settings_factory(loader_file_format=loader_file_format)
    input_dir = Path(settings.input_dir)
    input_dir.mkdir(exist_ok=True)
    (input_dir / "library.xml").write_text(SIMPLE_LIBRARY_XML, encoding="utf-8")
    fresh_xml_to_dict_reader()

    load_info = run_xml_ingest_pipeline(settings)
    book_df = check_book_list_results(load_info)
    assert sorted(book_df["book___id"].tolist()) == ["1", "2", "3"]


def test_run_xml_ingest_pipeline_pass_gzip_files_are_loaded(
    settings_factory: Callable[..., XmlToDictSettings],
    fresh_xml_to_dict_reader: Callable[[], Any],
    write_gzip_xml_file: Callable[[Path, str, str], Path],
) -> None:
    """Gzip-compressed xml files matching the glob are decompressed and loaded."""
    settings = settings_factory()
    input_dir = Path(settings.input_dir)
    input_dir.mkdir(exist_ok=True)
    (input_dir / "plain.xml").write_text(SIMPLE_LIBRARY_XML, encoding="utf-8")
    write_gzip_xml_file(input_dir, "library.xml.gz", SIMPLE_LIBRARY_XML)
    fresh_xml_to_dict_reader()

    load_info = run_xml_ingest_pipeline(settings)
    book_df = check_book_list_results(load_info)
    # one copy of the three books from the plain file, one from the gzipped copy
    assert sorted(book_df["book___id"].tolist()) == ["1", "1", "2", "2", "3", "3"]


def test_run_xml_ingest_pipeline_pass_custom_table_name_is_respected(
    settings_factory: Callable[..., XmlToDictSettings],
    fresh_xml_to_dict_reader: Callable[[], Any],
) -> None:
    """Rows land in the table named by table_name, not in a name derived from the xml tag."""
    settings = settings_factory(table_name="library_entries")
    input_dir = Path(settings.input_dir)
    input_dir.mkdir(exist_ok=True)
    (input_dir / "library.xml").write_text(SIMPLE_LIBRARY_XML, encoding="utf-8")
    fresh_xml_to_dict_reader()

    load_info = run_xml_ingest_pipeline(settings)
    book_df = check_book_list_results(load_info, "library_entries")
    # the record dict is keyed by the xml tag, so the flattened column prefix is `book`
    assert sorted(book_df["book___id"].tolist()) == ["1", "2", "3"]


def test_run_xml_ingest_pipeline_pass_no_matching_files_yields_no_data_table(
    settings_factory: Callable[..., XmlToDictSettings],
    fresh_xml_to_dict_reader: Callable[[], Any],
) -> None:
    """An input dir with no matching xml files does not fail the run and creates no data table."""
    settings = settings_factory()
    Path(settings.input_dir).mkdir(exist_ok=True)
    fresh_xml_to_dict_reader()

    load_info = run_xml_ingest_pipeline(settings)
    assert load_info is not None
    assert load_info.has_failed_jobs is False
    dataset = load_info.pipeline.dataset()
    # only dlt's own metadata tables exist; the data table is never created
    assert set(dataset.tables) == DEFAULT_DLT_TABLES


def test_cli_pass_runs_end_to_end_from_command_line_arguments(
    fresh_xml_to_dict_reader: Callable[[], Any],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    dlt_destination_config: str,
) -> None:
    """cli() reads command-line arguments and runs the pipeline, producing the expected output."""
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
        "--use-destination",
        dlt_destination_config,
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

    load_info = cli()
    book_df = check_book_list_results(load_info)
    assert sorted(book_df["book___id"].tolist()) == ["1", "2", "3"]
