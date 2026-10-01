"""End-to-end tests for the xml2db_ingest pipeline, using a small library.xsd schema."""

import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest
from dlt.common.pipeline import LoadInfo

from cdm_data_loaders.pipelines.xml2db.pipeline import cli, run_xml2db_ingest_pipeline
from cdm_data_loaders.pipelines.xml2db.settings import PIPELINE_NAME, Xml2DbSettings
from tests.integration.pipelines.conftest import SIMPLE_LIBRARY_XML
from tests.integration.pipelines.xml2db.xml2db_reference_helpers import read_iceberg_tables

EXPECTED_BOOK_IDS = ["1", "2", "3"]
EXPECTED_LIBRARY_ROOT_COUNT_TWO_FILES = 2
LIBRARY_TABLES = {"library", "book", "library_book"}

OVERLAPPING_LIBRARY_XML = """<?xml version="1.0"?>
<library>
    <book id="3"><title>The Tommyknockers</title></book>
    <book id="4"><title>It</title></book>
</library>
"""


def read_library_tables(load_info: LoadInfo | None, output_dir: str) -> dict[str, list[dict[str, Any]]]:
    """Check a library run succeeded and read back its iceberg tables."""
    assert load_info is not None
    assert load_info.has_failed_jobs is False
    return read_iceberg_tables(Path(output_dir) / load_info.dataset_name)


def check_book_list_results(load_info: LoadInfo | None, output_dir: str) -> dict[str, list[dict[str, Any]]]:
    """Check over the output of parsing SIMPLE_LIBRARY_XML once: one root row, three books, three links."""
    tables = read_library_tables(load_info, output_dir)
    assert set(tables) == LIBRARY_TABLES
    assert set(tables["book"][0]) >= {"pk_book", "id", "title"}
    assert len(tables["library"]) == 1
    assert sorted(row["id"] for row in tables["book"]) == EXPECTED_BOOK_IDS
    assert sorted(row["title"] for row in tables["book"]) == ["The Shining", "The Stand", "The Tommyknockers"]
    assert len(tables["library_book"]) == len(EXPECTED_BOOK_IDS)
    return tables


def write_library_file(settings: Xml2DbSettings, name: str, content: str) -> None:
    """Write an XML file into the settings' input directory."""
    input_dir = Path(settings.input_dir)
    input_dir.mkdir(exist_ok=True)
    (input_dir / name).write_text(content, encoding="utf-8")


def test_run_xml2db_ingest_pipeline_pass_writes_expected_output(
    settings_factory: Callable[..., Xml2DbSettings],
) -> None:
    """Running the pipeline loads one row per library root and one row per book, as iceberg tables."""
    settings = settings_factory()
    write_library_file(settings, "library.xml", SIMPLE_LIBRARY_XML)

    check_book_list_results(run_xml2db_ingest_pipeline(settings), settings.output_dir)

    for table_name in LIBRARY_TABLES:
        assert (Path(settings.output_dir) / settings.dataset_name / table_name / "metadata").is_dir()


def test_run_xml2db_ingest_pipeline_pass_gzip_files_are_loaded(
    settings_factory: Callable[..., Xml2DbSettings],
    write_gzip_file: Callable[[Path, str, str], Path],
) -> None:
    """Gzip-compressed xml files matching the glob are decompressed and loaded.

    Both files contain the same three books. `book` is a content-addressed table, so the two
    files' identical book rows are merged into one copy of each -- unlike `library`, the schema's
    root table, which is not content-addressed (see `is_xml2db_content_addressed_table`) and
    keeps one row per source file.
    """
    settings = settings_factory()
    write_library_file(settings, "plain.xml", SIMPLE_LIBRARY_XML)
    write_gzip_file(Path(settings.input_dir), "library.xml.gz", SIMPLE_LIBRARY_XML)

    tables = read_library_tables(run_xml2db_ingest_pipeline(settings), settings.output_dir)

    assert len(tables["library"]) == EXPECTED_LIBRARY_ROOT_COUNT_TWO_FILES
    assert sorted(row["id"] for row in tables["book"]) == EXPECTED_BOOK_IDS
    assert len(tables["library_book"]) == len(EXPECTED_BOOK_IDS) * EXPECTED_LIBRARY_ROOT_COUNT_TWO_FILES


def test_run_xml2db_ingest_pipeline_pass_no_matching_files_yields_no_data_table(
    settings_factory: Callable[..., Xml2DbSettings],
) -> None:
    """An input dir with no matching xml files does not fail the run and creates no data table."""
    settings = settings_factory()
    Path(settings.input_dir).mkdir(exist_ok=True)

    tables = read_library_tables(run_xml2db_ingest_pipeline(settings), settings.output_dir)

    assert tables == {}


def test_run_xml2db_ingest_pipeline_pass_custom_short_name_used_for_root_table(
    settings_factory: Callable[..., Xml2DbSettings],
) -> None:
    """The `short_name` setting only affects the virtual root table name for multi-root schemas.

    library.xsd has a single root element, so xml2db names the root table after that element
    ("library") regardless of `short_name`; this just confirms the setting is accepted and does
    not break a single-root schema.
    """
    settings = settings_factory(short_name="my_schema")
    write_library_file(settings, "library.xml", SIMPLE_LIBRARY_XML)

    check_book_list_results(run_xml2db_ingest_pipeline(settings), settings.output_dir)


def test_run_xml2db_ingest_pipeline_pass_rerun_into_same_dataset_adds_no_rows(
    settings_factory: Callable[..., Xml2DbSettings],
) -> None:
    """Running the same input into the same dataset again leaves every table exactly as it was."""
    settings = settings_factory()
    write_library_file(settings, "library.xml", SIMPLE_LIBRARY_XML)

    first = check_book_list_results(run_xml2db_ingest_pipeline(settings), settings.output_dir)
    second = check_book_list_results(run_xml2db_ingest_pipeline(settings), settings.output_dir)

    assert {name: sorted(rows, key=str) for name, rows in second.items()} == {
        name: sorted(rows, key=str) for name, rows in first.items()
    }


def test_run_xml2db_ingest_pipeline_pass_later_run_merges_only_new_rows_into_existing_tables(
    settings_factory: Callable[..., Xml2DbSettings],
) -> None:
    """A later run over a new file adds only the rows not already in the tables.

    The new file repeats book 3 and adds book 4, so only book 4 is new. Its root row and the
    links from its root are new too, because root keys are per source file.
    """
    settings = settings_factory()
    write_library_file(settings, "library.xml", SIMPLE_LIBRARY_XML)
    first = check_book_list_results(run_xml2db_ingest_pipeline(settings), settings.output_dir)

    write_library_file(settings, "later.xml", OVERLAPPING_LIBRARY_XML)
    second = read_library_tables(run_xml2db_ingest_pipeline(settings), settings.output_dir)

    assert sorted(row["id"] for row in second["book"]) == [*EXPECTED_BOOK_IDS, "4"]
    first_book_pks = {row["pk_book"] for row in first["book"]}
    assert first_book_pks <= {row["pk_book"] for row in second["book"]}
    assert len(second["library"]) == EXPECTED_LIBRARY_ROOT_COUNT_TWO_FILES
    # one link per book per library file that the book appears in: 3 in the first file, 2 in the second
    assert len(second["library_book"]) == len(EXPECTED_BOOK_IDS) + 2


def test_cli_pass_runs_end_to_end_from_command_line_arguments(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    dlt_destination_config: str,
    library_xsd: Path,
) -> None:
    """cli() reads command-line arguments and runs the pipeline, producing the expected output."""
    input_dir = tmp_path / "cli_input"
    input_dir.mkdir()
    (input_dir / "library.xml").write_text(SIMPLE_LIBRARY_XML, encoding="utf-8")
    output_dir = tmp_path / "cli_output"
    output_dir.mkdir()
    log_config_file = tmp_path / "logging.json"
    log_config_file.write_text('{"version": 1}')

    argv = [
        PIPELINE_NAME,
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
        "--xsd-file",
        str(library_xsd),
        "--use-output-dir-for-pipeline-metadata",
        "false",
        "--dev-mode",
        "false",
    ]
    monkeypatch.setattr(sys, "argv", argv)

    check_book_list_results(cli(), str(output_dir))
