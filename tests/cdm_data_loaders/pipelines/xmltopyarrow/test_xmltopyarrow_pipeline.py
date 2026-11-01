"""Unit tests for run_xmltopyarrow_pipeline and cli wiring."""

import gzip
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pyarrow as pa
import pytest
from dlt.extract import DltResource

import cdm_data_loaders.pipelines.xmltopyarrow.pipeline as xmltopyarrow_module
from cdm_data_loaders.core.fields import LoaderFileFormatEnum
from cdm_data_loaders.core.settings import CtsSettings
from cdm_data_loaders.core.transforms import TableTransform
from cdm_data_loaders.pipelines.xmltopyarrow.pipeline import (
    cli,
    run_xmltopyarrow_pipeline,
    uniprot_cli,
    uniref50_cli,
    uniref90_cli,
    uniref100_cli,
)
from cdm_data_loaders.pipelines.xmltopyarrow.settings import (
    PIPELINE_NAME,
    UNIPROT_PIPELINE_NAME,
    UNIREF_FIFTY,
    UNIREF_HUNDRED,
    UNIREF_NINETY,
    UniProtXmlToPyArrowSettings,
    UniRef50XmlToPyArrowSettings,
    UniRef90XmlToPyArrowSettings,
    UniRef100XmlToPyArrowSettings,
    XmlToPyArrowSettings,
)
from tests.cdm_data_loaders.pipelines.conftest import copy_test_files
from tests.cdm_data_loaders.pipelines.helpers import (
    assert_binds_filesystem_reader,
    assert_core_run_pipeline_args,
    fetch_all_duckdb,
    run_with_duckdb_pipeline,
)
from tests.fs_backends import FsBackend
from tests.helpers import register_temp_module, settings_to_argv
from tests.xml_samples import TWO_BOOK_LIBRARY_XML


@pytest.mark.parametrize("loader_file_format", list(LoaderFileFormatEnum), ids=str)
def test_run_xmltopyarrow_pipeline_pass_sets_core_run_pipeline_args_correctly(
    settings_factory: Callable[..., XmlToPyArrowSettings],
    loader_file_format: str,
) -> None:
    """run_xmltopyarrow_pipeline builds the reader and delegates to run_pipeline with the correct args."""
    settings = settings_factory(loader_file_format=loader_file_format)
    assert_core_run_pipeline_args(xmltopyarrow_module, run_xmltopyarrow_pipeline, settings, PIPELINE_NAME)


def test_run_xmltopyarrow_pipeline_pass_binds_reader_before_run_pipeline(
    settings_factory: Callable[..., XmlToPyArrowSettings],
) -> None:
    """The resource passed to run_pipeline is the bound reader piped into the filesystem source."""
    settings = settings_factory()
    assert_binds_filesystem_reader(xmltopyarrow_module, run_xmltopyarrow_pipeline, settings, "xmltopyarrow_reader")


def test_cli_pass_runs_end_to_end_from_command_line_arguments(
    tmp_path: Path,
    settings_factory: Callable[..., Any],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """cli() reads command-line arguments and runs the pipeline via run_xmltopyarrow_pipeline.

    The real XmlToPyArrowSettings parses argv (seeded by the autouse dlt
    config isolation fixture), and core.run_pipeline is redirected to a real
    DuckDB pipeline so the full flow is validated.
    """
    settings = settings_factory(dataset_name="cli_dataset", dlt_dev_mode=False)
    (Path(settings.input_dir) / "library.xml").write_text(TWO_BOOK_LIBRARY_XML, encoding="utf-8")
    monkeypatch.setattr(sys, "argv", [PIPELINE_NAME, *settings_to_argv(settings)])

    load_info = run_with_duckdb_pipeline(
        tmp_path,
        xmltopyarrow_module,
        cli.run,
        expected_pipeline_name=PIPELINE_NAME,
        expected_dataset_name="cli_dataset",
        pipeline_name_prefix="test_xmltopyarrow_cli_pipeline",
    )

    assert fetch_all_duckdb(load_info.pipeline, "SELECT COUNT(*) FROM book") == [(2,)]


@pytest.mark.parametrize("missing_field", ["dataset_name", "table_name", "xml_tag", "xsd_file"])
def test_cli_fail_missing_required_args_raise(
    settings_factory: Callable[..., Any],
    monkeypatch: pytest.MonkeyPatch,
    missing_field: str,
) -> None:
    """cli() without required pipeline-specific args raises before running the pipeline."""
    settings = settings_factory()
    monkeypatch.setattr(sys, "argv", [PIPELINE_NAME, *settings_to_argv(settings, exclude=[missing_field])])

    with (
        patch.object(xmltopyarrow_module, "run_pipeline") as mock_run_pipeline,
        pytest.raises(Exception, match=missing_field),
    ):
        cli()

    mock_run_pipeline.assert_not_called()


@pytest.mark.parametrize(
    ("settings_cls", "expected_pipeline_name"),
    [
        (UniProtXmlToPyArrowSettings, UNIPROT_PIPELINE_NAME),
        (UniRef50XmlToPyArrowSettings, UNIREF_FIFTY),
        (UniRef90XmlToPyArrowSettings, UNIREF_NINETY),
        (UniRef100XmlToPyArrowSettings, UNIREF_HUNDRED),
    ],
)
def test_run_subclass_pipeline_pass_sets_core_run_pipeline_args_correctly(
    settings_factory: Callable[..., Any],
    *,
    settings_cls: type[XmlToPyArrowSettings],
    expected_pipeline_name: str,
) -> None:
    """run_xmltopyarrow_pipeline delegates to run_pipeline with the subclass pipeline_name."""
    settings = settings_factory(settings_cls)

    with patch.object(xmltopyarrow_module, "run_pipeline") as mock_run_pipeline:
        run_xmltopyarrow_pipeline(settings)

    mock_run_pipeline.assert_called_once()
    _, kwargs = mock_run_pipeline.call_args
    assert kwargs["settings"] == settings
    assert kwargs["pipeline_kwargs"] == {
        "pipeline_name": expected_pipeline_name,
        "dataset_name": expected_pipeline_name,
    }
    assert isinstance(kwargs["resource"], DltResource)


@pytest.mark.parametrize(
    ("prog_name", "cli_obj", "sample_xml_rel_dir"),
    [
        (UNIPROT_PIPELINE_NAME, uniprot_cli, "xsd/examples/uniprot"),
        (UNIREF_FIFTY, uniref50_cli, "xsd/examples/uniref"),
        (UNIREF_NINETY, uniref90_cli, "xsd/examples/uniref"),
        (UNIREF_HUNDRED, uniref100_cli, "xsd/examples/uniref"),
    ],
)
def test_subclass_cli_pass_runs_end_to_end_with_default_settings(
    tmp_path: Path,
    test_data_dir: Path,
    settings_factory: Callable[..., Any],
    monkeypatch: pytest.MonkeyPatch,
    *,
    prog_name: str,
    cli_obj: Any,  # noqa: ANN401
    sample_xml_rel_dir: str,
) -> None:
    """UniProt and UniRef CLIs load entry rows end-to-end using their default settings."""
    base_settings = settings_factory(CtsSettings, dlt_dev_mode=False)
    copy_test_files(test_data_dir / sample_xml_rel_dir, ["minimal.xml.gz"], Path(base_settings.input_dir))
    monkeypatch.setattr(sys, "argv", [prog_name, *settings_to_argv(base_settings)])

    load_info = run_with_duckdb_pipeline(
        tmp_path,
        xmltopyarrow_module,
        cli_obj.run,
        expected_pipeline_name=prog_name,
        expected_dataset_name=prog_name,
        pipeline_name_prefix=f"test_{prog_name}_cli_pipeline",
    )

    assert fetch_all_duckdb(load_info.pipeline, "SELECT COUNT(*) FROM entry") == [(1,)]


@pytest.mark.parametrize("relative_path", ["library.xml", "sub/library.xml.gz"])
def test_run_xmltopyarrow_pipeline_pass_per_file_reader_reads_file_items_any_backend(
    settings_factory: Callable[..., Any], fs_backend: FsBackend, relative_path: str
) -> None:
    """The pipeline's per-file reader reads file items from any backend, without assuming they are local files."""
    data = TWO_BOOK_LIBRARY_XML.encode("utf-8")
    fs_backend.put(relative_path, gzip.compress(data) if relative_path.endswith(".gz") else data)
    settings = settings_factory()

    with (
        patch.object(xmltopyarrow_module, "build_per_file_filesystem_resource") as mock_build,
        patch.object(xmltopyarrow_module, "run_pipeline"),
    ):
        run_xmltopyarrow_pipeline(settings)
    file_fn = mock_build.call_args.args[1]

    items = [item for file_item in fs_backend.file_items() for item in file_fn(file_item, settings)]

    assert [(item.meta.table_name, item.data.column("_id").to_pylist()) for item in items] == [("book", ["1", "2"])]


def test_run_xmltopyarrow_pipeline_pass_filters_and_splits_with_transforms_module(
    settings_factory: Callable[..., Any],
    fs_backend: FsBackend,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The xmltopyarrow pipeline filters and splits elements into Arrow tables using TABLE_TRANSFORMS."""
    fs_backend.put("library.xml", TWO_BOOK_LIBRARY_XML.encode("utf-8"))
    transform = TableTransform(
        transform_fn=lambda row: (
            {}
            if row["_id"] == "1"
            else {
                "book": [{"_id": row["_id"], "title": row["title"]}],
                "book_id_only": [{"book_id": row["_id"]}],
            }
        ),
        table_to_schema_map={
            "book": pa.schema([pa.field("_id", pa.string()), pa.field("title", pa.string())]),
            "book_id_only": pa.schema([pa.field("book_id", pa.string())]),
        },
    )
    transforms_mod = register_temp_module(
        monkeypatch,
        "_test_xmltopyarrow_transforms",
        TABLE_TRANSFORMS={"book": transform},
    )
    settings = settings_factory(transforms_module=transforms_mod)

    with (
        patch.object(xmltopyarrow_module, "build_per_file_filesystem_resource") as mock_build,
        patch.object(xmltopyarrow_module, "run_pipeline"),
    ):
        run_xmltopyarrow_pipeline(settings)
    file_fn = mock_build.call_args.args[1]

    items = [item for file_item in fs_backend.file_items() for item in file_fn(file_item, settings)]
    assert [(item.meta.table_name, item.data.to_pylist()) for item in items] == [
        ("book", [{"_id": "2", "title": "The Stand"}]),
        ("book_id_only", [{"book_id": "2"}]),
    ]
