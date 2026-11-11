"""Tests for _process_xsv_file, run_xsv_pipeline, and cli wiring.

These tests drive the real qsv binary (see the `qsv_cmd` fixture in conftest.py); tests are
skipped when qsv is not available on PATH or via the QSV_BIN env var.
"""

import sys
import threading
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest
from dlt.sources.filesystem import filesystem

import cdm_data_loaders.pipelines.xsv.pipeline as xsv_pipeline_module
from cdm_data_loaders.core.fields import REJECTED_TABLE_NAME
from cdm_data_loaders.pipelines.core import PipelineCli
from cdm_data_loaders.pipelines.xsv.pipeline import (
    XsvWorkPaths,
    _process_xsv_file,
    cli,
    run_xsv_pipeline,
)
from cdm_data_loaders.pipelines.xsv.settings import PIPELINE_NAME, XsvIngestSettings
from cdm_data_loaders.readers.jsonschema_xsv.xsv_validator.helpers import CleanerValidatorArgs
from cdm_data_loaders.readers.jsonschema_xsv.xsv_validator.qsv import clean_validate_file
from cdm_data_loaders.utils.buffer import REJECTED_PYARROW_SCHEMA
from tests.cdm_data_loaders.pipelines.helpers import (
    fetch_all_duckdb,
    make_file_item,
    run_with_duckdb_pipeline,
)
from tests.cdm_data_loaders.pipelines.xsv.conftest import VALID_SCHEMA_URI
from tests.fs_backends import FsBackend
from tests.helpers import register_temp_module, settings_to_argv

SIMPLE_SCHEMA: dict[str, Any] = {
    "$schema": VALID_SCHEMA_URI,
    "required": ["number", "name"],
    "properties": {"number": {"type": "integer"}, "name": {"type": "string"}},
    "x-xsv-config": {"x-delimiter": ",", "x-has-header": True},
}


"""_process_xsv_file"""


def test_process_xsv_file_pass_routes_valid_rows_to_table(
    settings_factory: Callable[..., XsvIngestSettings],
    work_paths_factory: Callable[[XsvIngestSettings], XsvWorkPaths],
) -> None:
    """A clean, valid file is loaded, with every row cast to its declared schema types."""
    settings = settings_factory(SIMPLE_SCHEMA, {"data.csv": "number,name\n1,alice\n2,bob\n"})
    work_paths = work_paths_factory(settings)

    results = list(_process_xsv_file(make_file_item(Path(settings.input_dir) / "data.csv"), settings, work_paths))

    assert len(results) == 1
    assert results[0].meta.table_name == "my_table"
    assert isinstance(results[0].data, pa.Table)
    assert results[0].data.to_pylist() == [
        {"number": 1, "name": "alice"},
        {"number": 2, "name": "bob"},
    ]


def test_process_xsv_file_pass_replaces_configured_null_placeholder(
    settings_factory: Callable[..., XsvIngestSettings],
    work_paths_factory: Callable[[XsvIngestSettings], XsvWorkPaths],
) -> None:
    """A configured null_regex placeholder is replaced with None in the loaded rows."""
    schema = {**SIMPLE_SCHEMA, "properties": {"number": {"type": "integer"}, "name": {"type": ["string", "null"]}}}
    schema["x-xsv-config"] = {**schema["x-xsv-config"], "x-null-regex": "^NA$"}
    settings = settings_factory(schema, {"data.csv": "number,name\n1,NA\n2,bob\n"})
    work_paths = work_paths_factory(settings)

    results = list(_process_xsv_file(make_file_item(Path(settings.input_dir) / "data.csv"), settings, work_paths))

    assert isinstance(results[0].data, pa.Table)
    assert results[0].data.to_pylist() == [{"number": 1, "name": None}, {"number": 2, "name": "bob"}]


def test_process_xsv_file_pass_routes_total_failure_to_rejected_table(
    settings_factory: Callable[..., XsvIngestSettings],
    work_paths_factory: Callable[[XsvIngestSettings], XsvWorkPaths],
) -> None:
    """A file where every row is ragged (wrong column count) yields no valid rows and one rejected row."""
    settings = settings_factory(SIMPLE_SCHEMA, {"bad.csv": "number,name\n1\n2,bob,extra\n"})
    work_paths = work_paths_factory(settings)

    results = list(_process_xsv_file(make_file_item(Path(settings.input_dir) / "bad.csv"), settings, work_paths))

    table_names = [result.meta.table_name for result in results]
    assert table_names == [REJECTED_TABLE_NAME]
    assert isinstance(results[0].data, pa.Table)
    assert results[0].data.schema == REJECTED_PYARROW_SCHEMA
    (rejected,) = results[0].data.to_pylist()
    assert rejected["table_name"] == "my_table"
    assert rejected["source_file"] == "bad.csv"
    assert rejected["line_no"] is None
    assert rejected["raw_record"] is None
    assert rejected["error_detail"]


@pytest.mark.parametrize("relative_path", ["data.csv", "sub/dir/data.csv"])
def test_process_xsv_file_pass_file_item_any_backend(
    settings_factory: Callable[..., XsvIngestSettings],
    work_paths_factory: Callable[[XsvIngestSettings], XsvWorkPaths],
    fs_backend: FsBackend,
    relative_path: str,
) -> None:
    """A file item on any backend is validated and loaded; remote files are downloaded, then deleted."""
    settings = settings_factory(SIMPLE_SCHEMA, {})
    work_paths = work_paths_factory(settings)
    fs_backend.put(relative_path, b"number,name\n1,alice\n2,bob\n")

    results = list(_process_xsv_file(fs_backend.file_item(relative_path), settings, work_paths))

    assert len(results) == 1
    assert results[0].meta.table_name == "my_table"
    assert isinstance(results[0].data, pa.Table)
    assert results[0].data.to_pylist() == [
        {"number": 1, "name": "alice"},
        {"number": 2, "name": "bob"},
    ]
    assert [path for path in work_paths.download_dir.rglob("*") if path.is_file()] == []
    assert fs_backend.fs.exists(fs_backend.path(relative_path))


def test_process_xsv_file_pass_rejected_file_is_relative_path(
    settings_factory: Callable[..., XsvIngestSettings],
    work_paths_factory: Callable[[XsvIngestSettings], XsvWorkPaths],
    fs_backend: FsBackend,
) -> None:
    """Rejected rows identify the file by its path relative to the input location, on every backend."""
    settings = settings_factory(SIMPLE_SCHEMA, {})
    work_paths = work_paths_factory(settings)
    fs_backend.put("sub/bad.csv", b"number,name\n1\n2,bob,extra\n")

    results = list(_process_xsv_file(fs_backend.file_item("sub/bad.csv"), settings, work_paths))

    assert [result.meta.table_name for result in results] == [REJECTED_TABLE_NAME]
    assert isinstance(results[0].data, pa.Table)
    assert [(row["table_name"], row["source_file"]) for row in results[0].data.to_pylist()] == [
        ("my_table", "sub/bad.csv")
    ]


def test_process_xsv_file_fail_remote_object_missing(
    settings_factory: Callable[..., XsvIngestSettings],
    work_paths_factory: Callable[[XsvIngestSettings], XsvWorkPaths],
    remote_fs_backend: FsBackend,
) -> None:
    """A remote file deleted after listing raises FileNotFoundError when it is downloaded."""
    settings = settings_factory(SIMPLE_SCHEMA, {})
    work_paths = work_paths_factory(settings)
    remote_fs_backend.put("gone.csv", b"number,name\n1,alice\n")
    item = remote_fs_backend.file_item("gone.csv")
    remote_fs_backend.fs.rm(remote_fs_backend.path("gone.csv"))

    with pytest.raises(FileNotFoundError):
        list(_process_xsv_file(item, settings, work_paths))


def test_process_xsv_file_pass_same_stem_files_do_not_lose_rows_or_rejections(
    settings_factory: Callable[..., XsvIngestSettings],
    work_paths_factory: Callable[[XsvIngestSettings], XsvWorkPaths],
) -> None:
    """Files with the same stem in different subdirectories do not reuse stale scratch files or lose rows."""
    settings = settings_factory(
        SIMPLE_SCHEMA,
        {
            "batch_1/data.csv": "number,name\n1,alice\n2,bob\n",
            "batch_2/data.csv": "number,name\n3,carol\n4,dave\n5\n",
        },
    )
    work_paths = work_paths_factory(settings)
    items = {
        item["relative_path"]: item for item in filesystem(bucket_url=str(settings.input_dir), file_glob="*/*.csv")
    }

    results_1 = list(_process_xsv_file(items["batch_1/data.csv"], settings, work_paths))
    results_2 = list(_process_xsv_file(items["batch_2/data.csv"], settings, work_paths))

    assert [(result.meta.table_name, result.data.to_pylist()) for result in results_1] == [
        ("my_table", [{"number": 1, "name": "alice"}, {"number": 2, "name": "bob"}]),
    ]
    assert [
        (result.meta.table_name, result.data.to_pylist())
        for result in results_2
        if result.meta.table_name == "my_table"
    ] == [
        ("my_table", [{"number": 3, "name": "carol"}, {"number": 4, "name": "dave"}]),
    ]
    rejected_batches = [result.data for result in results_2 if result.meta.table_name == REJECTED_TABLE_NAME]
    assert len(rejected_batches) == 1
    assert isinstance(rejected_batches[0], pa.Table)
    assert [(row["table_name"], row["source_file"]) for row in rejected_batches[0].to_pylist()] == [
        ("my_table", "batch_2/data.csv"),
    ]


def test_process_xsv_file_pass_concurrent_same_stem_files_do_not_overwrite_validated_output(
    settings_factory: Callable[..., XsvIngestSettings],
    work_paths_factory: Callable[[XsvIngestSettings], XsvWorkPaths],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Concurrent _process_xsv_file calls for same-stem files do not overwrite each other's validated output."""
    settings = settings_factory(
        SIMPLE_SCHEMA,
        {
            "batch_1/data.csv": "number,name\n1,alice\n2,bob\n",
            "batch_2/data.csv": "number,name\n3,carol\n4,dave\n",
        },
    )
    work_paths = work_paths_factory(settings)
    items = {
        item["relative_path"]: item for item in filesystem(bucket_url=str(settings.input_dir), file_glob="*/*.csv")
    }
    item_1 = items["batch_1/data.csv"]
    item_2 = items["batch_2/data.csv"]

    barrier = threading.Barrier(2, timeout=10)
    clean_lock = threading.Lock()

    def _synced_clean_validate_file(args: CleanerValidatorArgs) -> str | None:
        with clean_lock:
            validated_name = clean_validate_file(args)
        barrier.wait()
        return validated_name

    monkeypatch.setattr(xsv_pipeline_module, "clean_validate_file", _synced_clean_validate_file)

    with ThreadPoolExecutor(max_workers=2) as pool:
        fut_1 = pool.submit(lambda: list(_process_xsv_file(item_1, settings, work_paths)))
        fut_2 = pool.submit(lambda: list(_process_xsv_file(item_2, settings, work_paths)))
        results_1 = fut_1.result()
        results_2 = fut_2.result()

    rows = [
        (row["number"], row["name"])
        for result in [*results_1, *results_2]
        if result.meta.table_name == "my_table"
        for row in result.data.to_pylist()
    ]
    assert sorted(rows) == [(1, "alice"), (2, "bob"), (3, "carol"), (4, "dave")]


"""run_xsv_pipeline"""


@pytest.mark.usefixtures("qsv_cmd")
def test_run_xsv_pipeline_pass_end_to_end_via_duckdb(
    settings_factory: Callable[..., XsvIngestSettings],
    tmp_path: Path,
) -> None:
    """The pipeline discovers data files (excluding the schema file), skipping non-matching files.

    Runs against a real DuckDB destination via a patched run_pipeline.
    """
    settings = settings_factory(
        SIMPLE_SCHEMA,
        {
            "part_1.csv": "number,name\n1,alice\n",
            "part_2.csv": "number,name\n2,bob\n",
        },
    )

    load_info = run_with_duckdb_pipeline(
        tmp_path,
        xsv_pipeline_module,
        lambda: run_xsv_pipeline(settings),
        expected_pipeline_name=PIPELINE_NAME,
        expected_dataset_name="xsv_test_dataset",
        pipeline_name_prefix="test_xsv_pipeline",
    )

    rows = fetch_all_duckdb(load_info.pipeline, "SELECT number, name FROM my_table ORDER BY number")
    assert [tuple(row) for row in rows] == [(1, "alice"), (2, "bob")]


@pytest.mark.usefixtures("qsv_cmd")
def test_run_xsv_pipeline_pass_same_stem_in_subdirectories_does_not_lose_data(
    settings_factory: Callable[..., XsvIngestSettings],
    tmp_path: Path,
) -> None:
    """Files with the same stem in different subdirectories are all loaded without losing rows or rejections."""
    settings = settings_factory(
        SIMPLE_SCHEMA,
        {
            "batch_1/data.csv": "number,name\n1,alice\n2,bob\n",
            "batch_2/data.csv": "number,name\n3,carol\n4,dave\n5\n",
            "batch_3/data.csv": "number,name\n6,eve\n7,frank\n",
        },
        file_glob="*/*.csv",
    )

    load_info = run_with_duckdb_pipeline(
        tmp_path,
        xsv_pipeline_module,
        lambda: run_xsv_pipeline(settings),
        expected_pipeline_name=PIPELINE_NAME,
        expected_dataset_name="xsv_test_dataset",
        pipeline_name_prefix="test_xsv_pipeline_same_stem",
    )

    rows = fetch_all_duckdb(load_info.pipeline, "SELECT number, name FROM my_table ORDER BY number, name")
    assert [tuple(row) for row in rows] == [
        (1, "alice"),
        (2, "bob"),
        (3, "carol"),
        (4, "dave"),
        (6, "eve"),
        (7, "frank"),
    ]
    rejected_files = fetch_all_duckdb(
        load_info.pipeline,
        "SELECT table_name, source_file FROM _rejected",
    )
    assert [tuple(row) for row in rejected_files] == [("my_table", "batch_2/data.csv")]


@pytest.mark.usefixtures("qsv_cmd")
def test_run_xsv_pipeline_pass_no_matching_files_loads_nothing(
    settings_factory: Callable[..., XsvIngestSettings],
    tmp_path: Path,
) -> None:
    """When no data files are present besides the schema, no rows are extracted."""
    settings = settings_factory(SIMPLE_SCHEMA, {}, file_glob="*.csv")

    load_info = run_with_duckdb_pipeline(
        tmp_path,
        xsv_pipeline_module,
        lambda: run_xsv_pipeline(settings),
        expected_pipeline_name=PIPELINE_NAME,
        expected_dataset_name="xsv_test_dataset",
        pipeline_name_prefix="test_xsv_pipeline_empty",
    )
    assert "my_table" not in load_info.pipeline.default_schema.tables


"""cli"""


def test_cli_pass_calls_run_cli_with_settings_class_and_pipeline_fn() -> None:
    """Cli is a PipelineCli wired to XsvIngestSettings and run_xsv_pipeline."""
    assert isinstance(cli, PipelineCli)
    assert cli.settings_cls is XsvIngestSettings
    assert cli.pipeline_fn is run_xsv_pipeline


@pytest.mark.usefixtures("qsv_cmd")
def test_cli_pass_runs_end_to_end_from_command_line_arguments(
    settings_factory: Callable[..., XsvIngestSettings],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """cli() reads command-line arguments and loads rows via a real DuckDB pipeline."""
    settings = settings_factory(
        SIMPLE_SCHEMA,
        {"data.csv": "number,name\n1,alice\n2,bob\n"},
        dataset_name="cli_dataset",
    )
    monkeypatch.setattr(sys, "argv", ["xsv", *settings_to_argv(settings)])

    load_info = run_with_duckdb_pipeline(
        tmp_path,
        xsv_pipeline_module,
        cli.run,
        expected_pipeline_name=PIPELINE_NAME,
        expected_dataset_name="cli_dataset",
        pipeline_name_prefix="test_xsv_cli_pipeline",
    )

    ((row_count,),) = fetch_all_duckdb(load_info.pipeline, "SELECT COUNT(*) FROM my_table")
    assert row_count == 2  # noqa: PLR2004


def test_process_xsv_file_pass_filters_and_splits_rows_with_transforms_module(
    settings_factory: Callable[..., XsvIngestSettings],
    work_paths_factory: Callable[[XsvIngestSettings], XsvWorkPaths],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """_process_xsv_file filters and splits validated CSV rows when transforms_module is configured."""
    transform = lambda row: (  # noqa: E731
        {}
        if row["number"] == 1
        else {
            "my_table": [{"number": row["number"], "name": row["name"].upper()}],
            "my_table_names": [{"name": row["name"]}],
        }
    )
    transforms_mod = register_temp_module(
        monkeypatch,
        "_test_xsv_transforms",
        TABLE_TRANSFORMS={"my_table": transform},
    )
    settings = settings_factory(
        SIMPLE_SCHEMA,
        {"data.csv": "number,name\n1,alice\n2,bob\n"},
        transforms_module=transforms_mod,
    )
    work_paths = work_paths_factory(settings)

    results = list(_process_xsv_file(make_file_item(Path(settings.input_dir) / "data.csv"), settings, work_paths))

    assert [(result.meta.table_name, result.data) for result in results] == [
        ("my_table", [{"number": 2, "name": "BOB"}]),
        ("my_table_names", [{"name": "bob"}]),
    ]


@pytest.mark.usefixtures("qsv_cmd")
def test_run_xsv_pipeline_pass_rejected_table_retains_null_columns(
    settings_factory: Callable[..., XsvIngestSettings],
    tmp_path: Path,
) -> None:
    """_rejected retains line_no and raw_record columns even when all rejected records have None for those fields."""
    settings = settings_factory(
        SIMPLE_SCHEMA,
        {"bad.csv": "number,name\n1\n"},
        file_glob="*.csv",
    )

    load_info = run_with_duckdb_pipeline(
        tmp_path,
        xsv_pipeline_module,
        lambda: run_xsv_pipeline(settings),
        expected_pipeline_name=PIPELINE_NAME,
        expected_dataset_name="xsv_test_dataset",
        pipeline_name_prefix="test_xsv_rejected_columns",
    )

    rejected_rows = fetch_all_duckdb(
        load_info.pipeline,
        "SELECT table_name, source_file, line_no, raw_record, error_detail FROM _rejected",
    )
    assert len(rejected_rows) == 1
    table_name, source_file, line_no, raw_record, error_detail = rejected_rows[0]
    assert table_name == "my_table"
    assert source_file == "bad.csv"
    assert line_no is None
    assert raw_record is None
    assert error_detail


def test_process_xsv_file_pass_jsonl_format_yields_lists_of_dicts(
    settings_factory: Callable[..., XsvIngestSettings],
    work_paths_factory: Callable[[XsvIngestSettings], XsvWorkPaths],
) -> None:
    """When loader_file_format is jsonl, _process_xsv_file yields list[dict] for both valid and _rejected pages."""
    settings = settings_factory(
        SIMPLE_SCHEMA,
        {"mixed.csv": "number,name\n1,alice\n2,bob\n3\n"},
        loader_file_format="jsonl",
    )
    work_paths = work_paths_factory(settings)

    results = list(_process_xsv_file(make_file_item(Path(settings.input_dir) / "mixed.csv"), settings, work_paths))
    by_table = {result.meta.table_name: result.data for result in results}

    assert set(by_table) == {"my_table", REJECTED_TABLE_NAME}
    assert isinstance(by_table["my_table"], list)
    assert by_table["my_table"] == [{"number": 1, "name": "alice"}, {"number": 2, "name": "bob"}]
    assert isinstance(by_table[REJECTED_TABLE_NAME], list)
    assert len(by_table[REJECTED_TABLE_NAME]) == 1
    assert by_table[REJECTED_TABLE_NAME][0]["table_name"] == "my_table"
    assert by_table[REJECTED_TABLE_NAME][0]["source_file"] == "mixed.csv"
