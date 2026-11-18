"""Unit tests for `cdm_data_loaders.core.filesystem`."""

import logging
from collections.abc import Iterator
from pathlib import Path
from typing import Any, Final

import dlt
import pyarrow as pa
import pytest
from dlt.common.storages.fsspec_filesystem import FileItemDict
from dlt.common.typing import TDataItems
from dlt.sources.filesystem import filesystem
from pydantic_settings import CliApp

from cdm_data_loaders.core.buffer import REJECTED_PYARROW_SCHEMA
from cdm_data_loaders.core.filesystem import (
    FilesystemTracker,
    attach_file_collector,
    build_file_ingest_kwargs,
    build_filesystem_resource,
    build_per_file_filesystem_resource,
    collect_file_listing,
    find_filesystem_resources,
    list_input_dir_files,
    make_per_file_reader,
    resolve_pipeline_name,
)
from cdm_data_loaders.core.preprocessors import FilePreprocessError
from cdm_data_loaders.core.settings import (
    FileFormatIngestSettings,
    InputLocalOutputSettings,
)
from cdm_data_loaders.pipelines.jsonlines.settings import JsonlExtractSettings
from cdm_data_loaders.pipelines.runner import DEFAULT_SCHEMA_CONTRACT
from cdm_data_loaders.pipelines.xml_common import XmlIngestSettings
from tests.helpers.cli_assertions import make_cts_settings, register_temp_module
from tests.helpers.dlt_isolation import isolated_dlt_config
from tests.helpers.fs_backends import MOTO_AWS_CREDENTIALS, S3_MOTO, FsBackend

TINY_PIPELINE_NAME: Final[str] = "test_fs_tiny_pipeline"


@pytest.mark.parametrize(
    ("item", "expected_ingested"),
    [
        pytest.param({"relative_path": "nested/sample.xml", "extra": 1}, ["nested/sample.xml"], id="dict_with_relpath"),
        pytest.param({"file_url": "file:///tmp/sample.xml"}, [], id="dict_without_relpath"),
        pytest.param("plain_string", [], id="non_dict_string"),
        pytest.param(None, [], id="none_item"),
    ],
)
def test_filesystem_tracker_record_file(item: Any, expected_ingested: list[str]) -> None:  # noqa: ANN401
    """FilesystemTracker.record_file records relative_path from dicts and returns item unchanged."""
    tracker = FilesystemTracker(ingested_files=[], preprocess_errors=[])
    returned = tracker.record_file(item)
    assert returned is item
    assert tracker.ingested_files == expected_ingested


def _fs_backend_dlt_secrets(
    fs_backend: FsBackend,
    request: pytest.FixtureRequest,
    monkeypatch: pytest.MonkeyPatch,
) -> dict[str, Any]:
    """Return dlt secrets mapping required for ``fs_backend`` in isolated dlt config."""
    if fs_backend.name != S3_MOTO:
        return {}
    endpoint_url = request.getfixturevalue("s3_moto_endpoint")
    monkeypatch.delenv("AWS_ENDPOINT_URL", raising=False)
    monkeypatch.delenv("AWS_ACCESS_KEY_ID", raising=False)
    monkeypatch.delenv("AWS_SECRET_ACCESS_KEY", raising=False)
    return {
        "sources": {
            "filesystem": {
                "credentials": {
                    **MOTO_AWS_CREDENTIALS,
                    "endpoint_url": endpoint_url,
                }
            }
        }
    }


@pytest.mark.parametrize(
    ("scenario", "expected_paths"),
    [
        pytest.param("empty", [], id="empty"),
        pytest.param("non_fs", [], id="non_fs"),
        pytest.param("single_fs", [["x.xml"]], id="single_fs"),
        pytest.param("mixed_shared_roots", [["x.xml"], ["y.txt"]], id="mixed_shared_roots"),
    ],
)
def test_find_filesystem_resources_pass_deduplicates_shared_roots(
    tmp_path: Path,
    scenario: str,
    expected_paths: list[list[str]],
) -> None:
    """find_filesystem_resources ignores non-filesystem resources and deduplicates shared roots in order."""
    dir_a = tmp_path / "dir_a"
    dir_b = tmp_path / "dir_b"
    dir_a.mkdir()
    dir_b.mkdir()
    (dir_a / "x.xml").write_text("<x/>", encoding="utf-8")
    (dir_b / "y.txt").write_text("y", encoding="utf-8")

    plain = dlt.resource([{"id": 1}], name="plain")
    fs_a = filesystem(bucket_url=str(dir_a), file_glob="*.xml")
    fs_b = filesystem(bucket_url=str(dir_b), file_glob="*.txt")

    @dlt.transformer(name="trans_a1")
    def trans_a1(items: TDataItems) -> Iterator[TDataItems]:
        yield from items

    @dlt.transformer(name="trans_a2")
    def trans_a2(items: TDataItems) -> Iterator[TDataItems]:
        yield from items

    if scenario == "empty":
        found = find_filesystem_resources([])
    elif scenario == "non_fs":
        found = find_filesystem_resources(plain)
    elif scenario == "single_fs":
        found = find_filesystem_resources(fs_a)
    else:
        found = find_filesystem_resources([plain, fs_a | trans_a1, fs_a | trans_a2, fs_b])

    assert [[item["relative_path"] for item in res] for res in found] == expected_paths


@pytest.mark.parametrize(
    ("target_kind", "expected"),
    [
        pytest.param("local_path", ["nested/a.xml", "nested/deep/b.json", "z.txt"], id="local_path"),
        pytest.param("file_url", ["nested/a.xml", "nested/deep/b.json", "z.txt"], id="file_url"),
        pytest.param("missing_dir", [], id="missing_dir"),
    ],
)
def test_list_input_dir_files_pass_local_path_file_url_and_missing_dir(
    tmp_path: Path,
    target_kind: str,
    expected: list[str],
) -> None:
    """list_input_dir_files handles local paths, file:// URLs, nested files, empty dirs, and missing dirs."""
    root = tmp_path / "root"
    (root / "nested" / "deep").mkdir(parents=True)
    (root / "empty_subdir").mkdir()
    (root / "z.txt").write_text("z", encoding="utf-8")
    (root / "nested" / "a.xml").write_text("<a/>", encoding="utf-8")
    (root / "nested" / "deep" / "b.json").write_text("{}", encoding="utf-8")

    fs_res = filesystem(bucket_url=str(root))
    if target_kind == "local_path":
        input_dir = str(root)
    elif target_kind == "file_url":
        input_dir = root.as_uri()
    else:
        input_dir = str(tmp_path / "does_not_exist")

    assert list_input_dir_files(fs_res, input_dir) == expected


@pytest.mark.parametrize(
    ("use_missing_prefix", "expected"),
    [
        pytest.param(False, ["sub/a.xml", "sub/deep/b.json", "z.txt"], id="populated_bucket"),
        pytest.param(True, [], id="missing_prefix"),
    ],
)
def test_list_input_dir_files_pass_across_fs_backends(
    fs_backend: FsBackend,
    request: pytest.FixtureRequest,
    monkeypatch: pytest.MonkeyPatch,
    use_missing_prefix: bool,
    expected: list[str],
) -> None:
    """list_input_dir_files returns sorted POSIX relative paths and empty lists for missing prefixes across backends."""
    fs_backend.put("z.txt", b"zeta")
    fs_backend.put("sub/a.xml", b"alpha")
    fs_backend.put("sub/deep/b.json", b"{}")
    dlt_secrets = _fs_backend_dlt_secrets(fs_backend, request, monkeypatch)

    with isolated_dlt_config(secrets=dlt_secrets):
        fs_res = filesystem(bucket_url=fs_backend.bucket_url)
        target_url = (
            f"{fs_backend.bucket_url.rstrip('/')}/missing_prefix" if use_missing_prefix else fs_backend.bucket_url
        )
        assert list_input_dir_files(fs_res, target_url) == expected


@pytest.mark.parametrize(
    ("mode", "expected_matched"),
    [
        pytest.param("single_resource", ["sub/a.xml.gz", "z.xml"], id="single_resource"),
        pytest.param("piped_multi_root_list", ["sub/notes.txt", "z.xml"], id="piped_multi_root_list"),
    ],
)
def test_collect_file_listing_pass_and_skips_reader(
    tmp_path: Path,
    mode: str,
    expected_matched: list[str],
) -> None:
    """collect_file_listing returns sorted input_dir_files and matched_files without running downstream transformers."""
    settings = make_cts_settings(
        XmlIngestSettings,
        tmp_path,
        dataset_name="listing_ds",
        loader_file_format="file_listing",
        file_glob="**/*.xml*",
    )
    in_dir = Path(settings.input_dir)
    (in_dir / "sub").mkdir()
    (in_dir / "z.xml").write_text("<z/>", encoding="utf-8")
    (in_dir / "sub" / "a.xml.gz").write_text("<a/>", encoding="utf-8")
    (in_dir / "sub" / "notes.txt").write_text("note", encoding="utf-8")

    def fail_if_called(_items: TDataItems, _cfg: XmlIngestSettings) -> Iterator[dict[str, Any]]:
        msg = "downstream reader must not execute during collect_file_listing"
        raise AssertionError(msg)

    if mode == "single_resource":
        target_resource = build_filesystem_resource(settings, fail_if_called, name="xml_listing_reader")
        result = collect_file_listing(settings, target_resource)
    else:
        shared_xml = filesystem(bucket_url=str(in_dir), file_glob="*.xml")
        sub_txt = filesystem(bucket_url=str(in_dir), file_glob="sub/*.txt")

        @dlt.transformer(name="unused_one")
        def unused_one(items: TDataItems) -> Iterator[TDataItems]:
            yield from items

        @dlt.transformer(name="unused_two")
        def unused_two(items: TDataItems) -> Iterator[TDataItems]:
            yield from items

        result = collect_file_listing(settings, [shared_xml | unused_one, shared_xml | unused_two, sub_txt])

    assert result == (["sub/a.xml.gz", "sub/notes.txt", "z.xml"], expected_matched)


@pytest.mark.parametrize(
    "invalid_resource",
    [
        pytest.param([], id="empty_list"),
        pytest.param(dlt.resource([{"id": 1}], name="non_filesystem"), id="non_filesystem_resource"),
    ],
)
def test_collect_file_listing_fail_without_filesystem_resource(
    tmp_path: Path,
    invalid_resource: Any,  # noqa: ANN401
) -> None:
    """collect_file_listing raises ValueError when no root dlt filesystem resource is present."""
    settings = make_cts_settings(
        FileFormatIngestSettings,
        tmp_path,
        dataset_name="listing_ds",
        loader_file_format="file_listing",
    )
    with pytest.raises(
        ValueError,
        match="loader_file_format='file_listing' requires at least one dlt filesystem resource",
    ):
        collect_file_listing(settings, invalid_resource)


@pytest.mark.parametrize(
    "scenario",
    ["empty", "non_fs", "multiple_roots"],
    ids=["empty", "non_fs", "multiple_roots"],
)
def test_attach_file_collector_pass_empty_non_fs_and_multiple_roots(
    tmp_path: Path,
    scenario: str,
) -> None:
    """attach_file_collector ignores non-filesystem resources and deduplicates shared filesystem roots."""
    if scenario == "empty":
        assert attach_file_collector([]) == FilesystemTracker(ingested_files=[], preprocess_errors=[])
    elif scenario == "non_fs":
        non_fs = dlt.resource([{"relative_path": "ignored.txt"}], name="custom_source")
        collector = attach_file_collector(non_fs)
        assert list(non_fs) == [{"relative_path": "ignored.txt"}]
        assert collector.ingested_files == []
        assert collector.preprocess_errors == []
    else:
        dir_a = tmp_path / "dir_a"
        dir_b = tmp_path / "dir_b"
        dir_a.mkdir()
        dir_b.mkdir()
        (dir_a / "a.txt").write_text("alpha", encoding="utf-8")
        (dir_b / "b.txt").write_text("beta", encoding="utf-8")

        shared_fs = filesystem(bucket_url=str(dir_a), file_glob="*.txt")
        second_fs = filesystem(bucket_url=str(dir_b), file_glob="*.txt")

        @dlt.transformer(name="branch_one")
        def branch_one(items: TDataItems) -> Iterator[dict[str, Any]]:
            for item in items:
                yield {"path": item["relative_path"], "branch": 1}

        @dlt.transformer(name="branch_two")
        def branch_two(items: TDataItems) -> Iterator[dict[str, Any]]:
            for item in items:
                yield {"path": item["relative_path"], "branch": 2}

        @dlt.transformer(name="branch_three")
        def branch_three(items: TDataItems) -> Iterator[dict[str, Any]]:
            for item in items:
                yield {"path": item["relative_path"], "branch": 3}

        resources = [shared_fs | branch_one, shared_fs | branch_two, second_fs | branch_three]
        collector = attach_file_collector(resources)
        pipeline = dlt.pipeline(pipeline_name="test_attach_collector", pipelines_dir=str(tmp_path / "pipes"))
        pipeline.extract(resources)

        assert sorted(collector.ingested_files) == ["a.txt", "b.txt"]
        assert collector.preprocess_errors == []


def test_attach_file_collector_pass_rebinds_existing_tracker(input_dir: Path, output_dir: Path) -> None:
    """attach_file_collector rebinds build_filesystem_resource's existing tracker without double-counting."""
    (input_dir / "only.txt").write_text("hello", encoding="utf-8")
    settings = InputLocalOutputSettings(input_dir=input_dir, output_dir=output_dir)

    def read_none(_items: TDataItems, _cfg: InputLocalOutputSettings) -> None:
        return None

    resource = build_filesystem_resource(settings, read_none, name="noop_reader", parallelized=False)
    collector = attach_file_collector(resource)
    assert list(resource) == []
    assert collector.ingested_files == ["only.txt"]
    assert collector.preprocess_errors == []


def test_build_filesystem_resource_pass_default_glob_and_callable_transformer(
    input_dir: Path, output_dir: Path
) -> None:
    """build_filesystem_resource uses settings.file_glob and wraps a callable in dlt.transformer with settings bound."""
    file_path = input_dir / "sample.xml"
    file_path.write_text("hello", encoding="utf-8")
    settings = InputLocalOutputSettings(input_dir=input_dir, output_dir=output_dir)

    def read_items(
        items: TDataItems,
        cfg: InputLocalOutputSettings,
        prefix: str,
    ) -> Iterator[dict[str, Any]]:
        assert cfg is settings
        for item in items:
            text = Path(item["file_url"].removeprefix("file://")).read_text(encoding="utf-8")
            yield {"text": f"{prefix}:{text}"}

    resource = build_filesystem_resource(settings, read_items, "tag", name="sample_reader")
    rows = list(resource)
    assert rows == [{"text": "tag:hello"}]


def test_build_filesystem_resource_pass_custom_glob_resource_transformer_and_contract(
    input_dir: Path, output_dir: Path
) -> None:
    """build_filesystem_resource accepts a DltResource transformer, explicit file_glob, and schema_contract."""
    matched = input_dir / "data.jsonl"
    matched.write_text("matched", encoding="utf-8")
    ignored = input_dir / "ignore.txt"
    ignored.write_text("ignored", encoding="utf-8")
    settings = InputLocalOutputSettings(input_dir=input_dir, output_dir=output_dir)

    @dlt.transformer(name="jsonl_test_reader")
    def read_jsonl_items(
        items: TDataItems,
        cfg: InputLocalOutputSettings,
    ) -> Iterator[dict[str, Any]]:
        assert cfg is settings
        for item in items:
            yield {"name": Path(item["file_url"].removeprefix("file://")).name}

    resource = build_filesystem_resource(
        settings,
        read_jsonl_items,
        name="jsonl_test_reader",
        file_glob="*.jsonl",
        schema_contract=DEFAULT_SCHEMA_CONTRACT,
    )
    rows = list(resource)
    assert rows == [{"name": "data.jsonl"}]


def test_build_filesystem_resource_pass_cts_wildcard_tokens_on_settings_file_glob(
    input_dir: Path, output_dir: Path
) -> None:
    """build_filesystem_resource discovers files matching settings.file_glob expanded from CTS wildcard tokens."""
    (input_dir / "alpha.ext.xml").write_text("<a/>", encoding="utf-8")
    (input_dir / "beta.ext.gz").write_text("<b/>", encoding="utf-8")
    (input_dir / "gamma.xml").write_text("<c/>", encoding="utf-8")
    settings = CliApp.run(
        XmlIngestSettings,
        cli_args=[
            "--input-dir",
            str(input_dir),
            "--output-dir",
            str(output_dir),
            "--use-destination",
            "cts",
            "--dataset-name",
            "ext_ds",
            "--file-glob",
            "__STAR__.ext.__STAR__",
        ],
    )
    assert settings.file_glob == "*.ext.*"

    def read_names(
        items: TDataItems,
        cfg: XmlIngestSettings,
    ) -> Iterator[dict[str, str]]:
        assert cfg is settings
        for item in items:
            yield {"name": Path(item["file_url"].removeprefix("file://")).name}

    resource = build_filesystem_resource(settings, read_names, name="cts_glob_reader")
    names = sorted(row["name"] for row in resource)
    assert names == ["alpha.ext.xml", "beta.ext.gz"]


def _skip_or_fail_preprocess(file_item: FileItemDict) -> FileItemDict | None:
    """Filter skip.jsonl, raise FilePreprocessError on bad.jsonl, and pass other files through."""
    rel = file_item["relative_path"]
    if rel == "skip.jsonl":
        return None
    if rel == "bad.jsonl":
        msg = "corrupt header"
        raise FilePreprocessError(msg)
    return file_item


@pytest.mark.parametrize(
    ("loader_file_format", "expect_arrow"),
    [
        pytest.param("parquet", True, id="pyarrow_rejected_page"),
        pytest.param("jsonl", False, id="dict_rejected_page"),
    ],
)
@pytest.mark.parametrize(
    ("include_valid_file", "table_override", "expected_valid"),
    [
        pytest.param(False, None, [], id="all_skipped_or_rejected"),
        pytest.param(True, "custom_events", ["good.jsonl"], id="mixed_valid_and_rejected"),
    ],
)
def test_build_filesystem_resource_pass_preprocessors_filter_reject_and_skip_empty_batch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    include_valid_file: bool,
    table_override: str | None,
    expected_valid: list[str],
    loader_file_format: str,
    *,
    expect_arrow: bool,
) -> None:
    """Pre-processors filter None items, emit _rejected rows on errors, and skip reader_fn when no valid files remain."""
    second_calls: list[str] = []

    def second_preprocess(file_item: FileItemDict) -> FileItemDict:
        second_calls.append(file_item["relative_path"])
        return file_item

    pre_mod = register_temp_module(
        monkeypatch,
        "_test_fs_preprocessors",
        FILE_PREPROCESSORS=[_skip_or_fail_preprocess, second_preprocess],
    )
    settings = make_cts_settings(
        JsonlExtractSettings,
        tmp_path,
        dataset_name="fs_prep_ds",
        table_name="events",
        loader_file_format=loader_file_format,
        preprocessors_module=pre_mod,
    )
    in_dir = Path(settings.input_dir)
    (in_dir / "skip.jsonl").write_text('{"id": 1}\n', encoding="utf-8")
    (in_dir / "bad.jsonl").write_text('{"id": 2}\n', encoding="utf-8")
    if include_valid_file:
        (in_dir / "good.jsonl").write_text('{"id": 3}\n', encoding="utf-8")

    reader_batches: list[list[str]] = []

    def read_items(items: list[FileItemDict], _cfg: JsonlExtractSettings) -> Iterator[dict[str, str]]:
        reader_batches.append([item["relative_path"] for item in items])
        for item in items:
            yield {"file": item["relative_path"]}

    resource = build_filesystem_resource(
        settings,
        read_items,
        name="prep_reader",
        table_name=table_override,
        parallelized=False,
    )
    collector = attach_file_collector(resource)
    emitted = list(resource)

    expected_error_msg = "File pre-processing failed for bad.jsonl: corrupt header"
    assert collector.preprocess_errors == [expected_error_msg]
    assert [r.getMessage() for r in caplog.records if r.levelno == logging.ERROR] == [expected_error_msg]
    assert second_calls == expected_valid
    assert reader_batches == ([expected_valid] if expected_valid else [])
    assert collector.ingested_files == expected_valid

    expected_rejected_row = {
        "table_name": table_override or "events",
        "source_file": "bad.jsonl",
        "line_no": None,
        "raw_record": None,
        "error_detail": "corrupt header",
    }
    expected_valid_rows = [{"file": name} for name in expected_valid]
    if expect_arrow:
        assert [x for x in emitted if isinstance(x, dict)] == expected_valid_rows
        (rejected_table,) = [x for x in emitted if isinstance(x, pa.Table)]
        assert rejected_table.schema == REJECTED_PYARROW_SCHEMA
        assert rejected_table.to_pylist() == [expected_rejected_row]
    else:
        assert emitted == [expected_rejected_row, *expected_valid_rows]


def test_build_filesystem_resource_pass_iceberg_format_hints(tmp_path: Path) -> None:
    """build_filesystem_resource applies Iceberg table and Parquet file format hints when loader_file_format is iceberg."""
    settings = make_cts_settings(
        FileFormatIngestSettings,
        tmp_path,
        dataset_name="iceberg_hints_ds",
        loader_file_format="iceberg",
    )

    def read_items(_items: TDataItems, _cfg: FileFormatIngestSettings) -> Iterator[dict[str, str]]:
        yield {"entity_id": "e1"}

    resource = build_filesystem_resource(settings, read_items, name="iceberg_reader", parallelized=False)
    base_schema = resource.compute_table_schema()
    assert base_schema["table_format"] == "iceberg"
    assert base_schema["file_format"] == "parquet"


@pytest.mark.parametrize(
    ("file_items", "extra_args", "expected_rows", "expected_calls"),
    [
        pytest.param([], (), [], [], id="empty_items"),
        pytest.param(
            [{"relative_path": "one.txt", "rows": [{"id": 1}]}],
            (),
            [{"id": 1}],
            [(({"relative_path": "one.txt", "rows": [{"id": 1}]},),)],
            id="single_item_no_extra_args",
        ),
        pytest.param(
            [
                {"relative_path": "first.txt", "rows": [{"id": 1}, {"id": 2}]},
                {"relative_path": "empty.txt", "rows": []},
                {"relative_path": "second.txt", "rows": [{"id": 3}]},
            ],
            ("prefix", 99),
            [{"id": 1}, {"id": 2}, {"id": 3}],
            [
                (({"relative_path": "first.txt", "rows": [{"id": 1}, {"id": 2}]}, "prefix", 99),),
                (({"relative_path": "empty.txt", "rows": []}, "prefix", 99),),
                (({"relative_path": "second.txt", "rows": [{"id": 3}]}, "prefix", 99),),
            ],
            id="multiple_items_with_extra_args_and_empty_file",
        ),
    ],
)
def test_make_per_file_reader_pass(
    file_items: list[dict[str, Any]],
    extra_args: tuple[Any, ...],
    expected_rows: list[dict[str, Any]],
    expected_calls: list[tuple[tuple[Any, ...], ...]],
) -> None:
    """make_per_file_reader calls file_fn(file_item, *args) for each item and yields all resulting records."""
    recorded_calls: list[tuple[tuple[Any, ...], ...]] = []

    def file_fn(file_item: dict[str, Any], *args: Any) -> list[dict[str, Any]]:  # noqa: ANN401
        recorded_calls.append(((file_item, *args),))
        return list(file_item["rows"])

    reader = make_per_file_reader(file_fn)
    assert list(reader(file_items, *extra_args)) == expected_rows
    assert recorded_calls == expected_calls


def test_make_per_file_reader_fail_propagates_file_fn_error() -> None:
    """make_per_file_reader propagates exceptions raised by file_fn when iterating."""

    def failing_file_fn(file_item: dict[str, Any]) -> Iterator[dict[str, Any]]:
        if file_item["relative_path"] == "bad.txt":
            err_msg = "corrupt file"
            raise ValueError(err_msg)
        yield {"path": file_item["relative_path"]}

    reader = make_per_file_reader(failing_file_fn)
    gen = reader([{"relative_path": "good.txt"}, {"relative_path": "bad.txt"}])
    assert next(gen) == {"path": "good.txt"}
    with pytest.raises(ValueError, match="corrupt file"):
        next(gen)


@pytest.mark.parametrize(
    ("file_glob", "reader_args", "schema_contract", "transformer_kwargs", "expected"),
    [
        pytest.param(
            None,
            ("tag",),
            None,
            {},
            [
                {"file": "a.xml", "arg": "tag", "content": "alpha"},
                {"file": "b.txt", "arg": "tag", "content": "beta"},
            ],
            id="default_glob_with_reader_args",
        ),
        pytest.param(
            "*.txt",
            ("custom",),
            DEFAULT_SCHEMA_CONTRACT,
            {"parallelized": False, "max_table_nesting": 0},
            [{"file": "b.txt", "arg": "custom", "content": "beta"}],
            id="explicit_glob_contract_and_transformer_kwargs",
        ),
        pytest.param(
            "*.missing",
            ("none",),
            None,
            {},
            [],
            id="no_matching_files",
        ),
    ],
)
def test_build_per_file_filesystem_resource_pass(
    input_dir: Path,
    output_dir: Path,
    file_glob: str | None,
    reader_args: tuple[str, ...],
    schema_contract: dict[str, str] | None,
    transformer_kwargs: dict[str, Any],
    expected: list[dict[str, Any]],
) -> None:
    """build_per_file_filesystem_resource wraps file_fn via make_per_file_reader and reads matching files."""
    (input_dir / "a.xml").write_text("alpha", encoding="utf-8")
    (input_dir / "b.txt").write_text("beta", encoding="utf-8")
    settings = InputLocalOutputSettings(input_dir=input_dir, output_dir=output_dir)

    def read_single_file(
        file_item: dict[str, Any],
        cfg: InputLocalOutputSettings,
        label: str,
    ) -> Iterator[dict[str, Any]]:
        assert cfg is settings
        path = Path(file_item["file_url"].removeprefix("file://"))
        yield {"file": path.name, "arg": label, "content": path.read_text(encoding="utf-8")}

    resource = build_per_file_filesystem_resource(
        settings,
        read_single_file,
        *reader_args,
        name="per_file_test_reader",
        file_glob=file_glob,
        schema_contract=schema_contract,  # type: ignore[arg-type]
        **transformer_kwargs,
    )
    assert resource.name == "per_file_test_reader"
    assert resource.schema_contract == schema_contract
    if "max_table_nesting" in transformer_kwargs:
        assert resource.max_table_nesting == transformer_kwargs["max_table_nesting"]
    assert sorted(resource, key=lambda row: row["file"]) == expected


def test_build_per_file_filesystem_resource_pass_across_fs_backends(
    fs_backend: FsBackend,
    output_dir: Path,
    request: pytest.FixtureRequest,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """build_per_file_filesystem_resource reads files from any fsspec backend configured via settings.input_dir."""
    fs_backend.put("a.txt", b"alpha")
    fs_backend.put("b.txt", b"beta")
    settings = InputLocalOutputSettings(input_dir=fs_backend.bucket_url, output_dir=output_dir)

    def read_single_file(
        file_item: FileItemDict,
        cfg: InputLocalOutputSettings,
    ) -> Iterator[dict[str, str]]:
        assert cfg is settings
        with file_item.open(mode="rt", encoding="utf-8") as fh:
            yield {"file": file_item["relative_path"], "content": fh.read()}

    dlt_secrets = _fs_backend_dlt_secrets(fs_backend, request, monkeypatch)

    with isolated_dlt_config(secrets=dlt_secrets):
        resource = build_per_file_filesystem_resource(
            settings,
            read_single_file,
            name="fs_backend_reader",
            file_glob="*.txt",
            parallelized=False,
        )
        rows = sorted(resource, key=lambda row: row["file"])

    assert rows == [
        {"file": "a.txt", "content": "alpha"},
        {"file": "b.txt", "content": "beta"},
    ]


@pytest.mark.parametrize(
    ("loader_file_format", "expected_run_kwargs"),
    [
        pytest.param("file_listing", {"loader_file_format": "file_listing"}, id="file_listing"),
        pytest.param("jsonl", {"loader_file_format": "jsonl"}, id="jsonl"),
        pytest.param("parquet", {"loader_file_format": "parquet"}, id="parquet"),
        pytest.param(
            "iceberg",
            {"loader_file_format": "parquet", "table_format": "iceberg"},
            id="iceberg",
        ),
    ],
)
@pytest.mark.parametrize(
    ("dataset_name", "fallback_dataset_name", "expected_dataset_name"),
    [
        pytest.param("custom_ds", False, "custom_ds", id="explicit_no_fallback"),
        pytest.param("custom_ds", True, "custom_ds", id="explicit_with_fallback"),
        pytest.param(None, False, None, id="none_no_fallback"),
        pytest.param(None, True, TINY_PIPELINE_NAME, id="none_with_fallback"),
    ],
)
def test_build_file_ingest_kwargs_pass(
    tmp_path: Path,
    dataset_name: str | None,
    fallback_dataset_name: bool,
    expected_dataset_name: str | None,
    loader_file_format: str,
    expected_run_kwargs: dict[str, str],
) -> None:
    """build_file_ingest_kwargs constructs pipeline_kwargs and pipeline_run_kwargs from FileFormatIngestSettings."""
    base_settings: FileFormatIngestSettings = make_cts_settings(
        FileFormatIngestSettings,
        tmp_path,
        dataset_name="custom_ds",
        loader_file_format=loader_file_format,
    )
    settings = base_settings.model_copy(update={"dataset_name": dataset_name})

    result = build_file_ingest_kwargs(
        settings,
        TINY_PIPELINE_NAME,
        fallback_dataset_name=fallback_dataset_name,
    )

    assert result == {
        "pipeline_kwargs": {
            "pipeline_name": TINY_PIPELINE_NAME,
            "dataset_name": expected_dataset_name,
        },
        "pipeline_run_kwargs": expected_run_kwargs,
    }


@pytest.mark.parametrize(
    ("settings_obj", "expected"),
    [
        pytest.param(type("Cfg", (), {"model_config": {"cli_prog_name": "preset_cli"}})(), "preset_cli", id="prog-set"),
        pytest.param(type("Cfg", (), {"model_config": {"cli_prog_name": None}})(), "default_pipe", id="prog-none"),
        pytest.param(type("Cfg", (), {"model_config": {"cli_prog_name": ""}})(), "default_pipe", id="prog-empty"),
        pytest.param(object(), "default_pipe", id="no-model-config"),
    ],
)
def test_resolve_pipeline_name_prefers_cli_prog_name_or_falls_back(
    settings_obj: object,
    expected: str,
) -> None:
    """resolve_pipeline_name returns cli_prog_name when non-empty and falls back to default_pipeline_name."""
    assert resolve_pipeline_name(settings_obj, "default_pipe") == expected


def test_build_file_ingest_kwargs_prefers_subclass_cli_prog_name(tmp_path: Path) -> None:
    """build_file_ingest_kwargs uses settings.model_config['cli_prog_name'] for pipeline_name and dataset fallback."""

    class PresetIngestSettings(FileFormatIngestSettings, cli_prog_name="preset_ingest"):
        pass

    base_settings: PresetIngestSettings = make_cts_settings(
        PresetIngestSettings,
        tmp_path,
        dataset_name="unused_ds",
        loader_file_format="parquet",
    )
    settings = base_settings.model_copy(update={"dataset_name": None})

    result = build_file_ingest_kwargs(settings, TINY_PIPELINE_NAME, fallback_dataset_name=True)

    assert result == {
        "pipeline_kwargs": {
            "pipeline_name": "preset_ingest",
            "dataset_name": "preset_ingest",
        },
        "pipeline_run_kwargs": {"loader_file_format": "parquet"},
    }
