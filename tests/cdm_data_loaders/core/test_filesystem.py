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
from dlt.destinations.impl.filesystem.iceberg_adapter import (
    PARTITION_HINT,
    TABLE_PROPERTIES_HINT,
    iceberg_partition,
)
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
)
from cdm_data_loaders.core.preprocessors import FilePreprocessError
from cdm_data_loaders.core.settings import (
    FileFormatIngestSettings,
    InputLocalOutputSettings,
)
from cdm_data_loaders.core.transforms.hints import OutputTableHints
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


def test_find_filesystem_resources_pass_deduplicates_shared_roots(tmp_path: Path) -> None:
    """find_filesystem_resources ignores non-filesystem resources and deduplicates shared roots in order."""
    assert find_filesystem_resources([]) == []
    plain = dlt.resource([{"id": 1}], name="plain")
    assert find_filesystem_resources(plain) == []

    fs_a = filesystem(bucket_url=str(tmp_path), file_glob="*.xml")
    fs_b = filesystem(bucket_url=str(tmp_path), file_glob="*.txt")

    @dlt.transformer(name="trans_a1")
    def trans_a1(items: TDataItems) -> Iterator[TDataItems]:
        yield from items

    @dlt.transformer(name="trans_a2")
    def trans_a2(items: TDataItems) -> Iterator[TDataItems]:
        yield from items

    assert find_filesystem_resources(fs_a) == [fs_a]
    found = find_filesystem_resources([plain, fs_a | trans_a1, fs_a | trans_a2, fs_b])
    assert [res._pipe.instance_id for res in found] == [  # noqa: SLF001
        fs_a._pipe.instance_id,  # noqa: SLF001
        fs_b._pipe.instance_id,  # noqa: SLF001
    ]


def test_list_input_dir_files_pass_local_path_file_url_and_missing_dir(tmp_path: Path) -> None:
    """list_input_dir_files handles local paths, file:// URLs, nested files, empty dirs, and missing dirs."""
    root = tmp_path / "root"
    (root / "nested" / "deep").mkdir(parents=True)
    (root / "empty_subdir").mkdir()
    (root / "z.txt").write_text("z", encoding="utf-8")
    (root / "nested" / "a.xml").write_text("<a/>", encoding="utf-8")
    (root / "nested" / "deep" / "b.json").write_text("{}", encoding="utf-8")

    fs_res = filesystem(bucket_url=str(root))
    expected = ["nested/a.xml", "nested/deep/b.json", "z.txt"]
    assert list_input_dir_files(fs_res, str(root)) == expected
    assert list_input_dir_files(fs_res, root.as_uri()) == expected
    assert list_input_dir_files(fs_res, str(tmp_path / "does_not_exist")) == []


def test_list_input_dir_files_pass_across_fs_backends(
    fs_backend: FsBackend,
    request: pytest.FixtureRequest,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """list_input_dir_files returns sorted POSIX relative paths and empty lists for missing prefixes across backends."""
    fs_backend.put("z.txt", b"zeta")
    fs_backend.put("sub/a.xml", b"alpha")
    fs_backend.put("sub/deep/b.json", b"{}")
    dlt_secrets = _fs_backend_dlt_secrets(fs_backend, request, monkeypatch)

    with isolated_dlt_config(secrets=dlt_secrets):
        fs_res = filesystem(bucket_url=fs_backend.bucket_url)
        assert list_input_dir_files(fs_res, fs_backend.bucket_url) == ["sub/a.xml", "sub/deep/b.json", "z.txt"]
        missing_url = f"{fs_backend.bucket_url.rstrip('/')}/missing_prefix"
        assert list_input_dir_files(fs_res, missing_url) == []


def test_collect_file_listing_pass_and_skips_reader(tmp_path: Path) -> None:
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

    xml_res = build_filesystem_resource(settings, fail_if_called, name="xml_listing_reader")
    assert collect_file_listing(settings, xml_res) == (
        ["sub/a.xml.gz", "sub/notes.txt", "z.xml"],
        ["sub/a.xml.gz", "z.xml"],
    )

    shared_xml = filesystem(bucket_url=str(in_dir), file_glob="*.xml")
    sub_txt = filesystem(bucket_url=str(in_dir), file_glob="sub/*.txt")

    @dlt.transformer(name="unused_one")
    def unused_one(items: TDataItems) -> Iterator[TDataItems]:
        yield from items

    @dlt.transformer(name="unused_two")
    def unused_two(items: TDataItems) -> Iterator[TDataItems]:
        yield from items

    assert collect_file_listing(settings, [shared_xml | unused_one, shared_xml | unused_two, sub_txt]) == (
        ["sub/a.xml.gz", "sub/notes.txt", "z.xml"],
        ["sub/notes.txt", "z.xml"],
    )


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


def test_attach_file_collector_pass_empty_non_fs_and_multiple_roots(tmp_path: Path) -> None:
    """attach_file_collector ignores non-filesystem resources and deduplicates shared filesystem roots."""
    assert attach_file_collector([]) == FilesystemTracker(ingested_files=[], preprocess_errors=[])

    non_fs = dlt.resource([{"relative_path": "ignored.txt"}], name="custom_source")
    non_fs_collector = attach_file_collector(non_fs)
    assert list(non_fs) == [{"relative_path": "ignored.txt"}]
    assert non_fs_collector.ingested_files == []

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


@pytest.mark.parametrize(
    ("loader_file_format", "expect_arrow"),
    [
        pytest.param("parquet", True, id="pyarrow_rejected_page"),
        pytest.param("jsonl", False, id="dict_rejected_page"),
    ],
)
def test_build_filesystem_resource_pass_preprocessors_filter_reject_and_skip_empty_batch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    loader_file_format: str,
    *,
    expect_arrow: bool,
) -> None:
    """Pre-processors filter None items, emit _rejected rows on errors, and skip reader_fn when no valid files remain."""
    second_calls: list[str] = []

    def first_preprocess(file_item: FileItemDict) -> FileItemDict | None:
        rel = file_item["relative_path"]
        if rel == "skip.jsonl":
            return None
        if rel == "bad.jsonl":
            msg = "corrupt header"
            raise FilePreprocessError(msg)
        return file_item

    def second_preprocess(file_item: FileItemDict) -> FileItemDict:
        second_calls.append(file_item["relative_path"])
        return file_item

    pre_mod = register_temp_module(
        monkeypatch,
        "_test_fs_preprocessors",
        FILE_PREPROCESSORS=[first_preprocess, second_preprocess],
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

    reader_batches: list[list[str]] = []

    def read_items(items: list[FileItemDict], _cfg: JsonlExtractSettings) -> Iterator[dict[str, str]]:
        reader_batches.append([item["relative_path"] for item in items])
        for item in items:
            yield {"file": item["relative_path"]}

    # when all discovered files are skipped or rejected, reader_fn is not called
    empty_res = build_filesystem_resource(settings, read_items, name="prep_reader", parallelized=False)
    empty_collector = attach_file_collector(empty_res)
    emitted = list(empty_res)

    assert reader_batches == []
    assert second_calls == []
    assert empty_collector.ingested_files == []
    assert empty_collector.preprocess_errors == ["File pre-processing failed for bad.jsonl: corrupt header"]
    assert [r.getMessage() for r in caplog.records if r.levelno == logging.ERROR] == [
        "File pre-processing failed for bad.jsonl: corrupt header"
    ]
    assert len(emitted) == 1
    expected_rejected_row = {
        "table_name": "events",
        "source_file": "bad.jsonl",
        "line_no": None,
        "raw_record": None,
        "error_detail": "corrupt header",
    }
    if expect_arrow:
        assert isinstance(emitted[0], pa.Table)
        assert emitted[0].schema == REJECTED_PYARROW_SCHEMA
        assert emitted[0].to_pylist() == [expected_rejected_row]
    else:
        assert emitted == [expected_rejected_row]

    # adding a valid file invokes second_preprocess and reader_fn only for that valid file
    (in_dir / "good.jsonl").write_text('{"id": 3}\n', encoding="utf-8")
    mixed_res = build_filesystem_resource(
        settings,
        read_items,
        name="prep_reader_mixed",
        table_name="custom_events",
        parallelized=False,
    )
    mixed_collector = attach_file_collector(mixed_res)
    mixed_items = list(mixed_res)

    assert second_calls == ["good.jsonl"]
    assert reader_batches == [["good.jsonl"]]
    assert mixed_collector.ingested_files == ["good.jsonl"]
    assert mixed_collector.preprocess_errors == ["File pre-processing failed for bad.jsonl: corrupt header"]
    expected_custom_rejected = {**expected_rejected_row, "table_name": "custom_events"}
    if expect_arrow:
        assert [x for x in mixed_items if isinstance(x, dict)] == [{"file": "good.jsonl"}]
        rejected_table = next(x for x in mixed_items if isinstance(x, pa.Table))
        assert rejected_table.to_pylist() == [expected_custom_rejected]
    else:
        assert mixed_items == [expected_custom_rejected, {"file": "good.jsonl"}]


def test_build_filesystem_resource_pass_iceberg_and_output_table_hints(tmp_path: Path) -> None:
    """build_filesystem_resource applies Iceberg format hints and settings.get_output_table_hints()."""
    commented_schema = pa.schema(
        [pa.field("entity_id", pa.string(), nullable=False, metadata={"comment": "Primary ID"})],
        metadata={"comment": "Entity table description"},
    )
    bucket_count = 16

    class _HintedIngestSettings(FileFormatIngestSettings):
        def get_output_table_hints(self) -> tuple[OutputTableHints, ...]:
            return (
                OutputTableHints(
                    table_name="entities",
                    pyarrow_schema=commented_schema,
                    iceberg_partitions=(iceberg_partition.bucket(bucket_count, "entity_id"),),
                ),
            )

    settings = make_cts_settings(
        _HintedIngestSettings,
        tmp_path,
        dataset_name="iceberg_hints_ds",
        loader_file_format="iceberg",
    )

    def read_items(_items: TDataItems, _cfg: _HintedIngestSettings) -> Iterator[dict[str, str]]:
        yield {"entity_id": "e1"}

    resource = build_filesystem_resource(settings, read_items, name="hinted_reader", parallelized=False)
    base_schema = resource.compute_table_schema()
    assert base_schema["table_format"] == "iceberg"
    assert base_schema["file_format"] == "parquet"

    marked = dlt.mark.with_table_name({"entity_id": "e1"}, "entities")
    variant_schema = resource.compute_table_schema(marked.data, marked.meta)
    assert variant_schema["description"] == "Entity table description"
    assert variant_schema["columns"]["entity_id"]["description"] == "Primary ID"
    assert variant_schema[TABLE_PROPERTIES_HINT] == {"comment": "Entity table description"}
    assert variant_schema[PARTITION_HINT] == [
        {"transform": "bucket", "source_column": "entity_id", "param_value": bucket_count}
    ]


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
