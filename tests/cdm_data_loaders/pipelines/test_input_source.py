"""Tests for cdm_data_loaders.pipelines.input_source."""

import gzip
from pathlib import Path
from typing import Any

import pytest
from dlt.common.configuration.specs import AwsCredentials

from cdm_data_loaders.core.settings import CtsSettings
from cdm_data_loaders.pipelines.input_source import (
    InputSource,
    build_input_files,
    input_subdir_exists,
    resolve_input_source,
)
from cdm_data_loaders.utils.file_items import open_input, source_label
from tests.conftest import INPUT_SOURCE_NAME, LOCAL_FS, InputSourceFactory, configure_input_source

CONTENT = b'{"id": 1}\n'


@pytest.fixture
def input_tree(tmp_path: Path) -> Path:
    """Directory with files at the top level and in a subdirectory."""
    root = tmp_path / "input_tree"
    (root / "entity").mkdir(parents=True)
    (root / "top.jsonl").write_bytes(CONTENT)
    (root / "other.txt").write_bytes(b"ignored")
    (root / "entity" / "one.jsonl").write_bytes(CONTENT)
    (root / "entity" / "two.jsonl.gz").write_bytes(gzip.compress(CONTENT))
    return root


def cts_settings(output_dir: Path, **kwargs: Any) -> CtsSettings:  # noqa: ANN401
    """CtsSettings with the given input fields."""
    return CtsSettings(output_dir=output_dir, use_destination=LOCAL_FS, **kwargs)  # pyright: ignore[reportCallIssue]


@pytest.mark.parametrize(
    ("bucket_url", "subdir", "expected"),
    [
        pytest.param("/some/dir", None, "/some/dir", id="no_subdir"),
        pytest.param("/some/dir", "", "/some/dir", id="empty_subdir"),
        pytest.param("/some/dir/", "entity", "/some/dir/entity", id="local_trailing_slash"),
        pytest.param("s3://bucket/prefix", "entity", "s3://bucket/prefix/entity", id="s3"),
        pytest.param("s3://bucket", "/entity/", "s3://bucket/entity", id="s3_bucket_root"),
        pytest.param("file:///some/dir", "a/b", "file:///some/dir/a/b", id="file_url"),
    ],
)
def test_input_source_join_pass(bucket_url: str, subdir: str | None, expected: str) -> None:
    """Subdirectories are joined with '/' for both local paths and URLs."""
    assert InputSource(bucket_url=bucket_url).join(subdir) == expected


@pytest.mark.parametrize(
    ("bucket_url", "expected"),
    [("/some/dir", "file"), ("file:///x", "file"), ("s3://bucket", "s3"), ("S3://bucket", "s3"), ("gs://b", "gs")],
)
def test_input_source_protocol_pass(bucket_url: str, expected: str) -> None:
    """Plain paths count as the file protocol."""
    assert InputSource(bucket_url=bucket_url).protocol == expected


def test_resolve_input_source_pass_input_dir(input_tree: Path, output_dir: Path) -> None:
    """Without use_resource, the local input_dir is used, with no credentials."""
    source = resolve_input_source(cts_settings(output_dir, input_dir=input_tree))
    assert source == InputSource(bucket_url=str(input_tree.resolve()))


def test_resolve_input_source_pass_local_resource(input_tree: Path, output_dir: Path) -> None:
    """A local filesystem source resolves to its bucket_url."""
    configure_input_source(INPUT_SOURCE_NAME, str(input_tree))
    source = resolve_input_source(cts_settings(output_dir, use_resource=INPUT_SOURCE_NAME))
    assert source.protocol == "file"
    assert source.bucket_url.endswith(str(input_tree))


def test_resolve_input_source_pass_s3_resource_with_credentials(output_dir: Path) -> None:
    """An S3 source resolves its credentials from the dlt secrets as AwsCredentials."""
    configure_input_source(
        INPUT_SOURCE_NAME,
        "s3://bucket/prefix",
        {"aws_access_key_id": "AK", "aws_secret_access_key": "SK", "endpoint_url": "http://localhost:1234"},
    )
    source = resolve_input_source(cts_settings(output_dir, use_resource=INPUT_SOURCE_NAME))
    assert source.bucket_url == "s3://bucket/prefix"
    assert isinstance(source.credentials, AwsCredentials)
    assert source.credentials.aws_access_key_id == "AK"
    assert source.credentials.endpoint_url == "http://localhost:1234"


def test_resolve_input_source_fail_not_configured(output_dir: Path) -> None:
    """A use_resource with no sources block in the dlt config is an error."""
    settings = cts_settings(output_dir, use_resource="nowhere")
    with pytest.raises(ValueError, match="Source 'nowhere' is not configured"):
        resolve_input_source(settings)


def test_resolve_input_source_fail_unsupported_type(output_dir: Path) -> None:
    """Only filesystem sources are supported."""
    configure_input_source(INPUT_SOURCE_NAME, "/x", resource_type="sql_database")
    settings = cts_settings(output_dir, use_resource=INPUT_SOURCE_NAME)
    with pytest.raises(ValueError, match="unsupported resource_type 'sql_database'"):
        resolve_input_source(settings)


def test_resolve_input_source_fail_missing_bucket_url(output_dir: Path) -> None:
    """A filesystem source without a bucket_url is invalid."""
    import dlt  # noqa: PLC0415

    dlt.config[f"sources.{INPUT_SOURCE_NAME}.resource_type"] = "filesystem"
    settings = cts_settings(output_dir, use_resource=INPUT_SOURCE_NAME)
    with pytest.raises(ValueError, match=f"Invalid source configuration for '{INPUT_SOURCE_NAME}'"):
        resolve_input_source(settings)


def _listed(settings: CtsSettings, file_glob: str, subdir: str | None = None) -> dict[str, bytes]:
    """Map each listed file's relative_path to its (decompressed) content."""
    listed: dict[str, bytes] = {}
    # iterating a resource directly flattens its pages into individual file items
    for item in build_input_files(settings, file_glob=file_glob, subdir=subdir):
        with open_input(item) as fh:
            listed[source_label(item)] = fh.read()
    return listed


@pytest.mark.s3
def test_build_input_files_pass_resource(
    input_tree: Path, output_dir: Path, input_source_factory: InputSourceFactory
) -> None:
    """Files are listed and streamed from local and S3 sources, honouring file_glob and subdir."""
    settings = cts_settings(output_dir, use_resource=input_source_factory(input_tree))
    assert _listed(settings, "*.jsonl*") == {"top.jsonl": CONTENT}
    assert _listed(settings, "**/*.jsonl*") == {
        "top.jsonl": CONTENT,
        "entity/one.jsonl": CONTENT,
        "entity/two.jsonl.gz": CONTENT,
    }
    assert _listed(settings, "*", subdir="entity") == {"one.jsonl": CONTENT, "two.jsonl.gz": CONTENT}


def test_build_input_files_pass_input_dir(input_tree: Path, output_dir: Path) -> None:
    """Files are listed from input_dir when use_resource is not set."""
    settings = cts_settings(output_dir, input_dir=input_tree)
    assert _listed(settings, "*", subdir="entity") == {"one.jsonl": CONTENT, "two.jsonl.gz": CONTENT}


def test_build_input_files_pass_resource_overrides_input_dir(
    input_tree: Path, tmp_path: Path, output_dir: Path
) -> None:
    """use_resource wins over input_dir."""
    other = tmp_path / "other_input"
    other.mkdir()
    (other / "decoy.jsonl").write_bytes(b"decoy")
    configure_input_source(INPUT_SOURCE_NAME, str(input_tree))
    settings = cts_settings(output_dir, input_dir=other, use_resource=INPUT_SOURCE_NAME)
    assert settings.input_dir is None
    assert _listed(settings, "*.jsonl") == {"top.jsonl": CONTENT}


@pytest.mark.s3
@pytest.mark.parametrize(("subdir", "expected"), [("entity", True), ("missing", False), ("top.jsonl", False)])
def test_input_subdir_exists_pass_resource(
    input_tree: Path,
    output_dir: Path,
    input_source_factory: InputSourceFactory,
    subdir: str,
    expected: bool,
) -> None:
    """Subdirectories (or S3 key prefixes) are detected; files and missing names are not directories."""
    settings = cts_settings(output_dir, use_resource=input_source_factory(input_tree))
    assert input_subdir_exists(settings, subdir) is expected


@pytest.mark.parametrize(("subdir", "expected"), [("entity", True), ("missing", False), ("top.jsonl", False)])
def test_input_subdir_exists_pass_input_dir(input_tree: Path, output_dir: Path, subdir: str, expected: bool) -> None:
    """Subdirectories of input_dir are detected."""
    assert input_subdir_exists(cts_settings(output_dir, input_dir=input_tree), subdir) is expected
