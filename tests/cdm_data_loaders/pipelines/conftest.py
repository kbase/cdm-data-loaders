"""Shared fixtures for pipelines tests."""

import copy
import gzip
import json
import os
from collections.abc import Callable, Generator, Iterable, Iterator
from itertools import count
from pathlib import Path
from typing import Any, Final
from unittest.mock import MagicMock, patch

import dlt
import duckdb
import pytest
import xmltodict
from dlt.common.pipeline import LoadInfo

from cdm_data_loaders.core.fields import GZIP_SUFFIX
from cdm_data_loaders.core.settings import CtsSettings
from cdm_data_loaders.pipelines import core
from cdm_data_loaders.pipelines.core import (
    DISABLE_COMPRESSION_ENV_VAR,
    LOAD_INFO_TABLE_NAME,
)
from cdm_data_loaders.readers.xml import DEFAULT_XMLTODICT_ARGS
from tests.cdm_data_loaders.pipelines.framework import (
    EXECUTION_MODES,
    ExecutionMode,
    PipelineTestRunner,
    ensure_docker_image,
)
from tests.cdm_data_loaders.pipelines.helpers import (
    duckdb_reference_from_jsonl,
    read_jsonl_file,
)
from tests.conftest import (
    LOCAL_FS,
    N_REFERENCE_XML_ENTRIES,
    REFERENCE_XML_FIXTURE_DIR,
    S3,
    TEST_DATA_DIR,
    _generate_dlt_config,
    preserve_logging_state,
)
from tests.helpers import make_cts_settings

DEFAULT_DLT_TABLES = {"_dlt_version", "_dlt_loads", "_dlt_pipeline_state", LOAD_INFO_TABLE_NAME}

HOOK_ENV_VAR: Final[str] = "RUNTIME__SLACK_INCOMING_HOOK"
HOOK_PART_ENV_VARS: Final[tuple[str, ...]] = ("VARIABLE_B", "VARIABLE_T", "CHAR_STR")
VALID_DESTINATIONS = [LOCAL_FS, S3]
LOAD_INFO_SAVE_FAILED: Final[str] = "Loading pipeline load info failed: "
LOAD_INFO_SAVE_FAILED_JOBS: Final[str] = "Loading pipeline load info had failed jobs: "

PIPELINE_TEST_DATA_DIR: Final[Path] = Path(__file__).parents[2] / "data" / "pipelines"
UNIREF_DATA_DIR: Final[Path] = PIPELINE_TEST_DATA_DIR / "uniref"
XMLTODICT_DATA_DIR: Final[Path] = PIPELINE_TEST_DATA_DIR / "xmltodict"


@pytest.fixture
def dlt_test_settings(tmp_path: Path, dlt_destination_config: str) -> CtsSettings:
    """Settings that load into the local_fs destination and keep pipeline metadata in a separate output_dir."""
    return make_cts_settings(
        CtsSettings,
        tmp_path,
        log_config_file=None,
        use_destination=dlt_destination_config,
        use_output_dir_for_pipeline_metadata=True,
    )


@pytest.fixture
def reference_data_dir() -> Path:
    """Return the directory containing all reference chunk configurations."""
    return TEST_DATA_DIR / "ncbi_rest_api" / "dataset_report"


@pytest.fixture(scope="session")
def reference_jsonl() -> Path:
    """Path to the reference JSONL file."""
    return TEST_DATA_DIR / "ncbi_rest_api" / "dataset_report" / "chunk_52" / "dataset" / "dataset_reports_52.jsonl"


@pytest.fixture(scope="session")
def jsonl_reference_entities(reference_jsonl: Path) -> list[dict[str, Any]]:
    """Parsed JSONL file."""
    return read_jsonl_file(reference_jsonl)


@pytest.fixture(scope="session")
def reference_dataset(reference_jsonl: Path) -> Generator[duckdb.DuckDBPyConnection]:
    """Load the reference JSONL into an in-memory DuckDB connection with nested structures.

    read_json_auto infers native STRUCT/STRUCT[] types for the nested content.
    """
    with duckdb_reference_from_jsonl(reference_jsonl) as conn:
        yield conn


def write_reference_xml_as_jsonl(source_xml: Path, jsonl_path: Path) -> int:
    """Parse the UniRef XML with xmltodict and write the reference JSONL.

    The top-level element is ignored; one JSON object per entry element is written,
    wrapped as {"entry": ...} so that nested content is preserved verbatim.
    """
    with gzip.open(source_xml, "rb") as fh:
        document = xmltodict.parse(fh.read(), **DEFAULT_XMLTODICT_ARGS)

    entries = document["UniRef50"]["entry"]
    with jsonl_path.open("w", encoding="utf-8") as out:
        for entry in entries:
            out.write(json.dumps({"entry": entry}, ensure_ascii=False) + "\n")
    return len(entries)


@pytest.fixture(scope="session")
def reference_xml_as_jsonl(reference_xml_data_dir: Path) -> Path:
    """Generate the reference JSONL from the chunk_100_el source, if not present."""
    jsonl_path = reference_xml_data_dir / "jsonl" / "uniref_100.jsonl"
    if not jsonl_path.exists():
        source_xml = reference_xml_data_dir / "chunk_100_el" / "uniref_100_part_01.xml.gz"
        n_entries = write_reference_xml_as_jsonl(source_xml, jsonl_path)
        assert n_entries == N_REFERENCE_XML_ENTRIES

    return jsonl_path


@pytest.fixture(scope="session")
def reference_xml_entries(reference_xml_as_jsonl: Path) -> list[dict[str, Any]]:
    """The canonical nested entry dicts read back from the reference JSONL."""
    ref_json = read_jsonl_file(reference_xml_as_jsonl)
    assert isinstance(ref_json, list)
    for line in ref_json:
        assert list(line.keys()) == ["entry"]
        entry = line.get("entry", {})
        for key in ("_id", "name", "property", "representativeMember"):
            assert key in entry
        assert entry.get("_id") is not None
        assert entry.get("_id").startswith("UniRef50_")
    assert len(ref_json) == N_REFERENCE_XML_ENTRIES
    return ref_json


@pytest.fixture(scope="session")
def reference_xml_dataset(reference_xml_as_jsonl: Path) -> Generator[duckdb.DuckDBPyConnection]:
    """Load the reference JSONL into an in-memory DuckDB connection with nested structures.

    read_json_auto infers native STRUCT/STRUCT[] types for the nested content;
    `member` infers as JSON because its shape varies across entries.
    """
    with duckdb_reference_from_jsonl(reference_xml_as_jsonl) as conn:
        yield conn


@pytest.fixture(scope="session")
def reference_xml_data_dir() -> Path:
    """Path to the uniref fixture directory."""
    return REFERENCE_XML_FIXTURE_DIR


@pytest.fixture(autouse=True)
def clean_env() -> Iterator[None]:
    """Remove env vars that core reads or writes, and restore the whole environment afterwards."""
    with patch.dict(os.environ):
        for name in (*HOOK_PART_ENV_VARS, HOOK_ENV_VAR, DISABLE_COMPRESSION_ENV_VAR):
            os.environ.pop(name, None)
        os.environ["RUNTIME__DLTHUB_TELEMETRY"] = "false"
        yield


@pytest.fixture
def slack_mock(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    """Replace the dlt Slack sender used by send_slack_message_carefully."""
    mock = MagicMock()
    monkeypatch.setattr(core, "send_slack_message", mock)
    return mock


@pytest.fixture
def careful_slack_mock(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    """Replace send_slack_message_carefully as called by run_pipeline."""
    mock = MagicMock()
    monkeypatch.setattr(core, "send_slack_message_carefully", mock)
    return mock


@pytest.fixture
def mock_init_logger(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    """Stop run_cli reconfiguring logging and point dlt.config at a plain dict."""
    mock = MagicMock()
    monkeypatch.setattr(core, "init_logger", mock)
    monkeypatch.setattr(dlt, "config", copy.deepcopy(_generate_dlt_config()))
    return mock


@pytest.fixture
def mock_pipeline(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    """Replace dlt.pipeline with a factory whose pipeline has no Slack hook and returns no load info."""
    pipeline = MagicMock()
    pipeline.runtime_config.slack_incoming_hook = None
    pipeline.run.return_value = None
    factory = MagicMock(return_value=pipeline)
    monkeypatch.setattr(dlt, "pipeline", factory)
    return factory


@pytest.fixture
def mock_dlt(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    """Replace dlt.destination and dlt.pipeline with a single mock, as run_ncbi_pipeline uses both."""
    mock = MagicMock()
    mock.pipeline.return_value.runtime_config.slack_incoming_hook = None
    mock.pipeline.return_value.run.return_value = MagicMock()
    monkeypatch.setattr(dlt, "destination", mock.destination)
    monkeypatch.setattr(dlt, "pipeline", mock.pipeline)
    return mock


@pytest.fixture(params=EXECUTION_MODES)
def execution_mode(request: pytest.FixtureRequest) -> ExecutionMode:
    """Execution mode for pipeline end-to-end tests (core, cli, or docker)."""
    return request.param


@pytest.fixture(scope="session")
def docker_image(request: pytest.FixtureRequest) -> str:
    """Build or resolve the Docker image tag used for @pytest.mark.docker pipeline tests."""
    configured_tag = request.config.getoption("--docker-image")
    return ensure_docker_image(configured_tag)


@pytest.fixture
def pipeline_runner(execution_mode: ExecutionMode, request: pytest.FixtureRequest) -> PipelineTestRunner:
    """Provide a PipelineTestRunner configured for the active execution_mode."""
    image_tag = request.config.getoption("--docker-image") if execution_mode == ExecutionMode.DOCKER else None
    return PipelineTestRunner(mode=execution_mode, docker_image=image_tag)


def isolated_run(
    tmp_path: Path,
    dataset_prefix: str,
    run_fn: Callable[..., LoadInfo | None],
    base_kwargs: dict[str, Any],
    pipeline_runner: PipelineTestRunner | None = None,
) -> Callable[..., tuple[LoadInfo | None, Path]]:
    """Return a factory that runs a pipeline with per-run isolated output state.

    Each call gets a unique output_dir, dataset_name, and log config file under
    tmp_path, so repeated runs never collide or accumulate rows.

    :param tmp_path: pytest tmp_path for this test
    :type tmp_path: Path
    :param dataset_prefix: prefix for each run's unique dataset name
    :type dataset_prefix: str
    :param run_fn: the pipeline run function to invoke with built settings
    :type run_fn: Callable[..., LoadInfo | None]
    :param base_kwargs: base settings kwargs; keys open to override per call
    :type base_kwargs: dict[str, Any]
    :param pipeline_runner: optional PipelineTestRunner to dispatch through
    :type pipeline_runner: PipelineTestRunner | None
    :return: factory(chunk_dir=None, **overrides) -> (load_info, output_dir)
    :rtype: Callable[..., tuple[LoadInfo | None, Path]]
    """
    run_counter = count()

    def _factory(**overrides: Any) -> tuple[LoadInfo | None, Path]:  # noqa: ANN401
        run_index = next(run_counter)
        kwargs: dict[str, Any] = {
            "dataset_name": f"{dataset_prefix}_{run_index}",
            "log_config_file": str(tmp_path / f"logging_{run_index}.conf"),
            "output_dir": str(tmp_path / f"output_{run_index}"),
            **base_kwargs,
        }
        kwargs.update(overrides)
        settings_cls = kwargs.pop("settings_cls")
        settings = make_cts_settings(settings_cls, tmp_path, **kwargs)
        with dlt.config.values({"destination.local_fs.bucket_url": str(settings.output_dir)}):
            load_info = pipeline_runner.run(run_fn, settings) if pipeline_runner is not None else run_fn(settings)
        return load_info, settings.output_dir

    return _factory


@pytest.fixture
def isolated_run_factory(tmp_path: Path) -> Callable[..., Callable[..., tuple[LoadInfo | None, Path]]]:
    """Return a builder for isolated pipeline run factories bound to this test's tmp_path."""

    def _build(
        dataset_prefix: str,
        run_fn: Callable[..., LoadInfo | None],
        base_kwargs: dict[str, Any],
        pipeline_runner: PipelineTestRunner | None = None,
    ) -> Callable[..., tuple[LoadInfo | None, Path]]:
        return isolated_run(tmp_path, dataset_prefix, run_fn, base_kwargs, pipeline_runner=pipeline_runner)

    return _build


@pytest.fixture
def destination_bucket(tmp_path: Path, dlt_destination_config: str) -> Path:  # noqa: ARG001
    """The bucket_url of the local_fs destination in dlt.config."""
    return tmp_path / "bucket"


@pytest.fixture
def restore_logging() -> Iterator[None]:
    """Undo the global logging changes made by init_logger, so caplog and later tests are unaffected."""
    with preserve_logging_state():
        yield


def copy_test_files(source_dir: Path, file_names: Iterable[str], dest_dir: Path) -> Path:
    """Copy test files into dest_dir, keeping relative paths. A name ending in .gz is a gzipped copy of the plain file.

    :param source_dir: directory holding the test files
    :type source_dir: Path
    :param file_names: relative paths of the files to copy
    :type file_names: Iterable[str]
    :param dest_dir: directory to copy the files to; created if needed
    :type dest_dir: Path
    :return: dest_dir
    :rtype: Path
    """
    dest_dir.mkdir(parents=True, exist_ok=True)
    for name in file_names:
        target = dest_dir / name
        target.parent.mkdir(parents=True, exist_ok=True)
        if name.endswith(GZIP_SUFFIX):
            content = (source_dir / name.removesuffix(GZIP_SUFFIX)).read_bytes()
            with gzip.open(target, "wb") as f:
                f.write(content)
        else:
            target.write_bytes((source_dir / name).read_bytes())
    return dest_dir


def sort_rows(rows: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    """Sort rows into a stable order so that whole tables can be compared."""
    return sorted(rows, key=lambda row: repr(sorted(row.items())))
