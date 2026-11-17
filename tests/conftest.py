"""Global pytest configuration, isolation fixtures, and shared root fixtures."""

import gc
import gzip
import importlib
import logging
import os
import shutil
import sys
from collections.abc import Callable, Generator
from contextlib import contextmanager
from copy import deepcopy
from functools import cache
from importlib.util import find_spec
from pathlib import Path
from types import ModuleType
from typing import Any, Final
from unittest.mock import patch

import boto3
import dlt
import pytest
from dlt.common.configuration.container import Container
from dlt.common.pipeline import PipelineContext
from frozendict import frozendict
from hypothesis import HealthCheck, settings
from moto import mock_aws
from pyspark.sql import SparkSession
from types_boto3_s3.client import S3Client

import cdm_data_loaders.io.s3.client as s3_client
from cdm_data_loaders.io.s3.client import get_s3_client, reset_s3_client
from tests._support.cli_assertions import HOOK_ENV_VAR, HOOK_PART_ENV_VARS
from tests._support.dlt_isolation import isolated_dlt_config
from tests._support.paths_and_constants import (
    BUCKETS,
    CASSETTES_DIR,
    LOCAL_FS,
    NATIVE_SPECLESS_PREFIXES,
    SPECS_ROOT,
    TEST_DATA_DIR,
    TRANSIENT_MODULE_NAMES,
    TRANSIENT_MODULE_PREFIXES,
    generate_dlt_config,
    is_marker_explicitly_selected,
    preserve_logging_state,
    preserve_xsv_metaschema_state,
)
from tests._support.state_audit import StateAuditPlugin
from tests._support.xml_samples import LIBRARY_XSD

settings.register_profile(
    "default",
    max_examples=25,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow],
)
settings.register_profile(
    "ci_thorough",
    max_examples=100,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow],
)
settings.load_profile(os.environ.get("HYPOTHESIS_PROFILE", "default"))

pytest_plugins = ["tests._support.fs_backends"]

SAVE_DIR: Final[str] = "spark.sql.warehouse.dir"

DEFAULT_VCR_CONFIG = frozendict(
    {
        "cassette_library_dir": str(CASSETTES_DIR),
        "record_mode": "once",
        "serializer": "yaml",
        "match_on": ["method", "scheme", "host", "path", "query"],
        "decode_compressed_response": True,
        "allow_playback_repeats": True,
    }
)


@pytest.fixture(scope="module")
def vcr_config() -> dict[str, Any]:
    """Return VCR configuration for recording and replaying HTTP cassettes.

    :return: VCR configuration dictionary.
    :rtype: dict[str, Any]
    """
    return {**DEFAULT_VCR_CONFIG}


@pytest.fixture
def library_xsd(tmp_path: Path) -> Path:
    """Write the minimal library XSD fixture to ``tmp_path``.

    :param tmp_path: Per-test temporary directory.
    :type tmp_path: Path
    :return: Path to the written ``library.xsd`` file.
    :rtype: Path
    """
    xsd_path = tmp_path / "library.xsd"
    xsd_path.write_text(LIBRARY_XSD, encoding="utf-8")
    return xsd_path


def pytest_addoption(parser: pytest.Parser) -> None:
    """Register custom command-line options for Docker, smoke, and state-audit test runs.

    :param parser: Pytest CLI option parser.
    :type parser: pytest.Parser
    """
    parser.addoption(
        "--run-docker",
        action="store_true",
        default=False,
        help="Build and run Docker container tests marked with @pytest.mark.docker.",
    )
    parser.addoption(
        "--docker-image",
        action="store",
        default=None,
        help="Pre-built Docker image tag to use for @pytest.mark.docker tests instead of building.",
    )
    parser.addoption(
        "--dlt-config-dir",
        action="store",
        default=None,
        help="Directory containing config.toml, secrets.toml, and optional settings.toml/.yaml for pipeline smoke tests.",
    )
    parser.addoption(
        "--settings-file",
        action="store",
        default=None,
        help="Path to a settings config file (.toml, .yaml, .yml, or .json) with environment Settings overrides.",
    )
    parser.addoption(
        "--smoke-pipeline",
        action="store",
        default=None,
        help="Filter pipeline smoke tests to a single pipeline name (e.g., pubmed, ncbi_datasets).",
    )
    parser.addoption(
        "--drop-existing-tables",
        action="store_true",
        default=False,
        help="Drop existing catalog/destination tables for the target pipeline before running each smoke test.",
    )
    parser.addoption(
        "--drop-tables-after",
        action="store_true",
        default=False,
        help="Drop catalog/destination tables added by the pipeline after each smoke test finishes.",
    )
    parser.addoption(
        "--state-audit",
        action="store_true",
        default=False,
        help="Audit collected modules and executed tests for unreverted global state mutations.",
    )


def pytest_configure(config: pytest.Config) -> None:
    """Register the passive global state mutation audit plugin when ``--state-audit`` is enabled.

    :param config: Active pytest configuration object.
    :type config: pytest.Config
    """
    if config.getoption("--state-audit") and not config.pluginmanager.has_plugin("cdm_state_audit"):
        config.pluginmanager.register(StateAuditPlugin(), name="cdm_state_audit")


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    """Skip opt-in ``docker`` and ``state_audit`` tests unless explicitly requested on the command line.

    :param config: Active pytest configuration object.
    :type config: pytest.Config
    :param items: Collected test items to filter or mark in place.
    :type items: list[pytest.Item]
    """
    run_docker = bool(config.getoption("--run-docker"))
    run_state_audit = is_marker_explicitly_selected(config.option.markexpr, "state_audit")
    if run_docker and run_state_audit:
        return
    skip_docker = pytest.mark.skip(reason="need --run-docker option to run Docker container tests")
    skip_state_audit = pytest.mark.skip(reason="need -m state_audit option to run global state mutation audit")
    for item in items:
        if not run_docker and "docker" in item.keywords:
            item.add_marker(skip_docker)
        if not run_state_audit and "state_audit" in item.keywords:
            item.add_marker(skip_state_audit)


@cache
def _find_notebook_utils() -> bool:
    """Return whether KBase Lakehouse ``berdl_notebook_utils`` is installed in the active environment.

    :return: ``True`` when both spark session and database helpers are importable.
    :rtype: bool
    """
    try:
        sss = find_spec("berdl_notebook_utils.setup_spark_session")
        db = find_spec("berdl_notebook_utils.spark.database")
    except Exception:
        logging.getLogger(__name__).exception("Notebook utils not available: requires_spark tests will fail")
        return False
    return sss is not None and db is not None


def pytest_runtest_setup(item: pytest.Item) -> None:
    """Skip or fail ``requires_spark`` tests when ``berdl_notebook_utils`` is unavailable.

    :param item: Active pytest test item.
    :type item: pytest.Item
    """
    if "requires_spark" not in item.keywords or _find_notebook_utils():
        return

    if is_marker_explicitly_selected(item.config.option.markexpr, "requires_spark"):
        pytest.fail(
            "Test is marked requires_spark, but Spark is not available in this environment.",
            pytrace=False,
        )
    pytest.skip("Test is marked requires_spark, but Spark is not available in this environment.")


def _is_transient_test_module(name: str, mod: ModuleType | None) -> bool:
    """Determine whether a ``sys.modules`` entry is a transient test or spec fixture module.

    :param name: Module key in ``sys.modules``.
    :type name: str
    :param mod: Module object bound in ``sys.modules``, or ``None``.
    :type mod: ModuleType | None
    :return: ``True`` if the entry is a synthetic or file-loaded spec fixture module.
    :rtype: bool
    """
    if mod is None:
        return False
    if name in TRANSIENT_MODULE_NAMES or name.startswith(TRANSIENT_MODULE_PREFIXES):
        return True
    if getattr(mod, "__spec__", None) is None and getattr(mod, "__file__", None) is None:
        return not name.startswith(NATIVE_SPECLESS_PREFIXES)
    if name == "specs" or name.startswith("specs."):
        return False
    mod_file = getattr(mod, "__file__", None)
    return mod_file is not None and Path(mod_file).resolve().is_relative_to(SPECS_ROOT)


@contextmanager
def _preserve_transient_sys_modules() -> Generator[None]:
    """Restore or remove any transient test/spec modules added or replaced during a block.

    :return: Context manager yielding ``None``.
    :rtype: Generator[None]
    """
    saved_modules = dict(sys.modules)
    try:
        yield
    finally:
        for name in list(sys.modules):
            current_mod = sys.modules.get(name)
            if name not in saved_modules:
                if _is_transient_test_module(name, current_mod):
                    sys.modules.pop(name, None)
            elif current_mod is not saved_modules[name] and (
                _is_transient_test_module(name, current_mod) or _is_transient_test_module(name, saved_modules[name])
            ):
                sys.modules[name] = saved_modules[name]


@pytest.fixture(autouse=True)
def _isolated_cli_and_env() -> Generator[None]:
    """Isolate ``sys.argv``, ``os.environ``, ``boto3.DEFAULT_SESSION``, S3 client cache, and transient modules.

    Declared before fixtures that depend on ``monkeypatch`` so teardown runs after ``monkeypatch.undo()``,
    ensuring decorators such as ``@mock_aws`` cannot leak credentials through ``monkeypatch``'s undo stack.

    :return: Generator yielding ``None``.
    :rtype: Generator[None]
    """
    clean_env = {
        k: v
        for k, v in deepcopy(os.environ).items()
        if not k.lower().startswith("cdl_") and k not in (*HOOK_PART_ENV_VARS, HOOK_ENV_VAR)
    }
    clean_env["RUNTIME__DLTHUB_TELEMETRY"] = "false"
    reset_s3_client()
    try:
        with (
            _preserve_transient_sys_modules(),
            patch.object(sys, "argv", ["pytest"]),
            patch.dict(os.environ, clean_env, clear=True),
            patch.object(boto3, "DEFAULT_SESSION", None),
        ):
            yield
    finally:
        reset_s3_client()


@pytest.fixture(autouse=True)
def isolated_logging(_isolated_cli_and_env: None) -> Generator[None]:
    """Reset ``init_logger``'s record of the applied config and undo any logging changes made by the test.

    :param _isolated_cli_and_env: Root CLI/env isolation fixture ordered outside logging isolation.
    :type _isolated_cli_and_env: None
    :return: Generator yielding ``None``.
    :rtype: Generator[None]
    """
    with preserve_logging_state():
        yield


@pytest.fixture(autouse=True)
def isolated_xsv_metaschema_state(_isolated_cli_and_env: None) -> Generator[None]:
    """Reset the XSV metaschema cache and validator registration around every test.

    :param _isolated_cli_and_env: Root CLI/env isolation fixture ordered outside metaschema isolation.
    :type _isolated_cli_and_env: None
    :return: Generator yielding ``None``.
    :rtype: Generator[None]
    """
    with preserve_xsv_metaschema_state():
        yield


@pytest.fixture(autouse=True)
def logging_setup(_isolated_dlt_config: None, caplog: pytest.LogCaptureFixture) -> None:
    """Configure test logger levels and propagation inside ``isolated_logging`` and after dlt runtime setup.

    :param _isolated_dlt_config: Root dlt config isolation fixture.
    :type _isolated_dlt_config: None
    :param caplog: Active pytest log capture fixture.
    :type caplog: pytest.LogCaptureFixture
    """
    vcr_logger = logging.getLogger("vcr")
    vcr_logger.setLevel(logging.ERROR)
    dlt_logger = logging.getLogger("dlt")
    dlt_logger.propagate = True
    caplog.set_level(logging.INFO)
    caplog.clear()


@pytest.fixture(autouse=True)
def isolate_dlt_pipelines_dir(
    _isolated_dlt_config: None,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> Generator[None]:
    """Set ``DLT_DATA_DIR`` to ``tmp_path`` and deactivate any active ``PipelineContext`` on teardown.

    :param _isolated_dlt_config: Root dlt config isolation fixture.
    :type _isolated_dlt_config: None
    :param tmp_path: Per-test temporary directory.
    :type tmp_path: Path
    :param monkeypatch: Active pytest monkeypatch fixture.
    :type monkeypatch: pytest.MonkeyPatch
    :return: Generator yielding ``None``.
    :rtype: Generator[None]
    """
    monkeypatch.setenv("DLT_DATA_DIR", str(tmp_path))
    yield
    if Container()[PipelineContext].is_active():
        Container()[PipelineContext].deactivate()
        gc.collect()


@pytest.fixture
def spark(tmp_path: Path) -> Generator[SparkSession]:
    """Generate a local Spark session with ``spark.sql.warehouse.dir`` set to ``tmp_path``.

    :param tmp_path: Per-test temporary directory.
    :type tmp_path: Path
    :return: Generator yielding ``SparkSession``.
    :rtype: Generator[SparkSession]
    """
    logger = logging.getLogger(__name__)
    try:
        spark_mod = importlib.import_module("berdl_notebook_utils.setup_spark_session")
    except ModuleNotFoundError:
        logger.exception("berdl_notebook_utils not available: cannot create a spark session")
        raise

    test_config = {
        "spark.sql.shuffle.partitions": 5,
        "spark.default.parallelism": 9,
        "spark.rdd.compress": False,
        "spark.shuffle.compress": False,
        "spark.ui.enabled": False,
        "spark.ui.showConsoleProgress": False,
        "spark.databricks.delta.snapshotPartitions": 2,
        "delta.log.cacheSize": 3,
        SAVE_DIR: str(tmp_path),
    }
    logger.info("starting spark session...")
    session: SparkSession = spark_mod.get_spark_session("test_app", local=True, override=test_config)
    save_dir = session.conf.get(SAVE_DIR).removeprefix("file:")  # pyright: ignore[reportOptionalMemberAccess]
    if save_dir != str(tmp_path):
        logger.error("spark dir: %s; save dir: %s", tmp_path, save_dir)
    yield session
    session.catalog.clearCache()
    session.stop()
    logger.info("stopping spark session...")
    shutil.rmtree(save_dir)


@pytest.fixture(scope="session")
def test_data_dir() -> Path:
    """Return the shared ``tests/data`` directory.

    :return: Path to ``tests/data``.
    :rtype: Path
    """
    return TEST_DATA_DIR


@pytest.fixture
def write_gzip_file() -> Callable[[Path, str, str], Path]:
    """Return a helper that writes UTF-8 text content to a gzip-compressed file.

    :return: Writer callable ``(directory, filename, content) -> Path``.
    :rtype: Callable[[Path, str, str], Path]
    """

    def _write(directory: Path, filename: str, content: str) -> Path:
        directory.mkdir(parents=True, exist_ok=True)
        file_path = directory / filename
        with gzip.open(file_path, "wb") as f:
            f.write(content.encode("utf-8"))
        return file_path

    return _write


@pytest.fixture
def write_gzip_jsonl_file(
    write_gzip_file: Callable[[Path, str, str], Path],
) -> Callable[[Path, str, list[str]], Path]:
    """Return a helper that writes JSON Lines strings to a gzip-compressed file.

    :param write_gzip_file: Gzip file writer fixture.
    :type write_gzip_file: Callable[[Path, str, str], Path]
    :return: Writer callable ``(directory, filename, lines) -> Path``.
    :rtype: Callable[[Path, str, list[str]], Path]
    """

    def _write(directory: Path, filename: str, lines: list[str]) -> Path:
        return write_gzip_file(directory, filename, "\n".join(lines) + "\n")

    return _write


@pytest.fixture(scope="session")
def json_test_strings() -> dict[str, str]:
    """Return a corpus of valid and malformed JSON test strings.

    :return: Mapping from label to raw JSON text.
    :rtype: dict[str, str]
    """
    return {
        "null": "null",
        "empty": "",
        "empty_str": '""',
        "str": "some random string",
        "quoted_str": '"some random string"',
        "ws": "\n\n\t\n\t   ",
        "quoted_ws": '"\n\n\t\n\t   "',
        "empty_object": "{}",
        "empty_array": "[]",
        "null_key": '{"key": {null: "value"}}',
        "empty_key": '{"key": {"": "value"}}',
        "unclosed_str": '{"key": "value}',
        "array_null": "[null]",
        "array_mixed": '[null, "", 0, 1, 1.2345, "string", ["that", "this"], {"this": "that"}]',
        "array_of_str": '["this", "that", "the", "other"]',
        "array_of_arrays": '[[1,2,3],["this","that"],["what?"]]',
        "array_of_objects": '[{"key": "value"}]',
        "object": '{"key": "value"}',
    }


@pytest.fixture(autouse=True)
def _isolated_dlt_config(request: pytest.FixtureRequest) -> Generator[None]:
    """Isolate every test's dlt config and secrets provider chain, seeded with standard test destinations.

    :param request: Active pytest fixture request.
    :type request: pytest.FixtureRequest
    :return: Generator yielding ``None``.
    :rtype: Generator[None]
    """
    request.getfixturevalue("isolated_logging")
    request.getfixturevalue("isolated_xsv_metaschema_state")
    with isolated_dlt_config(generate_dlt_config()):
        yield


@pytest.fixture
def dlt_destination_config(tmp_path: Path) -> Generator[str]:
    """Point the ``local_fs`` dlt destination at a per-test directory under ``tmp_path``.

    :param tmp_path: Per-test temporary directory.
    :type tmp_path: Path
    :return: Generator yielding destination name ``"local_fs"``.
    :rtype: Generator[str]
    """
    bucket = tmp_path / "bucket"
    bucket.mkdir()
    with dlt.config.values(
        {
            "destination.local_fs.destination_type": "filesystem",
            "destination.local_fs.bucket_url": str(bucket),
        }
    ):
        yield LOCAL_FS


@pytest.fixture
def destination_bucket(tmp_path: Path, dlt_destination_config: str) -> Path:
    """Return the ``bucket_url`` directory of the ``local_fs`` destination in ``dlt.config``.

    :param tmp_path: Per-test temporary directory.
    :type tmp_path: Path
    :param dlt_destination_config: Active destination name fixture ensuring ``bucket`` exists.
    :type dlt_destination_config: str
    :return: Destination bucket directory path.
    :rtype: Path
    """
    _ = dlt_destination_config
    return tmp_path / "bucket"


@pytest.fixture
def mock_s3_client(monkeypatch: pytest.MonkeyPatch) -> Generator[S3Client]:
    """Yield a Moto-backed ``S3Client`` pre-seeded with ``TEST_BUCKET`` and ``ALT_BUCKET``.

    :param monkeypatch: Active pytest monkeypatch fixture.
    :type monkeypatch: pytest.MonkeyPatch
    :return: Generator yielding ``S3Client``.
    :rtype: Generator[S3Client]
    """
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.delenv("AWS_ENDPOINT_URL", raising=False)
    monkeypatch.delenv("AWS_ENDPOINT_URL_S3", raising=False)
    monkeypatch.setattr(boto3, "DEFAULT_SESSION", None)

    with mock_aws():
        reset_s3_client()
        client = get_s3_client()
        for bucket in BUCKETS:
            client.create_bucket(Bucket=bucket)
        reset_s3_client()

        with patch.object(s3_client, "get_s3_client", return_value=client):
            yield client

        reset_s3_client()
