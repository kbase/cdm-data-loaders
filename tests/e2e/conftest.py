"""Shared fixtures for end-to-end, Docker, Ceph, and reference pipeline tests."""

from collections.abc import Callable, Generator
from pathlib import Path, PurePosixPath
from typing import Any
from unittest.mock import patch

import duckdb
import pytest
from dlt.common.pipeline import LoadInfo
from types_boto3_s3.client import S3Client

from cdm_data_loaders.io.s3 import client as s3_client_mod
from cdm_data_loaders.io.s3.client import get_s3_client, reset_s3_client
from tests.helpers.fixtures_xsv import resolve_qsv_cmd
from tests.helpers.paths_and_constants import (
    N_REFERENCE_XML_ENTRIES,
    REFERENCE_XML_FIXTURE_DIR,
    TEST_DATA_DIR,
)
from tests.helpers.table_readers import duckdb_reference_from_jsonl, read_jsonl_file
from tests.e2e._helpers import (
    bucket_name_from_node,
    ceph_reachable,
    check_existing_bucket,
    isolated_run,
    write_reference_xml_as_jsonl,
)
from tests.e2e.framework import (
    EXECUTION_MODES,
    ExecutionMode,
    PipelineTestRunner,
    ensure_docker_image,
)


@pytest.fixture(scope="session")
def qsv_cmd() -> str:
    """Resolve the ``qsv`` executable path or skip when unavailable.

    :return: Resolved ``qsv`` executable path.
    :rtype: str
    """
    return resolve_qsv_cmd()


@pytest.fixture
def reference_data_dir() -> Path:
    """Return the directory containing all NCBI Datasets reference chunk configurations.

    :return: Path to ``tests/data/ncbi_datasets/dataset_report``.
    :rtype: Path
    """
    return TEST_DATA_DIR / "ncbi_datasets" / "dataset_report"


@pytest.fixture(scope="session")
def reference_jsonl() -> Path:
    """Return the path to the 52-record NCBI Datasets reference JSONL file.

    :return: Path to ``dataset_reports_52.jsonl``.
    :rtype: Path
    """
    return TEST_DATA_DIR / "ncbi_datasets" / "dataset_report" / "chunk_52" / "dataset" / "dataset_reports_52.jsonl"


@pytest.fixture(scope="session")
def jsonl_reference_entities(reference_jsonl: Path) -> list[dict[str, Any]]:
    """Return parsed records from ``reference_jsonl``.

    :param reference_jsonl: Path to the reference JSONL file.
    :type reference_jsonl: Path
    :return: Parsed list of entity dictionaries.
    :rtype: list[dict[str, Any]]
    """
    return read_jsonl_file(reference_jsonl)


@pytest.fixture(scope="session")
def reference_dataset(reference_jsonl: Path) -> Generator[duckdb.DuckDBPyConnection]:
    """Load ``reference_jsonl`` into an in-memory DuckDB connection with nested structures.

    :param reference_jsonl: Path to the reference JSONL file.
    :type reference_jsonl: Path
    :return: Generator yielding the open ``DuckDBPyConnection``.
    :rtype: Generator[duckdb.DuckDBPyConnection]
    """
    with duckdb_reference_from_jsonl(reference_jsonl) as conn:
        yield conn


@pytest.fixture(scope="session")
def reference_xml_data_dir() -> Path:
    """Return the path to the UniRef XML reference fixture directory.

    :return: Path to ``tests/data/uniprot/uniref``.
    :rtype: Path
    """
    return REFERENCE_XML_FIXTURE_DIR


@pytest.fixture(scope="session")
def reference_xml_as_jsonl(reference_xml_data_dir: Path) -> Path:
    """Generate the reference JSONL from the ``chunk_100_el`` UniRef XML source if absent.

    :param reference_xml_data_dir: UniRef reference directory.
    :type reference_xml_data_dir: Path
    :return: Path to ``uniref_100.jsonl``.
    :rtype: Path
    """
    jsonl_path = reference_xml_data_dir / "jsonl" / "uniref_100.jsonl"
    if not jsonl_path.exists():
        source_xml = reference_xml_data_dir / "chunk_100_el" / "uniref_100_part_01.xml.gz"
        n_entries = write_reference_xml_as_jsonl(source_xml, jsonl_path)
        assert n_entries == N_REFERENCE_XML_ENTRIES
    return jsonl_path


@pytest.fixture(scope="session")
def reference_xml_entries(reference_xml_as_jsonl: Path) -> list[dict[str, Any]]:
    """Return the canonical nested entry dicts read back from ``reference_xml_as_jsonl``.

    :param reference_xml_as_jsonl: Path to ``uniref_100.jsonl``.
    :type reference_xml_as_jsonl: Path
    :return: List of ``{"entry": ...}`` dicts.
    :rtype: list[dict[str, Any]]
    """
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
    """Load ``reference_xml_as_jsonl`` into an in-memory DuckDB connection.

    :param reference_xml_as_jsonl: Path to ``uniref_100.jsonl``.
    :type reference_xml_as_jsonl: Path
    :return: Generator yielding the open ``DuckDBPyConnection``.
    :rtype: Generator[duckdb.DuckDBPyConnection]
    """
    with duckdb_reference_from_jsonl(reference_xml_as_jsonl) as conn:
        yield conn


@pytest.fixture(params=EXECUTION_MODES)
def execution_mode(request: pytest.FixtureRequest) -> ExecutionMode:
    """Return the active pipeline execution mode (``core``, ``cli``, or ``docker``).

    :param request: Active pytest fixture request.
    :type request: pytest.FixtureRequest
    :return: Active ``ExecutionMode``.
    :rtype: ExecutionMode
    """
    return request.param


@pytest.fixture(scope="session")
def docker_image(request: pytest.FixtureRequest) -> str:
    """Build or resolve the Docker image tag used for ``@pytest.mark.docker`` pipeline tests.

    :param request: Active pytest fixture request.
    :type request: pytest.FixtureRequest
    :return: Docker image tag string.
    :rtype: str
    """
    configured_tag = request.config.getoption("--docker-image")
    return ensure_docker_image(configured_tag)


@pytest.fixture
def pipeline_runner(execution_mode: ExecutionMode, request: pytest.FixtureRequest) -> PipelineTestRunner:
    """Provide a ``PipelineTestRunner`` configured for the active ``execution_mode``.

    :param execution_mode: Active execution mode.
    :type execution_mode: ExecutionMode
    :param request: Active pytest fixture request.
    :type request: pytest.FixtureRequest
    :return: Configured ``PipelineTestRunner``.
    :rtype: PipelineTestRunner
    """
    image_tag = request.config.getoption("--docker-image") if execution_mode == ExecutionMode.DOCKER else None
    return PipelineTestRunner(mode=execution_mode, docker_image=image_tag)


@pytest.fixture
def isolated_run_factory(tmp_path: Path) -> Callable[..., Callable[..., tuple[LoadInfo | None, Path]]]:
    """Return a builder for isolated pipeline run factories bound to ``tmp_path``.

    :param tmp_path: Per-test temporary directory.
    :type tmp_path: Path
    :return: Builder callable delegating to :func:`isolated_run`.
    :rtype: Callable[..., Callable[..., tuple[LoadInfo | None, Path]]]
    """

    def _build(
        dataset_prefix: str,
        run_fn: Callable[..., LoadInfo | None],
        base_kwargs: dict[str, Any],
        pipeline_runner: PipelineTestRunner | None = None,
    ) -> Callable[..., tuple[LoadInfo | None, Path]]:
        return isolated_run(tmp_path, dataset_prefix, run_fn, base_kwargs, pipeline_runner=pipeline_runner)

    return _build


@pytest.fixture
def ceph_s3_client() -> Generator[S3Client]:
    """Yield a real boto3 ``S3Client`` pointed at the local CEPH instance, or skip when unreachable.

    :return: Generator yielding ``S3Client``.
    :rtype: Generator[S3Client]
    """
    if not ceph_reachable():
        pytest.skip("CEPH server not available")

    reset_s3_client()
    real_client = get_s3_client()
    with patch.object(s3_client_mod, "get_s3_client", return_value=real_client):
        yield real_client
    reset_s3_client()


@pytest.fixture
def s3_client(request: pytest.FixtureRequest) -> S3Client:
    """Resolve either ``ceph_s3_client`` or ``mock_s3_client`` from indirect parametrization.

    :param request: Active pytest fixture request carrying ``request.param``.
    :type request: pytest.FixtureRequest
    :return: Selected ``S3Client``.
    :rtype: S3Client
    :raises ValueError: If ``request.param`` is neither ``"ceph"`` nor ``"mock"``.
    """
    backend = request.param
    if backend not in ("ceph", "mock"):
        err_msg = f"Invalid s3 client: {backend}"
        raise ValueError(err_msg)
    return request.getfixturevalue(f"{backend}_s3_client")


@pytest.fixture
def test_bucket(s3_client: S3Client, request: pytest.FixtureRequest) -> PurePosixPath:
    """Create or empty a per-test S3 bucket derived from the pytest node ID.

    :param s3_client: Active ``S3Client``.
    :type s3_client: S3Client
    :param request: Active pytest fixture request.
    :type request: pytest.FixtureRequest
    :return: Bucket name as a ``PurePosixPath``.
    :rtype: PurePosixPath
    """
    bucket = bucket_name_from_node(request.node.nodeid)
    check_existing_bucket(s3_client, bucket)
    return PurePosixPath(bucket)


@pytest.fixture
def staging_test_bucket(s3_client: S3Client, request: pytest.FixtureRequest) -> PurePosixPath:
    """Create or empty a per-test staging S3 bucket derived from the pytest node ID.

    :param s3_client: Active ``S3Client``.
    :type s3_client: S3Client
    :param request: Active pytest fixture request.
    :type request: pytest.FixtureRequest
    :return: Staging bucket name as a ``PurePosixPath``.
    :rtype: PurePosixPath
    """
    bucket = bucket_name_from_node(request.node.nodeid, prefix="staging")
    check_existing_bucket(s3_client, bucket)
    return PurePosixPath(bucket)
