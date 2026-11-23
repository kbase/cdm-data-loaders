"""Tests for explicit Iceberg and dlt catalog cleanup hook installation."""

import gc
import importlib
import logging
import os
import sys
import threading
import warnings
from pathlib import Path
from types import TracebackType
from typing import Final
from unittest.mock import MagicMock, patch

import dlt
import pytest
from dlt.common.libs import pyiceberg as dlt_pyiceberg
from dlt.common.libs.pyiceberg import get_iceberg_tables
from dlt.destinations.impl.filesystem.filesystem import FilesystemClient
from pyiceberg import catalog as pyiceberg_cat
from pyiceberg.catalog import sql as pyiceberg_sql
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError
from requests import Session
from sqlalchemy.exc import IntegrityError

import cdm_data_loaders.pipelines as pipelines_pkg
import cdm_data_loaders.utils.iceberg as iceberg_pkg
from cdm_data_loaders.core.settings import CtsSettings
from cdm_data_loaders.pipelines.runner import run_pipeline
from cdm_data_loaders.utils.iceberg.cleanup import (
    DLT_LOAD_CATALOG_ATTR,
    PYICEBERG_ENV_CONFIG_ATTR,
    install_iceberg_catalog_cleanup,
)

TINY_PIPELINE_NAME: Final[str] = "test_iceberg_cleanup_tiny_pipeline"
PATCH_TARGETS: Final[tuple[tuple[object, str], ...]] = (
    (pyiceberg_sql, "create_engine"),
    (dlt_pyiceberg, DLT_LOAD_CATALOG_ATTR),
    (SqlCatalog, "create_namespace"),
    (SqlCatalog, "__init__"),
    (SqlCatalog, "close"),
    (RestCatalog, "__init__"),
    (RestCatalog, "close"),
    (FilesystemClient, "__exit__"),
)


def test_iceberg_catalog_cleanup_pass_not_installed_at_import_time_and_idempotent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Importing iceberg_cleanup and core leaves pyiceberg and dlt unpatched; installing is idempotent."""
    for target, attr in PATCH_TARGETS:
        monkeypatch.setattr(target, attr, getattr(target, attr))
    before_import = [(target, attr, getattr(target, attr)) for target, attr in PATCH_TARGETS]

    monkeypatch.delitem(sys.modules, "cdm_data_loaders.utils.iceberg.cleanup", raising=False)
    monkeypatch.delitem(sys.modules, "cdm_data_loaders.pipelines.runner", raising=False)
    monkeypatch.delattr(iceberg_pkg, "cleanup", raising=False)
    monkeypatch.delattr(pipelines_pkg, "runner", raising=False)

    fresh_cleanup = importlib.import_module("cdm_data_loaders.utils.iceberg.cleanup")
    importlib.import_module("cdm_data_loaders.pipelines.runner")

    assert fresh_cleanup.is_iceberg_catalog_cleanup_installed() is False
    for target, attr, orig_val in before_import:
        assert getattr(target, attr) is orig_val, f"{target}.{attr} was mutated at import time"

    fresh_cleanup.install_iceberg_catalog_cleanup()
    assert fresh_cleanup.is_iceberg_catalog_cleanup_installed() is True
    after_first_install = [(target, attr, getattr(target, attr)) for target, attr, _ in before_import]
    for (_, attr, orig_val), (_, _, patched_val) in zip(before_import, after_first_install, strict=True):
        assert patched_val is not orig_val, f"{attr} was not patched by install_iceberg_catalog_cleanup()"

    fresh_cleanup.install_iceberg_catalog_cleanup()
    assert fresh_cleanup.is_iceberg_catalog_cleanup_installed() is True
    for target, attr, patched_val in after_first_install:
        assert getattr(target, attr) is patched_val, f"{attr} was double-wrapped on second call"


def test_iceberg_catalog_cleanup_pass_installed_by_run_pipeline(
    dlt_test_settings: CtsSettings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Calling run_pipeline installs the Iceberg catalog cleanup hooks."""
    for target, attr in PATCH_TARGETS:
        monkeypatch.setattr(target, attr, getattr(target, attr))
    monkeypatch.delitem(sys.modules, "cdm_data_loaders.utils.iceberg.cleanup", raising=False)
    monkeypatch.delitem(sys.modules, "cdm_data_loaders.pipelines.runner", raising=False)
    monkeypatch.delattr(iceberg_pkg, "cleanup", raising=False)
    monkeypatch.delattr(pipelines_pkg, "runner", raising=False)

    fresh_cleanup = importlib.import_module("cdm_data_loaders.utils.iceberg.cleanup")
    fresh_core = importlib.import_module("cdm_data_loaders.pipelines.runner")
    assert fresh_cleanup.is_iceberg_catalog_cleanup_installed() is False

    load_info = fresh_core.run_pipeline(
        dlt_test_settings,
        dlt.resource([{"id": 1, "name": "alpha"}], name="items"),
        pipeline_kwargs={"pipeline_name": TINY_PIPELINE_NAME, "dataset_name": "check_install_ds"},
    )

    assert load_info is not None
    assert fresh_cleanup.is_iceberg_catalog_cleanup_installed() is True


def test_iceberg_catalog_cleanup_pass_no_sqlite_resource_warning_on_pipeline_run(
    dlt_test_settings: CtsSettings,
) -> None:
    """Running an Iceberg pipeline and collecting garbage emits no unclosed sqlite3 ResourceWarning."""
    rows = [{"id": 1, "name": "alpha"}, {"id": 2, "name": "beta"}]
    resource = dlt.resource(rows, name="items")

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always", ResourceWarning)
        load_info = run_pipeline(
            dlt_test_settings,
            resource,
            pipeline_kwargs={"pipeline_name": TINY_PIPELINE_NAME, "dataset_name": "iceberg_ds"},
            pipeline_run_kwargs={"loader_file_format": "parquet", "table_format": "iceberg"},
        )
        assert load_info is not None
        assert not load_info.has_failed_jobs

        tables = get_iceberg_tables(load_info.pipeline)
        assert set(tables) == {"items"}
        assert len(tables["items"].scan().to_arrow()) == len(rows)
        del tables
        del load_info
        gc.collect()

    sqlite_warnings = [
        w for w in caught if issubclass(w.category, ResourceWarning) and "sqlite3.Connection" in str(w.message)
    ]
    assert sqlite_warnings == []


def test_iceberg_catalog_cleanup_pass_rest_catalog_closes_session_on_dereference() -> None:
    """Dereferencing a RestCatalog (e.g. Polaris) closes its underlying requests.Session via finalizer."""
    install_iceberg_catalog_cleanup()
    mock_session = MagicMock(spec=Session)
    with (
        patch.object(RestCatalog, "_fetch_config"),
        patch.object(RestCatalog, "_create_session", return_value=mock_session),
    ):
        catalog = RestCatalog("polaris", uri="http://polaris:8181/api/catalog")
        assert catalog.name == "polaris"
        assert mock_session.close.call_count == 0
        del catalog
        gc.collect()

    mock_session.close.assert_called_once_with()


@pytest.mark.parametrize(
    "exc_args",
    [
        pytest.param((None, None, None), id="normal-exit"),
        pytest.param((RuntimeError, RuntimeError("boom"), None), id="error-exit"),
    ],
)
def test_iceberg_catalog_cleanup_pass_filesystem_client_exit_closes_catalog(
    exc_args: tuple[type[BaseException] | None, BaseException | None, TracebackType | None],
) -> None:
    """FilesystemClient.__exit__ clears _catalog and closes it on both normal and error exits."""
    install_iceberg_catalog_cleanup()
    catalog = SqlCatalog("ephemeral", uri="sqlite:///:memory:")
    client = object.__new__(FilesystemClient)
    client._catalog = catalog
    client._catalog_namespaces = {"ns"}

    with patch.object(catalog, "close", wraps=catalog.close) as spy_close:
        client.__exit__(*exc_args)

    assert client._catalog is None
    assert client._catalog_namespaces == set()
    spy_close.assert_called_once_with()


def test_iceberg_catalog_cleanup_pass_cross_thread_sql_catalog_disposal(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Disposing a SqlCatalog on the main thread after creation on a worker thread emits no errors or warnings."""
    install_iceberg_catalog_cleanup()
    holder: list[SqlCatalog] = []

    def _create_on_worker() -> None:
        holder.append(SqlCatalog("worker_ephemeral", uri="sqlite:///:memory:"))

    worker = threading.Thread(target=_create_on_worker)
    worker.start()
    worker.join()
    assert len(holder) == 1

    with (
        caplog.at_level(logging.ERROR, logger="sqlalchemy.pool"),
        warnings.catch_warnings(record=True) as caught,
    ):
        warnings.simplefilter("always", ResourceWarning)
        holder.clear()
        gc.collect()

    assert caplog.records == []
    sqlite_warnings = [
        w for w in caught if issubclass(w.category, ResourceWarning) and "sqlite3.Connection" in str(w.message)
    ]
    assert sqlite_warnings == []


@pytest.mark.parametrize(
    ("namespace", "properties"),
    [
        pytest.param("ns_one", None, id="without-properties"),
        pytest.param("ns_two", {"owner": "cdm"}, id="with-properties"),
    ],
)
def test_iceberg_catalog_cleanup_pass_sql_catalog_create_namespace(
    namespace: str,
    properties: dict[str, str] | None,
) -> None:
    """SqlCatalog.create_namespace creates namespaces with and without properties when cleanup is installed."""
    install_iceberg_catalog_cleanup()
    with SqlCatalog("create_ns", uri="sqlite:///:memory:") as catalog:
        catalog.create_namespace(namespace, properties)
        assert (namespace,) in catalog.list_namespaces()


def test_iceberg_catalog_cleanup_fail_sql_catalog_integrity_error_becomes_namespace_exists() -> None:
    """SqlCatalog.create_namespace translates SQLAlchemy IntegrityError into NamespaceAlreadyExistsError."""
    install_iceberg_catalog_cleanup()
    with (
        SqlCatalog("dup_ns", uri="sqlite:///:memory:") as catalog,
        patch("sqlalchemy.orm.Session.commit", side_effect=IntegrityError("INSERT", {}, Exception("UNIQUE"))),
        pytest.raises(NamespaceAlreadyExistsError, match="Namespace ns_two already exists") as exc_info,
    ):
        catalog.create_namespace("ns_two", {"owner": "cdm"})
    assert isinstance(exc_info.value.__cause__, IntegrityError)


def _fresh_install_cleanup(monkeypatch: pytest.MonkeyPatch) -> object:
    """Reload cleanup module in isolation and install hooks."""
    for target, attr in PATCH_TARGETS:
        monkeypatch.setattr(target, attr, getattr(target, attr))
    monkeypatch.delitem(sys.modules, "cdm_data_loaders.utils.iceberg.cleanup", raising=False)
    monkeypatch.delattr(iceberg_pkg, "cleanup", raising=False)
    fresh_cleanup = importlib.import_module("cdm_data_loaders.utils.iceberg.cleanup")
    fresh_cleanup.install_iceberg_catalog_cleanup()
    return fresh_cleanup


def test_iceberg_catalog_cleanup_pass_rest_catalog_close_invokes_session_close() -> None:
    """RestCatalog.close() invokes session.close() via the wrapped close hook."""
    install_iceberg_catalog_cleanup()
    mock_session = MagicMock(spec=Session)
    with (
        patch.object(RestCatalog, "_fetch_config"),
        patch.object(RestCatalog, "_create_session", return_value=mock_session),
    ):
        catalog = RestCatalog("polaris", uri="http://polaris:8181/api/catalog")
        catalog.close()
        count_after_first_close = mock_session.close.call_count
        assert count_after_first_close > 0
        catalog.close()
        assert mock_session.close.call_count == count_after_first_close + 1


def test_iceberg_catalog_cleanup_pass_rest_catalog_without_session_has_no_finalizer() -> None:
    """RestCatalog skips registering a finalizer when _create_session returns None."""
    install_iceberg_catalog_cleanup()
    with (
        patch.object(RestCatalog, "_fetch_config"),
        patch.object(RestCatalog, "_create_session", return_value=None),
    ):
        no_session_catalog = RestCatalog("polaris_none", uri="http://polaris:8181/api/catalog")
        assert not hasattr(no_session_catalog, "_finalizer")


@pytest.mark.parametrize(
    ("url", "call_kwargs", "expected_kwargs"),
    [
        pytest.param("sqlite:///a.db", {}, {"connect_args": {"check_same_thread": False}}, id="sqlite-default"),
        pytest.param(
            "sqlite:///b.db",
            {"connect_args": {"timeout": 10}},
            {"connect_args": {"timeout": 10, "check_same_thread": False}},
            id="sqlite-preserves-other-connect-args",
        ),
        pytest.param(
            "sqlite:///c.db",
            {"connect_args": {"check_same_thread": True}},
            {"connect_args": {"check_same_thread": True}},
            id="sqlite-preserves-explicit-check-same-thread",
        ),
        pytest.param("postgresql://localhost/db", {}, {}, id="non-sqlite-unchanged"),
    ],
)
def test_iceberg_catalog_cleanup_pass_create_engine_connect_args(
    monkeypatch: pytest.MonkeyPatch,
    url: str,
    call_kwargs: dict[str, object],
    expected_kwargs: dict[str, object],
) -> None:
    """Patched create_engine sets check_same_thread=False only for sqlite URLs and preserves existing args."""
    recorded: list[tuple[object, dict[str, object]]] = []

    def fake_create_engine(engine_url: object, *_args: object, **kwargs: object) -> object:
        recorded.append((engine_url, dict(kwargs)))
        return object()

    monkeypatch.setattr(pyiceberg_sql, "create_engine", fake_create_engine)
    _fresh_install_cleanup(monkeypatch)

    pyiceberg_sql.create_engine(url, **call_kwargs)
    assert recorded == [(url, expected_kwargs)]


@pytest.mark.parametrize(
    ("catalog_name", "create_yaml", "initial_env_home", "expect_tmp_path_home"),
    [
        pytest.param("cat1", False, None, False, id="no-yaml-no-env"),
        pytest.param("cat2", True, None, True, id="home-yaml-sets-and-restores-env"),
        pytest.param("cat3", True, "/custom/home", False, id="existing-env-preserved"),
    ],
)
def test_iceberg_catalog_cleanup_pass_load_catalog_env_and_home(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    catalog_name: str,
    create_yaml: bool,
    initial_env_home: str | None,
    expect_tmp_path_home: bool,
) -> None:
    """Patched _load_catalog_from_pyiceberg resets _ENV_CONFIG and temporarily sets PYICEBERG_HOME."""
    seen_env_home: list[str | None] = []
    seen_env_configs: list[object] = []

    def fake_orig_load(name: str) -> str:
        seen_env_home.append(os.environ.get("PYICEBERG_HOME"))
        seen_env_configs.append(getattr(pyiceberg_cat, PYICEBERG_ENV_CONFIG_ATTR))
        return f"loaded:{name}"

    monkeypatch.setattr(dlt_pyiceberg, DLT_LOAD_CATALOG_ATTR, fake_orig_load)
    _fresh_install_cleanup(monkeypatch)

    monkeypatch.setattr(Path, "home", classmethod(lambda _cls: tmp_path))
    if create_yaml:
        (tmp_path / ".pyiceberg.yaml").write_text("catalog: {}\n")
    if initial_env_home is None:
        monkeypatch.delenv("PYICEBERG_HOME", raising=False)
    else:
        monkeypatch.setenv("PYICEBERG_HOME", initial_env_home)

    sentinel_cfg = object()
    monkeypatch.setattr(pyiceberg_cat, PYICEBERG_ENV_CONFIG_ATTR, sentinel_cfg)

    result = getattr(dlt_pyiceberg, DLT_LOAD_CATALOG_ATTR)(catalog_name)
    expected_seen_home = str(tmp_path) if expect_tmp_path_home else initial_env_home
    assert result == f"loaded:{catalog_name}"
    assert seen_env_home == [expected_seen_home]
    assert seen_env_configs[0] is not sentinel_cfg
    assert os.environ.get("PYICEBERG_HOME") == initial_env_home


def test_iceberg_catalog_cleanup_fail_load_catalog_restores_pyiceberg_home_on_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Patched _load_catalog_from_pyiceberg clears temporary PYICEBERG_HOME even when loading raises."""

    def fake_orig_load(_name: str) -> str:
        msg = "load error"
        raise ValueError(msg)

    monkeypatch.setattr(dlt_pyiceberg, DLT_LOAD_CATALOG_ATTR, fake_orig_load)
    _fresh_install_cleanup(monkeypatch)
    monkeypatch.setattr(Path, "home", classmethod(lambda _cls: tmp_path))
    monkeypatch.delenv("PYICEBERG_HOME", raising=False)
    (tmp_path / ".pyiceberg.yaml").write_text("catalog: {}\n")

    with pytest.raises(ValueError, match="load error"):
        getattr(dlt_pyiceberg, DLT_LOAD_CATALOG_ATTR)("boom")
    assert "PYICEBERG_HOME" not in os.environ


@pytest.mark.parametrize(
    ("catalog", "exc_args"),
    [
        pytest.param(None, (None, None, None), id="none-catalog-normal-exit"),
        pytest.param(object(), (RuntimeError, RuntimeError("e"), None), id="non-closable-catalog-error-exit"),
    ],
)
def test_iceberg_catalog_cleanup_pass_filesystem_client_exit_without_closable_catalog(
    monkeypatch: pytest.MonkeyPatch,
    catalog: object,
    exc_args: tuple[type[BaseException] | None, BaseException | None, TracebackType | None],
) -> None:
    """FilesystemClient.__exit__ clears namespaces and delegates to orig __exit__ for None or non-closable catalogs."""
    orig_exits: list[tuple[object, object, object]] = []

    def fake_orig_exit(
        _self: FilesystemClient,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        orig_exits.append((exc_type, exc_val, exc_tb))

    monkeypatch.setattr(FilesystemClient, "__exit__", fake_orig_exit)
    _fresh_install_cleanup(monkeypatch)

    client = object.__new__(FilesystemClient)
    client._catalog = catalog
    client._catalog_namespaces = {"ns"}
    client.__exit__(*exc_args)
    assert client._catalog is None
    assert client._catalog_namespaces == set()
    assert orig_exits == [exc_args]


def test_iceberg_catalog_cleanup_fail_filesystem_client_exit_calls_orig_exit_on_close_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """FilesystemClient.__exit__ still calls orig __exit__ when catalog.close() raises."""
    orig_exits: list[tuple[object, object, object]] = []

    def fake_orig_exit(
        _self: FilesystemClient,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        orig_exits.append((exc_type, exc_val, exc_tb))

    monkeypatch.setattr(FilesystemClient, "__exit__", fake_orig_exit)
    _fresh_install_cleanup(monkeypatch)

    failing_cat = MagicMock()
    failing_cat.close.side_effect = RuntimeError("close failed")
    client = object.__new__(FilesystemClient)
    client._catalog = failing_cat
    client._catalog_namespaces = {"ns"}
    with pytest.raises(RuntimeError, match="close failed"):
        client.__exit__(None, None, None)
    assert orig_exits == [(None, None, None)]
