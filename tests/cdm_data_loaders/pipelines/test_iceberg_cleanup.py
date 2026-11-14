"""Tests for explicit Iceberg and dlt catalog cleanup hook installation."""

import gc
import importlib
import logging
import sys
import threading
import warnings
from types import TracebackType
from typing import Final
from unittest.mock import MagicMock, patch

import dlt
import pytest
from dlt.common.libs import pyiceberg as dlt_pyiceberg
from dlt.common.libs.pyiceberg import get_iceberg_tables
from dlt.destinations.impl.filesystem.filesystem import FilesystemClient
from pyiceberg.catalog import sql as pyiceberg_sql
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError
from requests import Session
from sqlalchemy.exc import IntegrityError

import cdm_data_loaders.pipelines as pipelines_pkg
from cdm_data_loaders.core.settings import CtsSettings
from cdm_data_loaders.pipelines.core import run_pipeline
from cdm_data_loaders.pipelines.iceberg_cleanup import (
    DLT_LOAD_CATALOG_ATTR,
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

    monkeypatch.delitem(sys.modules, "cdm_data_loaders.pipelines.iceberg_cleanup", raising=False)
    monkeypatch.delitem(sys.modules, "cdm_data_loaders.pipelines.core", raising=False)
    monkeypatch.delattr(pipelines_pkg, "iceberg_cleanup", raising=False)
    monkeypatch.delattr(pipelines_pkg, "core", raising=False)

    fresh_cleanup = importlib.import_module("cdm_data_loaders.pipelines.iceberg_cleanup")
    importlib.import_module("cdm_data_loaders.pipelines.core")

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
    monkeypatch.delitem(sys.modules, "cdm_data_loaders.pipelines.iceberg_cleanup", raising=False)
    monkeypatch.delitem(sys.modules, "cdm_data_loaders.pipelines.core", raising=False)
    monkeypatch.delattr(pipelines_pkg, "iceberg_cleanup", raising=False)
    monkeypatch.delattr(pipelines_pkg, "core", raising=False)

    fresh_cleanup = importlib.import_module("cdm_data_loaders.pipelines.iceberg_cleanup")
    fresh_core = importlib.import_module("cdm_data_loaders.pipelines.core")
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


def test_iceberg_catalog_cleanup_pass_sql_catalog_integrity_error_becomes_namespace_exists() -> None:
    """SqlCatalog.create_namespace translates SQLAlchemy IntegrityError into NamespaceAlreadyExistsError."""
    install_iceberg_catalog_cleanup()
    with SqlCatalog("dup_ns", uri="sqlite:///:memory:") as catalog:
        catalog.create_namespace("ns_one")
        with (
            patch("sqlalchemy.orm.Session.commit", side_effect=IntegrityError("INSERT", {}, Exception("UNIQUE"))),
            pytest.raises(NamespaceAlreadyExistsError, match="Namespace ns_two already exists"),
        ):
            catalog.create_namespace("ns_two", {"owner": "cdm"})
