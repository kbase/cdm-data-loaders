"""Explicit resource-cleanup and compatibility hooks for PyIceberg and dlt filesystem catalogs."""

import os
import weakref
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from types import TracebackType
from typing import Final

from dlt.common.libs import pyiceberg as dlt_pyiceberg
from dlt.destinations.impl.filesystem.filesystem import FilesystemClient
from pyiceberg import catalog as pyiceberg_catalog
from pyiceberg.catalog import Catalog
from pyiceberg.catalog import sql as pyiceberg_sql
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError
from pyiceberg.utils.config import Config
from sqlalchemy.engine import URL, Engine
from sqlalchemy.exc import IntegrityError

DLT_LOAD_CATALOG_ATTR: Final[str] = "_load_catalog_from_pyiceberg"
PYICEBERG_ENV_CONFIG_ATTR: Final[str] = "_ENV_CONFIG"


@dataclass(slots=True)
class _CleanupState:
    """Mutable state tracking whether Iceberg catalog cleanup hooks have been installed."""

    installed: bool = False


_cleanup_state: _CleanupState = _CleanupState()


def is_iceberg_catalog_cleanup_installed() -> bool:
    """Return whether the PyIceberg and dlt catalog cleanup hooks are currently installed.

    :return: ``True`` if :func:`install_iceberg_catalog_cleanup` has executed, ``False`` otherwise
    :rtype: bool
    """
    return _cleanup_state.installed


def _wrap_catalog_finalizer[C: (SqlCatalog, RestCatalog)](
    catalog_cls: type[C],
    get_cleanup_fn: Callable[[C], Callable[[], object] | None],
) -> None:
    """Attach a weakref finalizer on catalog initialization and invoke it on ``close()``.

    :param catalog_cls: PyIceberg catalog class whose ``__init__`` and ``close`` methods are wrapped
    :type catalog_cls: type[C]
    :param get_cleanup_fn: callback returning a zero-arg resource cleanup callable for an initialized catalog
    :type get_cleanup_fn: Callable[[C], Callable[[], object] | None]
    :return: None
    :rtype: None
    """
    orig_init = catalog_cls.__init__
    orig_close = catalog_cls.close

    def _init(self: C, name: str, **properties: str) -> None:
        try:
            orig_init(self, name, **properties)
        finally:
            if (cleanup := get_cleanup_fn(self)) is not None:
                self._finalizer = weakref.finalize(self, cleanup)  # type: ignore[attr-defined]

    def _close(self: C) -> None:
        if (finalizer := getattr(self, "_finalizer", None)) is not None:
            finalizer()
        orig_close(self)

    catalog_cls.__init__ = _init  # type: ignore[method-assign]
    catalog_cls.close = _close  # type: ignore[method-assign]


def install_iceberg_catalog_cleanup() -> None:
    """Idempotently install weakref finalizers and resource cleanup hooks on PyIceberg and dlt catalogs.

    ``SqlCatalog`` holds a SQLAlchemy ``Engine`` <-> ``SingletonThreadPool`` reference cycle and
    ``RestCatalog`` holds a ``requests.Session`` <-> ``AuthManager`` reference cycle; neither defines
    a finalizer, and ``dlt``'s ``FilesystemClient.__exit__`` is a no-op. Furthermore, ``dlt`` runs load
    jobs on worker threads while garbage collection or callers may dispose catalogs on the main thread,
    which fails with ``sqlite3.ProgrammingError`` unless ``check_same_thread=False`` is set on SQLite
    connections.

    :return: None
    :rtype: None
    """
    if _cleanup_state.installed:
        return
    _cleanup_state.installed = True

    orig_create_engine = pyiceberg_sql.create_engine

    def _create_engine(url: str | URL, *args: object, **kwargs: object) -> Engine:
        if str(url).startswith("sqlite"):
            raw_connect_args = kwargs.get("connect_args")
            connect_args: dict[str, object] = dict(raw_connect_args) if isinstance(raw_connect_args, Mapping) else {}
            connect_args.setdefault("check_same_thread", False)
            kwargs["connect_args"] = connect_args
        return orig_create_engine(url, *args, **kwargs)  # type: ignore[arg-type]

    pyiceberg_sql.create_engine = _create_engine

    orig_load_from_pyiceberg: Callable[[str], Catalog] = getattr(dlt_pyiceberg, DLT_LOAD_CATALOG_ATTR)
    getattr(pyiceberg_catalog, PYICEBERG_ENV_CONFIG_ATTR)

    def _load_catalog_from_pyiceberg(catalog_name: str) -> Catalog:
        setattr(pyiceberg_catalog, PYICEBERG_ENV_CONFIG_ATTR, Config())
        if not os.environ.get("PYICEBERG_HOME") and (Path.home() / ".pyiceberg.yaml").is_file():
            os.environ["PYICEBERG_HOME"] = str(Path.home())
            try:
                return orig_load_from_pyiceberg(catalog_name)
            finally:
                os.environ.pop("PYICEBERG_HOME", None)
        return orig_load_from_pyiceberg(catalog_name)

    setattr(dlt_pyiceberg, DLT_LOAD_CATALOG_ATTR, _load_catalog_from_pyiceberg)

    orig_create_namespace = SqlCatalog.create_namespace

    def _create_namespace(
        self: SqlCatalog,
        namespace: str | tuple[str, ...],
        properties: dict[str, str] | None = None,
    ) -> None:
        try:
            if properties is None:
                orig_create_namespace(self, namespace)
            else:
                orig_create_namespace(self, namespace, properties)
        except IntegrityError as exc:
            msg = f"Namespace {namespace} already exists"
            raise NamespaceAlreadyExistsError(msg) from exc

    SqlCatalog.create_namespace = _create_namespace  # type: ignore[method-assign]

    _wrap_catalog_finalizer(
        SqlCatalog,
        lambda cat: cat.engine.dispose if hasattr(cat, "engine") else None,
    )
    _wrap_catalog_finalizer(
        RestCatalog,
        lambda cat: session.close if (session := getattr(cat, "_session", None)) is not None else None,
    )

    orig_fs_exit = FilesystemClient.__exit__

    def _fs_client_exit(
        self: FilesystemClient,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        catalog = self._catalog
        self._catalog = None
        self._catalog_namespaces.clear()
        try:
            if catalog is not None and hasattr(catalog, "close"):
                catalog.close()
        finally:
            orig_fs_exit(self, exc_type, exc_val, exc_tb)  # type: ignore[arg-type]

    FilesystemClient.__exit__ = _fs_client_exit  # type: ignore[method-assign]
