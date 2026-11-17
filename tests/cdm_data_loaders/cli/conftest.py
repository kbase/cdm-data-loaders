"""Shared SQLite PyIceberg catalog fixtures for ``tests/cdm_data_loaders/cli``."""

from collections.abc import Generator
from pathlib import Path

import pytest
from pyiceberg.catalog import Catalog
from pyiceberg.schema import Schema

from tests._support.iceberg_catalog import (
    CATALOG_NAME,
    SIMPLE_ICEBERG_SCHEMA,
    WIDE_ICEBERG_SCHEMA,
    configure_sqlite_catalog_env,
    open_sqlite_catalog,
)


@pytest.fixture
def simple_schema() -> Schema:
    """Return a minimal two-column PyIceberg ``Schema``.

    :return: Simple PyIceberg schema.
    :rtype: Schema
    """
    return SIMPLE_ICEBERG_SCHEMA


@pytest.fixture
def wide_schema() -> Schema:
    """Return a PyIceberg ``Schema`` exercising scalars, structs, lists, and maps.

    :return: Wide PyIceberg schema.
    :rtype: Schema
    """
    return WIDE_ICEBERG_SCHEMA


@pytest.fixture
def catalog_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Configure a disposable SQLite PyIceberg catalog in ``os.environ`` under ``tmp_path``.

    :param tmp_path: Per-test temporary directory.
    :type tmp_path: Path
    :param monkeypatch: Active pytest monkeypatch fixture.
    :type monkeypatch: pytest.MonkeyPatch
    :return: Configured ``tmp_path``.
    :rtype: Path
    """
    return configure_sqlite_catalog_env(tmp_path, monkeypatch, catalog_name=CATALOG_NAME)


@pytest.fixture
def catalog(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Generator[Catalog]:
    """Open the configured SQLite PyIceberg catalog and close connections on teardown.

    :param tmp_path: Per-test temporary directory.
    :type tmp_path: Path
    :param monkeypatch: Active pytest monkeypatch fixture.
    :type monkeypatch: pytest.MonkeyPatch
    :return: Generator yielding ``Catalog``.
    :rtype: Generator[Catalog]
    """
    with open_sqlite_catalog(tmp_path, monkeypatch, catalog_name=CATALOG_NAME) as loaded_catalog:
        yield loaded_catalog
