"""Shared SQLite-backed PyIceberg catalog helpers and sample Iceberg schemas for tests."""

from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path
from typing import Final

import pytest
from pyiceberg import catalog as pyiceberg_catalog
from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.partitioning import PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.table.metadata import new_table_metadata
from pyiceberg.table.sorting import SortOrder
from pyiceberg.types import (
    DateType,
    DecimalType,
    DoubleType,
    IntegerType,
    ListType,
    LongType,
    MapType,
    NestedField,
    StringType,
    StructType,
    TimestamptzType,
)
from pyiceberg.utils.config import Config

CATALOG_NAME: Final[str] = "testcat"

SIMPLE_ICEBERG_SCHEMA: Final[Schema] = Schema(
    NestedField(1, "id", LongType(), required=True, doc="primary key"),
    NestedField(2, "name", StringType(), doc="display name"),
)

WIDE_ICEBERG_SCHEMA: Final[Schema] = Schema(
    NestedField(1, "id", LongType(), required=True, doc="primary key"),
    NestedField(2, "name", StringType(), required=True),
    NestedField(3, "count", IntegerType(), required=False),
    NestedField(4, "ratio", DoubleType(), required=False),
    NestedField(5, "as_of", TimestamptzType(), required=False),
    NestedField(6, "born_on", DateType(), required=False),
    NestedField(7, "rate", DecimalType(12, 4), required=False),
    NestedField(8, "tags", ListType(element_id=9, element=StringType(), element_required=False), required=False),
    NestedField(
        10,
        "attrs",
        MapType(key_id=11, key_type=StringType(), value_id=12, value_type=DoubleType(), value_required=False),
        required=False,
    ),
    NestedField(
        13,
        "address",
        StructType(
            NestedField(14, "street", StringType(), required=True),
            NestedField(15, "city", StringType(), required=False),
        ),
        required=False,
    ),
)


def make_iceberg_table(
    identifier: tuple[str, ...],
    schema: Schema,
    partition_spec: PartitionSpec | None = None,
    properties: dict[str, str] | None = None,
) -> Table:
    """Build a PyIceberg ``Table`` backed by in-memory metadata without catalog IO.

    :param identifier: Qualified table identifier tuple.
    :type identifier: tuple[str, ...]
    :param schema: Iceberg schema for the table.
    :type schema: Schema
    :param partition_spec: Optional partition spec.
    :type partition_spec: PartitionSpec | None
    :param properties: Optional table properties dict.
    :type properties: dict[str, str] | None
    :return: Constructed ``Table`` object.
    :rtype: Table
    """
    metadata = new_table_metadata(
        schema,
        partition_spec if partition_spec is not None else PartitionSpec(),
        SortOrder(),
        f"file:///tmp/{'/'.join(identifier)}",
        properties or {},
    )
    return Table(identifier, metadata, f"{metadata.location}/metadata/00000.metadata.json", None, None)


def configure_sqlite_catalog_env(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    catalog_name: str = CATALOG_NAME,
    db_filename: str = "catalog.db",
) -> Path:
    """Configure environment variables for a file-backed SQLite PyIceberg SQL catalog under ``tmp_path``.

    :param tmp_path: Per-test temporary directory.
    :type tmp_path: Path
    :param monkeypatch: Active pytest monkeypatch fixture.
    :type monkeypatch: pytest.MonkeyPatch
    :param catalog_name: PyIceberg catalog name.
    :type catalog_name: str
    :param db_filename: SQLite database filename inside ``tmp_path``.
    :type db_filename: str
    :return: ``tmp_path``.
    :rtype: Path
    """
    db_path = tmp_path / db_filename
    warehouse = tmp_path / "warehouse"
    warehouse.mkdir(exist_ok=True)
    upper_name = catalog_name.upper()
    monkeypatch.setenv(f"PYICEBERG_CATALOG__{upper_name}__TYPE", "sql")
    monkeypatch.setenv(f"PYICEBERG_CATALOG__{upper_name}__URI", f"sqlite:///{db_path.as_posix()}")
    monkeypatch.setenv(f"PYICEBERG_CATALOG__{upper_name}__WAREHOUSE", f"file://{warehouse.as_posix()}")
    monkeypatch.setattr(pyiceberg_catalog, "_ENV_CONFIG", Config())
    return tmp_path


@contextmanager
def open_sqlite_catalog(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    catalog_name: str = CATALOG_NAME,
    db_filename: str = "catalog.db",
    namespace: str | None = None,
) -> Generator[Catalog]:
    """Configure and yield a SQLite-backed PyIceberg ``Catalog``, closing its engine on exit.

    :param tmp_path: Per-test temporary directory.
    :type tmp_path: Path
    :param monkeypatch: Active pytest monkeypatch fixture.
    :type monkeypatch: pytest.MonkeyPatch
    :param catalog_name: PyIceberg catalog name.
    :type catalog_name: str
    :param db_filename: SQLite database filename inside ``tmp_path``.
    :type db_filename: str
    :param namespace: Optional namespace to create before yielding.
    :type namespace: str | None
    :return: Context manager yielding ``Catalog``.
    :rtype: Generator[Catalog]
    """
    configure_sqlite_catalog_env(tmp_path, monkeypatch, catalog_name=catalog_name, db_filename=db_filename)
    with load_catalog(catalog_name) as catalog:
        if namespace is not None:
            catalog.create_namespace(namespace)
        yield catalog


def configure_pyiceberg_home_sqlite_catalog(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    catalog_name: str = "default",
    db_filename: str = "iceberg_catalog.db",
) -> tuple[Path, dict[str, str]]:
    """Write a ``.pyiceberg.yaml`` SQLite catalog config under an isolated ``PYICEBERG_HOME`` and initialize DDL.

    :param tmp_path: Per-test temporary directory.
    :type tmp_path: Path
    :param monkeypatch: Active pytest monkeypatch fixture.
    :type monkeypatch: pytest.MonkeyPatch
    :param catalog_name: Catalog entry name in ``.pyiceberg.yaml``.
    :type catalog_name: str
    :param db_filename: SQLite database filename inside ``tmp_path``.
    :type db_filename: str
    :return: Pair of ``(warehouse_dir, catalog_cfg)``.
    :rtype: tuple[Path, dict[str, str]]
    """
    fake_home = tmp_path / "pyiceberg_home"
    fake_home.mkdir(exist_ok=True)
    monkeypatch.setenv("PYICEBERG_HOME", str(fake_home))

    catalog_db = tmp_path / db_filename
    warehouse_dir = tmp_path / "warehouse"
    warehouse_dir.mkdir(exist_ok=True)
    catalog_cfg = {
        "type": "sql",
        "uri": f"sqlite:///{catalog_db.as_posix()}",
        "warehouse": f"file://{warehouse_dir.as_posix()}",
    }
    (fake_home / ".pyiceberg.yaml").write_text(
        f"catalog:\n  {catalog_name}:\n    type: sql\n    uri: {catalog_cfg['uri']}\n    warehouse: {catalog_cfg['warehouse']}\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(pyiceberg_catalog, "_ENV_CONFIG", Config())

    # initialize SQLite catalog tables once so parallel dlt load workers do not race on DDL
    with load_catalog(catalog_name, **catalog_cfg):
        pass
    return warehouse_dir, catalog_cfg
