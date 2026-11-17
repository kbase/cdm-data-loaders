"""Shared PyArrow schema, table, and JSON fixture helpers for converter and pipeline spec tests."""

import datetime as dt
import importlib.util
import io
import json
import sys
from decimal import Decimal
from pathlib import Path
from types import ModuleType
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq


def list_field(
    name: str,
    item_type: pa.DataType,
    *,
    nullable: bool = True,
    item_nullable: bool = True,
) -> pa.Field:
    """Build a PyArrow list field with configurable outer and item nullability.

    :param name: Field name.
    :type name: str
    :param item_type: Element ``pa.DataType``.
    :type item_type: pa.DataType
    :param nullable: Whether the list column is nullable.
    :type nullable: bool
    :param item_nullable: Whether list items are nullable.
    :type item_nullable: bool
    :return: Constructed ``pa.Field``.
    :rtype: pa.Field
    """
    return pa.field(name, pa.list_(pa.field("item", item_type, nullable=item_nullable)), nullable=nullable)


def load_module(module_file: Path) -> ModuleType:
    """Load and execute a Python module from a file path.

    :param module_file: Path to the Python module file.
    :type module_file: Path
    :return: Executed module object.
    :rtype: ModuleType
    :raises RuntimeError: If ``importlib`` cannot construct a loader spec.
    """
    mod_name = f"_fixture_{module_file.parent.name}_{module_file.stem}"
    spec = importlib.util.spec_from_file_location(mod_name, module_file)
    if spec is None or spec.loader is None:
        msg = f"Could not load module from {module_file}"
        raise RuntimeError(msg)
    module = importlib.util.module_from_spec(spec)
    sys.modules[mod_name] = module
    spec.loader.exec_module(module)
    return module


def load_fixture_attr[T](module_file: Path, attr_name: str, expected_type: type[T]) -> T:
    """Load a typed constant attribute from a fixture Python module.

    :param module_file: Path to the Python fixture module.
    :type module_file: Path
    :param attr_name: Module attribute name to retrieve.
    :type attr_name: str
    :param expected_type: Expected runtime class of the attribute value.
    :type expected_type: type[T]
    :return: Loaded attribute value cast to ``T``.
    :rtype: T
    :raises TypeError: If the attribute does not match ``expected_type``.
    """
    module = load_module(module_file)
    value = getattr(module, attr_name)
    if not isinstance(value, expected_type):
        msg = f"Expected {attr_name} in {module_file} to be a {expected_type.__name__}, got {type(value)}"
        raise TypeError(msg)
    return value


def load_input_schema(schema_file: Path, attr_name: str = "SCHEMA") -> pa.Schema:
    """Load a ``pa.Schema`` constant from a ``schema.py`` fixture file.

    :param schema_file: Path to the Python schema fixture file.
    :type schema_file: Path
    :param attr_name: Module attribute holding the PyArrow schema.
    :type attr_name: str
    :return: Loaded ``pa.Schema`` instance.
    :rtype: pa.Schema
    """
    return load_fixture_attr(schema_file, attr_name, pa.Schema)


def load_input_table(table_file: Path, attr_name: str = "TABLE") -> pa.Table:
    """Load a ``pa.Table`` constant from ``table.py`` with sibling ``schema.py`` bound as ``schema``.

    :param table_file: Path to the Python table fixture file.
    :type table_file: Path
    :param attr_name: Module attribute holding the PyArrow table.
    :type attr_name: str
    :return: Loaded ``pa.Table`` instance.
    :rtype: pa.Table
    """
    previous_schema_mod = sys.modules.get("schema")
    sys.modules["schema"] = load_module(table_file.parent / "schema.py")
    try:
        return load_fixture_attr(table_file, attr_name, pa.Table)
    finally:
        if previous_schema_mod is None:
            sys.modules.pop("schema", None)
        else:
            sys.modules["schema"] = previous_schema_mod


def load_expected_schema(schema_file: Path, attr_name: str = "EXPECTED_SCHEMA") -> pa.Schema:
    """Load a ``pa.Schema`` constant from an ``expected_schema.py`` fixture file.

    :param schema_file: Path to ``expected_schema.py``.
    :type schema_file: Path
    :param attr_name: Attribute holding the expected ``pa.Schema``.
    :type attr_name: str
    :return: Loaded ``pa.Schema``.
    :rtype: pa.Schema
    """
    return load_fixture_attr(schema_file, attr_name, pa.Schema)


def load_expected_table(table_file: Path, attr_name: str = "EXPECTED_TABLE") -> pa.Table:
    """Load a ``pa.Table`` constant from an ``expected_table.py`` fixture file.

    :param table_file: Path to ``expected_table.py``.
    :type table_file: Path
    :param attr_name: Attribute holding the expected ``pa.Table``.
    :type attr_name: str
    :return: Loaded ``pa.Table``.
    :rtype: pa.Table
    """
    previous_schema_mod = sys.modules.get("expected_schema")
    sys.modules["expected_schema"] = load_module(table_file.parent / "expected_schema.py")
    try:
        return load_fixture_attr(table_file, attr_name, pa.Table)
    finally:
        if previous_schema_mod is None:
            sys.modules.pop("expected_schema", None)
        else:
            sys.modules["expected_schema"] = previous_schema_mod


def load_json_object(path: Path) -> dict[str, Any]:
    """Load a JSON object dictionary from disk.

    :param path: Path to the JSON file.
    :type path: Path
    :return: Parsed JSON dictionary.
    :rtype: dict[str, Any]
    :raises TypeError: If the root JSON value is not a dict.
    """
    loaded = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(loaded, dict):
        msg = f"Expected a JSON object in {path}, got {type(loaded)}"
        raise TypeError(msg)
    return loaded


def load_json_records(path: Path) -> list[dict[str, Any]]:
    """Load a list of JSON record dictionaries from disk.

    :param path: Path to the JSON file.
    :type path: Path
    :return: Parsed list of row dictionaries.
    :rtype: list[dict[str, Any]]
    :raises TypeError: If the root JSON value is not a list.
    """
    loaded = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(loaded, list):
        msg = f"Expected a list of JSON records in {path}, got {type(loaded)}"
        raise TypeError(msg)
    return loaded


def json_default(obj: object) -> str:
    """Serialize Python date, time, Decimal, and bytes values from Arrow tables for JSON comparison.

    :param obj: Non-JSON-native scalar value.
    :type obj: object
    :return: String representation matching JSON fixture files.
    :rtype: str
    :raises TypeError: If ``obj`` is not a supported scalar value.
    """
    if isinstance(obj, dt.date | dt.time):
        return obj.isoformat()
    if isinstance(obj, Decimal):
        return str(obj)
    if isinstance(obj, bytes):
        return obj.decode("utf-8")
    msg = f"Object of type {type(obj)} is not JSON serializable"
    raise TypeError(msg)


def assert_expected_table_valid(
    records: list[Any],
    expected_json_path: Path,
    expected_table_path: Path,
    *,
    expected_count: int | None = None,
    schema_attr: str = "EXPECTED_SCHEMA",
    table_attr: str = "EXPECTED_TABLE",
) -> None:
    """Verify input record count, expected JSON, and expected PyArrow table against ``expected_schema.py``.

    :param records: Input records list.
    :type records: list[Any]
    :param expected_json_path: Path to ``*_expected.json``.
    :type expected_json_path: Path
    :param expected_table_path: Path to ``*_expected_table.py``.
    :type expected_table_path: Path
    :param expected_count: Optional expected row count.
    :type expected_count: int | None
    :param schema_attr: Attribute name in ``expected_schema.py``.
    :type schema_attr: str
    :param table_attr: Attribute name in ``expected_table.py``.
    :type table_attr: str
    """
    expected = load_json_records(expected_json_path)
    assert len(records) == len(expected)
    if expected_count is not None:
        assert len(expected) == expected_count

    expected_pa_schema = load_expected_schema(expected_table_path.parent / "expected_schema.py", schema_attr)
    expected_table = load_expected_table(expected_table_path, table_attr)
    expected_table.validate(full=True)
    assert expected_table.schema.equals(expected_pa_schema, check_metadata=True)
    assert expected_table.num_rows == len(expected)


def assert_schema_matches_expected(
    pa_schema: pa.Schema,
    expected_schema_path: Path,
    schema_attr: str = "EXPECTED_SCHEMA",
) -> None:
    """Verify a generated PyArrow schema equals the expected schema in ``expected_schema.py``.

    :param pa_schema: Generated ``pa.Schema``.
    :type pa_schema: pa.Schema
    :param expected_schema_path: Path to ``expected_schema.py``.
    :type expected_schema_path: Path
    :param schema_attr: Attribute name in ``expected_schema.py``.
    :type schema_attr: str
    """
    expected_schema = load_expected_schema(expected_schema_path, schema_attr)
    assert pa_schema.equals(expected_schema, check_metadata=True)


def assert_rows_and_table_match_expected(
    rows: list[dict[str, Any]],
    pa_schema: pa.Schema,
    expected_json_path: Path,
    expected_table_path: Path,
    *,
    table_attr: str = "EXPECTED_TABLE",
) -> None:
    """Verify converted rows match expected JSON, equal expected PyArrow Table, and round-trip via Parquet.

    :param rows: Converted row dictionaries.
    :type rows: list[dict[str, Any]]
    :param pa_schema: Target ``pa.Schema``.
    :type pa_schema: pa.Schema
    :param expected_json_path: Path to ``*_expected.json``.
    :type expected_json_path: Path
    :param expected_table_path: Path to ``*_expected_table.py``.
    :type expected_table_path: Path
    :param table_attr: Attribute name in ``expected_table.py``.
    :type table_attr: str
    """
    expected = load_json_records(expected_json_path)
    serialized_rows = json.loads(json.dumps(rows, default=json_default))
    assert serialized_rows == expected

    expected_table = load_expected_table(expected_table_path, table_attr)
    table = pa.Table.from_pylist(rows, schema=pa_schema)
    table.validate(full=True)
    assert table.equals(expected_table, check_metadata=True)

    buf = io.BytesIO()
    pq.write_table(table, buf)
    buf.seek(0)
    roundtrip_table = pq.read_table(buf)
    assert roundtrip_table.num_rows == expected_table.num_rows
    assert roundtrip_table.to_pylist() == expected_table.to_pylist()
