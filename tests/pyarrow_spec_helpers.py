"""Shared helpers for XSD-to-PyArrow and JSON Schema-to-PyArrow specification tests."""

import datetime as dt
import importlib.util
import io
import json
import sys
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
    """Build a PyArrow list field with configurable outer and item nullability."""
    return pa.field(name, pa.list_(pa.field("item", item_type, nullable=item_nullable)), nullable=nullable)


def load_module(module_file: Path) -> ModuleType:
    """Load and execute a Python module from a file path."""
    spec = importlib.util.spec_from_file_location(module_file.stem, module_file)
    if spec is None or spec.loader is None:
        msg = f"Could not load module from {module_file}"
        raise RuntimeError(msg)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def load_fixture_attr[T](module_file: Path, attr_name: str, expected_type: type[T]) -> T:
    """Load a typed constant attribute from a fixture Python module."""
    module = load_module(module_file)
    value = getattr(module, attr_name)
    if not isinstance(value, expected_type):
        msg = f"Expected {attr_name} in {module_file} to be a {expected_type.__name__}, got {type(value)}"
        raise TypeError(msg)
    return value


def load_expected_schema(schema_file: Path, attr_name: str = "EXPECTED_SCHEMA") -> pa.Schema:
    """Load a pa.Schema constant from an expected_schema.py fixture file."""
    return load_fixture_attr(schema_file, attr_name, pa.Schema)


def load_expected_table(table_file: Path, attr_name: str = "EXPECTED_TABLE") -> pa.Table:
    """Load a pa.Table constant from an expected_table.py fixture file."""
    previous_schema_mod = sys.modules.get("expected_schema")
    sys.modules["expected_schema"] = load_module(table_file.parent / "expected_schema.py")
    try:
        return load_fixture_attr(table_file, attr_name, pa.Table)
    finally:
        if previous_schema_mod is None:
            sys.modules.pop("expected_schema", None)
        else:
            sys.modules["expected_schema"] = previous_schema_mod


def json_default(obj: object) -> str:
    """Serialize Python date and bytes values from Arrow tables for comparison with expected JSON."""
    if isinstance(obj, dt.date):
        return obj.isoformat()
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
    """Verify input record count, expected JSON, and expected PyArrow table against expected_schema.py."""
    expected = json.loads(expected_json_path.read_text(encoding="utf-8"))
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
    """Verify a generated PyArrow schema equals the expected schema in expected_schema.py."""
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
    """Verify converted rows match expected JSON, equal expected PyArrow Table, and round-trip via Parquet."""
    expected = json.loads(expected_json_path.read_text(encoding="utf-8"))
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
