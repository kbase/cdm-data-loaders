"""Unit tests for PyArrow Python schema code generation."""

from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest
from pyiceberg import types as iceberg_types
from pyiceberg.schema import Schema

from cdm_data_loaders.converters.emitters.pyarrow import PyArrowEmitter
from cdm_data_loaders.converters.emitters.pyarrow_codegen.formatting import render_scalar_type_expr
from cdm_data_loaders.converters.emitters.pyarrow_codegen.naming import to_upper_snake
from cdm_data_loaders.converters.emitters.pyarrow_codegen.renderer import (
    render_python_schema_module,
    write_python_schema_file,
)
from cdm_data_loaders.converters.sources.dlt import DltReader
from cdm_data_loaders.converters.sources.iceberg import IcebergReader


def test_render_python_schema_module_and_write_file_pass_iceberg_and_dlt(tmp_path: Path) -> None:
    """render_python_schema_module and write_python_schema_file emit executable Python modules."""
    iceberg_schema = Schema(
        iceberg_types.NestedField(1, "id", iceberg_types.LongType(), required=True),
        iceberg_types.NestedField(2, "price", iceberg_types.DecimalType(10, 2)),
        iceberg_types.NestedField(3, "hash", iceberg_types.FixedType(16)),
        iceberg_types.NestedField(4, "at", iceberg_types.TimeType()),
        iceberg_types.NestedField(
            5,
            "inner",
            iceberg_types.StructType(iceberg_types.NestedField(6, "x", iceberg_types.IntegerType(), required=True)),
        ),
    )
    iceberg_doc = IcebergReader().read(iceberg_schema)
    emitter = PyArrowEmitter()

    source = render_python_schema_module(emitter, {"iceberg_record": iceberg_doc})
    ns: dict[str, Any] = {}
    exec(compile(source, "<iceberg_schema>", "exec"), ns)  # noqa: S102
    assert ns["SCHEMA"].equals(emitter.emit(iceberg_doc), check_metadata=True)
    assert ns["ICEBERG_RECORD_SCHEMA"].equals(emitter.emit(iceberg_doc), check_metadata=True)

    dlt_docs = DltReader().read(
        {
            "orders": {
                "columns": {
                    "id": {"data_type": "bigint", "precision": 16, "nullable": False},
                    "balance": {"data_type": "wei"},
                    "seen": {"data_type": "timestamp", "precision": 3},
                }
            },
            "items": {
                "columns": {
                    "sku": {"data_type": "text", "nullable": False},
                    "raw": {"data_type": "binary"},
                }
            },
        }
    )
    out_file = tmp_path / "dlt_schemas.py"
    written_path = write_python_schema_file(emitter, dlt_docs, out_file)
    assert written_path == out_file.resolve()
    written_source = out_file.read_text(encoding="utf-8")

    dlt_ns: dict[str, Any] = {}
    exec(compile(written_source, str(out_file), "exec"), dlt_ns)  # noqa: S102
    assert set(dlt_ns["SCHEMAS"]) == {"orders", "items"}
    assert dlt_ns["SCHEMAS"]["orders"].equals(emitter.emit(dlt_docs["orders"]), check_metadata=True)
    assert dlt_ns["SCHEMAS"]["items"].equals(emitter.emit(dlt_docs["items"]), check_metadata=True)


@pytest.mark.parametrize(
    ("data_type", "expected_expr"),
    [
        pytest.param(pa.string(), "pa.string()", id="string"),
        pytest.param(pa.large_string(), "pa.large_string()", id="large-string"),
        pytest.param(pa.binary(16), "pa.binary(16)", id="fixed-binary"),
        pytest.param(pa.decimal128(10, 2), "pa.decimal128(10, 2)", id="decimal128"),
        pytest.param(pa.decimal256(76, 0), "pa.decimal256(76, 0)", id="decimal256"),
        pytest.param(pa.timestamp("us", tz="UTC"), 'pa.timestamp("us", tz="UTC")', id="timestamp-utc"),
        pytest.param(pa.timestamp("ms"), 'pa.timestamp("ms")', id="timestamp-naive"),
        pytest.param(pa.time32("ms"), 'pa.time32("ms")', id="time32"),
        pytest.param(pa.time64("us"), 'pa.time64("us")', id="time64"),
    ],
)
def test_render_scalar_type_expr_pass_types(data_type: pa.DataType, expected_expr: str) -> None:
    """render_scalar_type_expr formats PyArrow scalar types as Python constructor calls."""
    assert render_scalar_type_expr(data_type) == expected_expr


@pytest.mark.parametrize(
    ("raw_name", "strip_suffix", "expected"),
    [
        pytest.param("PubmedArticleType", True, "PUBMED_ARTICLE", id="strip-type-suffix"),
        pytest.param("PubmedArticleType", False, "PUBMED_ARTICLE_TYPE", id="keep-type-suffix"),
        pytest.param("123_tag", False, "F_123_TAG", id="leading-digit"),
        pytest.param("---", False, "FIELD", id="non-alnum"),
    ],
)
def test_to_upper_snake_pass_identifiers(raw_name: str, strip_suffix: bool, expected: str) -> None:
    """to_upper_snake normalizes identifiers to valid UPPER_SNAKE_CASE constants."""
    assert to_upper_snake(raw_name, strip_type_suffix=strip_suffix) == expected
