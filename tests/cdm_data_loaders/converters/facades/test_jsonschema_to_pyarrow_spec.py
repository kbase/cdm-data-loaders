"""Specification tests for JSON Schema-to-PyArrow schema generation and JSON-to-Arrow record coercion.

Cases, rules and error fixtures are declared in manifests under ``specs/schema_converters/jsonschema_to_pyarrow``
(``rules.yaml``, ``*/case.yaml`` and ``spec/errors/manifest.yaml``). The same manifests render
``docs/jsonschema_to_pyarrow_specification.md`` via ``docs_macros`` Jinja2 templates.
"""

import datetime as dt
import json
from decimal import Decimal
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest
from _pytest.mark.structures import ParameterSet
from frozendict import frozendict
from jsonschema import Draft202012Validator

from cdm_data_loaders.coercion.json_to_arrow import coerce_json_record_for_pyarrow
from cdm_data_loaders.coercion.pydantic_to_arrow import coerce_pydantic_record_for_pyarrow
from cdm_data_loaders.converters.emitters.pyarrow import PyArrowEmitter
from cdm_data_loaders.converters.emitters.pyarrow_codegen.renderer import render_python_schema_module
from cdm_data_loaders.converters.exceptions.facades.jsonschema_to_pyarrow import (
    JSONSCHEMA_TO_PYARROW_ERRORS,
    JSONSchemaToPyArrowError,
    RecursiveNonJsonTypeError,
)
from cdm_data_loaders.converters.facades.jsonschema_to_pyarrow import (
    JSONSchemaToPyArrow,
    generate_pyarrow_schema,
)
from cdm_data_loaders.validation.jsonschema.metaschema import CUSTOM_META_SCHEMA_URI
from specs._loader.converter_spec import Case, ErrorCase, Run, load_cases, load_error_cases
from tests.helpers.paths_and_constants import BASE_DIR
from tests.helpers.pyarrow_spec import (
    assert_expected_table_valid,
    assert_rows_and_table_match_expected,
    assert_schema_matches_expected,
    load_json_object,
    load_json_records,
)

JSONSCHEMA_DATA_DIR = (BASE_DIR / "specs" / "schema_converters" / "jsonschema_to_pyarrow").resolve()
CASES: list[Case] = load_cases(JSONSCHEMA_DATA_DIR / "spec") + load_cases(JSONSCHEMA_DATA_DIR / "examples")
ERROR_CASES: list[ErrorCase] = load_error_cases(JSONSCHEMA_DATA_DIR / "spec" / "errors" / "manifest.yaml")
ERRORS_BY_CODE: dict[str, type[JSONSchemaToPyArrowError]] = {cls.code: cls for cls in JSONSCHEMA_TO_PYARROW_ERRORS}

RUN_PARAMS: list[ParameterSet] = [
    pytest.param(case, run, id=f"{case.suite}-{case.name}-{run.id}") for case in CASES for run in case.runs
]
MAXIMAL_RUN_PARAMS: list[ParameterSet] = [
    pytest.param(run.data, id=f"{case.name}-{run.id}") for case in CASES for run in case.runs if run.id == "maximal"
]
ERROR_PARAMS: list[ParameterSet] = [pytest.param(err, id=f"{err.code}-{err.file.stem}") for err in ERROR_CASES]


def _max_recursion_depth(run: Run) -> int:
    return int(run.params.get("max_recursion_depth", 2))


def _collect_multi_item_lists(value: object, path: str = "$") -> dict[str, list[str]]:
    """Collect canonical JSON element signatures for every list with more than one element."""
    lists_by_path: dict[str, list[str]] = {}
    if isinstance(value, list):
        if len(value) > 1:
            lists_by_path[path] = [json.dumps(item, sort_keys=True) for item in value]
        for idx, item in enumerate(value):
            lists_by_path.update(_collect_multi_item_lists(item, f"{path}[{idx}]"))
    elif isinstance(value, dict):
        for key, child in value.items():
            lists_by_path.update(_collect_multi_item_lists(child, f"{path}.{key}"))
    return lists_by_path


@pytest.mark.parametrize(("case", "run"), RUN_PARAMS)
def test_fixtures_pass_records_and_expected_table_validate(case: Case, run: Run) -> None:
    """Input records validate against the schema and the expected table matches the expected JSON and schema."""
    schema = load_json_object(case.schema)
    Draft202012Validator.check_schema(schema)
    validator = Draft202012Validator(schema)
    records = load_json_records(run.data)
    for record in records:
        validator.validate(record)
    assert_expected_table_valid(
        records,
        run.expected_json,
        run.expected_table,
        expected_count=run.expected_count,
        schema_attr=run.schema_attr,
        table_attr=run.table_attr,
    )


@pytest.mark.parametrize("data_path", MAXIMAL_RUN_PARAMS)
def test_maximal_examples_pass_repeated_items_have_distinct_content(data_path: Path) -> None:
    """Every multi-element array in a maximal JSON example contains distinct elements."""
    lists_by_path = _collect_multi_item_lists(load_json_records(data_path))
    duplicates = {path: sigs for path, sigs in lists_by_path.items() if len(set(sigs)) < len(sigs)}
    assert not duplicates


# Schema generation and coercion


@pytest.mark.parametrize(("case", "run"), RUN_PARAMS)
def test_generate_pyarrow_schema_pass_cases(case: Case, run: Run) -> None:
    """generate_pyarrow_schema produces the exact expected PyArrow schema."""
    pa_schema = generate_pyarrow_schema(load_json_object(case.schema), max_recursion_depth=_max_recursion_depth(run))
    assert_schema_matches_expected(pa_schema, case.expected_schema, run.schema_attr)


@pytest.mark.parametrize(("case", "run"), RUN_PARAMS)
def test_convert_to_python_pass_cases(case: Case, run: Run) -> None:
    """JSONSchemaToPyArrow.convert_to_python renders an executable module whose SCHEMA matches convert()."""
    converter = JSONSchemaToPyArrow(max_recursion_depth=_max_recursion_depth(run))
    raw_schema = load_json_object(case.schema)
    expected = converter.convert(raw_schema)
    py_source = converter.convert_to_python(raw_schema)
    ns: dict[str, object] = {}
    exec(py_source, ns)  # noqa: S102
    assert isinstance(ns["SCHEMA"], pa.Schema)
    assert ns["SCHEMA"].equals(expected, check_metadata=True)


def test_convert_to_python_file_and_shared_defs_pass(tmp_path: Path) -> None:
    """JSONSchemaToPyArrow.convert_to_python_file writes shared $defs struct constants and validates empty mappings."""
    schema: dict[str, Any] = {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "properties": {
            "primary": {"$ref": "#/$defs/Address"},
            "secondary": {"$ref": "#/$defs/Address"},
        },
        "required": ["primary"],
        "$defs": {
            "Address": {
                "type": "object",
                "properties": {"city": {"type": "string"}},
                "required": ["city"],
            },
        },
    }
    converter = JSONSchemaToPyArrow()
    out_path = tmp_path / "address_schema.py"
    assert converter.convert_to_python_file(schema, out_path, docstring="Address schema.") == out_path
    source = out_path.read_text(encoding="utf-8")
    assert '"""Address schema."""' in source
    assert "ADDRESS_STRUCT: Final[pa.StructType]" in source

    ns: dict[str, object] = {}
    exec(source, ns)  # noqa: S102
    assert isinstance(ns["SCHEMA"], pa.Schema)
    assert ns["SCHEMA"].equals(converter.convert(schema), check_metadata=True)

    with pytest.raises(ValueError, match="empty SchemaDocument mapping"):
        render_python_schema_module(PyArrowEmitter(), {})


@pytest.mark.parametrize(("case", "run"), RUN_PARAMS)
def test_coerce_json_record_for_pyarrow_pass_cases(case: Case, run: Run) -> None:
    """Records coerce to the expected rows, build the expected table and round-trip through Parquet."""
    depth = _max_recursion_depth(run)
    pa_schema = generate_pyarrow_schema(load_json_object(case.schema), max_recursion_depth=depth)
    rows = [coerce_json_record_for_pyarrow(record, pa_schema) for record in load_json_records(run.data)]
    assert_rows_and_table_match_expected(
        rows, pa_schema, run.expected_json, run.expected_table, table_attr=run.table_attr
    )


def test_coerce_json_record_for_pyarrow_pass_custom_output_schema_and_pydantic_parity() -> None:
    """Custom OUTPUT_SCHEMAS types coerce with identical row output across JSON and Pydantic coercers."""
    custom_schema = pa.schema(
        [
            pa.field("ts_utc", pa.timestamp("us", tz="UTC")),
            pa.field("ts_naive", pa.timestamp("us")),
            pa.field("tod", pa.time64("us")),
            pa.field("dec128", pa.decimal128(10, 2)),
            pa.field("dec256", pa.decimal256(76, 4)),
            pa.field("num_int", pa.int64()),
            pa.field("num_float", pa.float64()),
            pa.field("xsd_hex", pa.large_binary(), metadata={"xsd_type": "xs:hexBinary"}),
            pa.field("pyd_utf8", pa.binary_view(), metadata={"pydantic_type": "binary"}),
            pa.field("pyd_b64", pa.binary(3), metadata={"pydantic_type": "binary (base64)"}),
            pa.field("view_str", pa.string_view()),
            pa.field("fixed_dates", pa.list_(pa.date32(), 2)),
            pa.field("view_list", pa.list_view(pa.int64())),
        ]
    )
    raw_record: dict[str, Any] = {
        "ts_utc": "2025-06-15T14:30:00+02:00",
        "ts_naive": "2025-06-15T14:30:00+02:00",
        "tod": "14:30:00+02:00",
        "dec128": 6.456,
        "dec256": "1234567890123456789012345678901234567890.12346",
        "num_int": "42",
        "num_float": "3.25",
        "xsd_hex": "48656c6c6f",
        "pyd_utf8": "hello",
        "pyd_b64": "YWJj",
        "view_str": {"ok": True},
        "fixed_dates": ["2025-06-15", "2025-06-16"],
        "view_list": ["10", 20.0],
        "extra_ignored": "drop",
    }
    coerced_json = coerce_json_record_for_pyarrow(raw_record, custom_schema)
    coerced_pydantic = coerce_pydantic_record_for_pyarrow(raw_record, custom_schema)
    assert coerced_json == coerced_pydantic
    assert coerced_json == {
        "ts_utc": dt.datetime(2025, 6, 15, 12, 30, tzinfo=dt.UTC),
        "ts_naive": dt.datetime(2025, 6, 15, 12, 30, tzinfo=dt.UTC).replace(tzinfo=None),
        "tod": dt.time(14, 30),
        "dec128": Decimal("6.46"),
        "dec256": Decimal("1234567890123456789012345678901234567890.1235"),
        "num_int": 42,
        "num_float": 3.25,
        "xsd_hex": b"Hello",
        "pyd_utf8": b"hello",
        "pyd_b64": b"abc",
        "view_str": '{"ok": true}',
        "fixed_dates": [dt.date(2025, 6, 15), dt.date(2025, 6, 16)],
        "view_list": [10, 20],
    }
    table = pa.Table.from_pylist([coerced_json], schema=custom_schema)
    table.validate(full=True)
    assert table.schema.equals(custom_schema, check_metadata=True)


def test_generate_pyarrow_schema_pass_kbase_xsv_metaschema_dialect() -> None:
    """The KBase Draft 2020-12 XSV metaschema URI is accepted identically to standard Draft 2020-12."""
    fixture_dir = JSONSCHEMA_DATA_DIR / "spec" / "01_scalars_and_formats"
    schema = load_json_object(fixture_dir / "schema.json")
    schema["$schema"] = CUSTOM_META_SCHEMA_URI
    assert_schema_matches_expected(generate_pyarrow_schema(schema), fixture_dir / "expected_schema.py")


@pytest.mark.parametrize("err", ERROR_PARAMS)
def test_generate_pyarrow_schema_fail_error_manifest(err: ErrorCase) -> None:
    """Each error fixture raises the JSONSchemaToPyArrowError subclass for its code."""
    schema = load_json_object(err.file)
    with pytest.raises(ERRORS_BY_CODE[err.code], match=err.match):
        generate_pyarrow_schema(schema)


@pytest.mark.parametrize(
    "forbidden_leaf_schema",
    [
        pytest.param({"type": "string", "format": "date"}, id="format-date"),
        pytest.param({"type": "string", "contentEncoding": "base64"}, id="contentEncoding-base64"),
        pytest.param({"type": "string", "contentEncoding": "base16"}, id="contentEncoding-base16"),
    ],
)
def test_generate_pyarrow_schema_fail_non_json_serializable_type_under_recursion(
    forbidden_leaf_schema: dict[str, str],
) -> None:
    """Date and Base64/Base16 types inside a recursive $defs cycle raise RecursiveNonJsonTypeError (E10)."""
    schema: dict[str, Any] = {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "properties": {"root": {"$ref": "#/$defs/Node"}},
        "$defs": {
            "Node": {
                "type": "object",
                "properties": {"payload": forbidden_leaf_schema, "child": {"$ref": "#/$defs/Node"}},
            },
        },
    }
    with pytest.raises(RecursiveNonJsonTypeError, match="recursive"):
        generate_pyarrow_schema(schema)


@pytest.mark.parametrize(
    ("format_map", "expected_schema"),
    [
        pytest.param(
            {"date-time": pa.timestamp("us", tz="UTC")},
            pa.schema(
                [
                    pa.field(
                        "created_at",
                        pa.timestamp("us", tz="UTC"),
                        nullable=False,
                        metadata={"jsonschema_type": "string (date-time)"},
                    ),
                    pa.field(
                        "updated_at",
                        pa.timestamp("us", tz="UTC"),
                        nullable=False,
                        metadata={"jsonschema_type": "string (date-time)"},
                    ),
                    pa.field(
                        "recorded_on",
                        pa.date32(),
                        nullable=False,
                        metadata={"jsonschema_type": "string (date)"},
                    ),
                ]
            ),
            id="timestamp-format-override",
        ),
        pytest.param(
            frozendict(),
            pa.schema(
                [
                    pa.field(
                        "created_at",
                        pa.string(),
                        nullable=False,
                        metadata={"jsonschema_type": "string (date-time)"},
                    ),
                    pa.field(
                        "updated_at",
                        pa.string(),
                        nullable=False,
                        metadata={"jsonschema_type": "string (date-time)"},
                    ),
                    pa.field(
                        "recorded_on",
                        pa.string(),
                        nullable=False,
                        metadata={"jsonschema_type": "string (date)"},
                    ),
                ]
            ),
            id="empty-format-map",
        ),
    ],
)
def test_jsonschema_to_pyarrow_pass_format_map_overrides_direct_and_ref_fields(
    format_map: dict[str, pa.DataType] | frozendict[str, pa.DataType],
    expected_schema: pa.Schema,
) -> None:
    """Custom format_map overrides apply to both inline and $ref-referenced formatted string fields."""
    schema: dict[str, Any] = {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "required": ["created_at", "updated_at", "recorded_on"],
        "properties": {
            "created_at": {"type": "string", "format": "date-time"},
            "updated_at": {"$ref": "#/$defs/IsoTimestamp"},
            "recorded_on": {"type": "string", "format": "date"},
        },
        "$defs": {
            "IsoTimestamp": {"type": "string", "format": "date-time"},
        },
    }
    converted = JSONSchemaToPyArrow(format_map=format_map).convert(schema)
    assert converted.equals(expected_schema, check_metadata=True)


@pytest.mark.parametrize(
    ("leaf_name", "leaf_format", "converter_kwargs"),
    [
        pytest.param("ts", "date-time", {}, id="default-datetime-string"),
        pytest.param("day", "date", {"format_map": {"date": pa.string()}}, id="date-overridden-to-string"),
    ],
)
def test_jsonschema_to_pyarrow_pass_format_map_string_formats_under_recursion(
    leaf_name: str,
    leaf_format: str,
    converter_kwargs: dict[str, dict[str, pa.DataType]],
) -> None:
    """Recursive $defs cycles succeed when formatted string fields map to JSON-serializable pa.string()."""
    recursive_schema: dict[str, Any] = {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "properties": {"root": {"$ref": "#/$defs/Node"}},
        "$defs": {
            "Node": {
                "type": "object",
                "properties": {
                    leaf_name: {"type": "string", "format": leaf_format},
                    "child": {"$ref": "#/$defs/Node"},
                },
            }
        },
    }
    converted = JSONSchemaToPyArrow(**converter_kwargs).convert(recursive_schema)
    assert converted.field("root").type.field(leaf_name).type == pa.string()


def test_jsonschema_to_pyarrow_fail_format_map_non_json_type_under_recursion() -> None:
    """Custom format_map mapping date-time to pa.timestamp raises RecursiveNonJsonTypeError under recursion."""
    dt_recursive_schema: dict[str, Any] = {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "properties": {"root": {"$ref": "#/$defs/Node"}},
        "$defs": {
            "Node": {
                "type": "object",
                "properties": {
                    "ts": {"type": "string", "format": "date-time"},
                    "child": {"$ref": "#/$defs/Node"},
                },
            }
        },
    }
    with pytest.raises(RecursiveNonJsonTypeError, match="date-time"):
        JSONSchemaToPyArrow(format_map={"date-time": pa.timestamp("us", tz="UTC")}).convert(dt_recursive_schema)
