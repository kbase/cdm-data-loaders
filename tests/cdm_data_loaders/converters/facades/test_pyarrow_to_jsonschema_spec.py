"""Specification and unit tests for PyArrow-to-JSON Schema generation and Arrow-to-JSON record serialization.

Cases, rules, and error fixtures are declared in manifests under
``specs/schema_converters/pyarrow_to_jsonschema`` (``rules.yaml``, ``*/case.yaml``, and
``spec/errors/manifest.yaml``). The same manifests generate ``docs/pyarrow_to_jsonschema_specification.md`` via
``scripts/gen_converter_spec.py``.
"""

import json
import math
from pathlib import Path
from typing import Any, Final

import pyarrow as pa
import pytest
from _pytest.mark.structures import ParameterSet
from frozendict import frozendict
from jsonschema import Draft202012Validator, FormatChecker
from pydantic import ValidationError

from cdm_data_loaders.coercion.pyarrow import (
    serialize_record_for_jsonschema,
    serialize_table_for_jsonschema,
)
from cdm_data_loaders.converters.core.extensions import ExtensionRegistry
from cdm_data_loaders.converters.facades.pyarrow_to_jsonschema import (
    ERROR_BY_CODE,
    PYARROW_TO_JSONSCHEMA_ERRORS,
    InvalidPyArrowSchemaError,
    PyArrowToJSONSchema,
    PyArrowToJSONSchemaError,
    UnsupportedArrowTypeError,
    convert_field,
    convert_type,
    generate_json_schema,
    resolve_scalar_type,
)
from scripts.gen_converter_spec import (
    PYARROW_TO_JSONSCHEMA,
    render,
    render_pyarrow_to_jsonschema_scalar_table,
)
from tests.cdm_data_loaders.converters.conftest import (
    JSON_SCHEMA_KEYWORDS,
    iter_extension_keys,
    iter_schema_keywords,
)
from tests.conftest import BASE_DIR
from tests.converter_spec import Case, ErrorCase, RuleCatalogue, Run, load_cases, load_error_cases, load_rules
from tests.pyarrow_spec_helpers import load_fixture_attr, load_input_schema, load_input_table

PYARROW_JS_DATA_DIR: Final[Path] = (BASE_DIR / "specs" / "schema_converters" / "pyarrow_to_jsonschema").resolve()
RULES: Final[RuleCatalogue] = load_rules(PYARROW_JS_DATA_DIR / "rules.yaml")
CASES: Final[tuple[Case, ...]] = tuple(
    load_cases(PYARROW_JS_DATA_DIR / "spec") + load_cases(PYARROW_JS_DATA_DIR / "examples")
)
ERROR_CASES: Final[tuple[ErrorCase, ...]] = tuple(
    load_error_cases(PYARROW_JS_DATA_DIR / "spec" / "errors" / "manifest.yaml")
)
ERRORS_BY_CODE: Final[frozendict[str, type[PyArrowToJSONSchemaError]]] = ERROR_BY_CODE

RUN_PARAMS: Final[tuple[ParameterSet, ...]] = tuple(
    pytest.param(case, run, id=f"{case.suite}-{case.name}-{run.id}") for case in CASES for run in case.runs
)
ERROR_PARAMS: Final[tuple[ParameterSet, ...]] = tuple(
    pytest.param(err, id=f"{err.code}-{err.file.stem}") for err in ERROR_CASES
)


class _CustomLabelExtensionType(pa.ExtensionType):
    """Minimal user-defined PyArrow extension type for rejection tests."""

    def __init__(self) -> None:
        """Initialize the extension type backed by UTF-8 strings."""
        super().__init__(pa.string(), "cdm.test.custom_label")

    def __arrow_ext_serialize__(self) -> bytes:
        """Serialize extension metadata bytes.

        :return: Empty serialized metadata payload.
        :rtype: bytes
        """
        return b""

    @classmethod
    def __arrow_ext_deserialize__(cls, storage_type: pa.DataType, serialized: bytes) -> "_CustomLabelExtensionType":
        """Deserialize extension metadata bytes into an instance.

        :param storage_type: Underlying Arrow storage type.
        :type storage_type: pa.DataType
        :param serialized: Serialized metadata payload.
        :type serialized: bytes
        :return: Reconstructed extension type instance.
        :rtype: _CustomLabelExtensionType
        """
        _ = (storage_type, serialized)
        return cls()


def _load_json_object(path: Path) -> dict[str, Any]:
    """Load a JSON object dictionary from disk.

    :param path: Path to the JSON file.
    :type path: Path
    :return: Parsed JSON dictionary.
    :rtype: dict[str, Any]
    :raises TypeError: If the root JSON value is not an object.
    """
    loaded = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(loaded, dict):
        msg = f"Expected a JSON object in {path}, got {type(loaded)}"
        raise TypeError(msg)
    return loaded


def _load_json_records(path: Path) -> list[dict[str, Any]]:
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


def test_manifests_pass_consistency() -> None:
    """Verify rule coverage, fixture file existence, and error code coverage."""
    unknown = {f"{case.suite}/{case.name}": sorted(set(case.rules) - RULES.rules.keys()) for case in CASES}
    assert {name: ids for name, ids in unknown.items() if ids} == {}

    used_rules = {rule_id for case in CASES for rule_id in case.rules}
    assert sorted(RULES.rules.keys() - used_rules) == []

    paths = [p for case in CASES for p in (case.schema, case.expected_schema)]
    paths += [p for case in CASES for run in case.runs for p in (run.data, run.expected_json, run.expected_table)]
    paths += [err.file for err in ERROR_CASES]
    assert [str(p) for p in paths if not p.is_file()] == []

    listed_errors = {err.file for err in ERROR_CASES}
    on_disk_errors = set((PYARROW_JS_DATA_DIR / "spec" / "errors").glob("*.py"))
    assert sorted(str(p) for p in on_disk_errors - listed_errors) == []

    assert len(ERRORS_BY_CODE) == len(PYARROW_TO_JSONSCHEMA_ERRORS)
    assert all(cls.code and cls.title and cls.__doc__ for cls in PYARROW_TO_JSONSCHEMA_ERRORS)
    manifest_codes = {err.code for err in ERROR_CASES}
    assert sorted(manifest_codes - ERRORS_BY_CODE.keys()) == []
    assert sorted(ERRORS_BY_CODE.keys() - manifest_codes) == []


@pytest.mark.docs
def test_specification_document_pass_up_to_date() -> None:
    """Verify the committed specification document and scalar table match scripts/gen_converter_spec.py."""
    assert PYARROW_TO_JSONSCHEMA.output.read_text(encoding="utf-8") == render(PYARROW_TO_JSONSCHEMA), (
        "docs are stale: run `uv run python -m scripts.gen_converter_spec`"
    )
    scalar_table = render_pyarrow_to_jsonschema_scalar_table()
    assert "| `pa.int64()` |" in scalar_table
    assert "| `pa.decimal128(10, 2)` |" in scalar_table
    assert '| `pa.timestamp("us", tz="UTC")` |' in scalar_table


@pytest.mark.parametrize(("case", "run"), RUN_PARAMS)
def test_fixtures_pass_table_and_ipc_roundtrip(case: Case, run: Run) -> None:
    """Verify each fixture table validates against its input schema and round-trips losslessly through Arrow IPC."""
    pa_schema = load_input_schema(case.schema, run.schema_attr)
    table = load_input_table(run.data, run.table_attr)
    table.validate(full=True)
    assert table.num_rows == run.expected_count
    assert table.schema.equals(pa_schema, check_metadata=True)

    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    roundtripped = pa.ipc.open_stream(sink.getvalue()).read_all()
    assert roundtripped.equals(table, check_metadata=True)


@pytest.mark.parametrize(("case", "run"), RUN_PARAMS)
def test_generate_json_schema_pass_cases(case: Case, run: Run) -> None:
    """Verify generate_json_schema and PyArrowToJSONSchema produce valid Draft 2020-12 schemas matching expected JSON."""
    pa_schema = load_input_schema(case.schema, run.schema_attr)
    expected_schema = _load_json_object(case.expected_schema)

    actual = generate_json_schema(pa_schema, **run.params)
    assert actual == expected_schema
    Draft202012Validator.check_schema(actual)

    non_standard_keywords = {
        kw for kw in iter_schema_keywords(actual) if kw not in JSON_SCHEMA_KEYWORDS and not kw.startswith("x-")
    }
    assert non_standard_keywords == set()

    extension_keys = list(iter_extension_keys(actual))
    if run.params.get("include_pyarrow_metadata") is False:
        assert extension_keys == []
    else:
        assert "x-pyarrow" in extension_keys

    converter = PyArrowToJSONSchema(**run.params)
    assert converter.convert(pa_schema) == expected_schema
    assert converter.convert_to_json(pa_schema) == case.expected_schema.read_text(encoding="utf-8")


@pytest.mark.parametrize(("case", "run"), RUN_PARAMS)
def test_serialize_table_and_record_for_jsonschema_pass_cases(case: Case, run: Run) -> None:
    """Verify serialized Arrow table rows match expected.json and validate against the generated JSON Schema."""
    pa_schema = load_input_schema(case.schema, run.schema_attr)
    table = load_input_table(run.data, run.table_attr)
    expected_rows = _load_json_records(run.expected_json)
    js_schema = _load_json_object(case.expected_schema)

    rows = serialize_table_for_jsonschema(table)
    assert rows == expected_rows

    validator = Draft202012Validator(
        js_schema,
        format_checker=FormatChecker(formats=["date", "date-time"]),
    )
    for idx, row in enumerate(rows):
        validator.validate(row)
        py_record = table.slice(idx, 1).to_pylist()[0]
        record_row = serialize_record_for_jsonschema(py_record, pa_schema)
        validator.validate(record_row)


@pytest.mark.parametrize("err", ERROR_PARAMS)
def test_generate_json_schema_fail_error_manifest(err: ErrorCase) -> None:
    """Verify each error fixture raises the expected PyArrowToJSONSchemaError subclass."""
    bad_input: Any = load_fixture_attr(err.file, "SCHEMA", object)
    with pytest.raises(ERRORS_BY_CODE[err.code], match=err.match):
        generate_json_schema(bad_input)


def test_convert_type_field_and_resolve_scalar_type_pass() -> None:
    """Verify standalone type, scalar, and field conversion entry points with and without x-pyarrow metadata."""
    assert resolve_scalar_type(pa.uint16(), include_pyarrow_metadata=False) == {
        "type": "integer",
        "minimum": 0,
        "maximum": 65535,
    }
    assert resolve_scalar_type(pa.dictionary(pa.int8(), pa.string(), ordered=True)) == {
        "type": "string",
        "x-pyarrow": {
            "arrow_type": "dictionary",
            "index_type": "int8",
            "value_type": "string",
            "ordered": True,
        },
    }
    assert convert_type(pa.binary(4), nullable=True, include_pyarrow_metadata=True) == {
        "anyOf": [
            {
                "type": "string",
                "contentEncoding": "base64",
                "x-pyarrow": {"arrow_type": "fixed_size_binary", "byte_width": 4},
            },
            {"type": "null"},
        ]
    }
    field = pa.field(
        "score",
        pa.float32(),
        nullable=True,
        metadata={"comment": "Normalized bitscore.", "unit": "bits"},
    )
    assert convert_field(field) == {
        "anyOf": [{"type": "number", "x-pyarrow": {"arrow_type": "float32"}}, {"type": "null"}],
        "description": "Normalized bitscore.",
        "x-pyarrow": {"metadata": {"unit": "bits"}},
    }


@pytest.mark.parametrize(
    "container_dtype",
    [
        pytest.param(pa.struct([pa.field("x", pa.int32())]), id="struct"),
        pytest.param(pa.list_(pa.int32()), id="list"),
        pytest.param(pa.large_list(pa.int32()), id="large-list"),
        pytest.param(pa.list_view(pa.int32()), id="list-view"),
        pytest.param(pa.large_list_view(pa.int32()), id="large-list-view"),
        pytest.param(pa.list_(pa.int32(), 2), id="fixed-size-list"),
        pytest.param(pa.map_(pa.string(), pa.int32()), id="map"),
    ],
)
def test_resolve_scalar_type_fail_container_types(container_dtype: pa.DataType) -> None:
    """Verify resolve_scalar_type rejects container PyArrow DataTypes with InvalidPyArrowSchemaError."""
    with pytest.raises(InvalidPyArrowSchemaError, match="container type"):
        resolve_scalar_type(container_dtype)


def test_facade_and_fragment_apis_fail_invalid_configuration_and_extension_types() -> None:
    """Verify registry validation, unnamed destination schemas, and custom pa.ExtensionType rejection."""
    with pytest.raises(ValidationError, match="ExtensionRegistry"):
        PyArrowToJSONSchema(extension_registry="not-a-registry")  # type: ignore[arg-type]

    schema = pa.schema([pa.field("id", pa.string(), nullable=False)])
    empty_registry = ExtensionRegistry()
    with pytest.raises(PyArrowToJSONSchemaError, match="x-pyarrow"):
        PyArrowToJSONSchema(extension_registry=empty_registry).convert(schema)
    with pytest.raises(PyArrowToJSONSchemaError, match="x-pyarrow"):
        convert_type(pa.string(), extension_registry=empty_registry)
    with pytest.raises(PyArrowToJSONSchemaError, match="x-pyarrow"):
        convert_field(pa.field("id", pa.string()), extension_registry=empty_registry)

    with pytest.raises(PyArrowToJSONSchemaError, match="requires a named document"):
        generate_json_schema(schema, destination={})

    ext_schema = pa.schema([pa.field("custom", _CustomLabelExtensionType(), nullable=False)])
    with pytest.raises(UnsupportedArrowTypeError, match="_CustomLabelExtensionType"):
        generate_json_schema(ext_schema)


def test_generate_json_schema_pass_destination_and_file_output(tmp_path: Path) -> None:
    """Verify generate_json_schema populates $defs in destination and convert_to_json_file writes valid JSON."""
    schema_a = pa.schema([pa.field("id", pa.string(), nullable=False)])
    schema_b = pa.schema([pa.field("count", pa.int32(), nullable=False)], metadata={"title": "CountsTable"})

    destination: dict[str, Any] = {"$schema": "https://json-schema.org/draft/2020-12/schema"}
    assert generate_json_schema(schema_a, schema_name="IdsTable", destination=destination) is destination
    assert generate_json_schema(schema_b, destination=destination) is destination
    assert set(destination["$defs"]) == {"IdsTable", "CountsTable"}
    assert "$schema" not in destination["$defs"]["IdsTable"]
    assert "$schema" not in destination["$defs"]["CountsTable"]
    Draft202012Validator.check_schema(destination)

    out_file = tmp_path / "schemas" / "ids_table.schema.json"
    converter = PyArrowToJSONSchema(schema_name="IdsTable", schema_id="https://example.org/ids")
    assert converter.convert_to_json_file(schema_a, out_file) == out_file
    loaded = _load_json_object(out_file)
    assert loaded["title"] == "IdsTable"
    assert loaded["$id"] == "https://example.org/ids"


def test_serialize_for_jsonschema_pass_and_fail_edge_cases() -> None:
    """Verify dict map normalization and error handling for non-tables, non-finite floats, and malformed maps."""
    map_schema = pa.schema([pa.field("attrs", pa.map_(pa.string(), pa.int32()), nullable=False)])
    serialized = serialize_record_for_jsonschema({"attrs": {"alpha": 1, "beta": 2}}, map_schema)
    assert serialized == {"attrs": [{"key": "alpha", "value": 1}, {"key": "beta", "value": 2}]}

    with pytest.raises(TypeError, match=r"pyarrow\.Table"):
        serialize_table_for_jsonschema({"not": "a_table"})  # type: ignore[arg-type]

    float_schema = pa.schema([pa.field("val", pa.float64(), nullable=False)])
    nan_table = pa.table({"val": pa.array([math.nan], type=pa.float64())}, schema=float_schema)
    with pytest.raises(ValueError, match="Non-finite float"):
        serialize_table_for_jsonschema(nan_table)
    with pytest.raises(ValueError, match="Non-finite float"):
        serialize_record_for_jsonschema({"val": math.inf}, float_schema)

    with pytest.raises(ValueError, match="Invalid map entry"):
        serialize_record_for_jsonschema({"attrs": [(1, 2, 3)]}, map_schema)
