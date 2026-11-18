"""Specification and unit tests for PyArrow-to-JSON Schema generation and Arrow-to-JSON record serialization.

Cases, rules, and error fixtures are declared in manifests under
``specs/schema_converters/pyarrow_to_jsonschema`` (``rules.yaml``, ``*/case.yaml``, and
``spec/errors/manifest.yaml``). The same manifests render ``docs/pyarrow_to_jsonschema_specification.md`` via
``docs_macros`` Jinja2 templates.
"""

import functools
import math
from collections.abc import Callable
from pathlib import Path
from typing import Any, Final

import pyarrow as pa
import pytest
from _pytest.mark.structures import ParameterSet
from frozendict import frozendict
from jsonschema import Draft202012Validator, FormatChecker
from pydantic import ValidationError

from cdm_data_loaders.coercion.arrow_to_records import (
    serialize_record_for_jsonschema,
    serialize_table_for_jsonschema,
)
from cdm_data_loaders.converters.core.extensions import ExtensionRegistry
from cdm_data_loaders.converters.exceptions.facades.pyarrow_to_jsonschema import (
    ERROR_BY_CODE,
    InvalidPyArrowSchemaError,
    PyArrowToJSONSchemaError,
    UnsupportedArrowTypeError,
)
from cdm_data_loaders.converters.facades.pyarrow_to_jsonschema import (
    PyArrowToJSONSchema,
    convert_field,
    convert_type,
    generate_json_schema,
    resolve_scalar_type,
)
from specs._loader.converter_spec import Case, ErrorCase, Run, load_cases, load_error_cases
from tests.cdm_data_loaders.converters._helpers import (
    JSON_SCHEMA_KEYWORDS,
    iter_extension_keys,
    iter_schema_keywords,
)
from tests.helpers.paths_and_constants import BASE_DIR
from tests.helpers.pyarrow_spec import (
    load_fixture_attr,
    load_input_schema,
    load_input_table,
    load_json_object,
    load_json_records,
)

PYARROW_JS_DATA_DIR: Final[Path] = (BASE_DIR / "specs" / "schema_converters" / "pyarrow_to_jsonschema").resolve()
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
    expected_schema = load_json_object(case.expected_schema)

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
    expected_rows = load_json_records(run.expected_json)
    js_schema = load_json_object(case.expected_schema)

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


@pytest.mark.parametrize(
    ("dtype", "kwargs", "expected"),
    [
        pytest.param(
            pa.uint16(),
            {"include_pyarrow_metadata": False},
            {"type": "integer", "minimum": 0, "maximum": 65535},
            id="uint16-without-metadata",
        ),
        pytest.param(
            pa.dictionary(pa.int8(), pa.string(), ordered=True),
            {},
            {
                "type": "string",
                "x-pyarrow": {
                    "arrow_type": "dictionary",
                    "index_type": "int8",
                    "value_type": "string",
                    "ordered": True,
                },
            },
            id="ordered-dictionary-with-metadata",
        ),
    ],
)
def test_resolve_scalar_type_pass_metadata_options(
    dtype: pa.DataType,
    kwargs: dict[str, bool],
    expected: dict[str, Any],
) -> None:
    """Verify resolve_scalar_type emits expected JSON Schema fragments with and without x-pyarrow metadata."""
    assert resolve_scalar_type(dtype, **kwargs) == expected


def test_convert_type_pass_nullable_fixed_size_binary() -> None:
    """Verify convert_type wraps nullable fixed-size binary types in anyOf with x-pyarrow metadata."""
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


def test_convert_field_pass_nullable_with_metadata() -> None:
    """Verify convert_field maps Arrow field comments to description and retains custom x-pyarrow metadata."""
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


@pytest.mark.parametrize(
    ("call", "error_type", "match"),
    [
        pytest.param(
            functools.partial(PyArrowToJSONSchema, extension_registry="not-a-registry"),  # type: ignore[arg-type]
            ValidationError,
            "ExtensionRegistry",
            id="invalid-extension-registry",
        ),
        pytest.param(
            functools.partial(
                PyArrowToJSONSchema(extension_registry=ExtensionRegistry()).convert,
                pa.schema([pa.field("id", pa.string(), nullable=False)]),
            ),
            PyArrowToJSONSchemaError,
            "x-pyarrow",
            id="facade-unregistered-x-pyarrow",
        ),
        pytest.param(
            functools.partial(convert_type, pa.string(), extension_registry=ExtensionRegistry()),
            PyArrowToJSONSchemaError,
            "x-pyarrow",
            id="convert-type-unregistered-x-pyarrow",
        ),
        pytest.param(
            functools.partial(convert_field, pa.field("id", pa.string()), extension_registry=ExtensionRegistry()),
            PyArrowToJSONSchemaError,
            "x-pyarrow",
            id="convert-field-unregistered-x-pyarrow",
        ),
        pytest.param(
            functools.partial(
                generate_json_schema,
                pa.schema([pa.field("id", pa.string(), nullable=False)]),
                destination={},
            ),
            PyArrowToJSONSchemaError,
            "requires a named document",
            id="unnamed-destination-document",
        ),
        pytest.param(
            functools.partial(
                generate_json_schema,
                pa.schema([pa.field("custom", _CustomLabelExtensionType(), nullable=False)]),
            ),
            UnsupportedArrowTypeError,
            "_CustomLabelExtensionType",
            id="custom-arrow-extension-type",
        ),
    ],
)
def test_facade_and_fragment_apis_fail_invalid_configuration_and_extension_types(
    call: Callable[[], object],
    error_type: type[Exception],
    match: str,
) -> None:
    """Verify registry validation, unnamed destination schemas, and custom pa.ExtensionType rejection."""
    with pytest.raises(error_type, match=match):
        call()


def test_generate_json_schema_pass_destination_accumulates_defs() -> None:
    """Verify generate_json_schema accumulates multiple named schemas under $defs in destination."""
    schema_a = pa.schema([pa.field("id", pa.string(), nullable=False)])
    schema_b = pa.schema([pa.field("count", pa.int32(), nullable=False)], metadata={"title": "CountsTable"})

    destination: dict[str, Any] = {"$schema": "https://json-schema.org/draft/2020-12/schema"}
    assert generate_json_schema(schema_a, schema_name="IdsTable", destination=destination) is destination
    assert generate_json_schema(schema_b, destination=destination) is destination
    assert set(destination["$defs"]) == {"IdsTable", "CountsTable"}
    assert "$schema" not in destination["$defs"]["IdsTable"]
    assert "$schema" not in destination["$defs"]["CountsTable"]
    Draft202012Validator.check_schema(destination)


def test_convert_to_json_file_pass_writes_schema_file(tmp_path: Path) -> None:
    """Verify PyArrowToJSONSchema.convert_to_json_file writes valid JSON Schema output to disk."""
    schema = pa.schema([pa.field("id", pa.string(), nullable=False)])
    out_file = tmp_path / "schemas" / "ids_table.schema.json"
    converter = PyArrowToJSONSchema(schema_name="IdsTable", schema_id="https://example.org/ids")
    assert converter.convert_to_json_file(schema, out_file) == out_file
    loaded = load_json_object(out_file)
    assert loaded["title"] == "IdsTable"
    assert loaded["$id"] == "https://example.org/ids"


def test_serialize_record_for_jsonschema_pass_dict_map() -> None:
    """Verify serialize_record_for_jsonschema normalizes Python dict map fields into key-value entry lists."""
    map_schema = pa.schema([pa.field("attrs", pa.map_(pa.string(), pa.int32()), nullable=False)])
    serialized = serialize_record_for_jsonschema({"attrs": {"alpha": 1, "beta": 2}}, map_schema)
    assert serialized == {"attrs": [{"key": "alpha", "value": 1}, {"key": "beta", "value": 2}]}


@pytest.mark.parametrize(
    ("record", "schema", "match"),
    [
        pytest.param(
            {"val": math.inf},
            pa.schema([pa.field("val", pa.float64(), nullable=False)]),
            "Non-finite float",
            id="non-finite-float",
        ),
        pytest.param(
            {"attrs": [(1, 2, 3)]},
            pa.schema([pa.field("attrs", pa.map_(pa.string(), pa.int32()), nullable=False)]),
            "Invalid map entry",
            id="invalid-map-entry-tuple",
        ),
    ],
)
def test_serialize_record_for_jsonschema_fail_edge_cases(
    record: dict[str, Any],
    schema: pa.Schema,
    match: str,
) -> None:
    """Verify serialize_record_for_jsonschema rejects non-finite floats and malformed map tuples."""
    with pytest.raises(ValueError, match=match):
        serialize_record_for_jsonschema(record, schema)


@pytest.mark.parametrize(
    ("bad_table", "error_type", "match"),
    [
        pytest.param({"not": "a_table"}, TypeError, r"pyarrow\.Table", id="non-table-mapping"),
        pytest.param(
            pa.table(
                {"val": pa.array([math.nan], type=pa.float64())},
                schema=pa.schema([pa.field("val", pa.float64(), nullable=False)]),
            ),
            ValueError,
            "Non-finite float",
            id="nan-float-column",
        ),
    ],
)
def test_serialize_table_for_jsonschema_fail_edge_cases(
    bad_table: object,
    error_type: type[Exception],
    match: str,
) -> None:
    """Verify serialize_table_for_jsonschema rejects non-Table inputs and non-finite float columns."""
    with pytest.raises(error_type, match=match):
        serialize_table_for_jsonschema(bad_table)  # type: ignore[arg-type]
