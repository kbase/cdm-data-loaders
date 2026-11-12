"""Specification and unit tests for PyArrow-to-LinkML schema generation and Arrow-to-LinkML record serialization.

Cases, rules, and error fixtures are declared in manifests under
``specs/schema_converters/pyarrow_to_linkml`` (``rules.yaml``, ``*/case.yaml``, and
``spec/errors/manifest.yaml``). The same manifests generate ``docs/pyarrow_to_linkml_specification.md`` via
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
from linkml.generators.pydanticgen import PydanticGenerator
from linkml.linter.linter import Linter
from linkml_runtime.linkml_model.meta import SchemaDefinition, SlotDefinition
from linkml_runtime.loaders import yaml_loader
from linkml_runtime.utils.formatutils import camelcase

from cdm_data_loaders.coercion.pyarrow import (
    serialize_record_for_linkml,
    serialize_table_for_linkml,
)
from cdm_data_loaders.converters.facades.pyarrow_to_linkml import (
    ERROR_BY_CODE,
    PYARROW_TO_LINKML_ERRORS,
    EmptyFieldNameError,
    InvalidPyArrowSchemaError,
    NestedMultivaluedSlotError,
    PyArrowToLinkML,
    PyArrowToLinkMLError,
    UnsupportedArrowTypeError,
    convert_field,
    convert_type,
    generate_linkml_schema,
    generate_linkml_yaml,
    resolve_scalar_type,
)
from scripts.gen_converter_spec import (
    PYARROW_TO_LINKML,
    render,
    render_pyarrow_to_linkml_scalar_table,
)
from tests.conftest import BASE_DIR
from tests.converter_spec import Case, ErrorCase, RuleCatalogue, Run, load_cases, load_error_cases, load_rules
from tests.pyarrow_spec_helpers import load_fixture_attr, load_input_schema, load_input_table

PYARROW_LML_DATA_DIR: Final[Path] = (BASE_DIR / "specs" / "schema_converters" / "pyarrow_to_linkml").resolve()
RULES: Final[RuleCatalogue] = load_rules(PYARROW_LML_DATA_DIR / "rules.yaml")
CASES: Final[tuple[Case, ...]] = tuple(
    load_cases(PYARROW_LML_DATA_DIR / "spec") + load_cases(PYARROW_LML_DATA_DIR / "examples")
)
ERROR_CASES: Final[tuple[ErrorCase, ...]] = tuple(
    load_error_cases(PYARROW_LML_DATA_DIR / "spec" / "errors" / "manifest.yaml")
)
ERRORS_BY_CODE: Final[frozendict[str, type[PyArrowToLinkMLError]]] = ERROR_BY_CODE

RUN_PARAMS: Final[tuple[ParameterSet, ...]] = tuple(
    pytest.param(case, run, id=f"{case.suite}-{case.name}-{run.id}") for case in CASES for run in case.runs
)
ERROR_PARAMS: Final[tuple[ParameterSet, ...]] = tuple(
    pytest.param(err, id=f"{err.code}-{err.file.stem}") for err in ERROR_CASES
)


class _CustomAccessionExtensionType(pa.ExtensionType):
    """Minimal user-defined PyArrow extension type for LinkML rejection tests."""

    def __init__(self) -> None:
        """Initialize the extension type backed by UTF-8 strings."""
        super().__init__(pa.string(), "cdm.test.custom_accession")

    def __arrow_ext_serialize__(self) -> bytes:
        """Serialize extension metadata bytes.

        :return: Empty serialized metadata payload.
        :rtype: bytes
        """
        return b""

    @classmethod
    def __arrow_ext_deserialize__(cls, storage_type: pa.DataType, serialized: bytes) -> "_CustomAccessionExtensionType":
        """Deserialize extension metadata bytes into an instance.

        :param storage_type: Underlying Arrow storage type.
        :type storage_type: pa.DataType
        :param serialized: Serialized metadata payload.
        :type serialized: bytes
        :return: Reconstructed extension type instance.
        :rtype: _CustomAccessionExtensionType
        """
        _ = (storage_type, serialized)
        return cls()


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
    on_disk_errors = set((PYARROW_LML_DATA_DIR / "spec" / "errors").glob("*.py"))
    assert sorted(str(p) for p in on_disk_errors - listed_errors) == []

    assert len(ERRORS_BY_CODE) == len(PYARROW_TO_LINKML_ERRORS)
    assert all(cls.code and cls.title and cls.__doc__ for cls in PYARROW_TO_LINKML_ERRORS)
    manifest_codes = {err.code for err in ERROR_CASES}
    assert sorted(manifest_codes - ERRORS_BY_CODE.keys()) == []
    assert sorted(ERRORS_BY_CODE.keys() - manifest_codes) == []


@pytest.mark.docs
def test_specification_document_pass_up_to_date() -> None:
    """Verify the committed specification document and scalar table match scripts/gen_converter_spec.py."""
    assert PYARROW_TO_LINKML.output.read_text(encoding="utf-8") == render(PYARROW_TO_LINKML), (
        "docs are stale: run `uv run python -m scripts.gen_converter_spec`"
    )
    scalar_table = render_pyarrow_to_linkml_scalar_table()
    assert "| `pa.uint8()` | `int` | `integer` | `0` .. `255` |" in scalar_table
    assert "| `pa.decimal128(10, 2)` | `str` (fixed-point decimal) | `decimal` | — |" in scalar_table
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
def test_generate_linkml_schema_pass_cases(case: Case, run: Run) -> None:
    """Verify generate_linkml_schema and PyArrowToLinkML emit valid LinkML models matching expected_schema.yaml."""
    pa_schema = load_input_schema(case.schema, run.schema_attr)
    expected_yaml = case.expected_schema.read_text(encoding="utf-8")

    lml_schema = generate_linkml_schema(pa_schema, **run.params)
    lml_yaml = generate_linkml_yaml(pa_schema, **run.params)
    assert lml_yaml == expected_yaml
    assert yaml_loader.loads(lml_yaml, target_class=SchemaDefinition) == lml_schema
    assert list(Linter.validate_schema(str(case.expected_schema))) == []

    converter = PyArrowToLinkML(**run.params)
    assert converter.convert(pa_schema) == lml_schema
    assert converter.convert_to_yaml(pa_schema) == expected_yaml


@pytest.mark.parametrize(("case", "run"), RUN_PARAMS)
def test_serialize_table_and_record_for_linkml_pass_cases(case: Case, run: Run) -> None:
    """Verify serialized Arrow table rows match expected.json and validate against compiled LinkML Pydantic models."""
    pa_schema = load_input_schema(case.schema, run.schema_attr)
    table = load_input_table(run.data, run.table_attr)
    expected_rows = _load_json_records(run.expected_json)

    rows = serialize_table_for_linkml(table)
    assert rows == expected_rows

    lml_schema = generate_linkml_schema(pa_schema, **run.params)
    root_cls = list(lml_schema.classes.keys())[-1]
    pyd_module = PydanticGenerator(lml_schema, extra_fields="forbid").compile_module()
    pyd_model = getattr(pyd_module, camelcase(root_cls))

    for idx, row in enumerate(rows):
        pyd_model.model_validate(row)
        py_record = table.slice(idx, 1).to_pylist()[0]
        record_row = serialize_record_for_linkml(py_record, pa_schema)
        pyd_model.model_validate(record_row)


@pytest.mark.parametrize("err", ERROR_PARAMS)
def test_generate_linkml_schema_fail_error_manifest(err: ErrorCase) -> None:
    """Verify each error fixture raises the expected PyArrowToLinkMLError subclass."""
    bad_input: Any = load_fixture_attr(err.file, "SCHEMA", object)
    with pytest.raises(ERRORS_BY_CODE[err.code], match=err.match):
        generate_linkml_schema(bad_input)


def test_convert_type_field_and_resolve_scalar_type_pass() -> None:
    """Verify standalone type, scalar, and field LinkML slot conversion entry points."""
    assert resolve_scalar_type(pa.int8()) == SlotDefinition(
        name="node",
        range="integer",
        minimum_value=-128,
        maximum_value=127,
    )
    assert resolve_scalar_type(pa.dictionary(pa.int8(), pa.uint8())) == SlotDefinition(
        name="node",
        range="integer",
        minimum_value=0,
        maximum_value=255,
    )
    assert convert_type(pa.list_(pa.uint8())) == SlotDefinition(
        name="node",
        range="integer",
        multivalued=True,
    )
    assert convert_type(pa.list_(pa.float32(), 4)) == SlotDefinition(
        name="node",
        range="float",
        multivalued=True,
        minimum_cardinality=4,
        maximum_cardinality=4,
    )
    assert convert_type(pa.map_(pa.string(), pa.int64())) == SlotDefinition(
        name="node",
        range="Node_node_map",
        multivalued=True,
    )
    field = pa.field("contig_len", pa.uint32(), nullable=False, metadata={"comment": "Length in bp."})
    assert convert_field(field) == SlotDefinition(
        name="contig_len",
        range="integer",
        required=True,
        minimum_value=0,
        maximum_value=4294967295,
        description="Length in bp.",
    )


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


def test_fragment_and_extension_type_apis_fail_invalid_inputs() -> None:
    """Verify fragment helpers translate empty field names, nested multivalued lists, and custom extension types."""
    with pytest.raises(EmptyFieldNameError, match="non-empty"):
        convert_field(pa.field("", pa.string()))
    with pytest.raises(NestedMultivaluedSlotError, match="nested 'array'"):
        convert_type(pa.list_(pa.list_(pa.string())))

    ext_schema = pa.schema([pa.field("accession", _CustomAccessionExtensionType(), nullable=False)])
    with pytest.raises(UnsupportedArrowTypeError, match="_CustomAccessionExtensionType"):
        generate_linkml_schema(ext_schema)


def test_generate_linkml_schema_pass_destination_sanitization_and_file_output(tmp_path: Path) -> None:
    """Verify multi-table destination grouping, leading-digit/punctuation title sanitization, and file output."""
    destination = SchemaDefinition(
        id="https://kbase.us/linkml/cdm_namespace",
        name="cdm_namespace",
        imports=["linkml:types"],
        default_prefix="cdm_namespace",
        prefixes={
            "cdm_namespace": "https://kbase.us/linkml/cdm_namespace#",
            "linkml": "https://w3id.org/linkml/",
        },
    )
    genomes = pa.schema(
        [pa.field("genome_id", pa.string(), nullable=False)],
        metadata={"title": "GenomeTable", "comment": "Genomes table."},
    )
    contigs = pa.schema(
        [pa.field("contig_id", pa.string(), nullable=False)],
        metadata={"title": "ContigTable", "comment": "Contigs table."},
    )
    assert generate_linkml_schema(genomes, destination=destination) is destination
    assert generate_linkml_schema(contigs, destination=destination) is destination
    assert set(destination.classes) == {"GenomeTable", "ContigTable"}
    assert destination.classes["GenomeTable"].description == "Genomes table."
    assert destination.classes["ContigTable"].description == "Contigs table."

    dest_path = tmp_path / "cdm_namespace.yaml"
    dest_path.write_text(
        PyArrowToLinkML(schema_name="cdm_namespace").convert_to_yaml(genomes),
        encoding="utf-8",
    )
    assert list(Linter.validate_schema(str(dest_path))) == []

    leading_digit_path = tmp_path / "leading_digit.yaml"
    PyArrowToLinkML(schema_name="123 report").convert_to_yaml_file(genomes, leading_digit_path)
    leading_loaded = yaml_loader.loads(leading_digit_path.read_text(encoding="utf-8"), target_class=SchemaDefinition)
    assert leading_loaded.name == "_123_report"
    assert "_123_report" in leading_loaded.classes

    punct_path = tmp_path / "nested" / "punct_only.yaml"
    assert PyArrowToLinkML(schema_name="!!!").convert_to_yaml_file(genomes, punct_path) == punct_path
    punct_loaded = yaml_loader.loads(punct_path.read_text(encoding="utf-8"), target_class=SchemaDefinition)
    assert punct_loaded.name == "schema"
    assert list(Linter.validate_schema(str(punct_path))) == []


def test_serialize_for_linkml_fail_null_list_items_and_nested_multivalued() -> None:
    """Verify LinkML serialization rejects null list items, nested lists/maps, and non-finite floats."""
    nullable_list_schema = pa.schema([pa.field("tags", pa.list_(pa.field("item", pa.string(), nullable=True)))])
    bad_list_table = pa.table({"tags": [["ok", None]]}, schema=nullable_list_schema)
    with pytest.raises(ValueError, match="null list element"):
        serialize_table_for_linkml(bad_list_table)
    with pytest.raises(ValueError, match="null list element"):
        serialize_record_for_linkml({"tags": ["ok", None]}, nullable_list_schema)

    nested_list_schema = pa.schema([pa.field("matrix", pa.list_(pa.list_(pa.int32())))])
    nested_list_table = pa.table({"matrix": [[[1, 2]]]}, schema=nested_list_schema)
    with pytest.raises(ValueError, match="nested list or map items"):
        serialize_table_for_linkml(nested_list_table)

    nested_map_schema = pa.schema([pa.field("maps", pa.list_(pa.map_(pa.string(), pa.int32())))])
    nested_map_table = pa.table({"maps": [[[("k", 1)]]]}, schema=nested_map_schema)
    with pytest.raises(ValueError, match="nested list or map items"):
        serialize_table_for_linkml(nested_map_table)

    float_schema = pa.schema([pa.field("ratio", pa.float32(), nullable=False)])
    with pytest.raises(ValueError, match="Non-finite float"):
        serialize_record_for_linkml({"ratio": math.nan}, float_schema)
