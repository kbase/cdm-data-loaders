"""Specification and unit tests for PyArrow-to-LinkML schema generation and Arrow-to-LinkML record serialization.

Cases, rules, and error fixtures are declared in manifests under
``specs/schema_converters/pyarrow_to_linkml`` (``rules.yaml``, ``*/case.yaml``, and
``spec/errors/manifest.yaml``). The same manifests render ``docs/pyarrow_to_linkml_specification.md`` via
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
from linkml.generators.pydanticgen import PydanticGenerator
from linkml.linter.linter import Linter
from linkml_runtime.linkml_model.meta import SchemaDefinition, SlotDefinition
from linkml_runtime.loaders import yaml_loader
from linkml_runtime.utils.formatutils import camelcase

from cdm_data_loaders.coercion.arrow_to_records import (
    serialize_record_for_linkml,
    serialize_table_for_linkml,
)
from cdm_data_loaders.converters.exceptions.facades.pyarrow_to_linkml import (
    ERROR_BY_CODE,
    EmptyFieldNameError,
    InvalidPyArrowSchemaError,
    NestedMultivaluedSlotError,
    PyArrowToLinkMLError,
    UnsupportedArrowTypeError,
)
from cdm_data_loaders.converters.facades.pyarrow_to_linkml import (
    PyArrowToLinkML,
    convert_field,
    convert_type,
    generate_linkml_schema,
    generate_linkml_yaml,
    resolve_scalar_type,
)
from specs._loader.converter_spec import Case, ErrorCase, Run, load_cases, load_error_cases
from tests.helpers.paths_and_constants import BASE_DIR
from tests.helpers.pyarrow_spec import (
    load_fixture_attr,
    load_input_schema,
    load_input_table,
    load_json_records,
)

PYARROW_LML_DATA_DIR: Final[Path] = (BASE_DIR / "specs" / "schema_converters" / "pyarrow_to_linkml").resolve()
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
    expected_rows = load_json_records(run.expected_json)

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


@pytest.mark.parametrize(
    ("dtype", "expected"),
    [
        pytest.param(
            pa.int8(),
            SlotDefinition(
                name="node",
                range="integer",
                minimum_value=-128,
                maximum_value=127,
            ),
            id="int8",
        ),
        pytest.param(
            pa.dictionary(pa.int8(), pa.uint8()),
            SlotDefinition(
                name="node",
                range="integer",
                minimum_value=0,
                maximum_value=255,
            ),
            id="dictionary-uint8",
        ),
    ],
)
def test_resolve_scalar_type_pass_integer_and_dictionary(
    dtype: pa.DataType,
    expected: SlotDefinition,
) -> None:
    """Verify resolve_scalar_type emits expected LinkML integer SlotDefinitions for int8 and dictionary types."""
    assert resolve_scalar_type(dtype) == expected


@pytest.mark.parametrize(
    ("dtype", "expected"),
    [
        pytest.param(
            pa.list_(pa.uint8()),
            SlotDefinition(
                name="node",
                range="integer",
                multivalued=True,
            ),
            id="variable-list",
        ),
        pytest.param(
            pa.list_(pa.float32(), 4),
            SlotDefinition(
                name="node",
                range="float",
                multivalued=True,
                minimum_cardinality=4,
                maximum_cardinality=4,
            ),
            id="fixed-size-list",
        ),
        pytest.param(
            pa.map_(pa.string(), pa.int64()),
            SlotDefinition(
                name="node",
                range="Node_node_map",
                multivalued=True,
            ),
            id="map",
        ),
    ],
)
def test_convert_type_pass_list_fixed_list_and_map(
    dtype: pa.DataType,
    expected: SlotDefinition,
) -> None:
    """Verify convert_type emits multivalued LinkML SlotDefinitions for variable lists, fixed lists, and maps."""
    assert convert_type(dtype) == expected


def test_convert_field_pass_required_with_description() -> None:
    """Verify convert_field maps Arrow field bounds, nullability, and comment metadata onto a SlotDefinition."""
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


@pytest.mark.parametrize(
    ("call", "error_type", "match"),
    [
        pytest.param(
            functools.partial(convert_field, pa.field("", pa.string())),
            EmptyFieldNameError,
            "non-empty",
            id="empty-field-name",
        ),
        pytest.param(
            functools.partial(convert_type, pa.list_(pa.list_(pa.string()))),
            NestedMultivaluedSlotError,
            "nested 'array'",
            id="nested-multivalued-list",
        ),
        pytest.param(
            functools.partial(
                generate_linkml_schema,
                pa.schema([pa.field("accession", _CustomAccessionExtensionType(), nullable=False)]),
            ),
            UnsupportedArrowTypeError,
            "_CustomAccessionExtensionType",
            id="custom-arrow-extension-type",
        ),
    ],
)
def test_fragment_and_extension_type_apis_fail_invalid_inputs(
    call: Callable[[], object],
    error_type: type[Exception],
    match: str,
) -> None:
    """Verify fragment helpers translate empty field names, nested multivalued lists, and custom extension types."""
    with pytest.raises(error_type, match=match):
        call()


def test_generate_linkml_schema_pass_destination_accumulates_classes() -> None:
    """Verify generate_linkml_schema accumulates multiple Arrow table schemas into a shared SchemaDefinition."""
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


@pytest.mark.parametrize(
    ("schema_name", "expected_name"),
    [
        pytest.param("cdm_namespace", "cdm_namespace", id="valid-identifier"),
        pytest.param("123 report", "_123_report", id="leading-digit-and-space"),
        pytest.param("!!!", "schema", id="punctuation-only"),
    ],
)
def test_pyarrow_to_linkml_pass_schema_name_sanitization(
    schema_name: str,
    expected_name: str,
) -> None:
    """Verify PyArrowToLinkML sanitizes valid, leading-digit, and punctuation-only schema_name values."""
    genomes = pa.schema(
        [pa.field("genome_id", pa.string(), nullable=False)],
        metadata={"title": "GenomeTable", "comment": "Genomes table."},
    )
    converted = PyArrowToLinkML(schema_name=schema_name).convert(genomes)
    assert converted.name == expected_name
    assert expected_name in converted.classes


def test_convert_to_yaml_pass_linter_validation(tmp_path: Path) -> None:
    """Verify PyArrowToLinkML.convert_to_yaml produces LinkML YAML that passes Linter validation."""
    genomes = pa.schema(
        [pa.field("genome_id", pa.string(), nullable=False)],
        metadata={"title": "GenomeTable", "comment": "Genomes table."},
    )
    dest_path = tmp_path / "cdm_namespace.yaml"
    dest_path.write_text(
        PyArrowToLinkML(schema_name="cdm_namespace").convert_to_yaml(genomes),
        encoding="utf-8",
    )
    assert list(Linter.validate_schema(str(dest_path))) == []


def test_convert_to_yaml_file_pass_writes_and_validates(tmp_path: Path) -> None:
    """Verify PyArrowToLinkML.convert_to_yaml_file creates parent directories and writes lint-clean LinkML YAML."""
    genomes = pa.schema(
        [pa.field("genome_id", pa.string(), nullable=False)],
        metadata={"title": "GenomeTable", "comment": "Genomes table."},
    )
    punct_path = tmp_path / "nested" / "punct_only.yaml"
    assert PyArrowToLinkML(schema_name="!!!").convert_to_yaml_file(genomes, punct_path) == punct_path
    punct_loaded = yaml_loader.loads(punct_path.read_text(encoding="utf-8"), target_class=SchemaDefinition)
    assert punct_loaded.name == "schema"
    assert list(Linter.validate_schema(str(punct_path))) == []


@pytest.mark.parametrize(
    ("call", "match"),
    [
        pytest.param(
            functools.partial(
                serialize_table_for_linkml,
                pa.table(
                    {"tags": [["ok", None]]},
                    schema=pa.schema([pa.field("tags", pa.list_(pa.field("item", pa.string(), nullable=True)))]),
                ),
            ),
            "null list element",
            id="table-null-list-item",
        ),
        pytest.param(
            functools.partial(
                serialize_record_for_linkml,
                {"tags": ["ok", None]},
                pa.schema([pa.field("tags", pa.list_(pa.field("item", pa.string(), nullable=True)))]),
            ),
            "null list element",
            id="record-null-list-item",
        ),
        pytest.param(
            functools.partial(
                serialize_table_for_linkml,
                pa.table(
                    {"matrix": [[[1, 2]]]},
                    schema=pa.schema([pa.field("matrix", pa.list_(pa.list_(pa.int32())))]),
                ),
            ),
            "nested list or map items",
            id="table-nested-list",
        ),
        pytest.param(
            functools.partial(
                serialize_table_for_linkml,
                pa.table(
                    {"maps": [[[("k", 1)]]]},
                    schema=pa.schema([pa.field("maps", pa.list_(pa.map_(pa.string(), pa.int32())))]),
                ),
            ),
            "nested list or map items",
            id="table-nested-map-in-list",
        ),
        pytest.param(
            functools.partial(
                serialize_record_for_linkml,
                {"ratio": math.nan},
                pa.schema([pa.field("ratio", pa.float32(), nullable=False)]),
            ),
            "Non-finite float",
            id="record-non-finite-float",
        ),
    ],
)
def test_serialize_for_linkml_fail_null_list_items_and_nested_multivalued(
    call: Callable[[], object],
    match: str,
) -> None:
    """Verify LinkML serialization rejects null list items, nested lists or maps, and non-finite floats."""
    with pytest.raises(ValueError, match=match):
        call()
