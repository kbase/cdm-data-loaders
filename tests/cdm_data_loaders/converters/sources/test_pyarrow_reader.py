"""Unit tests for cdm_data_loaders.converters.sources.pyarrow.PyArrowReader."""

from typing import Any, Final

import pyarrow as pa
import pytest

from cdm_data_loaders.converters.core.ir import NodeHints, Provenance, TypedNode
from cdm_data_loaders.converters.exceptions.sources.pyarrow import PyArrowReaderError
from cdm_data_loaders.converters.sources.pyarrow import PyArrowReader, _format_path


class _DummyExtType(pa.ExtensionType):
    def __init__(self) -> None:
        super().__init__(pa.int64(), "dummy.ext")

    def __arrow_ext_serialize__(self) -> bytes:
        return b""

    @classmethod
    def __arrow_ext_deserialize__(cls, _storage_type: pa.DataType, _serialized: bytes) -> "_DummyExtType":
        return cls()


LIST_ELEM_FIELD: Final[pa.Field] = pa.field(
    "item", pa.int64(), nullable=False, metadata={"comment": "elem desc", "k": "v"}
)


@pytest.mark.parametrize(
    ("path", "expected"),
    [
        pytest.param((), "<root>", id="empty-root"),
        pytest.param(("a", 0, "b"), "a.0.b", id="nested-segments"),
    ],
)
def test_format_path_segments(path: tuple[str | int, ...], expected: str) -> None:
    """Verify _format_path formats empty and nested path tuples."""
    assert _format_path(path) == expected


@pytest.mark.parametrize(
    ("bad_schema", "pattern"),
    [
        pytest.param("not_a_schema", r"Expected a pyarrow\.Schema", id="non-schema"),
        pytest.param(pa.schema([]), "Empty PyArrow schema with 0 fields", id="empty-schema"),
        pytest.param(
            pa.schema([pa.field("dup", pa.int64()), pa.field("dup", pa.string())]),
            "Duplicate field name 'dup' in PyArrow schema",
            id="duplicate-field",
        ),
    ],
)
def test_pyarrow_reader_read_invalid_input_raises(bad_schema: object, pattern: str) -> None:
    """Verify PyArrowReader.read rejects invalid, empty, or duplicate-field schemas."""
    with pytest.raises(PyArrowReaderError, match=pattern):
        PyArrowReader().read(bad_schema)  # type: ignore[arg-type]


def test_pyarrow_reader_read_field_invalid_type_raises() -> None:
    """Verify PyArrowReader.read_field rejects non-Field inputs."""
    with pytest.raises(PyArrowReaderError, match=r"Expected a pyarrow\.Field"):
        PyArrowReader().read_field("not_a_field")  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("bad_type", "path", "pattern"),
    [
        pytest.param("not_a_type", (), r"Expected a pyarrow\.DataType", id="non-datatype"),
        pytest.param(pa.struct([]), ("nested",), "Empty PyArrow struct with 0 fields", id="empty-struct"),
        pytest.param(
            pa.struct([pa.field("x", pa.int64()), pa.field("x", pa.string())]),
            ("nested",),
            "Duplicate field name 'x' in PyArrow struct at 'nested'",
            id="duplicate-struct-field",
        ),
        pytest.param(pa.null(), ("col",), "Standalone PyArrow null type", id="standalone-null"),
        pytest.param(_DummyExtType(), ("col",), "Unsupported PyArrow extension type", id="extension-type"),
        pytest.param(pa.duration("s"), ("col",), "duration/interval", id="duration"),
        pytest.param(pa.month_day_nano_interval(), ("col",), "duration/interval", id="interval"),
        pytest.param(pa.union([pa.field("a", pa.int32())], mode="sparse"), ("col",), "union type", id="union"),
        pytest.param(pa.run_end_encoded(pa.int32(), pa.string()), ("col",), "run_end_encoded", id="run-end-encoded"),
        pytest.param(pa.decimal128(5, -1), ("col",), "Invalid PyArrow decimal", id="decimal-negative-scale"),
        pytest.param(pa.decimal128(5, 6), ("col",), "Invalid PyArrow decimal", id="decimal-scale-exceeds-precision"),
    ],
)
def test_pyarrow_reader_read_node_invalid_input_raises(
    bad_type: object,
    path: tuple[str | int, ...],
    pattern: str,
) -> None:
    """Verify PyArrowReader.read_node rejects unsupported and malformed PyArrow data types."""
    with pytest.raises(PyArrowReaderError, match=pattern):
        PyArrowReader().read_node(bad_type, path=path)  # type: ignore[arg-type]


def test_pyarrow_reader_read_preserves_metadata_and_requiredness() -> None:
    """Verify PyArrowReader.read preserves schema metadata, annotations, provenance, and required fields."""
    schema = pa.schema(
        [
            pa.field("req_id", pa.int64(), nullable=False, metadata={"comment": "ID", "pydantic_type": "int"}),
            pa.field("opt_name", pa.string(), nullable=True, metadata={"custom": "v"}),
        ],
        metadata={"title": "MetaTitle", "$id": "https://example.org/meta", "comment": "Doc desc", "extra": "keep"},
    )
    doc = PyArrowReader().read(schema)

    assert doc.name == "MetaTitle"
    assert doc.identifier == "https://example.org/meta"
    assert doc.annotations["description"] == "Doc desc"
    assert doc.root.annotations["description"] == "Doc desc"
    assert doc.extensions["x-pyarrow"]["metadata"] == {"extra": "keep"}
    assert doc.root.required_names == ("req_id",)
    assert doc.root.additional_properties == TypedNode(type="never", nullable=False)
    assert doc.provenance == Provenance("pyarrow", ())
    assert doc.root.provenance == Provenance("pyarrow", ())


def test_pyarrow_reader_read_overrides_name_and_id() -> None:
    """Verify PyArrowReader.read uses explicit schema_name and schema_id overrides over metadata."""
    schema = pa.schema(
        [pa.field("a", pa.int32())],
        metadata={"title": "MetaTitle", "$id": "https://example.org/meta"},
    )
    doc = PyArrowReader().read(schema, schema_name="OverrideName", schema_id="https://example.org/override")

    assert doc.name == "OverrideName"
    assert doc.identifier == "https://example.org/override"


def test_pyarrow_reader_read_excludes_metadata_when_disabled() -> None:
    """Verify PyArrowReader omits x-pyarrow metadata when include_pyarrow_metadata is False."""
    doc = PyArrowReader(include_pyarrow_metadata=False).read(pa.schema([pa.field("a", pa.int32())]))

    assert doc.name is None
    assert doc.identifier is None
    assert not doc.annotations
    assert not doc.extensions


def test_pyarrow_reader_read_field_metadata_and_provenance() -> None:
    """Verify PyArrowReader.read_field preserves requiredness, description, provenance, and extensions."""
    field = PyArrowReader().read_field(
        pa.field("col", pa.int32(), nullable=False, metadata={"comment": "c", "xsd_type": "xs:int"})
    )

    assert field.required is True
    assert field.annotations["description"] == "c"
    assert field.provenance == Provenance("pyarrow", ("col",), {"xsd_type": "xs:int"})
    assert field.extensions["x-pyarrow"]["metadata"] == {"xsd_type": "xs:int"}


@pytest.mark.parametrize(
    ("list_type", "expected_arrow"),
    [
        pytest.param(pa.list_(LIST_ELEM_FIELD), "list", id="list"),
        pytest.param(pa.large_list(LIST_ELEM_FIELD), "large_list", id="large-list"),
        pytest.param(pa.list_view(LIST_ELEM_FIELD), "list_view", id="list-view"),
        pytest.param(pa.large_list_view(LIST_ELEM_FIELD), "large_list_view", id="large-list-view"),
        pytest.param(pa.list_(LIST_ELEM_FIELD, 3), "fixed_size_list", id="fixed-size-list"),
    ],
)
@pytest.mark.parametrize(
    "include_metadata",
    [
        pytest.param(True, id="with-metadata"),
        pytest.param(False, id="without-metadata"),
    ],
)
def test_pyarrow_reader_read_node_list_variants(
    list_type: pa.DataType,
    expected_arrow: str,
    include_metadata: bool,
) -> None:
    """Verify PyArrowReader.read_node handles all PyArrow list variants with and without metadata."""
    node = PyArrowReader(include_pyarrow_metadata=include_metadata).read_node(list_type, nullable=True, path=("arr",))

    assert node.type == "array"
    assert node.nullable is True
    assert isinstance(node.items, TypedNode)
    assert node.items.annotations["description"] == "elem desc"
    if include_metadata:
        assert node.extensions["x-pyarrow"]["arrow_type"] == expected_arrow
        assert node.items.extensions["x-pyarrow"]["metadata"] == {"k": "v"}
    else:
        assert not node.items.extensions


@pytest.mark.parametrize(
    ("include_metadata", "ordered"),
    [
        pytest.param(True, True, id="ordered-with-metadata"),
        pytest.param(False, False, id="unordered-without-metadata"),
    ],
)
def test_pyarrow_reader_read_node_dictionary(include_metadata: bool, ordered: bool) -> None:
    """Verify PyArrowReader.read_node lowers dictionary types and preserves ordering metadata when enabled."""
    node = PyArrowReader(include_pyarrow_metadata=include_metadata).read_node(
        pa.dictionary(pa.int8(), pa.string(), ordered=ordered),
        nullable=True,
    )

    assert node.type == "string"
    assert node.nullable is True
    if include_metadata:
        assert node.extensions["x-pyarrow"]["arrow_type"] == "dictionary"
        assert node.extensions["x-pyarrow"]["ordered"] is True
    else:
        assert not node.extensions


def test_pyarrow_reader_read_node_map() -> None:
    """Verify PyArrowReader.read_node lowers PyArrow map types with non-null keys and keys_sorted metadata."""
    node = PyArrowReader().read_node(pa.map_(pa.string(), pa.int32(), keys_sorted=True))

    assert node.type == "map"
    assert node.key_type is not None
    assert node.key_type.nullable is False
    assert node.extensions["x-pyarrow"]["keys_sorted"] is True


def test_pyarrow_reader_read_node_struct() -> None:
    """Verify PyArrowReader.read_node lowers PyArrow struct types and tracks required fields."""
    node = PyArrowReader().read_node(pa.struct([pa.field("a", pa.bool_(), nullable=False)]))

    assert node.type == "object"
    assert node.required_names == ("a",)


@pytest.mark.parametrize(
    ("arrow_type", "expected_hints", "expected_arrow_meta", "absent_meta_keys"),
    [
        pytest.param(
            pa.decimal256(40, 4),
            NodeHints(logical_type="decimal", precision=40, scale=4, bit_width=256),
            {},
            (),
            id="decimal256",
        ),
        pytest.param(
            pa.binary(8),
            NodeHints(logical_type="fixed", length=8),
            {},
            (),
            id="fixed-size-binary",
        ),
        pytest.param(
            pa.time32("s"),
            None,
            {"arrow_type": "time32"},
            (),
            id="time32",
        ),
        pytest.param(
            pa.time64("ns"),
            None,
            {"arrow_type": "time64"},
            (),
            id="time64",
        ),
        pytest.param(
            pa.timestamp("us"),
            NodeHints(logical_type="timestamp-without-tz", timezone=False),
            {},
            ("tz",),
            id="timestamp-without-tz",
        ),
        pytest.param(
            pa.timestamp("us", tz="UTC"),
            NodeHints(logical_type="timestamp-with-tz", timezone=True),
            {"tz": "UTC"},
            (),
            id="timestamp-with-tz",
        ),
    ],
)
def test_pyarrow_reader_read_node_scalars(
    arrow_type: pa.DataType,
    expected_hints: NodeHints | None,
    expected_arrow_meta: dict[str, Any],
    absent_meta_keys: tuple[str, ...],
) -> None:
    """Verify PyArrowReader.read_node maps scalar PyArrow types to hints and x-pyarrow metadata."""
    node = PyArrowReader().read_node(arrow_type)

    if expected_hints is not None:
        assert node.hints == expected_hints
    for key, val in expected_arrow_meta.items():
        assert node.extensions["x-pyarrow"][key] == val
    for key in absent_meta_keys:
        assert key not in node.extensions["x-pyarrow"]
