"""Unit tests for XSD source reader, IR emission, and bundled XSD root/level resolution."""

import functools
from collections.abc import Callable
from pathlib import Path
from typing import Any, Final

import pyarrow as pa
import pytest
import xmlschema
from pydantic import ValidationError
from pyspark.sql.types import StructType

from cdm_data_loaders.converters.core.ir import Provenance, TypedNode
from cdm_data_loaders.converters.emitters.dlt import DltEmitter
from cdm_data_loaders.converters.emitters.json_schema.emitter import JsonSchemaEmitter
from cdm_data_loaders.converters.emitters.pyarrow import PyArrowEmitter
from cdm_data_loaders.converters.emitters.pyspark import PySparkEmitter
from cdm_data_loaders.converters.exceptions.facades.xsd_to_pyarrow import (
    ERROR_BY_CODE,
    XmlTagNotFoundError,
    XsdToPyArrowError,
)
from cdm_data_loaders.converters.facades.xsd_to_pyarrow import XsdToPyArrow
from cdm_data_loaders.converters.sources.xsd import (
    XsdReader,
    _string_ir_node,
    _wrap_truncated_leaf_ir_field,
    simple_type_to_ir_node,
)
from cdm_data_loaders.schemas.uniref.schema import SCHEMAS as UNIREF_SCHEMAS
from cdm_data_loaders.utils.xml_utils import local_name
from cdm_data_loaders.validation.xsd.traversal import (
    load_schema,
    resolve_elements_at_level,
    resolve_root_elements,
)
from tests.helpers.paths_and_constants import BASE_DIR
from tests.helpers.pyarrow_spec import load_module

XSD_DATA_DIR: Final[Path] = (BASE_DIR / "specs" / "schema_converters" / "xsd_to_pyarrow").resolve()


@pytest.mark.parametrize(
    ("xsd_rel_path", "root_tag", "expected_roots"),
    [
        pytest.param("examples/uniprot/uniprot.xsd", None, ["uniprot"], id="uniprot-deduced"),
        pytest.param(
            "examples/uniref/uniref.xsd",
            None,
            ["UniRef100", "UniRef90", "UniRef50", "UniRef"],
            id="uniref-deduced",
        ),
        pytest.param(
            "examples/pubmed_article/pubmed.xsd",
            None,
            ["PubmedArticleSet", "BookDocumentSet", "PubmedBookArticleSet", "ContractNumber", "URL"],
            id="pubmed-deduced",
        ),
        pytest.param(
            "examples/pubmed_article/pubmed.xsd",
            "PubmedArticleSet",
            ["PubmedArticleSet"],
            id="pubmed-explicit-root-tag",
        ),
    ],
)
def test_resolve_root_elements_pass_deduces_or_resolves_roots(
    xsd_rel_path: str,
    root_tag: str | None,
    expected_roots: list[str],
) -> None:
    """resolve_root_elements deduces unreferenced global roots or resolves an explicit root_tag."""
    schema = load_schema(XSD_DATA_DIR / xsd_rel_path)
    roots = resolve_root_elements(schema, root_tag=root_tag)
    assert [local_name(elem.name) for elem in roots if elem.name is not None] == expected_roots


@pytest.mark.parametrize(
    ("xsd_rel_path", "level", "root_tag", "expected_tags"),
    [
        pytest.param(
            "examples/pubmed_article/pubmed.xsd",
            1,
            "PubmedArticleSet",
            ["PubmedArticle", "PubmedBookArticle", "DeleteCitation"],
            id="pubmed-level-1",
        ),
        pytest.param(
            "examples/pubmed_article/pubmed.xsd",
            2,
            "PubmedArticleSet",
            ["MedlineCitation", "PubmedData", "BookDocument", "PubmedBookData", "PMID"],
            id="pubmed-level-2",
        ),
        pytest.param(
            "examples/uniprot/uniprot.xsd",
            1,
            None,
            ["entry", "copyright"],
            id="uniprot-level-1",
        ),
        pytest.param(
            "examples/uniprot/uniprot.xsd",
            2,
            None,
            [
                "accession",
                "name",
                "protein",
                "gene",
                "organism",
                "organismHost",
                "geneLocation",
                "reference",
                "comment",
                "dbReference",
                "proteinExistence",
                "keyword",
                "feature",
                "evidence",
                "sequence",
            ],
            id="uniprot-level-2",
        ),
        pytest.param(
            "examples/uniref/uniref.xsd",
            0,
            "UniRef50",
            ["UniRef50"],
            id="uniref-level-0",
        ),
        pytest.param(
            "examples/uniref/uniref.xsd",
            1,
            None,
            ["entry"],
            id="uniref-level-1",
        ),
        pytest.param(
            "examples/uniref/uniref.xsd",
            2,
            None,
            ["name", "property", "representativeMember", "member"],
            id="uniref-level-2",
        ),
    ],
)
def test_resolve_elements_at_level_pass_level_1_and_level_2(
    xsd_rel_path: str,
    level: int,
    root_tag: str | None,
    expected_tags: list[str],
) -> None:
    """resolve_elements_at_level deduces all element declarations at level 0, 1, and 2."""
    schema = load_schema(XSD_DATA_DIR / xsd_rel_path)
    elements = resolve_elements_at_level(schema, level, root_tag=root_tag)
    assert list(elements.keys()) == expected_tags


@pytest.mark.parametrize(
    ("level", "exc_cls", "match"),
    [
        pytest.param(-1, ValueError, "level must be >= 0", id="negative-level"),
        pytest.param(10, XmlTagNotFoundError, "No XML elements found at level 10", id="beyond-max-depth"),
    ],
)
def test_resolve_elements_at_level_fail_invalid_or_empty_level(
    level: int,
    exc_cls: type[Exception],
    match: str,
) -> None:
    """resolve_elements_at_level raises ValueError for level < 0 and XmlTagNotFoundError beyond max depth."""
    schema = load_schema(XSD_DATA_DIR / "examples" / "uniref" / "uniref.xsd")
    with pytest.raises(exc_cls, match=match):
        resolve_elements_at_level(schema, level, error_type=XsdToPyArrowError, error_by_code=ERROR_BY_CODE)


def test_resolve_root_elements_fail_empty_schema() -> None:
    """resolve_root_elements raises XmlTagNotFoundError when an XSD schema declares no global elements."""
    empty_schema = xmlschema.XMLSchema(
        """<?xml version="1.0" encoding="UTF-8"?>
        <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
          <xs:simpleType name="OnlyType"><xs:restriction base="xs:string"/></xs:simpleType>
        </xs:schema>
        """
    )
    with pytest.raises(XmlTagNotFoundError, match="No global elements found in XSD schema"):
        resolve_root_elements(empty_schema, error_type=XsdToPyArrowError, error_by_code=ERROR_BY_CODE)


def test_resolve_root_elements_pass_cyclic_global_elements() -> None:
    """resolve_root_elements falls back to all global elements when mutual references leave none unreferenced."""
    cyclic_schema = xmlschema.XMLSchema(
        """<?xml version="1.0" encoding="UTF-8"?>
        <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
          <xs:element name="alpha">
            <xs:complexType><xs:sequence><xs:element ref="beta" minOccurs="0"/></xs:sequence></xs:complexType>
          </xs:element>
          <xs:element name="beta">
            <xs:complexType><xs:sequence><xs:element ref="alpha" minOccurs="0"/></xs:sequence></xs:complexType>
          </xs:element>
        </xs:schema>
        """
    )
    assert [local_name(e.name) for e in resolve_root_elements(cyclic_schema) if e.name] == ["alpha", "beta"]


@pytest.mark.parametrize(
    ("target", "emitter"),
    [
        ("pyarrow", PyArrowEmitter(max_nesting=100)),
        ("pyspark", PySparkEmitter(max_nesting=100)),
        ("dlt", DltEmitter(schema_name="uniref_entry")),
        ("jsonschema", JsonSchemaEmitter()),
    ],
    ids=["pyarrow", "pyspark", "dlt", "jsonschema"],
)
def test_xsd_reader_pass_ir_and_emitters(
    target: str,
    emitter: PyArrowEmitter | PySparkEmitter | DltEmitter | JsonSchemaEmitter,
) -> None:
    """XsdReader builds SchemaDocument IR with source='xsd' provenance and emits to PyArrow, PySpark, dlt, and JSON Schema."""
    uniref_path = XSD_DATA_DIR / "examples" / "uniref" / "uniref.xsd"
    doc = XsdReader(xml_tag="entry").read(uniref_path)

    assert doc.name == "entry"
    assert doc.provenance.source == "xsd"
    assert doc.root.type == "object"
    assert not doc.root.nullable

    emitted = emitter.emit(doc)
    match target:
        case "pyarrow":
            assert isinstance(emitted, pa.Schema)
            assert emitted.equals(UNIREF_SCHEMAS["entry"], check_metadata=True)
        case "pyspark":
            assert isinstance(emitted, StructType)
            assert "_id" in {f.name for f in emitted.fields}
        case "dlt":
            assert isinstance(emitted, dict)
            assert "_id" in emitted["tables"]["uniref_entry"]["columns"]
        case "jsonschema":
            assert isinstance(emitted, dict)
            assert emitted["type"] == "object"
            assert "_id" in emitted["properties"]
        case _:
            pytest.fail(f"Unhandled target: {target}")


@pytest.mark.parametrize(
    "output_mode",
    ["schema", "empty-table", "python-module", "level-python-module"],
    ids=["schema", "empty-table", "python-module", "level-python-module"],
)
def test_xsd_to_pyarrow_facade_pass_outputs(tmp_path: Path, output_mode: str) -> None:
    """XsdToPyArrow converts XSD elements to PyArrow schemas, empty tables, and executable Python modules."""
    uniref_path = XSD_DATA_DIR / "examples" / "uniref" / "uniref.xsd"
    facade = XsdToPyArrow(xml_tag="entry")
    match output_mode:
        case "schema":
            assert facade.convert(uniref_path).equals(UNIREF_SCHEMAS["entry"], check_metadata=True)
        case "empty-table":
            empty_table = facade.convert_to_table(uniref_path)
            assert empty_table.num_rows == 0
            assert empty_table.schema.equals(UNIREF_SCHEMAS["entry"], check_metadata=True)
        case "python-module":
            py_src = facade.convert_to_python(uniref_path)
            out_file = tmp_path / "uniref_entry_schema.py"
            assert facade.convert_to_python_file(uniref_path, out_file) == out_file
            assert out_file.read_text(encoding="utf-8") == py_src
            loaded_mod = load_module(out_file)
            assert isinstance(loaded_mod.SCHEMA, pa.Schema)
            assert loaded_mod.SCHEMA.equals(UNIREF_SCHEMAS["entry"], check_metadata=True)
        case "level-python-module":
            level_file = tmp_path / "uniref_level_schema.py"
            assert facade.convert_level_to_python_file(uniref_path, level_file, 1) == level_file
            assert level_file.read_text(encoding="utf-8") == facade.convert_level_to_python(uniref_path, 1)
        case _:
            pytest.fail(f"Unhandled output_mode: {output_mode}")


@pytest.mark.parametrize(
    ("call", "error_type", "match"),
    [
        pytest.param(
            functools.partial(XsdReader, max_recursion_depth=0),
            ValidationError,
            "max_recursion_depth",
            id="zero-max-recursion-depth",
        ),
        pytest.param(
            functools.partial(XsdReader, max_recursion_depth=True),  # type: ignore[arg-type]
            ValidationError,
            "max_recursion_depth",
            id="bool-max-recursion-depth",
        ),
        pytest.param(
            functools.partial(
                XsdReader(error_type=XsdToPyArrowError, error_by_code=ERROR_BY_CODE).read,
                XSD_DATA_DIR / "examples" / "uniref" / "uniref.xsd",
            ),
            XmlTagNotFoundError,
            "xml_tag must be specified",
            id="missing-xml-tag-with-multiple-roots",
        ),
    ],
)
def test_xsd_reader_fail_invalid_options(
    call: Callable[[], object],
    error_type: type[Exception],
    match: str,
) -> None:
    """XsdReader validates max_recursion_depth and requires xml_tag when multiple roots exist."""
    with pytest.raises(error_type, match=match):
        call()


IR_HELPERS_XSD: Final[str] = """<?xml version="1.0" encoding="UTF-8"?>
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema" targetNamespace="urn:test:ir" xmlns="urn:test:ir">
  <xs:simpleType name="DocIntList">
    <xs:annotation><xs:documentation>List of ints</xs:documentation></xs:annotation>
    <xs:list itemType="xs:integer"/>
  </xs:simpleType>
  <xs:complexType name="DocRecType">
    <xs:annotation><xs:documentation>Recursive type doc</xs:documentation></xs:annotation>
    <xs:sequence>
      <xs:element name="val" type="xs:string"/>
      <xs:element name="child" type="DocRecType" minOccurs="0" maxOccurs="unbounded"/>
    </xs:sequence>
  </xs:complexType>
  <xs:element name="root">
    <xs:annotation><xs:documentation>Root element doc</xs:documentation></xs:annotation>
    <xs:complexType>
      <xs:sequence>
        <xs:element name="ints" type="DocIntList"/>
        <xs:element name="rec" type="DocRecType" minOccurs="0"/>
      </xs:sequence>
      <xs:attribute name="req_attr" type="xs:NMTOKENS" use="required"/>
    </xs:complexType>
  </xs:element>
</xs:schema>
"""

READER_IR_XSD: Final[str] = """<?xml version="1.0" encoding="UTF-8"?>
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema" targetNamespace="urn:test:ir" xmlns="urn:test:ir">
  <xs:simpleType name="DocIntList">
    <xs:annotation><xs:documentation>List of ints</xs:documentation></xs:annotation>
    <xs:list itemType="xs:integer"/>
  </xs:simpleType>
  <xs:element name="root">
    <xs:annotation><xs:documentation>Root element doc</xs:documentation></xs:annotation>
    <xs:complexType>
      <xs:sequence>
        <xs:element name="ints" type="DocIntList"/>
        <xs:element name="val" type="xs:string"/>
      </xs:sequence>
      <xs:attribute name="req_attr" type="xs:NMTOKENS" use="required"/>
    </xs:complexType>
  </xs:element>
</xs:schema>
"""


@pytest.fixture
def ir_helpers_schema(tmp_path: Path) -> xmlschema.XMLSchema:
    """Compile the XSD schema fixture used by IR helper tests."""
    xsd_path = tmp_path / "ir_helpers.xsd"
    xsd_path.write_text(IR_HELPERS_XSD, encoding="utf-8")
    return xmlschema.XMLSchema(str(xsd_path))


@pytest.fixture
def reader_ir_schema(tmp_path: Path) -> xmlschema.XMLSchema:
    """Compile the XSD schema fixture used by XsdReader provenance tests."""
    xsd_path = tmp_path / "reader_ir.xsd"
    xsd_path.write_text(READER_IR_XSD, encoding="utf-8")
    return xmlschema.XMLSchema(str(xsd_path))


@pytest.mark.parametrize(
    ("path", "nullable", "metadata", "expected_metadata"),
    [
        pytest.param(("a",), True, {"k": "v"}, {"k": "v"}, id="nullable-with-metadata"),
        pytest.param(("b",), False, None, {}, id="non-nullable-default-metadata"),
    ],
)
def test_string_ir_node_pass(
    path: tuple[str, ...],
    *,
    nullable: bool,
    metadata: dict[str, Any] | None,
    expected_metadata: dict[str, Any],
) -> None:
    """_string_ir_node preserves nullable, provenance path, and metadata."""
    node = (
        _string_ir_node(path, nullable=nullable)
        if metadata is None
        else _string_ir_node(path, nullable=nullable, metadata=metadata)
    )
    assert node.nullable is nullable
    assert node.provenance == Provenance("xsd", path, expected_metadata)


@pytest.mark.parametrize(
    ("type_kind", "path", "nullable", "expected_item_type", "expected_xsd_type"),
    [
        pytest.param("builtin", ("attr",), False, "string", "xs:NMTOKEN", id="builtin-nmtokens"),
        pytest.param("custom", ("ints",), True, "integer", "xs:integer", id="custom-int-list"),
    ],
)
def test_simple_type_to_ir_node_pass_list_types(
    ir_helpers_schema: xmlschema.XMLSchema,
    type_kind: str,
    path: tuple[str, ...],
    *,
    nullable: bool,
    expected_item_type: str,
    expected_xsd_type: str,
) -> None:
    """simple_type_to_ir_node maps built-in and custom XSD list types with item provenance."""
    xsd_type = (
        ir_helpers_schema.elements["root"].type.attributes["req_attr"].type
        if type_kind == "builtin"
        else ir_helpers_schema.types["DocIntList"]
    )
    node = simple_type_to_ir_node(xsd_type, path, nullable=nullable)
    assert node.type == "array"
    assert node.nullable is nullable
    assert node.provenance == Provenance("xsd", path)
    assert node.items is not None
    assert node.items.type == expected_item_type
    assert node.items.nullable is True
    assert node.items.provenance == Provenance("xsd", (*path, "item"), {"xsd_type": expected_xsd_type})


@pytest.mark.parametrize(
    ("is_list", "nullable", "expected_required", "expected_node_type"),
    [
        pytest.param(True, True, False, "array", id="list-nullable"),
        pytest.param(False, False, True, "string", id="scalar-required"),
    ],
)
def test_wrap_truncated_leaf_ir_field_pass(
    ir_helpers_schema: xmlschema.XMLSchema,
    *,
    is_list: bool,
    nullable: bool,
    expected_required: bool,
    expected_node_type: str,
) -> None:
    """_wrap_truncated_leaf_ir_field attaches type documentation and truncated_component_id provenance."""
    rec_elem = ir_helpers_schema.elements["root"].type.content[1]
    child_elem = rec_elem.type.content[1]
    expected_meta = {"truncated_component_id": id(child_elem.type)}

    field = _wrap_truncated_leaf_ir_field(child_elem, ("rec", "child"), is_list=is_list, nullable=nullable)
    assert field.name == "child"
    assert field.required is expected_required
    assert field.annotations == {"description": "Recursive type doc"}
    assert field.provenance == Provenance("xsd", ("rec", "child"), expected_meta)
    assert field.node.type == expected_node_type
    assert field.node.nullable is nullable
    assert field.node.provenance == Provenance("xsd", ("rec", "child"), expected_meta)
    if is_list:
        assert field.node.items is not None
        assert field.node.items.nullable is True
        assert field.node.items.provenance == Provenance("xsd", ("rec", "child", "item"), expected_meta)


def test_xsd_reader_read_pass_annotations_namespace_and_cache(reader_ir_schema: xmlschema.XMLSchema) -> None:
    """XsdReader.read attaches element annotations, targetNamespace, provenance, and leaves struct_cache empty."""
    cache: dict[tuple[int, bool], TypedNode] = {}
    doc = XsdReader(max_recursion_depth=1).read(reader_ir_schema, struct_cache=cache)
    assert doc.name == "root"
    assert doc.identifier == "urn:test:ir"
    assert doc.annotations == {"description": "Root element doc"}
    assert doc.root.annotations == {"description": "Root element doc"}
    assert doc.provenance == Provenance("xsd", ("root",))
    fields_by_name = {f.name: f for f in doc.root.properties}
    assert fields_by_name["_req_attr"].required is True
    assert fields_by_name["ints"].annotations == {"description": "List of ints"}
    assert fields_by_name["ints"].provenance == Provenance("xsd", ("ints",))
    assert fields_by_name["val"].provenance == Provenance("xsd", ("val",), {"xsd_type": "xs:string"})
    assert cache == {}


def test_xsd_reader_read_at_level_pass_scalar_leaf_provenance(reader_ir_schema: xmlschema.XMLSchema) -> None:
    """XsdReader.read_at_level marks scalar leaf tables with scalar_leaf_table provenance and #text property."""
    level_docs = XsdReader().read_at_level(reader_ir_schema, 1, root_tag="root")
    assert "val" in level_docs
    assert level_docs["val"].root.provenance == Provenance("xsd", (), {"scalar_leaf_table": True})
    assert level_docs["val"].root.properties[0].name == "#text"
