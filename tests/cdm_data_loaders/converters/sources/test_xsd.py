"""Unit tests for XSD source reader, IR emission, and bundled XSD root/level resolution."""

from pathlib import Path
from typing import Final

import pyarrow as pa
import pytest
import xmlschema

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
from cdm_data_loaders.converters.sources.xsd import XsdReader
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


def test_resolve_root_elements_fail_or_cyclic_global_elements() -> None:
    """resolve_root_elements raises XmlTagNotFoundError on empty schemas and falls back to all roots on cycles."""
    empty_schema = xmlschema.XMLSchema(
        """<?xml version="1.0" encoding="UTF-8"?>
        <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
          <xs:simpleType name="OnlyType"><xs:restriction base="xs:string"/></xs:simpleType>
        </xs:schema>
        """
    )
    with pytest.raises(XmlTagNotFoundError, match="No global elements found in XSD schema"):
        resolve_root_elements(empty_schema, error_type=XsdToPyArrowError, error_by_code=ERROR_BY_CODE)

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


def test_xsd_reader_and_xsd_to_pyarrow_facade_pass_ir_and_emitters(tmp_path: Path) -> None:
    """XsdReader builds SchemaDocument IR with source='xsd' provenance and emits to PyArrow, Python files, PySpark, dlt, and JSON Schema."""
    uniref_path = XSD_DATA_DIR / "examples" / "uniref" / "uniref.xsd"
    reader = XsdReader(xml_tag="entry")
    doc = reader.read(uniref_path)

    assert doc.name == "entry"
    assert doc.provenance.source == "xsd"
    assert doc.root.type == "object"
    assert not doc.root.nullable

    arrow_schema = PyArrowEmitter(max_nesting=100).emit(doc)
    assert arrow_schema.equals(UNIREF_SCHEMAS["entry"], check_metadata=True)

    facade = XsdToPyArrow(xml_tag="entry")
    assert facade.convert(uniref_path).equals(UNIREF_SCHEMAS["entry"], check_metadata=True)
    empty_table = facade.convert_to_table(uniref_path)
    assert empty_table.num_rows == 0
    assert empty_table.schema.equals(UNIREF_SCHEMAS["entry"], check_metadata=True)

    py_src = facade.convert_to_python(uniref_path)
    out_file = tmp_path / "uniref_entry_schema.py"
    assert facade.convert_to_python_file(uniref_path, out_file) == out_file
    assert out_file.read_text(encoding="utf-8") == py_src

    loaded_mod = load_module(out_file)
    assert isinstance(loaded_mod.SCHEMA, pa.Schema)
    assert loaded_mod.SCHEMA.equals(UNIREF_SCHEMAS["entry"], check_metadata=True)

    level_file = tmp_path / "uniref_level_schema.py"
    assert facade.convert_level_to_python_file(uniref_path, level_file, 1) == level_file
    assert level_file.read_text(encoding="utf-8") == facade.convert_level_to_python(uniref_path, 1)

    spark_schema = PySparkEmitter(max_nesting=100).emit(doc)
    assert "_id" in {f.name for f in spark_schema.fields}

    dlt_stored = DltEmitter(schema_name="uniref_entry").emit(doc)
    assert "_id" in dlt_stored["tables"]["uniref_entry"]["columns"]

    json_schema = JsonSchemaEmitter().emit(doc)
    assert json_schema["type"] == "object"
    assert "_id" in json_schema["properties"]

    with pytest.raises(ValueError, match="max_recursion_depth"):
        XsdReader(max_recursion_depth=0)
    with pytest.raises(TypeError, match="max_recursion_depth"):
        XsdReader(max_recursion_depth=True)  # type: ignore[arg-type]
    with pytest.raises(XmlTagNotFoundError, match="xml_tag must be specified"):
        XsdReader(error_type=XsdToPyArrowError, error_by_code=ERROR_BY_CODE).read(uniref_path)
