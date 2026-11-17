"""Unit tests for XML-to-Arrow record coercion helpers."""

from pathlib import Path
from typing import Final

import pyarrow as pa
import pytest
import xmlschema
from lxml.etree import fromstring

from cdm_data_loaders.coercion.xml import (
    convert_xml_element,
    make_xml_element_converter,
    make_xml_level_converters,
)
from cdm_data_loaders.converters.exceptions.facades.xsd_to_pyarrow import WildcardAttributeError
from cdm_data_loaders.schemas.uniprot.schema import SCHEMAS as UNIPROT_SCHEMAS
from cdm_data_loaders.validation.xsd.traversal import load_schema
from tests.helpers.paths_and_constants import BASE_DIR

XSD_DATA_DIR: Final[Path] = (BASE_DIR / "specs" / "schema_converters" / "xsd_to_pyarrow").resolve()


def test_make_xml_level_converters_pass_struct_and_leaf_elements() -> None:
    """make_xml_level_converters converts both complex struct elements and scalar leaf elements at a level."""
    uniprot_xsd = load_schema(XSD_DATA_DIR / "examples" / "uniprot" / "uniprot.xsd")
    converters = make_xml_level_converters(uniprot_xsd, 1)
    assert set(converters.keys()) == {"entry", "copyright"}

    copyright_elem = fromstring(
        b'<copyright xmlns="https://uniprot.org/uniprot">Copyrighted by the UniProt Consortium</copyright>'
    )
    assert converters["copyright"](copyright_elem) == {"#text": "Copyrighted by the UniProt Consortium"}
    table = pa.Table.from_pylist([converters["copyright"](copyright_elem)], schema=UNIPROT_SCHEMAS["copyright"])
    assert table.to_pylist() == [{"#text": "Copyrighted by the UniProt Consortium"}]


def test_make_xml_level_converters_fail_nested_unsupported_xsd_construct() -> None:
    """Raise XsdToPyArrowError when a level element's complex subtree contains xs:anyAttribute."""
    bad_level_xsd = """<?xml version="1.0" encoding="UTF-8"?>
    <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
      <xs:complexType name="BadChild">
        <xs:sequence><xs:element name="v" type="xs:string"/></xs:sequence>
        <xs:anyAttribute processContents="skip"/>
      </xs:complexType>
      <xs:element name="root">
        <xs:complexType>
          <xs:sequence>
            <xs:element name="item">
              <xs:complexType>
                <xs:sequence><xs:element name="bad" type="BadChild"/></xs:sequence>
              </xs:complexType>
            </xs:element>
          </xs:sequence>
        </xs:complexType>
      </xs:element>
    </xs:schema>
    """
    with pytest.raises(WildcardAttributeError, match="xs:anyAttribute"):
        make_xml_level_converters(xmlschema.XMLSchema(bad_level_xsd), 1)


@pytest.mark.parametrize(
    ("bad_depth", "exc_cls"),
    [
        pytest.param(0, ValueError, id="zero"),
        pytest.param(-2, ValueError, id="negative"),
        pytest.param(True, TypeError, id="bool-true"),
        pytest.param(False, TypeError, id="bool-false"),
    ],
)
def test_xml_coercion_entry_points_fail_invalid_max_recursion_depth(
    bad_depth: int,
    exc_cls: type[Exception],
) -> None:
    """Reject non-positive or boolean max_recursion_depth across XML coercion entry points."""
    simple_xsd = xmlschema.XMLSchema(
        """<?xml version="1.0" encoding="UTF-8"?>
        <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
          <xs:element name="root">
            <xs:complexType>
              <xs:sequence><xs:element name="id" type="xs:string"/></xs:sequence>
            </xs:complexType>
          </xs:element>
        </xs:schema>
        """
    )
    elem = fromstring(b"<root><id>1</id></root>")
    with pytest.raises(exc_cls, match="max_recursion_depth"):
        make_xml_element_converter(simple_xsd, "root", max_recursion_depth=bad_depth)
    with pytest.raises(exc_cls, match="max_recursion_depth"):
        convert_xml_element(elem, simple_xsd, "root", max_recursion_depth=bad_depth)
    with pytest.raises(exc_cls, match="max_recursion_depth"):
        make_xml_level_converters(simple_xsd, 0, max_recursion_depth=bad_depth)
