"""Unit tests for XML-to-Arrow record coercion helpers."""

import datetime as dt
import functools
from pathlib import Path
from typing import Any, Final

import pyarrow as pa
import pytest
import xmlschema
from lxml.etree import fromstring

from cdm_data_loaders.coercion.xml import (
    convert_xml_element,
    make_xml_element_converter,
    make_xml_level_converters,
)
from cdm_data_loaders.converters.exceptions.facades.xsd_to_pyarrow import (
    FieldNameCollisionError,
    WildcardAttributeError,
)
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
    converted_copyright = converters["copyright"](copyright_elem)
    assert converted_copyright == {"#text": "Copyrighted by the UniProt Consortium"}
    table = pa.Table.from_pylist([converted_copyright], schema=UNIPROT_SCHEMAS["copyright"])
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
    "entry_point",
    [
        pytest.param("make_xml_element_converter", id="make-xml-element-converter"),
        pytest.param("convert_xml_element", id="convert-xml-element"),
        pytest.param("make_xml_level_converters", id="make-xml-level-converters"),
    ],
)
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
    entry_point: str,
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
    if entry_point == "make_xml_element_converter":
        call = functools.partial(make_xml_element_converter, simple_xsd, "root", max_recursion_depth=bad_depth)
    elif entry_point == "convert_xml_element":
        elem = fromstring(b"<root><id>1</id></root>")
        call = functools.partial(convert_xml_element, elem, simple_xsd, "root", max_recursion_depth=bad_depth)
    else:
        call = functools.partial(make_xml_level_converters, simple_xsd, 0, max_recursion_depth=bad_depth)

    with pytest.raises(exc_cls, match="max_recursion_depth"):
        call()


COERCION_XSD: Final[str] = """<?xml version="1.0" encoding="UTF-8"?>
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
  <xs:simpleType name="IntList">
    <xs:list itemType="xs:integer"/>
  </xs:simpleType>
  <xs:complexType name="MixedAttrType" mixed="true">
    <xs:sequence>
      <xs:element name="b" type="xs:string" minOccurs="0"/>
      <xs:element name="br" minOccurs="0"><xs:complexType/></xs:element>
    </xs:sequence>
    <xs:attribute name="id" type="xs:string"/>
  </xs:complexType>
  <xs:complexType name="SimpleContentType">
    <xs:simpleContent>
      <xs:extension base="xs:integer">
        <xs:attribute name="unit" type="xs:string"/>
      </xs:extension>
    </xs:simpleContent>
  </xs:complexType>
  <xs:complexType name="EmptyWithAttrType">
    <xs:attribute name="flag" type="xs:boolean"/>
  </xs:complexType>
  <xs:complexType name="ScalarHolderType">
    <xs:sequence>
      <xs:element name="hex_val" type="xs:hexBinary" minOccurs="0"/>
      <xs:element name="b64_val" type="xs:base64Binary" minOccurs="0"/>
      <xs:element name="date_val" type="xs:date" minOccurs="0"/>
    </xs:sequence>
  </xs:complexType>
  <xs:complexType name="NodeType">
    <xs:sequence>
      <xs:element name="req_str" type="xs:string"/>
      <xs:element name="opt_str" type="xs:string" minOccurs="0" nillable="true"/>
      <xs:element name="nmtokens" type="xs:NMTOKENS" minOccurs="0"/>
      <xs:element name="int_list" type="IntList" minOccurs="0"/>
      <xs:element name="mixed_attr" type="MixedAttrType" minOccurs="0"/>
      <xs:element name="sc" type="SimpleContentType" minOccurs="0"/>
      <xs:element name="empty_attr" type="EmptyWithAttrType" minOccurs="0"/>
      <xs:element name="marker" minOccurs="0" nillable="true"><xs:complexType/></xs:element>
      <xs:element name="child" type="NodeType" minOccurs="0" maxOccurs="unbounded" nillable="true"/>
    </xs:sequence>
    <xs:attribute name="kind" type="xs:string"/>
  </xs:complexType>
  <xs:element name="root" type="NodeType"/>
  <xs:element name="scalars" type="ScalarHolderType"/>
</xs:schema>
"""


@pytest.fixture
def coercion_schema() -> xmlschema.XMLSchema:
    """Compile the XSD schema fixture used by convert_xml_element tests."""
    return xmlschema.XMLSchema(COERCION_XSD)


@pytest.mark.parametrize(
    ("xml_bytes", "xml_tag", "max_recursion_depth", "expected"),
    [
        pytest.param(
            b"<scalars><hex_val>0a1b</hex_val><b64_val>YWJj</b64_val><date_val>2026-01-02Z</date_val></scalars>",
            "scalars",
            None,
            {"hex_val": b"\x0a\x1b", "b64_val": b"abc", "date_val": dt.date(2026, 1, 2)},
            id="scalars",
        ),
        pytest.param(
            (
                b'<root xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" kind="top">'
                b"<req_str/>"
                b'<opt_str xsi:nil=" 1 "/>'
                b"<nmtokens>a  b</nmtokens>"
                b"<int_list>1 2 3</int_list>"
                b'<mixed_attr id="m1">hi &lt;b&gt; <b xsi:nil="false" title="q&quot;r">bold</b> tail<br/></mixed_attr>'
                b'<sc unit="kg">42</sc>'
                b'<empty_attr flag="1"/>'
                b'<marker xsi:nil="true"/>'
                b'<child xsi:nil="true"/>'
                b'<child kind="leaf"><req_str>x</req_str></child>'
                b"</root>"
            ),
            "root",
            1,
            {
                "_kind": "top",
                "req_str": "",
                "nmtokens": ["a", "b"],
                "int_list": [1, 2, 3],
                "mixed_attr": {
                    "_id": "m1",
                    "#text": 'hi &lt;b&gt; <b title="q&quot;r">bold</b> tail<br/>',
                },
                "sc": {"_unit": "kg", "#text": 42},
                "empty_attr": {"_flag": True},
                "child": [None, '{"_kind": "leaf", "req_str": "x"}'],
            },
            id="mixed-nil-lists-and-truncated-child",
        ),
        pytest.param(
            b'<root xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:nil="true" kind="k"/>',
            "root",
            None,
            {"_kind": "k"},
            id="nil-root-with-attribute",
        ),
        pytest.param(
            b'<root xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:nil="1"/>',
            "root",
            None,
            {},
            id="nil-root-empty",
        ),
    ],
)
def test_convert_xml_element_pass_scalars_mixed_nil_and_lists(
    coercion_schema: xmlschema.XMLSchema,
    xml_bytes: bytes,
    xml_tag: str,
    max_recursion_depth: int | None,
    expected: dict[str, Any],
) -> None:
    """convert_xml_element coerces scalars, xsi:nil elements, mixed inline XML, lists, and truncated recursion."""
    result = (
        convert_xml_element(fromstring(xml_bytes), coercion_schema, xml_tag)
        if max_recursion_depth is None
        else convert_xml_element(
            fromstring(xml_bytes),
            coercion_schema,
            xml_tag,
            max_recursion_depth=max_recursion_depth,
        )
    )
    assert result == expected


def test_convert_xml_element_fail_attribute_child_collision() -> None:
    """convert_xml_element raises FieldNameCollisionError when an attribute and child map to the same name."""
    collision_xsd = xmlschema.XMLSchema(
        """<?xml version="1.0" encoding="UTF-8"?>
        <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
          <xs:element name="root">
            <xs:complexType>
              <xs:sequence><xs:element name="_id" type="xs:string"/></xs:sequence>
              <xs:attribute name="id" type="xs:string"/>
            </xs:complexType>
          </xs:element>
        </xs:schema>
        """
    )
    with pytest.raises(FieldNameCollisionError, match="Field name collision"):
        convert_xml_element(fromstring(b'<root id="1"><_id>2</_id></root>'), collision_xsd, "root")


def test_make_xml_level_converters_pass_nil_leaf_element() -> None:
    """make_xml_level_converters returns an empty dict for a nil scalar leaf element."""
    leaf_xsd = xmlschema.XMLSchema(
        """<?xml version="1.0" encoding="UTF-8"?>
        <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
          <xs:element name="root">
            <xs:complexType>
              <xs:sequence><xs:element name="note" type="xs:string" nillable="true"/></xs:sequence>
            </xs:complexType>
          </xs:element>
        </xs:schema>
        """
    )
    note_conv = make_xml_level_converters(leaf_xsd, 1)["note"]
    assert note_conv(fromstring(b'<note xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:nil="true"/>')) == {}
