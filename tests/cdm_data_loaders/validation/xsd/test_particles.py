"""Unit tests for XSD model-group particle cardinality and validation helpers."""

import pytest
import xmlschema

from cdm_data_loaders.validation.xsd.particles import (
    group_max_occurs_by_name,
    group_min_occurs_by_name,
    is_any_attribute,
    is_any_element,
    particle_max_occurs_by_name,
    raise_xsd_code,
    resolve_child_cardinality,
    validate_group_particles,
)


class CustomE4Error(ValueError):
    """Test-local E4 error subclass."""


def test_raise_xsd_code_uses_mapped_or_fallback_exception() -> None:
    """raise_xsd_code raises the mapped exception class when present and error_type otherwise."""
    with pytest.raises(CustomE4Error, match="wildcard"):
        raise_xsd_code("E4", "wildcard", error_type=RuntimeError, error_by_code={"E4": CustomE4Error})
    with pytest.raises(RuntimeError, match="fallback"):
        raise_xsd_code("E9", "fallback", error_type=RuntimeError, error_by_code={"E4": CustomE4Error})


def test_group_cardinality_and_resolve_child_cardinality_choice_and_sequence() -> None:
    """group_min_occurs_by_name, group_max_occurs_by_name, and resolve_child_cardinality compute sequence and choice bounds."""
    xsd_text = """<?xml version="1.0" encoding="UTF-8"?>
    <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
      <xs:element name="root">
        <xs:complexType>
          <xs:choice minOccurs="2" maxOccurs="3">
            <xs:element name="shared" type="xs:string" minOccurs="1" maxOccurs="2"/>
            <xs:sequence>
              <xs:element name="branchOnly" type="xs:string"/>
              <xs:element name="shared" type="xs:string" minOccurs="1" maxOccurs="1"/>
            </xs:sequence>
          </xs:choice>
        </xs:complexType>
      </xs:element>
    </xs:schema>
    """
    schema = xmlschema.XMLSchema(xsd_text)
    group = schema.elements["root"].type.model_group  # type: ignore[union-attr]
    assert group is not None
    expected_min_shared = 2
    expected_max_shared = 6
    assert group_min_occurs_by_name(group, "shared") == expected_min_shared
    assert group_max_occurs_by_name(group, "shared") == expected_max_shared
    assert group_min_occurs_by_name(group, "branchOnly") == 0
    assert group_max_occurs_by_name(group, "missing") == 0
    assert particle_max_occurs_by_name(object(), "shared") == 0

    children = {el.local_name: el for el in group.iter_elements()}
    assert resolve_child_cardinality(group, children["shared"]) == (True, False)
    assert resolve_child_cardinality(group, children["shared"], parent_nillable=True) == (True, True)


@pytest.mark.parametrize(
    ("body_xml", "expected_match"),
    [
        pytest.param(
            '<xs:sequence><xs:any processContents="lax"/></xs:sequence>',
            "xs:any wildcard",
            id="xs-any-element",
        ),
        pytest.param(
            '<xs:sequence><xs:element ref="absEl"/></xs:sequence>',
            "abstract element",
            id="abstract-element",
        ),
        pytest.param(
            '<xs:sequence><xs:element ref="headEl"/></xs:sequence>',
            "substitutionGroup",
            id="substitution-group",
        ),
        pytest.param(
            '<xs:sequence><xs:element name="item" type="AbsType"/></xs:sequence>',
            "abstract type",
            id="abstract-type",
        ),
    ],
)
def test_validate_group_particles_rejects_unsupported_constructs(body_xml: str, expected_match: str) -> None:
    """validate_group_particles rejects xs:any, abstract elements, substitution groups, and abstract types."""
    xsd_text = f"""<?xml version="1.0" encoding="UTF-8"?>
    <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
      <xs:complexType name="AbsType" abstract="true">
        <xs:sequence><xs:element name="x" type="xs:string"/></xs:sequence>
      </xs:complexType>
      <xs:element name="absEl" type="xs:string" abstract="true"/>
      <xs:element name="headEl" type="xs:string"/>
      <xs:element name="subEl" type="xs:string" substitutionGroup="headEl"/>
      <xs:element name="root">
        <xs:complexType>{body_xml}<xs:anyAttribute processContents="lax"/></xs:complexType>
      </xs:element>
    </xs:schema>
    """
    schema = xmlschema.XMLSchema(xsd_text)
    complex_type = schema.elements["root"].type
    assert any(is_any_attribute(a) for a in complex_type.attributes.values())  # type: ignore[union-attr]
    group = complex_type.model_group  # type: ignore[union-attr]
    assert group is not None
    if "xs:any" in body_xml:
        assert any(is_any_element(item) for item in group.content)
    with pytest.raises(ValueError, match=expected_match):
        validate_group_particles(schema, group)
