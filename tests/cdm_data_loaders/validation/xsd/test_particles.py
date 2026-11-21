"""Unit tests for XSD model-group particle cardinality and validation helpers."""

from typing import Final

import pytest
import xmlschema
from xmlschema.validators import XsdGroup

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

CHOICE_AND_SEQUENCE_XSD: Final[str] = """<?xml version="1.0" encoding="UTF-8"?>
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


class CustomE4Error(ValueError):
    """Test-local E4 error subclass."""


def _choice_and_sequence_group() -> XsdGroup:
    """Compile ``CHOICE_AND_SEQUENCE_XSD`` and return the root ``XsdGroup``.

    :return: Root model group from ``CHOICE_AND_SEQUENCE_XSD``.
    :rtype: XsdGroup
    """
    schema = xmlschema.XMLSchema(CHOICE_AND_SEQUENCE_XSD)
    group = schema.elements["root"].type.model_group  # type: ignore[union-attr]
    assert isinstance(group, XsdGroup)
    return group


@pytest.mark.parametrize(
    ("code", "expected_exc", "match"),
    [
        pytest.param("E4", CustomE4Error, "wildcard", id="mapped-code"),
        pytest.param("E9", RuntimeError, "fallback", id="fallback-exception"),
    ],
)
def test_raise_xsd_code_uses_mapped_or_fallback_exception(
    code: str,
    expected_exc: type[Exception],
    match: str,
) -> None:
    """raise_xsd_code raises the mapped exception class when present and error_type otherwise."""
    with pytest.raises(expected_exc, match=match):
        raise_xsd_code(code, match, error_type=RuntimeError, error_by_code={"E4": CustomE4Error})


@pytest.mark.parametrize(
    ("local_name", "expected_min"),
    [
        pytest.param("shared", 2, id="shared-across-choice-branches"),
        pytest.param("branchOnly", 0, id="single-choice-branch"),
    ],
)
def test_group_min_occurs_by_name_pass_choice_and_sequence(
    local_name: str,
    expected_min: int,
) -> None:
    """group_min_occurs_by_name computes minimum occurrences across choice and sequence branches."""
    group = _choice_and_sequence_group()
    assert group_min_occurs_by_name(group, local_name) == expected_min


@pytest.mark.parametrize(
    ("local_name", "expected_max"),
    [
        pytest.param("shared", 6, id="shared-across-choice-branches"),
        pytest.param("missing", 0, id="missing-element"),
    ],
)
def test_group_max_occurs_by_name_pass_choice_and_sequence(
    local_name: str,
    expected_max: int,
) -> None:
    """group_max_occurs_by_name computes maximum occurrences across choice and sequence branches."""
    group = _choice_and_sequence_group()
    assert group_max_occurs_by_name(group, local_name) == expected_max


def test_particle_max_occurs_by_name_pass_non_element_particle() -> None:
    """particle_max_occurs_by_name returns 0 for non-group and non-element particles."""
    assert particle_max_occurs_by_name(object(), "shared") == 0


@pytest.mark.parametrize(
    ("parent_nillable", "expected"),
    [
        pytest.param(False, (True, False), id="non-nillable-parent"),
        pytest.param(True, (True, True), id="nillable-parent"),
    ],
)
def test_resolve_child_cardinality_pass_parent_nillable(
    parent_nillable: bool,
    expected: tuple[bool, bool],
) -> None:
    """resolve_child_cardinality computes list and nullability flags with and without a nillable parent."""
    group = _choice_and_sequence_group()
    children = {el.local_name: el for el in group.iter_elements()}
    assert resolve_child_cardinality(group, children["shared"], parent_nillable=parent_nillable) == expected


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


GROUP_REF_XSD: Final[str] = """<?xml version="1.0" encoding="UTF-8"?>
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
  <xs:group name="BadGroup">
    <xs:sequence><xs:any processContents="skip"/></xs:sequence>
  </xs:group>
  <xs:element name="root">
    <xs:complexType><xs:sequence><xs:group ref="BadGroup"/></xs:sequence></xs:complexType>
  </xs:element>
</xs:schema>
"""

NESTED_GROUPS_XSD: Final[str] = """<?xml version="1.0" encoding="UTF-8"?>
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
  <xs:complexType name="AbsType" abstract="true">
    <xs:sequence><xs:element name="x" type="xs:string"/></xs:sequence>
  </xs:complexType>
  <xs:element name="absEl" type="xs:string" abstract="true"/>
  <xs:element name="headEl" type="xs:string"/>
  <xs:element name="subEl" type="xs:string" substitutionGroup="headEl"/>
  <xs:element name="nestedAbs">
    <xs:complexType><xs:sequence><xs:sequence><xs:element ref="absEl"/></xs:sequence></xs:sequence></xs:complexType>
  </xs:element>
  <xs:element name="nestedSub">
    <xs:complexType><xs:sequence><xs:sequence><xs:element ref="headEl"/></xs:sequence></xs:sequence></xs:complexType>
  </xs:element>
  <xs:element name="nestedAbsType">
    <xs:complexType><xs:sequence><xs:sequence><xs:element name="it" type="AbsType"/></xs:sequence></xs:sequence></xs:complexType>
  </xs:element>
</xs:schema>
"""

GROUP_OCCURS_EDGE_XSD: Final[str] = """<?xml version="1.0" encoding="UTF-8"?>
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
  <xs:element name="optGroup">
    <xs:complexType>
      <xs:sequence minOccurs="0" maxOccurs="1">
        <xs:element name="a" type="xs:string" minOccurs="1"/>
      </xs:sequence>
    </xs:complexType>
  </xs:element>
  <xs:element name="zeroMaxGroup">
    <xs:complexType>
      <xs:sequence minOccurs="0" maxOccurs="0">
        <xs:element name="a" type="xs:string" minOccurs="1" maxOccurs="1"/>
      </xs:sequence>
    </xs:complexType>
  </xs:element>
  <xs:element name="emptyGroup">
    <xs:complexType><xs:sequence/></xs:complexType>
  </xs:element>
  <xs:element name="unboundedGroup">
    <xs:complexType>
      <xs:sequence maxOccurs="unbounded">
        <xs:element name="a" type="xs:string"/>
      </xs:sequence>
    </xs:complexType>
  </xs:element>
</xs:schema>
"""


class CustomFallbackError(Exception):
    """Test-local fallback exception class."""


class CustomCodeError(Exception):
    """Test-local code-mapped exception class."""


@pytest.mark.parametrize(
    ("xsd_text", "element_tag", "error_by_code", "expected_exc", "expected_match"),
    [
        pytest.param(
            GROUP_REF_XSD,
            "root",
            {},
            CustomFallbackError,
            "xs:any wildcard element is not supported",
            id="group-ref-fallback",
        ),
        pytest.param(
            GROUP_REF_XSD,
            "root",
            {"E4": CustomCodeError},
            CustomCodeError,
            "xs:any wildcard element is not supported",
            id="group-ref-mapped-e4",
        ),
        pytest.param(
            NESTED_GROUPS_XSD,
            "nestedAbs",
            {},
            CustomFallbackError,
            "abstract element 'absEl' is not supported",
            id="nested-abstract-element-fallback",
        ),
        pytest.param(
            NESTED_GROUPS_XSD,
            "nestedAbs",
            {"E6": CustomCodeError},
            CustomCodeError,
            "abstract element 'absEl' is not supported",
            id="nested-abstract-element-mapped-e6",
        ),
        pytest.param(
            NESTED_GROUPS_XSD,
            "nestedSub",
            {},
            CustomFallbackError,
            "substitutionGroup on element 'headEl' is not supported",
            id="nested-substitution-group-fallback",
        ),
        pytest.param(
            NESTED_GROUPS_XSD,
            "nestedSub",
            {"E7": CustomCodeError},
            CustomCodeError,
            "substitutionGroup on element 'headEl' is not supported",
            id="nested-substitution-group-mapped-e7",
        ),
        pytest.param(
            NESTED_GROUPS_XSD,
            "nestedAbsType",
            {},
            CustomFallbackError,
            "abstract type 'AbsType' is not supported",
            id="nested-abstract-type-fallback",
        ),
        pytest.param(
            NESTED_GROUPS_XSD,
            "nestedAbsType",
            {"E6": CustomCodeError},
            CustomCodeError,
            "abstract type 'AbsType' is not supported",
            id="nested-abstract-type-mapped-e6",
        ),
    ],
)
def test_validate_group_particles_fail_group_ref_and_nested_error_forwarding(
    xsd_text: str,
    element_tag: str,
    error_by_code: dict[str, type[Exception]],
    expected_exc: type[Exception],
    expected_match: str,
) -> None:
    """validate_group_particles resolves xs:group refs, recurses into nested groups, and forwards error classes."""
    schema = xmlschema.XMLSchema(xsd_text)
    group = schema.elements[element_tag].type.model_group  # type: ignore[union-attr]
    assert isinstance(group, XsdGroup)
    with pytest.raises(expected_exc, match=expected_match):
        validate_group_particles(schema, group, error_type=CustomFallbackError, error_by_code=error_by_code)


@pytest.mark.parametrize(
    ("element_tag", "expected_min"),
    [
        pytest.param("optGroup", 0, id="min-occurs-zero-group"),
        pytest.param("emptyGroup", 0, id="empty-group"),
    ],
)
def test_group_min_occurs_by_name_pass_zero_and_empty(element_tag: str, expected_min: int) -> None:
    """group_min_occurs_by_name returns 0 for minOccurs=0 and empty model groups."""
    schema = xmlschema.XMLSchema(GROUP_OCCURS_EDGE_XSD)
    group = schema.elements[element_tag].type.model_group  # type: ignore[union-attr]
    assert isinstance(group, XsdGroup)
    assert group_min_occurs_by_name(group, "a") == expected_min


@pytest.mark.parametrize(
    ("element_tag", "expected_max"),
    [
        pytest.param("zeroMaxGroup", 0, id="max-occurs-zero-group"),
        pytest.param("emptyGroup", 0, id="empty-group"),
        pytest.param("unboundedGroup", None, id="unbounded-group"),
    ],
)
def test_group_max_occurs_by_name_pass_zero_empty_and_unbounded(
    element_tag: str,
    expected_max: int | None,
) -> None:
    """group_max_occurs_by_name handles maxOccurs=0, empty groups, and unbounded groups."""
    schema = xmlschema.XMLSchema(GROUP_OCCURS_EDGE_XSD)
    group = schema.elements[element_tag].type.model_group  # type: ignore[union-attr]
    assert isinstance(group, XsdGroup)
    assert group_max_occurs_by_name(group, "a") == expected_max
