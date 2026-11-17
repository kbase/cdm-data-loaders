"""Unit tests for XSD scalar, list, union, and documentation helpers."""

from types import SimpleNamespace

import pytest
import xmlschema

from cdm_data_loaders.validation.xsd.scalars import (
    XSD_SCALAR_KINDS,
    builtin_xsd_name,
    extract_doc_comment,
    iter_simple_builtin_names,
    resolve_xsd_scalar_kind,
    unwrap_item_type,
    unwrap_member_types,
)

SCALAR_SCHEMA_XSD = """<?xml version="1.0" encoding="UTF-8"?>
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
  <xs:simpleType name="RestrictedInt">
    <xs:restriction base="xs:int">
      <xs:minInclusive value="0"/>
    </xs:restriction>
  </xs:simpleType>
  <xs:simpleType name="IntUnion">
    <xs:union memberTypes="xs:int RestrictedInt"/>
  </xs:simpleType>
  <xs:simpleType name="HeterogeneousUnion">
    <xs:union memberTypes="xs:int xs:boolean"/>
  </xs:simpleType>
  <xs:simpleType name="RestrictedUnion">
    <xs:restriction base="IntUnion"/>
  </xs:simpleType>
  <xs:simpleType name="IntList">
    <xs:list itemType="xs:int"/>
  </xs:simpleType>
  <xs:simpleType name="RestrictedIntList">
    <xs:restriction base="IntList">
      <xs:minLength value="1"/>
    </xs:restriction>
  </xs:simpleType>
  <xs:simpleType name="AnnotatedType">
    <xs:annotation>
      <xs:documentation>  Type   level
      comment  </xs:documentation>
    </xs:annotation>
    <xs:restriction base="xs:string"/>
  </xs:simpleType>
  <xs:element name="globalRef" type="xs:string">
    <xs:annotation>
      <xs:documentation>Ref element doc</xs:documentation>
    </xs:annotation>
  </xs:element>
  <xs:element name="root">
    <xs:complexType>
      <xs:sequence>
        <xs:element ref="globalRef"/>
        <xs:element name="typedChild" type="AnnotatedType"/>
        <xs:element name="plainChild" type="xs:NMTOKENS"/>
      </xs:sequence>
    </xs:complexType>
  </xs:element>
</xs:schema>
"""


@pytest.mark.parametrize(
    ("type_name", "expected_kind", "expected_xsd"),
    [
        pytest.param("RestrictedInt", "int32", "xs:int", id="restricted-atomic"),
        pytest.param("IntUnion", "int32", "xs:int | xs:int", id="homogeneous-union"),
        pytest.param("RestrictedUnion", "int32", "xs:int | xs:int", id="restricted-union"),
        pytest.param("HeterogeneousUnion", "string", "xs:int | xs:boolean", id="heterogeneous-union"),
    ],
)
def test_resolve_xsd_scalar_kind_pass_types(
    type_name: str,
    expected_kind: str,
    expected_xsd: str,
) -> None:
    """resolve_xsd_scalar_kind resolves atomic restrictions and homogeneous/heterogeneous unions."""
    schema = xmlschema.XMLSchema(SCALAR_SCHEMA_XSD)
    assert resolve_xsd_scalar_kind(schema.types[type_name]) == (expected_kind, expected_xsd)
    assert "xs:date" in XSD_SCALAR_KINDS


def test_unwrap_and_iter_simple_builtin_names_pass_all_shapes() -> None:
    """unwrap_item_type, unwrap_member_types, builtin_xsd_name, and iter_simple_builtin_names handle lists, unions, and fallbacks."""
    schema = xmlschema.XMLSchema(SCALAR_SCHEMA_XSD)
    restricted_list = schema.types["RestrictedIntList"]
    item_type = unwrap_item_type(restricted_list)
    assert item_type is not None
    assert builtin_xsd_name(item_type) == "xs:int"
    assert unwrap_item_type(schema.types["RestrictedInt"]) is None
    assert unwrap_member_types(schema.types["RestrictedInt"]) == []
    expected_union_members = 2
    assert len(unwrap_member_types(schema.types["RestrictedUnion"])) == expected_union_members

    root_type = schema.elements["root"].type
    plain_child = next(el for el in root_type.model_group.iter_elements() if el.local_name == "plainChild")  # type: ignore[union-attr]
    assert iter_simple_builtin_names(plain_child.type) == ["xs:NMTOKEN"]
    assert iter_simple_builtin_names(restricted_list) == ["xs:int"]
    assert iter_simple_builtin_names(schema.types["HeterogeneousUnion"]) == ["xs:int", "xs:boolean"]

    synthetic = SimpleNamespace(name=None, base_type=None)
    assert builtin_xsd_name(synthetic) == "xs:string"  # type: ignore[arg-type]


def test_extract_doc_comment_pass_ref_fallback_and_missing() -> None:
    """extract_doc_comment resolves component, ref, and fallback_type documentation and returns None when absent."""
    schema = xmlschema.XMLSchema(SCALAR_SCHEMA_XSD)
    root_type = schema.elements["root"].type
    children = {el.local_name: el for el in root_type.model_group.iter_elements()}  # type: ignore[union-attr]

    assert extract_doc_comment(children["globalRef"]) == "Ref element doc"
    assert extract_doc_comment(children["typedChild"], children["typedChild"].type) == "Type level comment"
    assert extract_doc_comment(children["plainChild"], children["plainChild"].type) is None
