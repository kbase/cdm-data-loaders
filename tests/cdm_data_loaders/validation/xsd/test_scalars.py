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


@pytest.mark.parametrize(
    ("type_name", "expected_local_name"),
    [
        pytest.param("RestrictedIntList", "int", id="restricted-list"),
        pytest.param("RestrictedInt", None, id="non-list-atomic"),
    ],
)
def test_unwrap_item_type_pass_list_and_non_list(
    type_name: str,
    expected_local_name: str | None,
) -> None:
    """unwrap_item_type unwraps restricted list item types and returns None for non-lists."""
    schema = xmlschema.XMLSchema(SCALAR_SCHEMA_XSD)
    item_type = unwrap_item_type(schema.types[type_name])
    actual_local_name = item_type.local_name if item_type is not None else None
    assert actual_local_name == expected_local_name


@pytest.mark.parametrize(
    ("type_name", "expected_member_names"),
    [
        pytest.param("RestrictedInt", [], id="non-union-atomic"),
        pytest.param("RestrictedUnion", ["int", "RestrictedInt"], id="restricted-union"),
    ],
)
def test_unwrap_member_types_pass_union_and_non_union(
    type_name: str,
    expected_member_names: list[str],
) -> None:
    """unwrap_member_types unwraps restricted union members and returns an empty list for non-unions."""
    schema = xmlschema.XMLSchema(SCALAR_SCHEMA_XSD)
    members = unwrap_member_types(schema.types[type_name])
    assert [m.local_name for m in members] == expected_member_names


@pytest.mark.parametrize(
    ("type_name", "expected_name"),
    [
        pytest.param("RestrictedInt", "xs:int", id="xsd-builtin-type"),
        pytest.param(None, "xs:string", id="nameless-fallback"),
    ],
)
def test_builtin_xsd_name_pass_builtin_and_fallback(
    type_name: str | None,
    expected_name: str,
) -> None:
    """builtin_xsd_name resolves XSD built-in names and falls back to xs:string when unnamed."""
    if type_name is None:
        candidate: object = SimpleNamespace(name=None, base_type=None)
    else:
        candidate = xmlschema.XMLSchema(SCALAR_SCHEMA_XSD).types[type_name]
    assert builtin_xsd_name(candidate) == expected_name  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("lookup_kind", "lookup_name", "expected_names"),
    [
        pytest.param("child", "plainChild", ["xs:NMTOKEN"], id="builtin-nmtokens"),
        pytest.param("type", "RestrictedIntList", ["xs:int"], id="restricted-list"),
        pytest.param("type", "HeterogeneousUnion", ["xs:int", "xs:boolean"], id="heterogeneous-union"),
    ],
)
def test_iter_simple_builtin_names_pass_list_and_union_shapes(
    lookup_kind: str,
    lookup_name: str,
    expected_names: list[str],
) -> None:
    """iter_simple_builtin_names expands built-in list types, xs:list restrictions, and xs:union members."""
    schema = xmlschema.XMLSchema(SCALAR_SCHEMA_XSD)
    if lookup_kind == "child":
        root_group = schema.elements["root"].type.model_group  # type: ignore[union-attr]
        target_type = next(el.type for el in root_group.iter_elements() if el.local_name == lookup_name)
    else:
        target_type = schema.types[lookup_name]
    assert target_type is not None
    assert iter_simple_builtin_names(target_type) == expected_names


@pytest.mark.parametrize(
    ("child_name", "expected_doc"),
    [
        pytest.param("globalRef", "Ref element doc", id="element-ref-doc"),
        pytest.param("typedChild", "Type level comment", id="type-fallback-doc"),
        pytest.param("plainChild", None, id="missing-doc"),
    ],
)
def test_extract_doc_comment_pass_ref_fallback_and_missing(
    child_name: str,
    expected_doc: str | None,
) -> None:
    """extract_doc_comment resolves component, ref, and fallback_type documentation and returns None when absent."""
    schema = xmlschema.XMLSchema(SCALAR_SCHEMA_XSD)
    root_type = schema.elements["root"].type
    children = {el.local_name: el for el in root_type.model_group.iter_elements()}  # type: ignore[union-attr]
    child = children[child_name]
    fallback_type = None if child_name == "globalRef" else child.type
    assert extract_doc_comment(child, fallback_type) == expected_doc
