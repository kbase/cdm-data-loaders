"""Unit tests for XSD schema loading, level resolution, and complex-type traversal helpers."""

from pathlib import Path

import pytest
import xmlschema

from cdm_data_loaders.validation.xsd.traversal import (
    TraversalContext,
    can_reach_any_type,
    check_no_forbidden_types_under_recursion,
    classify_leaf_element_type,
    collect_attributes,
    collect_unique_children,
    is_empty_marker_complex_type,
    load_schema,
    prepare_target_traversal,
    require_complex_type,
    resolve_elements_at_level,
    resolve_root_elements,
    resolve_target_element,
)

HIERARCHY_XSD = """<?xml version="1.0" encoding="UTF-8"?>
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema"
           targetNamespace="http://example.org/ns"
           xmlns:tns="http://example.org/ns"
           elementFormDefault="qualified">
  <xs:complexType name="EmptyMarkerType"/>
  <xs:complexType name="MixedLeafType" mixed="true">
    <xs:sequence>
      <xs:element name="b" type="xs:string" minOccurs="0"/>
    </xs:sequence>
  </xs:complexType>
  <xs:complexType name="SimpleContentLeafType">
    <xs:simpleContent>
      <xs:extension base="xs:int"/>
    </xs:simpleContent>
  </xs:complexType>
  <xs:element name="record">
    <xs:complexType>
      <xs:sequence>
        <xs:element name="flag" type="tns:EmptyMarkerType"/>
        <xs:element name="note" type="tns:MixedLeafType"/>
        <xs:element name="count" type="tns:SimpleContentLeafType"/>
      </xs:sequence>
      <xs:attribute name="id" type="xs:string" use="required"/>
    </xs:complexType>
  </xs:element>
  <xs:element name="catalog">
    <xs:complexType>
      <xs:sequence>
        <xs:element ref="tns:record" maxOccurs="unbounded"/>
      </xs:sequence>
    </xs:complexType>
  </xs:element>
</xs:schema>
"""


def test_load_schema_and_level_resolution(tmp_path: Path) -> None:
    """load_schema, resolve_root_elements, resolve_elements_at_level, and leaf classifiers resolve multi-level XSDs."""
    xsd_file = tmp_path / "hierarchy.xsd"
    xsd_file.write_text(HIERARCHY_XSD, encoding="utf-8")
    schema = load_schema(xsd_file)

    roots = resolve_root_elements(schema)
    assert [r.local_name for r in roots] == ["catalog"]
    explicit = resolve_root_elements(schema, root_tag="{http://example.org/ns}record")
    assert [r.local_name for r in explicit] == ["record"]

    level1 = resolve_elements_at_level(schema, 1)
    assert list(level1) == ["record"]
    level2 = resolve_elements_at_level(schema, 2)
    assert list(level2) == ["flag", "note", "count"]

    assert is_empty_marker_complex_type(level2["flag"].type) is True  # type: ignore[arg-type]
    assert classify_leaf_element_type(level2["flag"].type) == ("empty_marker", None)
    assert classify_leaf_element_type(level2["note"].type) == ("mixed", None)
    kind, content_type = classify_leaf_element_type(level2["count"].type) or ("simple", None)
    assert kind == "simple"
    assert content_type is not None
    assert classify_leaf_element_type(level1["record"].type) is None

    max_depth = 2
    target_el, complex_type, ctx = prepare_target_traversal(schema, "record", max_recursion_depth=max_depth)
    assert target_el.local_name == "record"
    assert [a.local_name for a in collect_attributes(complex_type)] == ["id"]
    assert [c.local_name for c in collect_unique_children(schema, complex_type)] == ["flag", "note", "count"]
    assert ctx.max_recursion_depth == max_depth

    with pytest.raises(ValueError, match="level must be >= 0"):
        resolve_elements_at_level(schema, -1)
    with pytest.raises(ValueError, match="No XML elements found at level 5"):
        resolve_elements_at_level(schema, 5)
    with pytest.raises(ValueError, match="not found"):
        resolve_target_element(schema, "nonexistent")
    with pytest.raises(ValueError, match="must have a complex type"):
        require_complex_type(content_type, "plain")


def test_recursion_reachability_and_forbidden_types() -> None:
    """can_reach_any_type detects ancestor cycles and check_no_forbidden_types_under_recursion rejects date/binary fields."""
    xsd_text = """<?xml version="1.0" encoding="UTF-8"?>
    <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
      <xs:complexType name="LeafStruct">
        <xs:sequence><xs:element name="label" type="xs:string"/></xs:sequence>
      </xs:complexType>
      <xs:complexType name="NodeType">
        <xs:sequence>
          <xs:element name="leaf" type="LeafStruct"/>
          <xs:element name="stamp" type="xs:date"/>
          <xs:element name="child" type="NodeType" minOccurs="0"/>
        </xs:sequence>
      </xs:complexType>
      <xs:element name="root" type="NodeType"/>
    </xs:schema>
    """
    schema = xmlschema.XMLSchema(xsd_text)
    node_type = schema.types["NodeType"]
    leaf_type = schema.types["LeafStruct"]
    assert can_reach_any_type(node_type, {id(node_type): 1}) is True  # type: ignore[arg-type]
    assert can_reach_any_type(leaf_type, {id(node_type): 1}) is False  # type: ignore[arg-type]
    assert can_reach_any_type(node_type, {}) is False  # type: ignore[arg-type]

    ctx = TraversalContext(schema=schema, type_counts={id(node_type): 1}, max_recursion_depth=2)
    with pytest.raises(ValueError, match="xs:date"):
        check_no_forbidden_types_under_recursion(node_type, ctx)
