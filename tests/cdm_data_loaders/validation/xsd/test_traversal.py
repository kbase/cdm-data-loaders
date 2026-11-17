"""Unit tests for XSD schema loading, level resolution, and complex-type traversal helpers."""

from pathlib import Path

import pytest
import xmlschema

from cdm_data_loaders.converters.exceptions.facades.xsd_to_pyarrow import (
    ERROR_BY_CODE,
    WildcardAttributeError,
    XsdToPyArrowError,
)
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
    validate_complex_subtree,
    validate_target_struct_element,
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


def test_traversal_context_methods_and_complex_subtree_validation() -> None:
    """Validate TraversalContext transitions and nested complexType error propagation."""
    nested_wildcard_xsd = """<?xml version="1.0" encoding="UTF-8"?>
    <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
      <xs:complexType name="InnerBad">
        <xs:sequence><xs:element name="k" type="xs:string"/></xs:sequence>
        <xs:anyAttribute processContents="skip"/>
      </xs:complexType>
      <xs:element name="entry">
        <xs:complexType>
          <xs:sequence>
            <xs:element name="inner" type="InnerBad" minOccurs="0"/>
          </xs:sequence>
        </xs:complexType>
      </xs:element>
    </xs:schema>
    """
    schema = xmlschema.XMLSchema(nested_wildcard_xsd)
    ctx = TraversalContext(
        schema=schema,
        type_counts={},
        max_recursion_depth=2,
        error_type=XsdToPyArrowError,
        error_by_code=ERROR_BY_CODE,
    )
    inner_type = schema.types["InnerBad"]
    stepped = ctx.with_type(id(inner_type))
    assert stepped.type_counts == {id(inner_type): 1}
    assert stepped.unbounded().type_counts == {}

    _, entry_type, entry_ctx = prepare_target_traversal(
        schema,
        "entry",
        max_recursion_depth=2,
        error_type=XsdToPyArrowError,
        error_by_code=ERROR_BY_CODE,
    )
    with pytest.raises(WildcardAttributeError, match="xs:anyAttribute"):
        validate_target_struct_element(entry_type, "entry", entry_ctx)

    recursive_ok_xsd = """<?xml version="1.0" encoding="UTF-8"?>
    <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
      <xs:complexType name="TreeNode">
        <xs:sequence>
          <xs:element name="label" type="xs:string"/>
          <xs:element name="child" type="TreeNode" minOccurs="0"/>
        </xs:sequence>
      </xs:complexType>
      <xs:element name="root" type="TreeNode"/>
    </xs:schema>
    """
    rec_schema = xmlschema.XMLSchema(recursive_ok_xsd)
    _, rec_complex, rec_ctx = prepare_target_traversal(rec_schema, "root", max_recursion_depth=2)
    validate_target_struct_element(rec_complex, "root", rec_ctx)
    validate_complex_subtree(rec_complex, rec_ctx)


@pytest.mark.parametrize(
    ("bad_depth", "exc_cls"),
    [
        pytest.param(0, ValueError, id="zero"),
        pytest.param(-1, ValueError, id="negative"),
        pytest.param(True, TypeError, id="bool-true"),
        pytest.param(False, TypeError, id="bool-false"),
    ],
)
def test_prepare_target_traversal_fail_invalid_max_recursion_depth(bad_depth: int, exc_cls: type[Exception]) -> None:
    """Reject non-positive or boolean max_recursion_depth in prepare_target_traversal."""
    schema = xmlschema.XMLSchema(HIERARCHY_XSD)
    with pytest.raises(exc_cls, match="max_recursion_depth"):
        prepare_target_traversal(schema, "record", max_recursion_depth=bad_depth)
