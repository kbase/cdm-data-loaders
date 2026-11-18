"""Unit tests for XSD schema loading, level resolution, and complex-type traversal helpers."""

from pathlib import Path
from typing import Final

import pytest
import xmlschema
from xmlschema.validators import XsdComplexType

from cdm_data_loaders.converters.exceptions.facades.xsd_to_pyarrow import (
    ERROR_BY_CODE,
    WildcardAttributeError,
    XsdToPyArrowError,
)
from cdm_data_loaders.validation.xsd.traversal import (
    LeafKind,
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

HIERARCHY_XSD: Final[str] = """<?xml version="1.0" encoding="UTF-8"?>
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

RECURSIVE_NODE_XSD: Final[str] = """<?xml version="1.0" encoding="UTF-8"?>
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


@pytest.fixture
def hierarchy_schema(tmp_path: Path) -> xmlschema.XMLSchema:
    """Write ``HIERARCHY_XSD`` to ``tmp_path`` and load it via ``load_schema``.

    :param tmp_path: Per-test temporary directory.
    :type tmp_path: Path
    :return: Loaded ``XMLSchema`` instance.
    :rtype: xmlschema.XMLSchema
    """
    xsd_file = tmp_path / "hierarchy.xsd"
    xsd_file.write_text(HIERARCHY_XSD, encoding="utf-8")
    return load_schema(xsd_file)


@pytest.mark.parametrize(
    ("root_tag", "expected_names"),
    [
        pytest.param(None, ["catalog"], id="inferred-unreferenced-root"),
        pytest.param("{http://example.org/ns}record", ["record"], id="explicit-clark-root-tag"),
    ],
)
def test_resolve_root_elements_pass(
    hierarchy_schema: xmlschema.XMLSchema,
    root_tag: str | None,
    expected_names: list[str],
) -> None:
    """resolve_root_elements infers unreferenced roots and resolves explicit Clark-notation root tags."""
    roots = resolve_root_elements(hierarchy_schema, root_tag=root_tag)
    assert [r.local_name for r in roots] == expected_names


@pytest.mark.parametrize(
    ("level", "expected_names"),
    [
        pytest.param(1, ["record"], id="level-1"),
        pytest.param(2, ["flag", "note", "count"], id="level-2"),
    ],
)
def test_resolve_elements_at_level_pass(
    hierarchy_schema: xmlschema.XMLSchema,
    level: int,
    expected_names: list[str],
) -> None:
    """resolve_elements_at_level returns deduplicated child elements at each positive depth."""
    resolved = resolve_elements_at_level(hierarchy_schema, level)
    assert list(resolved) == expected_names


@pytest.mark.parametrize(
    ("level", "expected_match"),
    [
        pytest.param(-1, "level must be >= 0", id="negative-level"),
        pytest.param(5, "No XML elements found at level 5", id="empty-deep-level"),
    ],
)
def test_resolve_elements_at_level_fail_invalid_or_empty_depth(
    hierarchy_schema: xmlschema.XMLSchema,
    level: int,
    expected_match: str,
) -> None:
    """resolve_elements_at_level rejects negative levels and depths with no reachable elements."""
    with pytest.raises(ValueError, match=expected_match):
        resolve_elements_at_level(hierarchy_schema, level)


@pytest.mark.parametrize(
    ("type_name", "expected"),
    [
        pytest.param("EmptyMarkerType", True, id="empty-marker"),
        pytest.param("MixedLeafType", False, id="mixed-non-empty"),
    ],
)
def test_is_empty_marker_complex_type_pass(
    hierarchy_schema: xmlschema.XMLSchema,
    type_name: str,
    expected: bool,
) -> None:
    """is_empty_marker_complex_type distinguishes empty marker complexTypes from content-bearing types."""
    xsd_type = hierarchy_schema.types[type_name]
    assert isinstance(xsd_type, XsdComplexType)
    assert is_empty_marker_complex_type(xsd_type) is expected


@pytest.mark.parametrize(
    ("source_kind", "name", "expected_kind", "expect_content_type"),
    [
        pytest.param("type", "EmptyMarkerType", "empty_marker", False, id="empty-marker"),
        pytest.param("type", "MixedLeafType", "mixed", False, id="mixed-leaf"),
        pytest.param("type", "SimpleContentLeafType", "simple", True, id="simple-content-leaf"),
        pytest.param("element", "record", None, False, id="complex-struct-returns-none"),
    ],
)
def test_classify_leaf_element_type_pass_leaf_and_struct_types(
    hierarchy_schema: xmlschema.XMLSchema,
    source_kind: str,
    name: str,
    expected_kind: LeafKind | None,
    expect_content_type: bool,
) -> None:
    """classify_leaf_element_type classifies empty-marker, mixed, simple-content, and struct complexTypes."""
    elem_type = hierarchy_schema.types[name] if source_kind == "type" else hierarchy_schema.elements[name].type
    classified = classify_leaf_element_type(elem_type)
    if expected_kind is None:
        assert classified is None
    else:
        assert classified is not None
        kind, content_type = classified
        assert kind == expected_kind
        assert (content_type is not None) is expect_content_type


def test_prepare_target_traversal_pass_collects_attributes_and_children(
    hierarchy_schema: xmlschema.XMLSchema,
) -> None:
    """prepare_target_traversal resolves target element, attributes, children, and traversal context."""
    max_depth = 2
    target_el, complex_type, ctx = prepare_target_traversal(
        hierarchy_schema,
        "record",
        max_recursion_depth=max_depth,
    )
    assert target_el.local_name == "record"
    assert [a.local_name for a in collect_attributes(complex_type)] == ["id"]
    assert [c.local_name for c in collect_unique_children(hierarchy_schema, complex_type)] == ["flag", "note", "count"]
    assert ctx.max_recursion_depth == max_depth


def test_resolve_target_element_fail_missing_tag(hierarchy_schema: xmlschema.XMLSchema) -> None:
    """resolve_target_element raises ValueError when the requested xml_tag is absent."""
    with pytest.raises(ValueError, match="not found"):
        resolve_target_element(hierarchy_schema, "nonexistent")


def test_require_complex_type_fail_simple_type(hierarchy_schema: xmlschema.XMLSchema) -> None:
    """require_complex_type raises ValueError when given a non-complex XSD type."""
    simple_content_type = hierarchy_schema.types["SimpleContentLeafType"].content  # type: ignore[attr-defined]
    with pytest.raises(ValueError, match="must have a complex type"):
        require_complex_type(simple_content_type, "plain")


@pytest.mark.parametrize(
    ("start_type_name", "ancestor_type_name", "expected"),
    [
        pytest.param("NodeType", "NodeType", True, id="recursive-self-ancestry"),
        pytest.param("LeafStruct", "NodeType", False, id="non-recursive-leaf"),
        pytest.param("NodeType", None, False, id="empty-ancestry"),
    ],
)
def test_can_reach_any_type_pass_ancestry_cases(
    start_type_name: str,
    ancestor_type_name: str | None,
    expected: bool,
) -> None:
    """can_reach_any_type detects ancestor cycles and returns False for leaf or empty ancestries."""
    schema = xmlschema.XMLSchema(RECURSIVE_NODE_XSD)
    start_type = schema.types[start_type_name]
    assert isinstance(start_type, XsdComplexType)
    target_ids = {id(schema.types[ancestor_type_name]): 1} if ancestor_type_name is not None else {}
    assert can_reach_any_type(start_type, target_ids) is expected


def test_check_no_forbidden_types_under_recursion_fail_date_field() -> None:
    """check_no_forbidden_types_under_recursion rejects xs:date fields inside recursive complex types."""
    schema = xmlschema.XMLSchema(RECURSIVE_NODE_XSD)
    node_type = schema.types["NodeType"]
    ctx = TraversalContext(schema=schema, type_counts={id(node_type): 1}, max_recursion_depth=2)
    with pytest.raises(ValueError, match="xs:date"):
        check_no_forbidden_types_under_recursion(node_type, ctx)


def test_traversal_context_pass_with_type_and_unbounded() -> None:
    """Verify TraversalContext.with_type increments type_counts and unbounded clears them."""
    schema = xmlschema.XMLSchema(HIERARCHY_XSD)
    ctx = TraversalContext(
        schema=schema,
        type_counts={},
        max_recursion_depth=2,
        error_type=XsdToPyArrowError,
        error_by_code=ERROR_BY_CODE,
    )
    record_type = schema.elements["record"].type
    stepped = ctx.with_type(id(record_type))
    assert stepped.type_counts == {id(record_type): 1}
    assert stepped.unbounded().type_counts == {}


def test_validate_target_struct_element_fail_nested_wildcard_attribute() -> None:
    """Raise WildcardAttributeError when a nested complexType declares xs:anyAttribute."""
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
    _, entry_type, entry_ctx = prepare_target_traversal(
        schema,
        "entry",
        max_recursion_depth=2,
        error_type=XsdToPyArrowError,
        error_by_code=ERROR_BY_CODE,
    )
    with pytest.raises(WildcardAttributeError, match="xs:anyAttribute"):
        validate_target_struct_element(entry_type, "entry", entry_ctx)


def test_validate_target_struct_element_and_complex_subtree_pass_recursive_type() -> None:
    """Validate recursive complexType subtrees without infinite recursion."""
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
