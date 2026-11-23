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


CAN_REACH_DESCENDANT_XSD: Final[str] = """<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
  <xs:complexType name="Target"/>
  <xs:complexType name="Middle">
    <xs:sequence>
      <xs:element name="self" type="Middle" minOccurs="0"/>
      <xs:element name="target" type="Target" minOccurs="0"/>
    </xs:sequence>
  </xs:complexType>
  <xs:complexType name="Wrapper">
    <xs:sequence>
      <xs:element name="mid" type="Middle"/>
    </xs:sequence>
  </xs:complexType>
  <xs:complexType name="ZeroWrapper">
    <xs:sequence>
      <xs:element name="mid" type="Middle" minOccurs="0" maxOccurs="0"/>
    </xs:sequence>
  </xs:complexType>
</xs:schema>"""

CAN_REACH_VISITED_CYCLE_XSD: Final[str] = """<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
  <xs:complexType name="Target"/>
  <xs:complexType name="Cycle">
    <xs:sequence><xs:element name="c" type="Cycle" minOccurs="0"/></xs:sequence>
  </xs:complexType>
  <xs:complexType name="Root">
    <xs:sequence>
      <xs:element name="t" type="Target"/>
      <xs:element name="c1" type="Cycle"/>
      <xs:element name="c2" type="Cycle"/>
    </xs:sequence>
  </xs:complexType>
</xs:schema>"""

FORBIDDEN_ATTR_RECURSION_XSD: Final[str] = """<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
  <xs:complexType name="NodeType">
    <xs:sequence><xs:element name="child" type="NodeType" minOccurs="0"/></xs:sequence>
    <xs:attribute name="when" type="xs:date"/>
  </xs:complexType>
</xs:schema>"""

FORBIDDEN_SIMPLE_CONTENT_RECURSION_XSD: Final[str] = """<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
  <xs:complexType name="DateText">
    <xs:simpleContent><xs:extension base="xs:dateTime"/></xs:simpleContent>
  </xs:complexType>
  <xs:complexType name="NodeType">
    <xs:sequence>
      <xs:element name="dt" type="DateText"/>
      <xs:element name="child" type="NodeType" minOccurs="0"/>
    </xs:sequence>
  </xs:complexType>
</xs:schema>"""

NAMESPACED_RECURSION_XSD: Final[str] = """<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema"
                   targetNamespace="http://example.org/ns"
                   xmlns:tns="http://example.org/ns">
  <xs:complexType name="NodeType">
    <xs:sequence>
      <xs:element name="stamp" type="xs:date"/>
      <xs:element name="child" type="tns:NodeType" minOccurs="0"/>
    </xs:sequence>
  </xs:complexType>
  <xs:complexType name="MixedRec" mixed="true">
    <xs:sequence>
      <xs:element name="stamp" type="xs:date"/>
      <xs:element name="child" type="tns:MixedRec" minOccurs="0"/>
    </xs:sequence>
  </xs:complexType>
</xs:schema>"""

DEEP_ELEMENT_XSD: Final[str] = """<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema"
                       targetNamespace="http://example.org/ns"
                       xmlns:tns="http://example.org/ns"
                       elementFormDefault="qualified">
  <xs:element name="root">
    <xs:complexType>
      <xs:sequence>
        <xs:element name="branch">
          <xs:complexType>
            <xs:sequence>
              <xs:element name="item" type="xs:string"/>
            </xs:sequence>
          </xs:complexType>
        </xs:element>
      </xs:sequence>
    </xs:complexType>
  </xs:element>
</xs:schema>"""

AMBIGUOUS_ELEMENT_XSD: Final[str] = """<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
  <xs:element name="root">
    <xs:complexType>
      <xs:sequence>
        <xs:element name="a">
          <xs:complexType><xs:sequence><xs:element name="dup" type="xs:string"/></xs:sequence></xs:complexType>
        </xs:element>
        <xs:element name="b">
          <xs:complexType><xs:sequence><xs:element name="dup" type="xs:int"/></xs:sequence></xs:complexType>
        </xs:element>
      </xs:sequence>
    </xs:complexType>
  </xs:element>
</xs:schema>"""

ABSTRACT_ELEMENT_XSD: Final[str] = """<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
  <xs:complexType name="AbsType" abstract="true">
    <xs:sequence><xs:element name="x" type="xs:string"/></xs:sequence>
  </xs:complexType>
  <xs:element name="absEl" type="xs:string" abstract="true"/>
  <xs:element name="absTyped" type="AbsType"/>
  <xs:element name="holder">
    <xs:complexType>
      <xs:sequence>
        <xs:element ref="absEl"/>
      </xs:sequence>
    </xs:complexType>
  </xs:element>
</xs:schema>"""

NO_GLOBAL_ELEMENTS_XSD: Final[str] = """<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
  <xs:complexType name="OnlyType"><xs:sequence><xs:element name="x" type="xs:string"/></xs:sequence></xs:complexType>
</xs:schema>"""

MUTUAL_CYCLE_XSD: Final[str] = """<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
  <xs:element name="a">
    <xs:complexType><xs:sequence><xs:element ref="b" minOccurs="0"/></xs:sequence></xs:complexType>
  </xs:element>
  <xs:element name="b">
    <xs:complexType><xs:sequence><xs:element ref="a" minOccurs="0"/></xs:sequence></xs:complexType>
  </xs:element>
</xs:schema>"""

SELF_REF_XSD: Final[str] = """<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
  <xs:element name="node">
    <xs:complexType>
      <xs:sequence>
        <xs:element ref="node" minOccurs="0"/>
        <xs:element ref="leaf" minOccurs="0" maxOccurs="0"/>
      </xs:sequence>
    </xs:complexType>
  </xs:element>
  <xs:element name="leaf" type="xs:string"/>
</xs:schema>"""

ZERO_OCCURS_BEFORE_REF_XSD: Final[str] = """<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
  <xs:element name="skip_target" type="xs:string"/>
  <xs:element name="real_target" type="xs:string"/>
  <xs:element name="root">
    <xs:complexType>
      <xs:sequence>
        <xs:element ref="skip_target" minOccurs="0" maxOccurs="0"/>
        <xs:element ref="real_target"/>
      </xs:sequence>
    </xs:complexType>
  </xs:element>
</xs:schema>"""

AMBIGUOUS_ROOTS_XSD: Final[str] = """<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
  <xs:element name="a">
    <xs:complexType><xs:sequence><xs:element name="dup" type="xs:string"/></xs:sequence></xs:complexType>
  </xs:element>
  <xs:element name="b">
    <xs:complexType><xs:sequence><xs:element name="dup" type="xs:int"/></xs:sequence></xs:complexType>
  </xs:element>
</xs:schema>"""

WILDCARD_ANY_XSD: Final[str] = """<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
  <xs:element name="root">
    <xs:complexType>
      <xs:sequence>
        <xs:any processContents="skip"/>
      </xs:sequence>
    </xs:complexType>
  </xs:element>
</xs:schema>"""

MIXED_NON_STRUCT_PARENTS_XSD: Final[str] = """<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
  <xs:complexType name="Shared">
    <xs:sequence><xs:element name="k" type="xs:string"/></xs:sequence>
  </xs:complexType>
  <xs:element name="plain" type="xs:string"/>
  <xs:element name="sc">
    <xs:complexType><xs:simpleContent><xs:extension base="xs:string"/></xs:simpleContent></xs:complexType>
  </xs:element>
  <xs:element name="mx">
    <xs:complexType mixed="true"><xs:sequence><xs:element name="ignored" type="xs:string"/></xs:sequence></xs:complexType>
  </xs:element>
  <xs:element name="s1" type="Shared"/>
  <xs:element name="s2" type="Shared"/>
</xs:schema>"""

TARGET_STRUCT_EDGE_XSD: Final[str] = """<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
  <xs:element name="empty"><xs:complexType/></xs:element>
  <xs:element name="plainSc">
    <xs:complexType><xs:simpleContent><xs:extension base="xs:string"/></xs:simpleContent></xs:complexType>
  </xs:element>
  <xs:element name="attrSc">
    <xs:complexType>
      <xs:simpleContent>
        <xs:extension base="xs:string"><xs:attribute name="unit" type="xs:string"/></xs:extension>
      </xs:simpleContent>
    </xs:complexType>
  </xs:element>
</xs:schema>"""

COLLISION_XSD: Final[str] = """<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
  <xs:element name="rec">
    <xs:complexType>
      <xs:sequence><xs:element name="_id" type="xs:string"/></xs:sequence>
      <xs:attribute name="id" type="xs:string"/>
    </xs:complexType>
  </xs:element>
</xs:schema>"""

SHARED_BAD_DAG_XSD: Final[str] = """<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
  <xs:complexType name="SharedBad">
    <xs:sequence><xs:element name="_v" type="xs:string"/></xs:sequence>
    <xs:attribute name="v" type="xs:string"/>
  </xs:complexType>
  <xs:element name="root">
    <xs:complexType>
      <xs:sequence>
        <xs:element name="first" type="SharedBad"/>
      </xs:sequence>
    </xs:complexType>
  </xs:element>
</xs:schema>"""

LEAF_ONLY_XSD: Final[str] = """<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
  <xs:element name="leaf" type="xs:string"/>
</xs:schema>"""


class CustomFallbackError(Exception):
    """Test-local fallback exception class."""


class CustomCodeError(Exception):
    """Test-local code-mapped exception class."""


@pytest.mark.parametrize(
    ("error_by_code", "expected_exc"),
    [
        pytest.param({}, CustomFallbackError, id="fallback-error-type"),
        pytest.param({"E1": CustomCodeError}, CustomCodeError, id="mapped-error-code"),
    ],
)
def test_traversal_context_raise_err_fail_fallback_and_code_map(
    hierarchy_schema: xmlschema.XMLSchema,
    error_by_code: dict[str, type[Exception]],
    expected_exc: type[Exception],
) -> None:
    """TraversalContext.raise_err uses error_by_code when matched and error_type otherwise."""
    ctx = TraversalContext(
        schema=hierarchy_schema,
        type_counts={},
        max_recursion_depth=2,
        error_type=CustomFallbackError,
        error_by_code=error_by_code,
    )
    with pytest.raises(expected_exc, match="boom"):
        ctx.raise_err("E1", "boom")


@pytest.mark.parametrize(
    ("xsd_text", "expected"),
    [
        pytest.param(
            """<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
              <xs:complexType name="T">
                <xs:attribute name="a" type="xs:string" use="prohibited"/>
              </xs:complexType>
            </xs:schema>""",
            True,
            id="prohibited-attr-only",
        ),
        pytest.param(
            """<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
              <xs:complexType name="T">
                <xs:sequence><xs:element name="a" type="xs:string" minOccurs="0" maxOccurs="0"/></xs:sequence>
              </xs:complexType>
            </xs:schema>""",
            True,
            id="zero-max-occurs-child-only",
        ),
        pytest.param(
            """<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
              <xs:complexType name="T">
                <xs:attribute name="a" type="xs:string" use="optional"/>
              </xs:complexType>
            </xs:schema>""",
            False,
            id="optional-attr",
        ),
        pytest.param(
            """<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
              <xs:complexType name="T">
                <xs:simpleContent><xs:extension base="xs:string"/></xs:simpleContent>
              </xs:complexType>
            </xs:schema>""",
            False,
            id="simple-content",
        ),
    ],
)
def test_is_empty_marker_complex_type_pass_edge_cases(xsd_text: str, expected: bool) -> None:
    """is_empty_marker_complex_type handles prohibited attrs, maxOccurs=0, and simpleContent."""
    schema = xmlschema.XMLSchema(xsd_text)
    xsd_type = schema.types["T"]
    assert isinstance(xsd_type, XsdComplexType)
    assert is_empty_marker_complex_type(xsd_type) is expected


def test_collect_attributes_pass_skips_prohibited() -> None:
    """collect_attributes omits prohibited attributes and keeps optional ones."""
    xsd_ok = """<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
      <xs:complexType name="T">
        <xs:attribute name="keep" type="xs:string" use="optional"/>
        <xs:attribute name="drop" type="xs:string" use="prohibited"/>
      </xs:complexType>
    </xs:schema>"""
    schema = xmlschema.XMLSchema(xsd_ok)
    xsd_type = schema.types["T"]
    assert isinstance(xsd_type, XsdComplexType)
    assert [a.local_name for a in collect_attributes(xsd_type)] == ["keep"]


@pytest.mark.parametrize(
    ("error_by_code", "expected_exc"),
    [
        pytest.param({}, CustomFallbackError, id="fallback-error"),
        pytest.param({"E5": CustomCodeError}, CustomCodeError, id="mapped-e5-error"),
    ],
)
def test_collect_attributes_fail_wildcard_attribute(
    error_by_code: dict[str, type[Exception]],
    expected_exc: type[Exception],
) -> None:
    """collect_attributes raises E5 on xs:anyAttribute using error_by_code or error_type."""
    xsd_bad = """<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
      <xs:complexType name="T">
        <xs:anyAttribute processContents="skip"/>
      </xs:complexType>
    </xs:schema>"""
    schema = xmlschema.XMLSchema(xsd_bad)
    xsd_type = schema.types["T"]
    assert isinstance(xsd_type, XsdComplexType)
    with pytest.raises(expected_exc, match="xs:anyAttribute"):
        collect_attributes(xsd_type, error_type=CustomFallbackError, error_by_code=error_by_code)


@pytest.mark.parametrize(
    ("xsd_text", "start_type_name", "target_type_name", "expected"),
    [
        pytest.param(
            CAN_REACH_DESCENDANT_XSD,
            "Wrapper",
            "Target",
            True,
            id="reachable-through-cyclic-middle",
        ),
        pytest.param(
            CAN_REACH_DESCENDANT_XSD,
            "ZeroWrapper",
            "Target",
            False,
            id="blocked-by-max-occurs-zero",
        ),
        pytest.param(
            CAN_REACH_VISITED_CYCLE_XSD,
            "Root",
            "Target",
            True,
            id="continues-after-visited-cycle",
        ),
    ],
)
def test_can_reach_any_type_pass_descendants_zero_occurs_and_cycles(
    xsd_text: str,
    start_type_name: str,
    target_type_name: str,
    expected: bool,
) -> None:
    """can_reach_any_type descends into non-zero complex children and continues past visited cycles."""
    schema = xmlschema.XMLSchema(xsd_text)
    start_type = schema.types[start_type_name]
    target_type = schema.types[target_type_name]
    assert isinstance(start_type, XsdComplexType)
    assert can_reach_any_type(start_type, {id(target_type): 1}) is expected


@pytest.mark.parametrize(
    ("xsd_text", "seed_type_count", "error_type", "error_by_code", "expected_exc", "expected_match"),
    [
        pytest.param(
            FORBIDDEN_ATTR_RECURSION_XSD,
            True,
            CustomFallbackError,
            {},
            CustomFallbackError,
            "@when",
            id="date-attribute-fallback",
        ),
        pytest.param(
            FORBIDDEN_SIMPLE_CONTENT_RECURSION_XSD,
            True,
            CustomFallbackError,
            {"E9": CustomCodeError},
            CustomCodeError,
            "xs:dateTime",
            id="simple-content-datetime-mapped-e9",
        ),
        pytest.param(
            NAMESPACED_RECURSION_XSD,
            False,
            ValueError,
            {},
            ValueError,
            "recursive type 'NodeType' contains",
            id="namespaced-recursive-type-local-name",
        ),
    ],
)
def test_check_no_forbidden_types_under_recursion_fail_forbidden_members(
    xsd_text: str,
    seed_type_count: bool,
    error_type: type[Exception],
    error_by_code: dict[str, type[Exception]],
    expected_exc: type[Exception],
    expected_match: str,
) -> None:
    """check_no_forbidden_types_under_recursion rejects date attributes, simpleContent extensions, and namespaced types."""
    schema = xmlschema.XMLSchema(xsd_text)
    node_type = schema.types["NodeType"]
    type_counts = {id(node_type): 1} if seed_type_count else {}
    ctx = TraversalContext(
        schema=schema,
        type_counts=type_counts,
        max_recursion_depth=2,
        error_type=error_type,
        error_by_code=error_by_code,
    )
    with pytest.raises(expected_exc, match=expected_match):
        check_no_forbidden_types_under_recursion(node_type, ctx)


def test_check_no_forbidden_types_under_recursion_pass_mixed_type() -> None:
    """check_no_forbidden_types_under_recursion skips model_group inspection for mixed complex types."""
    schema = xmlschema.XMLSchema(NAMESPACED_RECURSION_XSD)
    mixed_type = schema.types["MixedRec"]
    ctx = TraversalContext(schema=schema, type_counts={}, max_recursion_depth=2)
    check_no_forbidden_types_under_recursion(mixed_type, ctx)


def test_resolve_target_element_pass_nested_clark_name() -> None:
    """resolve_target_element finds a nested qualified element by Clark-notation tag."""
    schema = xmlschema.XMLSchema(DEEP_ELEMENT_XSD)
    found = resolve_target_element(schema, "{http://example.org/ns}item")
    assert found.local_name == "item"


@pytest.mark.parametrize(
    ("xsd_text", "xml_tag", "error_by_code", "expected_exc", "expected_match"),
    [
        pytest.param(DEEP_ELEMENT_XSD, "missing", {}, CustomFallbackError, "not found", id="missing-tag-fallback"),
        pytest.param(
            DEEP_ELEMENT_XSD,
            "missing",
            {"E1": CustomCodeError},
            CustomCodeError,
            "not found",
            id="missing-tag-mapped-e1",
        ),
        pytest.param(
            AMBIGUOUS_ELEMENT_XSD,
            "dup",
            {},
            CustomFallbackError,
            "Ambiguous xml_tag 'dup'",
            id="ambiguous-tag-fallback",
        ),
        pytest.param(
            AMBIGUOUS_ELEMENT_XSD,
            "dup",
            {"E2": CustomCodeError},
            CustomCodeError,
            "Ambiguous xml_tag 'dup'",
            id="ambiguous-tag-mapped-e2",
        ),
        pytest.param(
            ABSTRACT_ELEMENT_XSD,
            "absEl",
            {},
            CustomFallbackError,
            "abstract element 'absEl'",
            id="abstract-element-fallback",
        ),
        pytest.param(
            ABSTRACT_ELEMENT_XSD,
            "absTyped",
            {"E6": CustomCodeError},
            CustomCodeError,
            "abstract type on 'absTyped'",
            id="abstract-type-mapped-e6",
        ),
    ],
)
def test_resolve_target_element_fail_missing_ambiguous_and_abstract(
    xsd_text: str,
    xml_tag: str,
    error_by_code: dict[str, type[Exception]],
    expected_exc: type[Exception],
    expected_match: str,
) -> None:
    """resolve_target_element forwards E1, E2, and E6 errors for missing, ambiguous, and abstract declarations."""
    schema = xmlschema.XMLSchema(xsd_text)
    with pytest.raises(expected_exc, match=expected_match):
        resolve_target_element(schema, xml_tag, error_type=CustomFallbackError, error_by_code=error_by_code)


@pytest.mark.parametrize(
    ("error_by_code", "expected_exc"),
    [
        pytest.param({}, CustomFallbackError, id="fallback-error"),
        pytest.param({"E1": CustomCodeError}, CustomCodeError, id="mapped-e1-error"),
    ],
)
def test_resolve_root_elements_fail_no_global_elements(
    error_by_code: dict[str, type[Exception]],
    expected_exc: type[Exception],
) -> None:
    """resolve_root_elements raises E1 when a schema declares no global elements."""
    schema = xmlschema.XMLSchema(NO_GLOBAL_ELEMENTS_XSD)
    with pytest.raises(expected_exc, match="No global elements"):
        resolve_root_elements(schema, error_type=CustomFallbackError, error_by_code=error_by_code)


@pytest.mark.parametrize(
    ("xsd_text", "expected_names"),
    [
        pytest.param(MUTUAL_CYCLE_XSD, ["a", "b"], id="mutual-cycle"),
        pytest.param(SELF_REF_XSD, ["node", "leaf"], id="self-ref-and-zero-occurs"),
        pytest.param(ZERO_OCCURS_BEFORE_REF_XSD, ["skip_target", "root"], id="zero-occurs-before-active-ref"),
    ],
)
def test_resolve_root_elements_pass_cycles_and_zero_occurs_refs(
    xsd_text: str,
    expected_names: list[str],
) -> None:
    """resolve_root_elements handles mutual cycles, self-references, and maxOccurs=0 refs."""
    schema = xmlschema.XMLSchema(xsd_text)
    assert [e.local_name for e in resolve_root_elements(schema)] == expected_names


@pytest.mark.parametrize(
    ("xsd_text", "level", "expected_names"),
    [
        pytest.param(HIERARCHY_XSD, 0, ["catalog"], id="level-0-root"),
        pytest.param(
            MIXED_NON_STRUCT_PARENTS_XSD,
            1,
            ["k"],
            id="skips-simple-mixed-and-duplicate-parents",
        ),
    ],
)
def test_resolve_elements_at_level_pass_level_zero_and_skips_non_struct_parents(
    xsd_text: str,
    level: int,
    expected_names: list[str],
) -> None:
    """resolve_elements_at_level returns root elements at level 0 and skips simple, mixed, and duplicate parents."""
    schema = xmlschema.XMLSchema(xsd_text)
    assert list(resolve_elements_at_level(schema, level)) == expected_names


@pytest.mark.parametrize(
    ("xsd_text", "level", "root_tag", "error_by_code", "expected_exc", "expected_match"),
    [
        pytest.param(
            HIERARCHY_XSD,
            5,
            None,
            {},
            CustomFallbackError,
            "No XML elements found at level 5",
            id="empty-deep-level-fallback",
        ),
        pytest.param(
            HIERARCHY_XSD,
            5,
            None,
            {"E1": CustomCodeError},
            CustomCodeError,
            "No XML elements found at level 5",
            id="empty-deep-level-mapped-e1",
        ),
        pytest.param(
            AMBIGUOUS_ROOTS_XSD,
            0,
            "missing",
            {},
            CustomFallbackError,
            "not found",
            id="missing-root-tag-fallback",
        ),
        pytest.param(
            AMBIGUOUS_ROOTS_XSD,
            0,
            "missing",
            {"E1": CustomCodeError},
            CustomCodeError,
            "not found",
            id="missing-root-tag-mapped-e1",
        ),
        pytest.param(
            AMBIGUOUS_ROOTS_XSD,
            1,
            None,
            {},
            CustomFallbackError,
            "Ambiguous xml_tag 'dup'",
            id="ambiguous-child-fallback",
        ),
        pytest.param(
            AMBIGUOUS_ROOTS_XSD,
            1,
            None,
            {"E2": CustomCodeError},
            CustomCodeError,
            "Ambiguous xml_tag 'dup'",
            id="ambiguous-child-mapped-e2",
        ),
        pytest.param(
            WILDCARD_ANY_XSD,
            1,
            None,
            {},
            CustomFallbackError,
            "xs:any",
            id="wildcard-any-fallback",
        ),
        pytest.param(
            WILDCARD_ANY_XSD,
            1,
            None,
            {"E4": CustomCodeError},
            CustomCodeError,
            "xs:any",
            id="wildcard-any-mapped-e4",
        ),
    ],
)
def test_resolve_elements_at_level_fail_error_forwarding(
    xsd_text: str,
    level: int,
    root_tag: str | None,
    error_by_code: dict[str, type[Exception]],
    expected_exc: type[Exception],
    expected_match: str,
) -> None:
    """resolve_elements_at_level forwards E1, E2, and E4 errors via error_by_code or error_type."""
    schema = xmlschema.XMLSchema(xsd_text)
    with pytest.raises(expected_exc, match=expected_match):
        resolve_elements_at_level(
            schema,
            level,
            root_tag=root_tag,
            error_type=CustomFallbackError,
            error_by_code=error_by_code,
        )


@pytest.mark.parametrize(
    ("tag", "error_by_code", "expected_exc"),
    [
        pytest.param("empty", {}, CustomFallbackError, id="empty-fallback"),
        pytest.param("empty", {"E3": CustomCodeError}, CustomCodeError, id="empty-mapped-e3"),
        pytest.param("plainSc", {}, CustomFallbackError, id="attrless-simple-content-fallback"),
        pytest.param("plainSc", {"E3": CustomCodeError}, CustomCodeError, id="attrless-simple-content-mapped-e3"),
    ],
)
def test_validate_target_struct_element_fail_empty_and_attrless_simple_content(
    tag: str,
    error_by_code: dict[str, type[Exception]],
    expected_exc: type[Exception],
) -> None:
    """validate_target_struct_element rejects empty and attribute-less simpleContent target elements."""
    schema = xmlschema.XMLSchema(TARGET_STRUCT_EDGE_XSD)
    _, elem_type, ctx = prepare_target_traversal(
        schema,
        tag,
        max_recursion_depth=2,
        error_type=CustomFallbackError,
        error_by_code=error_by_code,
    )
    with pytest.raises(expected_exc):
        validate_target_struct_element(elem_type, tag, ctx)


def test_validate_target_struct_element_pass_simple_content_with_attributes() -> None:
    """validate_target_struct_element accepts simpleContent target elements that declare attributes."""
    schema = xmlschema.XMLSchema(TARGET_STRUCT_EDGE_XSD)
    _, elem_type, ctx = prepare_target_traversal(
        schema,
        "attrSc",
        max_recursion_depth=2,
        error_type=CustomFallbackError,
    )
    validate_target_struct_element(elem_type, "attrSc", ctx)


@pytest.mark.parametrize(
    ("xsd_text", "xml_tag", "error_by_code", "expected_exc", "expected_match"),
    [
        pytest.param(
            COLLISION_XSD,
            "rec",
            {"E8": CustomCodeError},
            CustomCodeError,
            r"Field name collision.*'_id'",
            id="direct-collision-mapped-e8",
        ),
        pytest.param(
            SHARED_BAD_DAG_XSD,
            "root",
            {},
            CustomFallbackError,
            "Field name collision",
            id="nested-collision-fallback",
        ),
    ],
)
def test_validate_complex_subtree_fail_field_name_collision(
    xsd_text: str,
    xml_tag: str,
    error_by_code: dict[str, type[Exception]],
    expected_exc: type[Exception],
    expected_match: str,
) -> None:
    """validate_complex_subtree detects E8 field-name collisions and forwards error classes."""
    schema = xmlschema.XMLSchema(xsd_text)
    _, complex_type, ctx = prepare_target_traversal(
        schema,
        xml_tag,
        max_recursion_depth=2,
        error_type=CustomFallbackError,
        error_by_code=error_by_code,
    )
    with pytest.raises(expected_exc, match=expected_match):
        validate_complex_subtree(complex_type, ctx)


def test_validate_complex_subtree_pass_skips_visited_type() -> None:
    """validate_complex_subtree skips complex types already present in the visited set."""
    schema = xmlschema.XMLSchema(SHARED_BAD_DAG_XSD)
    _, root_type, root_ctx = prepare_target_traversal(
        schema,
        "root",
        max_recursion_depth=2,
        error_type=CustomFallbackError,
    )
    visited = {id(schema.types["SharedBad"])}
    validate_complex_subtree(root_type, root_ctx, visited=visited)


@pytest.mark.parametrize(
    ("xml_tag", "error_by_code", "expected_exc", "expected_match"),
    [
        pytest.param("missing", {}, CustomFallbackError, "not found", id="missing-tag-fallback"),
        pytest.param(
            "leaf",
            {"E3": CustomCodeError},
            CustomCodeError,
            "must have a complex type",
            id="simple-type-mapped-e3",
        ),
    ],
)
def test_prepare_target_traversal_fail_forwards_error_classes(
    xml_tag: str,
    error_by_code: dict[str, type[Exception]],
    expected_exc: type[Exception],
    expected_match: str,
) -> None:
    """prepare_target_traversal forwards error_type and error_by_code to element and type resolution."""
    schema = xmlschema.XMLSchema(LEAF_ONLY_XSD)
    with pytest.raises(expected_exc, match=expected_match):
        prepare_target_traversal(
            schema,
            xml_tag,
            max_recursion_depth=2,
            error_type=CustomFallbackError,
            error_by_code=error_by_code,
        )
