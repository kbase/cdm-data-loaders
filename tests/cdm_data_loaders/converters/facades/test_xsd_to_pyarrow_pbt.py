"""Property-based tests for XSD-to-PyArrow schema generation, XML conversion, and codegen helpers."""

import ast
import functools
import json
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any, Final

import pyarrow as pa
import pytest
import xmlschema
from _pytest.mark.structures import ParameterSet
from hypothesis import given
from hypothesis import strategies as st
from hypothesis.strategies import SearchStrategy
from lxml.etree import Element, SubElement, fromstring
from xmlschema.validators import XsdComplexType, XsdElement, XsdType

from cdm_data_loaders.coercion.xml import (
    _serialize_inner_xml,
    convert_xml_element,
    make_xml_element_converter,
)
from cdm_data_loaders.converters.core.ir import Field as SchemaField
from cdm_data_loaders.converters.core.ir import Provenance, SchemaDocument, TypedNode
from cdm_data_loaders.converters.emitters.pyarrow import PyArrowEmitter
from cdm_data_loaders.converters.emitters.pyarrow_codegen import (
    _fmt_str,
    _IRComponentCollector,
    _to_upper_snake,
)
from cdm_data_loaders.converters.facades.xsd_to_pyarrow import (
    generate_pyarrow_schema,
    resolve_scalar_type,
)
from cdm_data_loaders.converters.readers.xsd import (
    BUILTIN_LIST_ITEM_TYPES,
    FORBIDDEN_RECURSIVE_XSD_TYPES,
    XSI_NIL,
    RecursiveNonJsonTypeError,
    _builtin_xsd_name,
    _classify_leaf_element_type,
    _collect_attributes,
    _collect_unique_children,
    _require_complex_type,
    _resolve_child_cardinality,
    _resolve_target_element,
    _unwrap_item_type,
    load_schema,
)
from tests.conftest import BASE_DIR
from tests.converter_spec import Case, Run, load_cases
from tests.pyarrow_pbt_helpers import (
    INT8_MAX,
    INT8_MIN,
    INT16_MAX,
    INT16_MIN,
    INT32_MAX,
    INT32_MIN,
    INT64_MAX,
    INT64_MIN,
    UINT8_MAX,
    UINT16_MAX,
    UINT32_MAX,
    UINT64_MAX,
    assert_coerced_rows_roundtrip,
    base64_str_strategy,
    hex_str_strategy,
    iso_date_str_strategy,
    iso_datetime_str_strategy,
    safe_text_strategy,
    stripped_text_strategy,
)

XSD_DATA_DIR: Final[Path] = (BASE_DIR / "specs" / "schema_converters" / "xsd_to_pyarrow").resolve()
CASES: Final[tuple[Case, ...]] = tuple(load_cases(XSD_DATA_DIR / "spec") + load_cases(XSD_DATA_DIR / "examples"))

CASE_RUN_PARAMS: Final[tuple[ParameterSet, ...]] = tuple(
    pytest.param(case, case.runs[0], id=f"{case.suite}-{case.name}") for case in CASES
)


def _xsd_simple_type_strategy(simple_type: XsdType | None) -> SearchStrategy[str]:
    """Build a Hypothesis strategy yielding lexical XML strings valid for ``simple_type``.

    :param simple_type: XSD simple type or ``None``.
    :type simple_type: XsdType | None
    :return: Hypothesis strategy yielding lexical strings.
    :rtype: SearchStrategy[str]
    """
    if simple_type is None:
        return stripped_text_strategy(min_size=1, max_size=16)

    enumeration = getattr(simple_type, "enumeration", None)
    if enumeration:
        non_empty_enums = [str(v) for v in enumeration if str(v).strip()]
        if non_empty_enums:
            return st.sampled_from(non_empty_enums)

    builtin_name = _builtin_xsd_name(simple_type)
    if builtin_name in BUILTIN_LIST_ITEM_TYPES:
        return st.lists(stripped_text_strategy(min_size=1, max_size=8), min_size=1, max_size=3).map(" ".join)

    if simple_type.is_list():
        item_type = _unwrap_item_type(simple_type)
        return st.lists(_xsd_simple_type_strategy(item_type), min_size=1, max_size=3).map(" ".join)

    member_types = getattr(simple_type, "member_types", None)
    if member_types:
        return st.one_of([_xsd_simple_type_strategy(m) for m in member_types])

    arrow_type, xsd_name = resolve_scalar_type(simple_type)
    return _arrow_scalar_lexical_strategy(arrow_type, xsd_name)


def _arrow_scalar_lexical_strategy(arrow_type: pa.DataType, xsd_name: str) -> SearchStrategy[str]:
    """Build a lexical XML string strategy matching a resolved ``(arrow_type, xsd_name)`` pair.

    :param arrow_type: Resolved PyArrow scalar type.
    :type arrow_type: pa.DataType
    :param xsd_name: Resolved XSD type name.
    :type xsd_name: str
    :return: Hypothesis strategy yielding lexical strings.
    :rtype: SearchStrategy[str]
    """
    int_bounds: dict[pa.DataType, tuple[int, int]] = {
        pa.int8(): (INT8_MIN, INT8_MAX),
        pa.int16(): (INT16_MIN, INT16_MAX),
        pa.int32(): (INT32_MIN, INT32_MAX),
        pa.int64(): (INT64_MIN, INT64_MAX),
        pa.uint8(): (0, UINT8_MAX),
        pa.uint16(): (0, UINT16_MAX),
        pa.uint32(): (0, UINT32_MAX),
        pa.uint64(): (0, UINT64_MAX),
    }
    if arrow_type in int_bounds:
        low, high = int_bounds[arrow_type]
        return st.integers(min_value=low, max_value=high).map(str)
    if pa.types.is_boolean(arrow_type):
        return st.sampled_from(["true", "false", "1", "0"])
    if pa.types.is_floating(arrow_type):
        return st.integers(min_value=-100_000, max_value=100_000).map(lambda k: str(k / 100.0))
    if pa.types.is_date32(arrow_type) or xsd_name == "xs:dateTime":
        return iso_date_str_strategy() if pa.types.is_date32(arrow_type) else iso_datetime_str_strategy()
    if pa.types.is_binary(arrow_type):
        return hex_str_strategy() if xsd_name == "xs:hexBinary" else base64_str_strategy()
    return stripped_text_strategy(min_size=1, max_size=16)


def _populate_attributes(
    draw: st.DrawFn,
    element: Element,
    complex_type: XsdComplexType,
    *,
    force_at_least_one: bool = False,
) -> bool:
    """Populate required and selected optional XML attributes on ``element``.

    :param draw: Hypothesis draw function.
    :type draw: st.DrawFn
    :param element: Target XML element.
    :type element: Element
    :param complex_type: XSD complex type declaring attributes.
    :type complex_type: XsdComplexType
    :param force_at_least_one: Ensure at least one attribute is set if any are declared.
    :type force_at_least_one: bool
    :return: True if at least one attribute was set on ``element``.
    :rtype: bool
    """
    attrs = [a for a in _collect_attributes(complex_type) if a.local_name and a.use != "prohibited"]
    req_attrs = [a for a in attrs if a.use == "required"]
    opt_attrs = [a for a in attrs if a.use != "required"]

    wrote_any = False
    for attr in req_attrs:
        assert attr.local_name is not None
        element.set(attr.local_name, draw(_xsd_simple_type_strategy(attr.type)))
        wrote_any = True

    if opt_attrs:
        min_pick = 1 if (force_at_least_one and not wrote_any) else 0
        max_pick = min(len(opt_attrs), 2)
        chosen = draw(
            st.lists(st.sampled_from(opt_attrs), min_size=min_pick, max_size=max_pick, unique_by=lambda a: a.local_name)
        )
        for attr in chosen:
            assert attr.local_name is not None
            element.set(attr.local_name, draw(_xsd_simple_type_strategy(attr.type)))
            wrote_any = True

    return wrote_any


def _child_hits_recursion_limit(
    child_decl: XsdElement,
    type_counts: Mapping[int, int],
    max_depth: int,
) -> bool:
    """Return True if ``child_decl`` has a complex struct type whose count has reached ``max_depth``.

    :param child_decl: Child XSD element declaration.
    :type child_decl: XsdElement
    :param type_counts: Active complex type expansion counts by ``id(elem_type)``.
    :type type_counts: Mapping[int, int]
    :param max_depth: Maximum recursion depth.
    :type max_depth: int
    :return: True if expanding ``child_decl`` would exceed ``max_depth``.
    :rtype: bool
    """
    if _classify_leaf_element_type(child_decl.type) is not None:
        return False
    if isinstance(child_decl.type, XsdComplexType):
        return type_counts.get(id(child_decl.type), 0) >= max_depth
    return False


def _populate_leaf_element(
    draw: st.DrawFn,
    elem: Element,
    kind: str,
    simple_type: XsdType | None,
) -> Element:
    """Populate a leaf XML element (``empty_marker``, ``mixed``, or ``scalar``).

    :param draw: Hypothesis draw function.
    :type draw: st.DrawFn
    :param elem: Target XML element.
    :type elem: Element
    :param kind: Leaf classification kind.
    :type kind: str
    :param simple_type: Optional XSD simple type.
    :type simple_type: XsdType | None
    :return: Populated ``elem``.
    :rtype: Element
    """
    if kind == "empty_marker":
        return elem
    if kind == "mixed":
        elem.text = draw(stripped_text_strategy(min_size=1, max_size=16))
        if draw(st.booleans()):
            inline = SubElement(elem, "b")
            inline.text = draw(stripped_text_strategy(min_size=1, max_size=8))
            inline.tail = draw(safe_text_strategy(min_size=0, max_size=8))
        return elem
    elem.text = draw(_xsd_simple_type_strategy(simple_type))
    return elem


def _populate_complex_children(
    draw: st.DrawFn,
    schema: xmlschema.XMLSchema,
    elem: Element,
    complex_type: XsdComplexType,
    next_counts: Mapping[int, int],
    max_depth: int,
    *,
    wrote_attr: bool,
) -> None:
    """Populate required and selected optional child elements on a complex struct ``elem``.

    :param draw: Hypothesis draw function.
    :type draw: st.DrawFn
    :param schema: Loaded ``xmlschema.XMLSchema``.
    :type schema: xmlschema.XMLSchema
    :param elem: Target XML element.
    :type elem: Element
    :param complex_type: XSD complex type with a model group.
    :type complex_type: XsdComplexType
    :param next_counts: Updated complex type expansion counts.
    :type next_counts: Mapping[int, int]
    :param max_depth: Maximum recursion depth.
    :type max_depth: int
    :param wrote_attr: Whether at least one XML attribute was already set on ``elem``.
    :type wrote_attr: bool
    """
    model_group = complex_type.model_group
    assert model_group is not None
    req_children: list[tuple[XsdElement, bool]] = []
    opt_children: list[tuple[XsdElement, bool]] = []

    for child_decl in _collect_unique_children(schema, complex_type):
        if child_decl.max_occurs == 0:
            continue
        is_list, child_nullable = _resolve_child_cardinality(model_group, child_decl)
        if not child_nullable:
            req_children.append((child_decl, is_list))
        elif not _child_hits_recursion_limit(child_decl, next_counts, max_depth):
            opt_children.append((child_decl, is_list))

    chosen_opt: list[tuple[XsdElement, bool]] = []
    if opt_children:
        min_opt = 1 if (not wrote_attr and not req_children) else 0
        max_opt = min(len(opt_children), 2)
        chosen_opt = draw(
            st.lists(
                st.sampled_from(opt_children),
                min_size=min_opt,
                max_size=max_opt,
                unique_by=lambda pair: pair[0].local_name,
            )
        )

    for child_decl, is_list in [*req_children, *chosen_opt]:
        count = draw(st.integers(min_value=1, max_value=2)) if is_list else 1
        can_be_nil = is_list or bool(wrote_attr or len(req_children) + len(chosen_opt) > 1)
        for _ in range(count):
            elem.append(
                _build_element_occurrence(
                    draw,
                    schema,
                    child_decl,
                    next_counts,
                    max_depth,
                    allow_nil=can_be_nil,
                )
            )


def _build_element_occurrence(
    draw: st.DrawFn,
    schema: xmlschema.XMLSchema,
    xsd_element: XsdElement,
    type_counts: Mapping[int, int],
    max_depth: int,
    *,
    allow_nil: bool = True,
) -> Element:
    """Construct a single ``lxml.etree.Element`` occurrence matching ``xsd_element``.

    :param draw: Hypothesis draw function.
    :type draw: st.DrawFn
    :param schema: Loaded ``xmlschema.XMLSchema``.
    :type schema: xmlschema.XMLSchema
    :param xsd_element: Target XSD element declaration.
    :type xsd_element: XsdElement
    :param type_counts: Active complex type expansion counts by ``id(elem_type)``.
    :type type_counts: Mapping[int, int]
    :param max_depth: Maximum recursion depth.
    :type max_depth: int
    :param allow_nil: Whether ``xsi:nil="true"`` may be emitted when ``xsd_element.nillable`` is True.
    :type allow_nil: bool
    :return: Constructed ``lxml.etree.Element``.
    :rtype: Element
    """
    tag_name = xsd_element.local_name or "item"
    elem = Element(tag_name)

    if allow_nil and xsd_element.nillable and draw(st.booleans()):
        if isinstance(xsd_element.type, XsdComplexType):
            _populate_attributes(draw, elem, xsd_element.type)
        elem.set(XSI_NIL, "true")
        return elem

    leaf_info = _classify_leaf_element_type(xsd_element.type)
    if leaf_info is not None:
        return _populate_leaf_element(draw, elem, leaf_info[0], leaf_info[1])

    complex_type = _require_complex_type(xsd_element.type, tag_name)
    next_counts = {**type_counts, id(complex_type): type_counts.get(id(complex_type), 0) + 1}

    if complex_type.has_simple_content() or complex_type.mixed:
        wrote_attr = _populate_attributes(draw, elem, complex_type)
        content_type = complex_type.content if isinstance(complex_type.content, XsdType) else None
        if not wrote_attr or draw(st.booleans()):
            elem.text = (
                draw(_xsd_simple_type_strategy(content_type))
                if complex_type.has_simple_content()
                else draw(stripped_text_strategy(min_size=1, max_size=16))
            )
        return elem

    if not complex_type.model_group:
        _populate_attributes(draw, elem, complex_type, force_at_least_one=True)
        return elem

    wrote_attr = _populate_attributes(draw, elem, complex_type)
    _populate_complex_children(draw, schema, elem, complex_type, next_counts, max_depth, wrote_attr=wrote_attr)
    return elem


def xml_elements_for_xsd(
    schema: xmlschema.XMLSchema,
    xml_tag: str,
    *,
    max_depth: int = 3,
) -> SearchStrategy[Element]:
    """Build a Hypothesis strategy generating ``lxml.etree.Element`` trees for ``(schema, xml_tag)``.

    :param schema: Loaded ``xmlschema.XMLSchema``.
    :type schema: xmlschema.XMLSchema
    :param xml_tag: Target XML element tag name.
    :type xml_tag: str
    :param max_depth: Maximum recursion depth for cyclic complex types.
    :type max_depth: int
    :return: Hypothesis strategy yielding ``lxml.etree.Element`` instances.
    :rtype: SearchStrategy[Element]
    """
    target_elem = _resolve_target_element(schema, xml_tag)

    @st.composite
    def _gen(draw: st.DrawFn) -> Element:
        return _build_element_occurrence(draw, schema, target_elem, {}, max_depth, allow_nil=False)

    return _gen()


@functools.cache
def _cached_xsd_context(
    schema_path: Path,
    xml_tag: str,
    depth: int,
) -> tuple[xmlschema.XMLSchema, pa.Schema, Callable[[Element], dict[str, Any]]]:
    """Load and cache the parsed ``XMLSchema``, emitted ``pa.Schema``, and reusable element converter."""
    schema = load_schema(schema_path)
    pa_schema = generate_pyarrow_schema(schema, xml_tag, max_recursion_depth=depth)
    reusable_converter = make_xml_element_converter(schema, xml_tag, max_recursion_depth=depth)
    return schema, pa_schema, reusable_converter


@pytest.mark.pbt
@pytest.mark.parametrize(("case", "run"), CASE_RUN_PARAMS)
@given(data=st.data())
def test_convert_xml_element_pbt_spec_cases(case: Case, run: Run, data: st.DataObject) -> None:
    """Generated XML elements for every XSD spec/example case convert and round-trip through Parquet."""
    assert run.xml_tag is not None
    xml_tag = run.xml_tag
    depth = int(run.params.get("max_recursion_depth", 2))
    schema, pa_schema, reusable_converter = _cached_xsd_context(case.schema, xml_tag, depth)

    elem = data.draw(xml_elements_for_xsd(schema, xml_tag, max_depth=depth + 1))
    row_direct = convert_xml_element(elem, schema, xml_tag, max_recursion_depth=depth)
    row_reusable = reusable_converter(elem)
    assert row_direct == row_reusable
    assert_coerced_rows_roundtrip([row_direct], pa_schema)

    elem.set("{http://www.w3.org/2001/XMLSchema-instance}schemaLocation", "http://example.org/ns schema.xsd")
    row_with_xsi = reusable_converter(elem)
    assert "_schemaLocation" not in row_with_xsi
    assert row_with_xsi == row_direct


@pytest.mark.pbt
@given(
    lead_text=safe_text_strategy(min_size=0, max_size=20),
    child_tags=st.lists(st.sampled_from(["b", "i", "sup", "sub"]), min_size=0, max_size=4),
    child_texts=st.lists(safe_text_strategy(min_size=0, max_size=16), min_size=4, max_size=4),
    child_tails=st.lists(safe_text_strategy(min_size=0, max_size=16), min_size=4, max_size=4),
    attr_vals=st.lists(safe_text_strategy(min_size=0, max_size=12), min_size=4, max_size=4),
)
def test_serialize_inner_xml_pbt_well_formed_and_idempotent(
    lead_text: str,
    child_tags: list[str],
    child_texts: list[str],
    child_tails: list[str],
    attr_vals: list[str],
) -> None:
    """_serialize_inner_xml produces well-formed XML fragments that are idempotent under re-parsing."""
    root = Element("mixedRoot")
    root.text = lead_text
    for idx, tag in enumerate(child_tags):
        child = SubElement(root, tag)
        if child_texts[idx]:
            child.text = child_texts[idx]
        if child_tails[idx]:
            child.tail = child_tails[idx]
        if attr_vals[idx]:
            child.set("label", attr_vals[idx])

    serialized = _serialize_inner_xml(root)
    reparsed = fromstring(f"<mixedRoot>{serialized}</mixedRoot>")
    assert _serialize_inner_xml(reparsed) == serialized


@pytest.mark.pbt
@given(
    raw_str=safe_text_strategy(min_size=0, max_size=40),
    raw_ident=safe_text_strategy(min_size=0, max_size=30),
    strip_suffix=st.booleans(),
)
def test_pyarrow_codegen_string_and_identifier_helpers_pbt(
    raw_str: str,
    raw_ident: str,
    strip_suffix: bool,
) -> None:
    """_fmt_str round-trips via ast.literal_eval and _to_upper_snake always emits a valid UPPER_SNAKE identifier."""
    formatted_literal = _fmt_str(raw_str)
    assert ast.literal_eval(formatted_literal) == raw_str

    ident = _to_upper_snake(raw_ident, strip_type_suffix=strip_suffix)
    assert len(ident) >= 1
    assert ident.isidentifier()
    assert not ident[0].isdigit()
    assert ident == ident.upper()


@pytest.mark.pbt
@given(
    base_names=st.lists(
        st.sampled_from(["item", "ItemType", "entry", "branch_a", "123_node", "dup"]),
        min_size=1,
        max_size=8,
    )
)
def test_assign_component_names_pbt_collision_free(base_names: list[str]) -> None:
    """_IRComponentCollector.assign_component_names assigns distinct valid *_STRUCT identifiers."""
    emitter = PyArrowEmitter()
    fields: list[SchemaField] = []
    for idx, name in enumerate(base_names):
        leaf = TypedNode(type="string", provenance=Provenance(source="xsd", path=(f"f_{idx}", name, "val")))
        struct_node = TypedNode(
            type="object",
            properties=(SchemaField(name=f"val_{idx}", node=leaf, required=True),),
            provenance=Provenance(
                source="xsd",
                path=(f"f_{idx}", name),
                metadata={"component_id": idx + 1, "type_name": name},
            ),
        )
        fields.append(SchemaField(name=f"col_{idx}", node=struct_node, required=True))

    doc = SchemaDocument(
        root=TypedNode(
            type="object",
            properties=tuple(fields),
            provenance=Provenance(source="xsd", path=("root",), metadata={"component_id": 0, "type_name": "root"}),
        )
    )
    pa_schema = emitter.emit(doc)
    collector = _IRComponentCollector()
    collector.register_document("root", doc)
    collector.collect_document("root", doc, pa_schema)
    assigned = collector.assign_component_names()
    names = list(assigned.values())
    assert len(names) == len(set(names))
    assert all(n.isidentifier() and n.endswith("_STRUCT") for n in names)


@pytest.mark.pbt
@given(
    forbidden_xsd_type=st.sampled_from(sorted(FORBIDDEN_RECURSIVE_XSD_TYPES)),
    depth=st.integers(min_value=1, max_value=4),
    as_attribute=st.booleans(),
)
def test_recursive_non_json_xsd_type_pbt_rejected(
    forbidden_xsd_type: str,
    depth: int,
    as_attribute: bool,
) -> None:
    """Recursive XSD complex types containing date, dateTime, or binary fields raise RecursiveNonJsonTypeError."""
    child_xml = f'<xs:element name="bad" type="{forbidden_xsd_type}" minOccurs="0"/>' if not as_attribute else ""
    attr_xml = f'<xs:attribute name="badAttr" type="{forbidden_xsd_type}"/>' if as_attribute else ""
    xsd_text = f"""<?xml version="1.0" encoding="UTF-8"?>
    <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
      <xs:complexType name="NodeType">
        <xs:sequence>
          {child_xml}
          <xs:element name="node" type="NodeType" minOccurs="0" maxOccurs="unbounded"/>
        </xs:sequence>
        {attr_xml}
      </xs:complexType>
      <xs:element name="root" type="NodeType"/>
    </xs:schema>
    """
    with pytest.raises(RecursiveNonJsonTypeError, match="recursive type"):
        generate_pyarrow_schema(xmlschema.XMLSchema(xsd_text), "root", max_recursion_depth=depth)


@pytest.mark.pbt
@given(
    depth=st.integers(min_value=1, max_value=3),
    leaf_label=stripped_text_strategy(min_size=1, max_size=12),
    leaf_weight=st.integers(min_value=-1000, max_value=1000),
)
def test_xsd_recursion_cutoff_pbt_json_valid(depth: int, leaf_label: str, leaf_weight: int) -> None:
    """Recursive XML elements exceeding max_recursion_depth serialize truncated subtrees as valid JSON."""
    schema_path = XSD_DATA_DIR / "spec" / "04_recursion_depth" / "schema.xsd"
    schema = load_schema(schema_path)
    pa_schema = generate_pyarrow_schema(schema, "record", max_recursion_depth=depth)

    record_el = Element("record", attrib={"id": "r1"})
    cursor = SubElement(record_el, "tree", attrib={"label": "root", "weight": "0"})
    for level in range(1, depth + 2):
        cursor = SubElement(cursor, "branch", attrib={"label": f"{leaf_label}_{level}", "weight": str(leaf_weight)})

    row = convert_xml_element(record_el, schema, "record", max_recursion_depth=depth)
    assert_coerced_rows_roundtrip([row], pa_schema)

    node_dict: Any = row["tree"]
    for _ in range(depth - 1):
        node_dict = node_dict["branch"][0]
    cutoff_str = node_dict["branch"][0]
    assert isinstance(cutoff_str, str)
    parsed = json.loads(cutoff_str)
    assert isinstance(parsed, dict)
    assert parsed["_weight"] == leaf_weight
