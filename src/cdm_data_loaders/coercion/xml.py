"""Convert lxml XML elements into typed Python dictionaries according to an XSD schema."""

import base64
import datetime as dt
import functools
import json
from collections.abc import Callable
from typing import Any, Final

import xmlschema
from lxml.etree import Element
from xmlschema.validators import XsdComplexType, XsdElement, XsdType

from cdm_data_loaders.converters.core.guards import DEFAULT_MAX_RECURSION_DEPTH
from cdm_data_loaders.converters.core.ir import NodeHints, NodeType
from cdm_data_loaders.converters.facades.xsd_to_pyarrow import (
    ERROR_BY_CODE,
    XsdToPyArrowError,
)
from cdm_data_loaders.converters.readers.xsd import (
    BUILTIN_LIST_ITEM_TYPES,
    _builtin_xsd_name,
    _classify_leaf_element_type,
    _collect_attributes,
    _collect_unique_children,
    _prepare_target_traversal,
    _require_complex_type,
    _resolve_child_cardinality,
    _resolve_scalar_spec,
    _TraversalContext,
    _unwrap_item_type,
    resolve_elements_at_level,
)
from cdm_data_loaders.utils.xml_utils import local_name

XSI_NAMESPACE: Final[str] = "{http://www.w3.org/2001/XMLSchema-instance}"
XSI_NIL: Final[str] = f"{XSI_NAMESPACE}nil"


def _is_ignored_attr(attr_name: str) -> bool:
    """Return True if an XML attribute is an ``xmlns`` or ``xsi:*`` attribute that should be stripped.

    :param attr_name: Attribute name (local or Clark notation).
    :type attr_name: str
    :return: ``True`` if the attribute should be ignored.
    :rtype: bool
    """
    return attr_name.startswith((XSI_NAMESPACE, "xsi:", "xmlns", "{http://www.w3.org/2000/xmlns/}"))


def _is_xsi_nil(element: Element) -> bool:
    """Return True if ``element`` carries ``xsi:nil='true'`` or ``xsi:nil='1'``.

    :param element: XML element to inspect.
    :type element: Element
    :return: ``True`` if the element is marked nil.
    :rtype: bool
    """
    val = element.attrib.get(XSI_NIL) or element.attrib.get("xsi:nil")
    return val is not None and val.strip() in {"true", "1"}


def _escape_xml_text(text: str) -> str:
    """Escape XML character entities ``&``, ``<``, and ``>`` in mixed-content text.

    :param text: Raw XML text content.
    :type text: str
    :return: XML-escaped text string.
    :rtype: str
    """
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _escape_xml_attr(value: str) -> str:
    """Escape XML character entities and double quotes in mixed-content attribute values.

    :param value: Raw XML attribute value.
    :type value: str
    :return: XML-escaped attribute string.
    :rtype: str
    """
    return _escape_xml_text(value).replace('"', "&quot;")


def _serialize_mixed_node(node: Element) -> str:
    """Serialize an inline child element inside a ``mixed='true'`` element without namespace declarations.

    :param node: Inline XML child element.
    :type node: Element
    :return: Serialized XML string without namespace declarations.
    :rtype: str
    """
    local = local_name(node.tag)
    attrs = "".join(
        f' {local_name(k)}="{_escape_xml_attr(v)}"' for k, v in node.attrib.items() if not _is_ignored_attr(k)
    )
    inner = _serialize_inner_xml(node)
    if not inner and len(node) == 0 and node.text is None:
        return f"<{local}{attrs}/>"
    return f"<{local}{attrs}>{inner}</{local}>"


def _serialize_inner_xml(element: Element) -> str:
    """Serialize the raw inner XML (text and inline child elements) of a ``mixed='true'`` element.

    :param element: XML element with mixed content.
    :type element: Element
    :return: Serialized inner XML fragment.
    :rtype: str
    """
    parts: list[str] = []
    if element.text:
        parts.append(_escape_xml_text(element.text))
    for child in element:
        if isinstance(child.tag, str):
            parts.append(_serialize_mixed_node(child))
        if child.tail:
            parts.append(_escape_xml_text(child.tail))
    return "".join(parts)


def _coerce_resolved_scalar(
    stripped: str,
    node_type: NodeType,
    hints: NodeHints,
    xsd_name: str,
) -> bool | int | float | dt.date | bytes | str:
    """Coerce a stripped lexical string using its resolved IR ``node_type``, ``hints``, and ``xsd_name``.

    :param stripped: Whitespace-stripped lexical XML string.
    :type stripped: str
    :param node_type: Resolved IR node type.
    :type node_type: NodeType
    :param hints: Resolved IR node hints.
    :type hints: NodeHints
    :param xsd_name: Resolved XSD built-in type name.
    :type xsd_name: str
    :return: Coerced Python scalar value.
    :rtype: bool | int | float | dt.date | bytes | str
    """
    if node_type == "boolean":
        return stripped in {"true", "1"}
    if node_type == "integer":
        return int(stripped)
    if node_type == "number":
        return float(stripped)
    if hints.logical_type == "date":
        return dt.date.fromisoformat(stripped[:10])
    if hints.logical_type == "binary":
        return bytes.fromhex(stripped) if xsd_name == "xs:hexBinary" else base64.b64decode(stripped)
    return stripped


def _coerce_scalar_text(raw_text: str, simple_type: XsdType | None) -> object:
    """Coerce a non-empty lexical XML string according to its XSD ``simple_type``.

    :param raw_text: Raw lexical XML string.
    :type raw_text: str
    :param simple_type: XSD simple type or ``None``.
    :type simple_type: XsdType | None
    :return: Coerced scalar or list of scalars.
    :rtype: object
    """
    if simple_type is None:
        return raw_text

    builtin_name = _builtin_xsd_name(simple_type)
    if builtin_name in BUILTIN_LIST_ITEM_TYPES:
        return raw_text.split()

    if simple_type.is_list():
        item_type = _unwrap_item_type(simple_type)
        return [_coerce_scalar_text(token, item_type) for token in raw_text.split()]

    node_type, hints, xsd_name = _resolve_scalar_spec(simple_type)
    return _coerce_resolved_scalar(raw_text.strip(), node_type, hints, xsd_name)


def _extract_struct_attributes(element: Element, complex_type: XsdComplexType) -> dict[str, Any]:
    """Extract and coerce declared XML attributes from ``element`` into ``_<name>`` dictionary entries.

    :param element: XML element to inspect.
    :type element: Element
    :param complex_type: XSD complex type declaring the element's attributes.
    :type complex_type: XsdComplexType
    :return: Dictionary mapping ``_<attr_name>`` to coerced attribute values.
    :rtype: dict[str, Any]
    """
    declared_attrs = {attr.local_name: attr for attr in _collect_attributes(complex_type) if attr.local_name}
    raw_attrs = {local_name(k): v for k, v in element.attrib.items() if not _is_ignored_attr(k)}
    return {
        f"_{attr_name}": _coerce_scalar_text(raw_attrs[attr_name], attr_decl.type)
        for attr_name, attr_decl in declared_attrs.items()
        if attr_name in raw_attrs
    }


def _group_child_elements_by_name(element: Element) -> dict[str, list[Element]]:
    """Group direct XML child elements by their local tag name in document order.

    :param element: Parent XML element.
    :type element: Element
    :return: Mapping from local tag name to ordered list of child elements.
    :rtype: dict[str, list[Element]]
    """
    children_by_name: dict[str, list[Element]] = {}
    for child_el in element:
        if isinstance(child_el.tag, str):
            children_by_name.setdefault(local_name(child_el.tag), []).append(child_el)
    return children_by_name


def _convert_complex_struct(
    ctx: _TraversalContext,
    element: Element,
    complex_type: XsdComplexType,
) -> dict[str, Any] | None:
    """Convert an XML ``element`` with a struct-mapped ``complex_type`` into a sparse dictionary.

    :param ctx: Active XSD traversal context.
    :type ctx: _TraversalContext
    :param element: XML element to convert.
    :type element: Element
    :param complex_type: XSD complex type of ``element``.
    :type complex_type: XsdComplexType
    :return: Sparse dictionary of coerced fields, or ``None`` if empty.
    :rtype: dict[str, Any] | None
    """
    result = _extract_struct_attributes(element, complex_type)

    if _is_xsi_nil(element):
        return result or None

    if complex_type.has_simple_content():
        text_val = (element.text or "").strip()
        if text_val:
            content_type = complex_type.content if isinstance(complex_type.content, XsdType) else None
            result["#text"] = _coerce_scalar_text(text_val, content_type)
        return result or None

    if complex_type.mixed:
        inner_xml = _serialize_inner_xml(element).strip()
        if inner_xml:
            result["#text"] = inner_xml
        return result or None

    model_group = complex_type.model_group
    if model_group is None:
        return result or None

    children_by_name = _group_child_elements_by_name(element)

    for child_decl in _collect_unique_children(ctx.schema, complex_type):
        child_name = child_decl.local_name
        matching_elements = children_by_name.get(child_name)
        if not matching_elements:
            continue

        is_list, nullable = _resolve_child_cardinality(model_group, child_decl)

        if is_list:
            result[child_name] = [
                _convert_child_occurrence(ctx, el, child_decl, nullable=True) for el in matching_elements
            ]
        else:
            val = _convert_child_occurrence(ctx, matching_elements[0], child_decl, nullable=nullable)
            if val is not None:
                result[child_name] = val

    return result or None


def _convert_text_or_empty_leaf(
    element: Element,
    *,
    is_mixed: bool,
    simple_type: XsdType | None,
    nullable: bool,
) -> object:
    """Convert an XML element that maps to a scalar or mixed-text leaf field.

    :param element: XML element to convert.
    :type element: Element
    :param is_mixed: Whether the element type is mixed content without attributes.
    :type is_mixed: bool
    :param simple_type: XSD simple type for non-mixed leaf elements.
    :type simple_type: XsdType | None
    :param nullable: Whether the field is nullable in the parent schema.
    :type nullable: bool
    :return: Coerced scalar value, empty string, or ``None``.
    :rtype: object
    """
    if _is_xsi_nil(element):
        return None
    raw_val = _serialize_inner_xml(element).strip() if is_mixed else (element.text or "").strip()
    if not raw_val:
        return "" if not nullable else None
    return raw_val if is_mixed else _coerce_scalar_text(raw_val, simple_type)


def _convert_child_occurrence(
    ctx: _TraversalContext,
    element: Element,
    xsd_element: XsdElement,
    *,
    nullable: bool,
) -> object:
    """Convert a single XML element occurrence according to ``xsd_element``.

    :param ctx: Active XSD traversal context.
    :type ctx: _TraversalContext
    :param element: XML element occurrence to convert.
    :type element: Element
    :param xsd_element: XSD element declaration.
    :type xsd_element: XsdElement
    :param nullable: Whether the occurrence is nullable.
    :type nullable: bool
    :return: Coerced scalar, nested dictionary, JSON string (at recursion cutoff), or ``None``.
    :rtype: object
    """
    elem_type = xsd_element.type
    leaf_info = _classify_leaf_element_type(elem_type)
    if leaf_info is not None:
        kind, simple_type = leaf_info
        if kind == "empty_marker":
            return None if _is_xsi_nil(element) else ""
        return _convert_text_or_empty_leaf(
            element,
            is_mixed=(kind == "mixed"),
            simple_type=simple_type,
            nullable=nullable,
        )

    elem_type = _require_complex_type(elem_type, xsd_element.local_name or "")
    type_id = id(elem_type)
    next_count = ctx.type_counts.get(type_id, 0) + 1
    if ctx.max_recursion_depth is not None and next_count > ctx.max_recursion_depth:
        unbounded_ctx = _TraversalContext(schema=ctx.schema, type_counts={}, max_recursion_depth=None)
        nested = _convert_complex_struct(unbounded_ctx, element, elem_type)
        return json.dumps(nested) if nested is not None else None

    next_ctx = _TraversalContext(
        schema=ctx.schema,
        type_counts={**ctx.type_counts, type_id: next_count},
        max_recursion_depth=ctx.max_recursion_depth,
    )
    return _convert_complex_struct(next_ctx, element, elem_type)


def _convert_leaf_element_to_dict(
    element: Element,
    *,
    ctx: _TraversalContext,
    target_elem: XsdElement,
) -> dict[str, Any]:
    """Convert a scalar or mixed-text XML element into a single-key ``#text`` dictionary.

    :param element: XML element to convert.
    :type element: Element
    :param ctx: Traversal context.
    :type ctx: _TraversalContext
    :param target_elem: Resolved XSD element declaration.
    :type target_elem: XsdElement
    :return: Dictionary with ``#text`` key if non-null, otherwise empty dictionary.
    :rtype: dict[str, Any]
    """
    val = _convert_child_occurrence(ctx, element, target_elem, nullable=True)
    return {"#text": val} if val is not None else {}


def _convert_struct_element_to_dict(
    element: Element,
    *,
    ctx: _TraversalContext,
    elem_type: XsdComplexType,
) -> dict[str, Any]:
    """Convert a complex struct XML element into a sparse dictionary.

    :param element: XML element to convert.
    :type element: Element
    :param ctx: Traversal context.
    :type ctx: _TraversalContext
    :param elem_type: Validated XSD complex type.
    :type elem_type: XsdComplexType
    :return: Sparse dictionary of coerced field values.
    :rtype: dict[str, Any]
    """
    return _convert_complex_struct(ctx, element, elem_type) or {}


def _make_resolved_element_converter(
    schema: xmlschema.XMLSchema,
    target_elem: XsdElement,
    tag_name: str,
    *,
    max_recursion_depth: int,
) -> Callable[[Element], dict[str, Any]]:
    """Build a converter function for a pre-resolved ``target_elem`` (struct or ``#text`` leaf).

    :param schema: Loaded XMLSchema instance.
    :type schema: xmlschema.XMLSchema
    :param target_elem: Resolved XSD element declaration.
    :type target_elem: XsdElement
    :param tag_name: Element local tag name.
    :type tag_name: str
    :param max_recursion_depth: Maximum structural recursion depth.
    :type max_recursion_depth: int
    :return: Function converting an lxml Element into a dictionary.
    :rtype: Callable[[Element], dict[str, Any]]
    """
    leaf_info = _classify_leaf_element_type(
        target_elem.type,
        error_type=XsdToPyArrowError,
        error_by_code=ERROR_BY_CODE,
    )
    if leaf_info is not None:
        leaf_ctx = _TraversalContext(
            schema=schema,
            type_counts={},
            max_recursion_depth=max_recursion_depth,
            error_type=XsdToPyArrowError,
            error_by_code=ERROR_BY_CODE,
        )
        return functools.partial(_convert_leaf_element_to_dict, ctx=leaf_ctx, target_elem=target_elem)

    elem_type = _require_complex_type(
        target_elem.type,
        tag_name,
        error_type=XsdToPyArrowError,
        error_by_code=ERROR_BY_CODE,
    )
    ctx = _TraversalContext(
        schema=schema,
        type_counts={id(elem_type): 1},
        max_recursion_depth=max_recursion_depth,
        error_type=XsdToPyArrowError,
        error_by_code=ERROR_BY_CODE,
    )
    return functools.partial(_convert_struct_element_to_dict, ctx=ctx, elem_type=elem_type)


def make_xml_element_converter(
    schema: xmlschema.XMLSchema,
    xml_tag: str,
    *,
    max_recursion_depth: int = DEFAULT_MAX_RECURSION_DEPTH,
) -> Callable[[Element], dict[str, Any]]:
    """Build a reusable element converter for ``xml_tag`` that resolves the XSD target once.

    :param schema: Loaded XMLSchema instance.
    :type schema: xmlschema.XMLSchema
    :param xml_tag: Target XML element name (local name or Clark-notation ``{ns}local``).
    :type xml_tag: str
    :param max_recursion_depth: Maximum structural recursion depth before JSON serialization.
    :type max_recursion_depth: int
    :return: Function converting an lxml Element into a sparse dictionary.
    :rtype: Callable[[Element], dict[str, Any]]
    """
    _, elem_type, ctx = _prepare_target_traversal(
        schema,
        xml_tag,
        max_recursion_depth=max_recursion_depth,
        error_type=XsdToPyArrowError,
        error_by_code=ERROR_BY_CODE,
    )
    return functools.partial(_convert_struct_element_to_dict, ctx=ctx, elem_type=elem_type)


def make_xml_level_converters(
    schema: xmlschema.XMLSchema,
    level: int,
    *,
    root_tag: str | None = None,
    max_recursion_depth: int = DEFAULT_MAX_RECURSION_DEPTH,
) -> dict[str, Callable[[Element], dict[str, Any]]]:
    """Build reusable element converters for all XML element types at depth ``level`` in an XSD schema.

    :param schema: Loaded XMLSchema instance.
    :type schema: xmlschema.XMLSchema
    :param level: Non-negative depth relative to the root element(s).
    :type level: int
    :param root_tag: Optional explicit root element name to anchor the traversal.
    :type root_tag: str | None
    :param max_recursion_depth: Maximum structural recursion depth before JSON serialization.
    :type max_recursion_depth: int
    :return: Mapping of element local tag name to its element-to-dict converter function.
    :rtype: dict[str, Callable[[Element], dict[str, Any]]]
    """
    elements_at_level = resolve_elements_at_level(
        schema,
        level,
        root_tag=root_tag,
        error_type=XsdToPyArrowError,
        error_by_code=ERROR_BY_CODE,
    )
    return {
        tag_name: _make_resolved_element_converter(
            schema,
            target_elem,
            tag_name,
            max_recursion_depth=max_recursion_depth,
        )
        for tag_name, target_elem in elements_at_level.items()
    }


def convert_xml_element(
    element: Element,
    schema: xmlschema.XMLSchema,
    xml_tag: str,
    *,
    max_recursion_depth: int = DEFAULT_MAX_RECURSION_DEPTH,
) -> dict[str, Any]:
    """Convert an lxml Element matching ``xml_tag`` into a typed dictionary according to the XSD.

    :param element: XML element to convert.
    :type element: Element
    :param schema: Loaded XMLSchema instance.
    :type schema: xmlschema.XMLSchema
    :param xml_tag: Target XML element name (local name or Clark-notation ``{ns}local``).
    :type xml_tag: str
    :param max_recursion_depth: Maximum structural recursion depth before JSON serialization.
    :type max_recursion_depth: int
    :return: Sparse dictionary of coerced field values matching the generated PyArrow schema.
    :rtype: dict[str, Any]
    """
    return make_xml_element_converter(
        schema,
        xml_tag,
        max_recursion_depth=max_recursion_depth,
    )(element)
