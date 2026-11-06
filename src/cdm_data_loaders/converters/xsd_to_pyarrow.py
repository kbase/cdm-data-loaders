"""Functions for reading XSD files, generating PyArrow schemas, and converting XML elements."""

import base64
import datetime as dt
import functools
import json
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import Any, Final

import pyarrow as pa
import xmlschema
from lxml.etree import Element
from pydantic import BaseModel, ConfigDict, Field, PositiveInt
from xmlschema.validators import XsdComplexType, XsdElement, XsdType

from cdm_data_loaders.converters.emitters.pyarrow import PyArrowEmitter
from cdm_data_loaders.converters.readers.xsd import (
    BUILTIN_LIST_ITEM_TYPES,
    DEFAULT_MAX_RECURSION_DEPTH,
    FORBIDDEN_RECURSIVE_XSD_TYPES,
    SCALAR_ARROW_TYPES,
    XSD_NAMESPACE,
    XSD_TO_PYARROW_ERRORS,
    XSI_NAMESPACE,
    XSI_NIL,
    AbstractComponentError,
    AmbiguousXmlTagError,
    FieldNameCollisionError,
    NonStructXmlTagError,
    RecursiveNonJsonTypeError,
    SubstitutionGroupError,
    WildcardAttributeError,
    WildcardElementError,
    XmlTagNotFoundError,
    XsdReader,
    XsdToPyArrowError,
    _builtin_xsd_name,
    _can_reach_any_type,
    _check_no_forbidden_types_under_recursion,
    _classify_leaf_element_type,
    _collect_attributes,
    _collect_unique_children,
    _extract_doc_comment,
    _is_empty_marker_complex_type,
    _prepare_target_traversal,
    _require_complex_type,
    _resolve_child_cardinality,
    _resolve_non_struct_element_node,
    _resolve_target_element,
    _simple_type_to_ir_node,
    _TraversalContext,
    _unwrap_item_type,
    load_schema,
    resolve_elements_at_level,
    resolve_root_elements,
)
from cdm_data_loaders.utils.xml_utils import local_name as _local_name

_DEFAULT_EMITTER: Final[PyArrowEmitter] = PyArrowEmitter(max_nesting=100)


def _build_field_metadata(
    *,
    xsd_type: str | None = None,
    comment: str | None = None,
) -> dict[str, str] | None:
    """Construct the ``pa.Field`` metadata dictionary from optional comment and XSD type strings."""
    metadata: dict[str, str] = {}
    if comment:
        metadata["comment"] = comment
    if xsd_type:
        metadata["xsd_type"] = xsd_type
    return metadata or None


def _resolve_scalar_type(simple_type: XsdType) -> tuple[pa.DataType, str]:
    """Map a non-list XSD simpleType (atomic or union) to ``(pa.DataType, xsd_type_str)``."""
    node = _simple_type_to_ir_node(simple_type, (), nullable=True)
    arrow_type = _DEFAULT_EMITTER.emit_node(node)
    xsd_type_str = str(node.provenance.metadata.get("xsd_type", "xs:string")) if node.provenance else "xs:string"
    return arrow_type, xsd_type_str


def _simple_type_to_arrow(simple_type: XsdType) -> tuple[pa.DataType, str | None]:
    """Map any XSD simpleType (atomic, union, or list) to ``(pa.DataType, leaf_xsd_type_or_None)``.

    When ``simple_type`` is a list type (``xs:list``, ``xs:NMTOKENS``, ``xs:IDREFS``, ``xs:ENTITIES``),
    the returned ``pa.DataType`` is ``pa.list_(pa.field("item", item_arrow_type, nullable=True, metadata={"xsd_type": ...}))``
    and the second element is ``None`` because ``"xsd_type"`` is attached to the inner ``"item"`` field.
    """
    node = _simple_type_to_ir_node(simple_type, (), nullable=True)
    pa_type = _DEFAULT_EMITTER.emit_node(node)
    xsd_type = (
        str(node.provenance.metadata["xsd_type"])
        if (node.provenance and "xsd_type" in node.provenance.metadata)
        else None
    )
    return pa_type, xsd_type


def _resolve_non_struct_element_type(elem_type: XsdType | None) -> tuple[pa.DataType, str | None] | None:
    """Return ``(pa_type, xsd_type_or_None)`` if ``elem_type`` maps to a leaf scalar rather than a struct."""
    node = _resolve_non_struct_element_node(elem_type, (), nullable=True)
    if node is None:
        return None
    pa_type = _DEFAULT_EMITTER.emit_node(node)
    xsd_type = (
        str(node.provenance.metadata["xsd_type"])
        if (node.provenance and "xsd_type" in node.provenance.metadata)
        else None
    )
    return pa_type, xsd_type


def generate_pyarrow_struct(
    schema: xmlschema.XMLSchema,
    xml_tag: str,
    *,
    max_recursion_depth: int = DEFAULT_MAX_RECURSION_DEPTH,
    struct_cache: dict[tuple[int, bool], Any] | None = None,
) -> pa.StructType:
    """Generate a reusable ``pa.StructType`` component for the target XML element from an XSD schema.

    :param schema: loaded XMLSchema instance
    :type  schema: xmlschema.XMLSchema
    :param xml_tag: target XML element name (local name or Clark-notation ``{ns}local``)
    :type  xml_tag: str
    :param max_recursion_depth: maximum structural recursion depth before JSON serialization
    :type  max_recursion_depth: int
    :param struct_cache: optional cache for sharing complex type components across calls
    :type  struct_cache: dict[tuple[int, bool], Any] | None
    :return: PyArrow StructType component whose fields correspond to the target element's fields
    :rtype : pa.StructType
    """
    doc = XsdReader(xml_tag=xml_tag, max_recursion_depth=max_recursion_depth).read(
        schema,
        xml_tag,
        struct_cache=struct_cache,
    )
    emitted = _DEFAULT_EMITTER.emit_node(doc.root)
    if not isinstance(emitted, pa.StructType):
        msg = f"Top-level xml_tag {xml_tag!r} did not emit a StructType"
        raise NonStructXmlTagError(msg)
    return emitted


def assemble_pyarrow_schema(
    component: pa.StructType | Sequence[pa.Field],
    *,
    metadata: Mapping[str, str | bytes] | None = None,
) -> pa.Schema:
    """Assemble a PyArrow table schema from a ``pa.StructType`` component or sequence of ``pa.Field``s.

    :param component: struct component or sequence of PyArrow fields
    :type  component: pa.StructType | Sequence[pa.Field]
    :param metadata: optional schema-level metadata
    :type  metadata: Mapping[str, str | bytes] | None
    :return: assembled PyArrow schema
    :rtype : pa.Schema
    """
    fields = list(component)
    if not fields:
        msg = "Cannot assemble a PyArrow schema from an empty component"
        raise NonStructXmlTagError(msg)
    return pa.schema(fields, metadata=metadata)


def generate_pyarrow_schema(
    schema: xmlschema.XMLSchema,
    xml_tag: str,
    *,
    max_recursion_depth: int = DEFAULT_MAX_RECURSION_DEPTH,
) -> pa.Schema:
    """Generate a PyArrow schema for the target XML element from an XSD schema.

    :param schema: loaded XMLSchema instance
    :type  schema: xmlschema.XMLSchema
    :param xml_tag: target XML element name (local name or Clark-notation ``{ns}local``)
    :type  xml_tag: str
    :param max_recursion_depth: maximum structural recursion depth before JSON serialization
    :type  max_recursion_depth: int
    :return: PyArrow schema whose top-level fields correspond to the target element's fields
    :rtype : pa.Schema
    """
    doc = XsdReader(xml_tag=xml_tag, max_recursion_depth=max_recursion_depth).read(schema, xml_tag)
    return _DEFAULT_EMITTER.emit(doc)


def generate_pyarrow_schemas_at_level(
    schema: xmlschema.XMLSchema,
    level: int,
    *,
    root_tag: str | None = None,
    max_recursion_depth: int = DEFAULT_MAX_RECURSION_DEPTH,
) -> dict[str, pa.Schema]:
    """Generate PyArrow table schemas for all XML element types at depth ``level`` in an XSD schema.

    Reusable struct components are cached and shared across all table schemas assembled
    at ``level``. Elements whose XSD type resolves to a scalar or mixed-text leaf are represented as
    a single-column ``pa.Schema`` with a nullable ``#text`` field.

    :param schema: loaded XMLSchema instance
    :type  schema: xmlschema.XMLSchema
    :param level: non-negative depth relative to the root element(s)
    :type  level: int
    :param root_tag: optional explicit root element name to anchor the traversal
    :type  root_tag: str | None
    :param max_recursion_depth: maximum structural recursion depth before JSON serialization
    :type  max_recursion_depth: int
    :return: mapping of element local tag name (table name) to its assembled PyArrow schema
    :rtype : dict[str, pa.Schema]
    """
    docs = XsdReader(max_recursion_depth=max_recursion_depth).read_at_level(schema, level, root_tag=root_tag)
    return {tag_name: _DEFAULT_EMITTER.emit(doc) for tag_name, doc in docs.items()}


class XsdToPyArrow(BaseModel):
    """Facade converting XML Schema Definition (XSD) documents into PyArrow schemas and empty tables."""

    model_config = ConfigDict(arbitrary_types_allowed=True, frozen=True)

    xml_tag: str | None = None
    max_recursion_depth: PositiveInt = Field(default=DEFAULT_MAX_RECURSION_DEPTH)

    def convert(self, schema: xmlschema.XMLSchema | str | Path, xml_tag: str | None = None) -> pa.Schema:
        """Convert an XSD schema element into a ``pa.Schema``.

        :param schema: loaded ``XMLSchema`` instance or path to an XSD file
        :type schema: xmlschema.XMLSchema | str | Path
        :param xml_tag: target XML element name (defaults to ``self.xml_tag`` or sole root element)
        :type xml_tag: str | None
        :return: converted PyArrow schema
        :rtype: pa.Schema
        """
        doc = XsdReader(xml_tag=self.xml_tag, max_recursion_depth=self.max_recursion_depth).read(schema, xml_tag)
        return _DEFAULT_EMITTER.emit(doc)

    def convert_to_table(self, schema: xmlschema.XMLSchema | str | Path, xml_tag: str | None = None) -> pa.Table:
        """Convert an XSD schema element into a zero-row ``pa.Table``.

        :param schema: loaded ``XMLSchema`` instance or path to an XSD file
        :type schema: xmlschema.XMLSchema | str | Path
        :param xml_tag: target XML element name (defaults to ``self.xml_tag`` or sole root element)
        :type xml_tag: str | None
        :return: zero-row PyArrow table with the converted schema
        :rtype: pa.Table
        """
        doc = XsdReader(xml_tag=self.xml_tag, max_recursion_depth=self.max_recursion_depth).read(schema, xml_tag)
        return _DEFAULT_EMITTER.emit_table(doc)

    def convert_to_python(
        self,
        schema: xmlschema.XMLSchema | str | Path,
        xml_tag: str | None = None,
        *,
        docstring: str | None = None,
    ) -> str:
        """Convert an XSD schema element into a static Python schema module.

        :param schema: loaded ``XMLSchema`` instance or path to an XSD file
        :type schema: xmlschema.XMLSchema | str | Path
        :param xml_tag: target XML element name (defaults to ``self.xml_tag`` or sole root element)
        :type xml_tag: str | None
        :param docstring: optional module docstring header
        :type docstring: str | None
        :return: formatted Python source code defining ``*_STRUCT``, ``*_SCHEMA``, ``SCHEMA``, and ``SCHEMAS``
        :rtype: str
        """
        doc = XsdReader(xml_tag=self.xml_tag, max_recursion_depth=self.max_recursion_depth).read(schema, xml_tag)
        return _DEFAULT_EMITTER.emit_python(doc, docstring=docstring)

    def convert_to_python_file(
        self,
        schema: xmlschema.XMLSchema | str | Path,
        output: str | Path,
        xml_tag: str | None = None,
        *,
        docstring: str | None = None,
    ) -> Path:
        """Convert an XSD schema element and write a static Python schema module to ``output``.

        :param schema: loaded ``XMLSchema`` instance or path to an XSD file
        :type schema: xmlschema.XMLSchema | str | Path
        :param output: destination file path for the generated Python module
        :type output: str | Path
        :param xml_tag: target XML element name (defaults to ``self.xml_tag`` or sole root element)
        :type xml_tag: str | None
        :param docstring: optional module docstring header
        :type docstring: str | None
        :return: resolved output ``Path``
        :rtype: Path
        """
        doc = XsdReader(xml_tag=self.xml_tag, max_recursion_depth=self.max_recursion_depth).read(schema, xml_tag)
        return _DEFAULT_EMITTER.emit_python_file(doc, output, docstring=docstring)

    def convert_level_to_python(
        self,
        schema: xmlschema.XMLSchema | str | Path,
        level: int = 1,
        *,
        root_tag: str | None = None,
        docstring: str | None = None,
    ) -> str:
        """Convert all XML element types at depth ``level`` in an XSD schema into a static Python schema module.

        :param schema: loaded ``XMLSchema`` instance or path to an XSD file
        :type schema: xmlschema.XMLSchema | str | Path
        :param level: non-negative depth relative to the root element(s)
        :type level: int
        :param root_tag: optional explicit root element name to anchor the traversal
        :type root_tag: str | None
        :param docstring: optional module docstring header
        :type docstring: str | None
        :return: formatted Python source code defining ``*_STRUCT``, ``*_SCHEMA``, ``SCHEMA``, and ``SCHEMAS``
        :rtype: str
        """
        docs = XsdReader(max_recursion_depth=self.max_recursion_depth).read_at_level(schema, level, root_tag=root_tag)
        return _DEFAULT_EMITTER.emit_python(docs, docstring=docstring)

    def convert_level_to_python_file(
        self,
        schema: xmlschema.XMLSchema | str | Path,
        output: str | Path,
        level: int = 1,
        *,
        root_tag: str | None = None,
        docstring: str | None = None,
    ) -> Path:
        """Convert all XML element types at depth ``level`` in an XSD schema and write a Python module to ``output``.

        :param schema: loaded ``XMLSchema`` instance or path to an XSD file
        :type schema: xmlschema.XMLSchema | str | Path
        :param output: destination file path for the generated Python module
        :type output: str | Path
        :param level: non-negative depth relative to the root element(s)
        :type level: int
        :param root_tag: optional explicit root element name to anchor the traversal
        :type root_tag: str | None
        :param docstring: optional module docstring header
        :type docstring: str | None
        :return: resolved output ``Path``
        :rtype: Path
        """
        docs = XsdReader(max_recursion_depth=self.max_recursion_depth).read_at_level(schema, level, root_tag=root_tag)
        return _DEFAULT_EMITTER.emit_python_file(docs, output, docstring=docstring)


def _is_ignored_attr(attr_name: str) -> bool:
    """Return True if an XML attribute is an ``xmlns`` or ``xsi:*`` attribute that should be stripped."""
    return attr_name.startswith((XSI_NAMESPACE, "xsi:", "xmlns", "{http://www.w3.org/2000/xmlns/}"))


def _is_xsi_nil(element: Element) -> bool:
    """Return True if ``element`` carries ``xsi:nil='true'`` or ``xsi:nil='1'``."""
    val = element.attrib.get(XSI_NIL) or element.attrib.get("xsi:nil")
    return val is not None and val.strip() in {"true", "1"}


def _escape_xml_text(text: str) -> str:
    """Escape XML character entities ``&``, ``<``, and ``>`` in mixed-content text."""
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _escape_xml_attr(value: str) -> str:
    """Escape XML character entities and double quotes in mixed-content attribute values."""
    return _escape_xml_text(value).replace('"', "&quot;")


def _serialize_mixed_node(node: Element) -> str:
    """Serialize an inline child element inside a ``mixed='true'`` element without namespace declarations."""
    local = _local_name(node.tag)
    attrs = "".join(
        f' {_local_name(k)}="{_escape_xml_attr(v)}"' for k, v in node.attrib.items() if not _is_ignored_attr(k)
    )
    inner = _serialize_inner_xml(node)
    if not inner and len(node) == 0 and node.text is None:
        return f"<{local}{attrs}/>"
    return f"<{local}{attrs}>{inner}</{local}>"


def _serialize_inner_xml(element: Element) -> str:
    """Serialize the raw inner XML (text and inline child elements) of a ``mixed='true'`` element."""
    parts: list[str] = []
    if element.text:
        parts.append(_escape_xml_text(element.text))
    for child in element:
        if isinstance(child.tag, str):
            parts.append(_serialize_mixed_node(child))
        if child.tail:
            parts.append(_escape_xml_text(child.tail))
    return "".join(parts)


def _coerce_resolved_scalar(stripped: str, arrow_type: pa.DataType, xsd_name: str) -> Any:  # noqa: ANN401
    """Coerce a stripped lexical string using its resolved ``arrow_type`` and ``xsd_name``."""
    if pa.types.is_boolean(arrow_type):
        return stripped in {"true", "1"}
    if pa.types.is_integer(arrow_type):
        return int(stripped)
    if pa.types.is_floating(arrow_type):
        return float(stripped)
    if pa.types.is_date32(arrow_type):
        return dt.date.fromisoformat(stripped[:10])
    if pa.types.is_binary(arrow_type):
        return bytes.fromhex(stripped) if xsd_name == "xs:hexBinary" else base64.b64decode(stripped)
    return stripped


def _coerce_scalar_text(raw_text: str, simple_type: XsdType | None) -> Any:  # noqa: ANN401
    """Coerce a non-empty lexical XML string according to its XSD ``simple_type``."""
    if simple_type is None:
        return raw_text

    builtin_name = _builtin_xsd_name(simple_type)
    if builtin_name in BUILTIN_LIST_ITEM_TYPES:
        return raw_text.split()

    if simple_type.is_list():
        item_type = _unwrap_item_type(simple_type)
        return [_coerce_scalar_text(token, item_type) for token in raw_text.split()]

    arrow_type, xsd_name = _resolve_scalar_type(simple_type)
    return _coerce_resolved_scalar(raw_text.strip(), arrow_type, xsd_name)


def _extract_struct_attributes(element: Element, complex_type: XsdComplexType) -> dict[str, Any]:
    """Extract and coerce declared XML attributes from ``element`` into ``_<name>`` dictionary entries."""
    declared_attrs = {attr.local_name: attr for attr in _collect_attributes(complex_type) if attr.local_name}
    raw_attrs = {_local_name(k): v for k, v in element.attrib.items() if not _is_ignored_attr(k)}
    return {
        f"_{attr_name}": _coerce_scalar_text(raw_attrs[attr_name], attr_decl.type)
        for attr_name, attr_decl in declared_attrs.items()
        if attr_name in raw_attrs
    }


def _group_child_elements_by_name(element: Element) -> dict[str, list[Element]]:
    """Group direct XML child elements by their local tag name in document order."""
    children_by_name: dict[str, list[Element]] = {}
    for child_el in element:
        if isinstance(child_el.tag, str):
            children_by_name.setdefault(_local_name(child_el.tag), []).append(child_el)
    return children_by_name


def _convert_complex_struct(
    ctx: _TraversalContext,
    element: Element,
    complex_type: XsdComplexType,
) -> dict[str, Any] | None:
    """Convert an XML ``element`` with a struct-mapped ``complex_type`` into a sparse dictionary."""
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
) -> Any:  # noqa: ANN401
    """Convert an XML element that maps to a scalar or mixed-text leaf field."""
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
) -> Any:  # noqa: ANN401
    """Convert a single XML element occurrence according to ``xsd_element``."""
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

    :param element: XML element to convert
    :type  element: Element
    :param ctx: traversal context
    :type  ctx: _TraversalContext
    :param target_elem: resolved XSD element declaration
    :type  target_elem: XsdElement
    :return: dictionary with ``#text`` key if non-null, otherwise empty dictionary
    :rtype : dict[str, Any]
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

    :param element: XML element to convert
    :type  element: Element
    :param ctx: traversal context
    :type  ctx: _TraversalContext
    :param elem_type: validated XSD complex type
    :type  elem_type: XsdComplexType
    :return: sparse dictionary of coerced field values
    :rtype : dict[str, Any]
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

    :param schema: loaded XMLSchema instance
    :type  schema: xmlschema.XMLSchema
    :param target_elem: resolved XSD element declaration
    :type  target_elem: XsdElement
    :param tag_name: element local tag name
    :type  tag_name: str
    :param max_recursion_depth: maximum structural recursion depth
    :type  max_recursion_depth: int
    :return: function converting an lxml Element into a dictionary
    :rtype : Callable[[Element], dict[str, Any]]
    """
    leaf_info = _classify_leaf_element_type(target_elem.type)
    if leaf_info is not None:
        leaf_ctx = _TraversalContext(
            schema=schema,
            type_counts={},
            max_recursion_depth=max_recursion_depth,
        )
        return functools.partial(_convert_leaf_element_to_dict, ctx=leaf_ctx, target_elem=target_elem)

    elem_type = _require_complex_type(target_elem.type, tag_name)
    ctx = _TraversalContext(
        schema=schema,
        type_counts={id(elem_type): 1},
        max_recursion_depth=max_recursion_depth,
    )
    return functools.partial(_convert_struct_element_to_dict, ctx=ctx, elem_type=elem_type)


def make_xml_element_converter(
    schema: xmlschema.XMLSchema,
    xml_tag: str,
    *,
    max_recursion_depth: int = DEFAULT_MAX_RECURSION_DEPTH,
) -> Callable[[Element], dict[str, Any]]:
    """Build a reusable element converter for ``xml_tag`` that resolves the XSD target once.

    :param schema: loaded XMLSchema instance
    :type  schema: xmlschema.XMLSchema
    :param xml_tag: target XML element name (local name or Clark-notation ``{ns}local``)
    :type  xml_tag: str
    :param max_recursion_depth: maximum structural recursion depth before JSON serialization
    :type  max_recursion_depth: int
    :return: function converting an lxml Element into a sparse dictionary
    :rtype : Callable[[Element], dict[str, Any]]
    """
    _, elem_type, ctx = _prepare_target_traversal(
        schema,
        xml_tag,
        max_recursion_depth=max_recursion_depth,
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

    :param schema: loaded XMLSchema instance
    :type  schema: xmlschema.XMLSchema
    :param level: non-negative depth relative to the root element(s)
    :type  level: int
    :param root_tag: optional explicit root element name to anchor the traversal
    :type  root_tag: str | None
    :param max_recursion_depth: maximum structural recursion depth before JSON serialization
    :type  max_recursion_depth: int
    :return: mapping of element local tag name to its element-to-dict converter function
    :rtype : dict[str, Callable[[Element], dict[str, Any]]]
    """
    elements_at_level = resolve_elements_at_level(schema, level, root_tag=root_tag)
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

    :param element: XML element to convert
    :type  element: Element
    :param schema: loaded XMLSchema instance
    :type  schema: xmlschema.XMLSchema
    :param xml_tag: target XML element name (local name or Clark-notation ``{ns}local``)
    :type  xml_tag: str
    :param max_recursion_depth: maximum structural recursion depth before JSON serialization
    :type  max_recursion_depth: int
    :return: sparse dictionary of coerced field values matching the generated PyArrow schema
    :rtype : dict[str, Any]
    """
    return make_xml_element_converter(
        schema,
        xml_tag,
        max_recursion_depth=max_recursion_depth,
    )(element)


__all__ = [
    "BUILTIN_LIST_ITEM_TYPES",
    "DEFAULT_MAX_RECURSION_DEPTH",
    "FORBIDDEN_RECURSIVE_XSD_TYPES",
    "SCALAR_ARROW_TYPES",
    "XSD_NAMESPACE",
    "XSD_TO_PYARROW_ERRORS",
    "XSI_NAMESPACE",
    "XSI_NIL",
    "AbstractComponentError",
    "AmbiguousXmlTagError",
    "FieldNameCollisionError",
    "NonStructXmlTagError",
    "RecursiveNonJsonTypeError",
    "SubstitutionGroupError",
    "WildcardAttributeError",
    "WildcardElementError",
    "XmlTagNotFoundError",
    "XsdToPyArrow",
    "XsdToPyArrowError",
    "_builtin_xsd_name",
    "_can_reach_any_type",
    "_check_no_forbidden_types_under_recursion",
    "_classify_leaf_element_type",
    "_collect_attributes",
    "_collect_unique_children",
    "_extract_doc_comment",
    "_is_empty_marker_complex_type",
    "_require_complex_type",
    "_resolve_child_cardinality",
    "_resolve_non_struct_element_type",
    "_resolve_target_element",
    "_simple_type_to_arrow",
    "assemble_pyarrow_schema",
    "convert_xml_element",
    "generate_pyarrow_schema",
    "generate_pyarrow_schemas_at_level",
    "generate_pyarrow_struct",
    "load_schema",
    "make_xml_element_converter",
    "make_xml_level_converters",
    "resolve_elements_at_level",
    "resolve_root_elements",
]
