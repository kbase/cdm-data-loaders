"""XSD schema loading, multi-level element resolution, and complex-type traversal helpers."""

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal, NoReturn

import xmlschema
from frozendict import frozendict
from xmlschema.validators import (
    XsdAttribute,
    XsdComplexType,
    XsdElement,
    XsdType,
)

from cdm_data_loaders.utils.xml_utils import local_name as _local_name
from cdm_data_loaders.validation.xsd.particles import (
    group_max_occurs_by_name,
    is_any_attribute,
    raise_xsd_code,
    validate_group_particles,
)
from cdm_data_loaders.validation.xsd.scalars import (
    FORBIDDEN_RECURSIVE_XSD_TYPES,
    iter_simple_builtin_names,
)

type LeafKind = Literal["simple", "mixed", "empty_marker"]


def load_schema(schema_path: str | Path) -> xmlschema.XMLSchema:
    """Load an XML Schema Definition (XSD) document into an ``xmlschema.XMLSchema`` object.

    :param schema_path: filesystem path to the XSD file
    :type schema_path: str | Path
    :return: loaded ``XMLSchema`` instance
    :rtype: xmlschema.XMLSchema
    """
    return xmlschema.XMLSchema(str(schema_path))


@dataclass(frozen=True, slots=True)
class TraversalContext:
    """Immutable traversal context for XSD schema lowering and XML-to-dict coercion."""

    schema: xmlschema.XMLSchema
    type_counts: Mapping[int, int]
    max_recursion_depth: int | None
    struct_cache: dict[tuple[int, bool], Any] = field(default_factory=dict)
    error_type: type[Exception] = ValueError
    error_by_code: Mapping[str, type[Exception]] = field(default_factory=frozendict)

    def raise_err(self, code: str, msg: str) -> NoReturn:
        """Raise the configured exception subclass for ``code`` or fall back to ``error_type``.

        :param code: specification error code (``E1``-``E9``)
        :type code: str
        :param msg: diagnostic error message
        :type msg: str
        :raises Exception: always raised using the resolved error class
        """
        raise_xsd_code(code, msg, error_type=self.error_type, error_by_code=self.error_by_code)


def _check_complex_type_forbidden_recursion(
    root_type: XsdType,
    current: XsdComplexType,
    ctx: TraversalContext,
) -> list[XsdType]:
    """Validate attributes on ``current`` and return child ``XsdType``s to inspect for forbidden recursive scalars.

    :param root_type: recursive ancestor ``XsdType`` for error reporting
    :type root_type: XsdType
    :param current: complex type being inspected
    :type current: XsdComplexType
    :param ctx: active traversal context
    :type ctx: TraversalContext
    :return: child ``XsdType`` objects reachable from ``current``
    :rtype: list[XsdType]
    """
    for attr in current.attributes.values():
        if isinstance(attr, XsdAttribute) and attr.type is not None:
            for xsd_name in iter_simple_builtin_names(attr.type):
                if xsd_name in FORBIDDEN_RECURSIVE_XSD_TYPES:
                    msg = (
                        f"Unsupported XSD feature: recursive type {root_type.local_name or root_type.name!r} "
                        f"contains attribute @{attr.local_name} of type {xsd_name!r}"
                    )
                    ctx.raise_err("E9", msg)

    if current.has_simple_content() and isinstance(current.content, XsdType):
        return [current.content]
    if not current.mixed and current.model_group is not None:
        return [
            child.type
            for child in current.model_group.iter_elements()
            if isinstance(child, XsdElement) and child.type is not None and child.max_occurs != 0
        ]
    return []


def check_no_forbidden_types_under_recursion(root_type: XsdType, ctx: TraversalContext) -> None:
    """Raise ``E9`` if a recursive type or any descendant type uses date, dateTime, or binary scalars.

    :param root_type: recursive ``XsdType`` to inspect
    :type root_type: XsdType
    :param ctx: active traversal context
    :type ctx: TraversalContext
    """
    visited: set[int] = set()
    stack: list[XsdType] = [root_type]

    while stack:
        current = stack.pop()
        if id(current) in visited:
            continue
        visited.add(id(current))

        if not isinstance(current, XsdComplexType):
            for xsd_name in iter_simple_builtin_names(current):
                if xsd_name in FORBIDDEN_RECURSIVE_XSD_TYPES:
                    msg = (
                        f"Unsupported XSD feature: recursive type {root_type.local_name or root_type.name!r} "
                        f"contains {xsd_name!r}, which cannot be serialized to JSON when recursion is truncated"
                    )
                    ctx.raise_err("E9", msg)
            continue

        stack.extend(_check_complex_type_forbidden_recursion(root_type, current, ctx))


def collect_unique_children(
    schema: xmlschema.XMLSchema,
    complex_type: XsdComplexType,
    *,
    error_type: type[Exception] = ValueError,
    error_by_code: Mapping[str, type[Exception]] = frozendict(),
) -> list[XsdElement]:
    """Validate and return unique child ``XsdElement`` declarations in content-model order.

    :param schema: loaded ``XMLSchema`` instance
    :type schema: xmlschema.XMLSchema
    :param complex_type: XSD complex type whose content model is inspected
    :type complex_type: XsdComplexType
    :param error_type: fallback exception class
    :type error_type: type[Exception]
    :param error_by_code: mapping of specification error codes to exception classes
    :type error_by_code: Mapping[str, type[Exception]]
    :return: unique child ``XsdElement`` declarations with non-zero maxOccurs
    :rtype: list[XsdElement]
    """
    model_group = complex_type.model_group
    if model_group is None:
        return []
    validate_group_particles(schema, model_group, error_type=error_type, error_by_code=error_by_code)
    seen_names: set[str] = set()
    children: list[XsdElement] = []
    for child in model_group.iter_elements():
        if not isinstance(child, XsdElement):
            continue
        max_occurs = group_max_occurs_by_name(model_group, child.local_name)
        if max_occurs == 0 or child.local_name in seen_names:
            continue
        seen_names.add(child.local_name)
        children.append(child)
    return children


def collect_attributes(
    complex_type: XsdComplexType,
    *,
    error_type: type[Exception] = ValueError,
    error_by_code: Mapping[str, type[Exception]] = frozendict(),
) -> list[XsdAttribute]:
    """Validate and return declared non-prohibited attributes on ``complex_type`` in declaration order.

    :param complex_type: XSD complex type whose attributes are inspected
    :type complex_type: XsdComplexType
    :param error_type: fallback exception class
    :type error_type: type[Exception]
    :param error_by_code: mapping of specification error codes to exception classes
    :type error_by_code: Mapping[str, type[Exception]]
    :return: list of declared ``XsdAttribute`` objects
    :rtype: list[XsdAttribute]
    """
    attrs: list[XsdAttribute] = []
    for attr in complex_type.attributes.values():
        if is_any_attribute(attr):
            msg = "Unsupported XSD feature: xs:anyAttribute wildcard is not supported"
            raise_xsd_code("E5", msg, error_type=error_type, error_by_code=error_by_code)
        if isinstance(attr, XsdAttribute) and getattr(attr, "use", None) != "prohibited":
            if attr.local_name is None:
                continue
            attrs.append(attr)
    return attrs


def is_empty_marker_complex_type(complex_type: XsdComplexType) -> bool:
    """Return ``True`` if ``complex_type`` has no attributes, no child elements, not simpleContent, and not mixed.

    :param complex_type: XSD complex type to inspect
    :type complex_type: XsdComplexType
    :return: ``True`` when ``complex_type`` is an empty marker type
    :rtype: bool
    """
    if complex_type.has_simple_content() or complex_type.mixed:
        return False
    if any(
        isinstance(a, XsdAttribute) and getattr(a, "use", None) != "prohibited"
        for a in complex_type.attributes.values()
    ):
        return False
    if complex_type.model_group is None:
        return True
    return not any(
        isinstance(el, XsdElement) and group_max_occurs_by_name(complex_type.model_group, el.local_name) != 0
        for el in complex_type.model_group.iter_elements()
    )


def classify_leaf_element_type(
    elem_type: XsdType | None,
    *,
    error_type: type[Exception] = ValueError,
    error_by_code: Mapping[str, type[Exception]] = frozendict(),
) -> tuple[LeafKind, XsdType | None] | None:
    """Classify ``elem_type`` as a non-struct leaf ``(kind, simple_type)`` or ``None`` if it maps to a struct.

    :param elem_type: XSD element type to classify
    :type elem_type: XsdType | None
    :param error_type: fallback exception class
    :type error_type: type[Exception]
    :param error_by_code: mapping of specification error codes to exception classes
    :type error_by_code: Mapping[str, type[Exception]]
    :return: ``(kind, simple_type)`` for leaf elements, or ``None`` for struct elements
    :rtype: tuple[LeafKind, XsdType | None] | None
    """
    if not isinstance(elem_type, XsdComplexType):
        return "simple", elem_type

    attrs = collect_attributes(elem_type, error_type=error_type, error_by_code=error_by_code)
    if elem_type.has_simple_content() and not attrs:
        content_type = elem_type.content if isinstance(elem_type.content, XsdType) else None
        return "simple", content_type

    if elem_type.mixed and not attrs:
        return "mixed", None

    if is_empty_marker_complex_type(elem_type):
        return "empty_marker", None

    return None


def can_reach_any_type(start_type: XsdComplexType, target_type_ids: Mapping[int, int]) -> bool:
    """Return ``True`` if ``start_type`` or any descendant complexType has its ``id`` in ``target_type_ids``.

    :param start_type: starting ``XsdComplexType`` to inspect
    :type start_type: XsdComplexType
    :param target_type_ids: mapping of active ancestor complexType object IDs
    :type target_type_ids: Mapping[int, int]
    :return: ``True`` if any complexType in the subtree is in ``target_type_ids``
    :rtype: bool
    """
    if not target_type_ids:
        return False
    visited: set[int] = set()
    stack: list[XsdComplexType] = [start_type]
    while stack:
        current = stack.pop()
        cur_id = id(current)
        if cur_id in target_type_ids:
            return True
        if cur_id in visited:
            continue
        visited.add(cur_id)
        if not current.has_simple_content() and not current.mixed and current.model_group is not None:
            stack.extend(
                child.type
                for child in current.model_group.iter_elements()
                if isinstance(child, XsdElement) and isinstance(child.type, XsdComplexType) and child.max_occurs != 0
            )
    return False


def _deduplicate_target_elements(
    matches: list[XsdElement],
    xml_tag: str,
    *,
    error_type: type[Exception] = ValueError,
    error_by_code: Mapping[str, type[Exception]] = frozendict(),
) -> XsdElement:
    """Verify all matching ``XsdElement`` declarations at the resolution depth share a single ``XsdType``.

    :param matches: matching ``XsdElement`` declarations
    :type matches: list[XsdElement]
    :param xml_tag: element tag name for error diagnostics
    :type xml_tag: str
    :param error_type: fallback exception class
    :type error_type: type[Exception]
    :param error_by_code: mapping of specification error codes to exception classes
    :type error_by_code: Mapping[str, type[Exception]]
    :return: deduplicated ``XsdElement``
    :rtype: XsdElement
    """
    by_type: dict[int, XsdElement] = {id(el.type): el for el in matches}
    if len(by_type) > 1:
        msg = f"Ambiguous xml_tag {xml_tag!r}: found {len(by_type)} declarations with different types"
        raise_xsd_code("E2", msg, error_type=error_type, error_by_code=error_by_code)
    target = next(iter(by_type.values()))
    target_ref = target.ref if target.ref is not None else target
    if getattr(target, "abstract", False) or getattr(target_ref, "abstract", False):
        msg = f"Unsupported XSD feature: abstract element {xml_tag!r} is not supported"
        raise_xsd_code("E6", msg, error_type=error_type, error_by_code=error_by_code)
    if target.type is not None and getattr(target.type, "abstract", False):
        msg = f"Unsupported XSD feature: abstract type on {xml_tag!r} is not supported"
        raise_xsd_code("E6", msg, error_type=error_type, error_by_code=error_by_code)
    return target


def resolve_target_element(
    schema: xmlschema.XMLSchema,
    xml_tag: str,
    *,
    error_type: type[Exception] = ValueError,
    error_by_code: Mapping[str, type[Exception]] = frozendict(),
) -> XsdElement:
    """Resolve ``xml_tag`` (local name or Clark notation) to a unique ``XsdElement`` declaration.

    :param schema: loaded ``XMLSchema`` instance
    :type schema: xmlschema.XMLSchema
    :param xml_tag: target XML element name (local name or Clark notation)
    :type xml_tag: str
    :param error_type: fallback exception class
    :type error_type: type[Exception]
    :param error_by_code: mapping of specification error codes to exception classes
    :type error_by_code: Mapping[str, type[Exception]]
    :return: resolved ``XsdElement`` declaration
    :rtype: XsdElement
    """
    if xml_tag.startswith("{") and "}" in xml_tag:
        ns_filter: str | None = xml_tag
        local_target = _local_name(xml_tag)
    else:
        ns_filter = None
        local_target = xml_tag

    def _matches(el: XsdElement) -> bool:
        if el.local_name != local_target:
            return False
        return ns_filter is None or el.name == ns_filter

    depth0 = [el for el in schema.elements.values() if isinstance(el, XsdElement) and _matches(el)]
    if depth0:
        return _deduplicate_target_elements(depth0, xml_tag, error_type=error_type, error_by_code=error_by_code)

    current_level: list[XsdElement] = [el for el in schema.elements.values() if isinstance(el, XsdElement)]
    visited_types: set[int] = set()
    while current_level:
        matches: list[XsdElement] = []
        next_level: list[XsdElement] = []
        for parent in current_level:
            p_type = parent.type
            if not isinstance(p_type, XsdComplexType) or id(p_type) in visited_types:
                continue
            visited_types.add(id(p_type))
            if p_type.model_group is None:
                continue
            for child in p_type.model_group.iter_elements():
                if not isinstance(child, XsdElement):
                    continue
                if _matches(child):
                    matches.append(child)
                next_level.append(child)
        if matches:
            return _deduplicate_target_elements(matches, xml_tag, error_type=error_type, error_by_code=error_by_code)
        current_level = next_level

    msg = f"Element {xml_tag!r} not found in XSD schema"
    return raise_xsd_code("E1", msg, error_type=error_type, error_by_code=error_by_code)


def require_complex_type(
    elem_type: XsdType | None,
    xml_tag: str,
    *,
    error_type: type[Exception] = ValueError,
    error_by_code: Mapping[str, type[Exception]] = frozendict(),
) -> XsdComplexType:
    """Validate that the resolved target element has an ``XsdComplexType``.

    :param elem_type: XSD type to validate
    :type elem_type: XsdType | None
    :param xml_tag: XML element tag name for error reporting
    :type xml_tag: str
    :param error_type: fallback exception class
    :type error_type: type[Exception]
    :param error_by_code: mapping of specification error codes to exception classes
    :type error_by_code: Mapping[str, type[Exception]]
    :return: validated ``XsdComplexType``
    :rtype: XsdComplexType
    """
    if not isinstance(elem_type, XsdComplexType):
        msg = f"Top-level xml_tag {xml_tag!r} must have a complex type with attributes or child elements"
        raise_xsd_code("E3", msg, error_type=error_type, error_by_code=error_by_code)
    return elem_type


def prepare_target_traversal(
    schema: xmlschema.XMLSchema,
    xml_tag: str,
    *,
    max_recursion_depth: int,
    error_type: type[Exception] = ValueError,
    error_by_code: Mapping[str, type[Exception]] = frozendict(),
) -> tuple[XsdElement, XsdComplexType, TraversalContext]:
    """Resolve ``xml_tag`` to its ``XsdElement``, validated ``XsdComplexType``, and initial ``TraversalContext``.

    :param schema: loaded ``XMLSchema`` instance
    :type schema: xmlschema.XMLSchema
    :param xml_tag: target XML element name
    :type xml_tag: str
    :param max_recursion_depth: maximum structural recursion depth
    :type max_recursion_depth: int
    :param error_type: fallback exception class
    :type error_type: type[Exception]
    :param error_by_code: mapping of specification error codes to exception classes
    :type error_by_code: Mapping[str, type[Exception]]
    :return: tuple of ``(target_elem, elem_type, traversal_context)``
    :rtype: tuple[XsdElement, XsdComplexType, TraversalContext]
    """
    target_elem = resolve_target_element(schema, xml_tag, error_type=error_type, error_by_code=error_by_code)
    elem_type = require_complex_type(target_elem.type, xml_tag, error_type=error_type, error_by_code=error_by_code)
    ctx = TraversalContext(
        schema=schema,
        type_counts={id(elem_type): 1},
        max_recursion_depth=max_recursion_depth,
        error_type=error_type,
        error_by_code=error_by_code,
    )
    return target_elem, elem_type, ctx


def _collect_referenced_global_element_names(schema: xmlschema.XMLSchema) -> set[str]:
    """Return qualified names of global ``XsdElement``s referenced as children inside other complex types.

    :param schema: loaded ``XMLSchema`` instance
    :type schema: xmlschema.XMLSchema
    :return: set of qualified element names referenced as non-self child elements
    :rtype: set[str]
    """
    global_elements = [el for el in schema.elements.values() if isinstance(el, XsdElement)]
    global_element_names = {el.name for el in global_elements if el.name is not None}
    global_element_ids = {id(el) for el in global_elements}

    referenced: set[str] = set()
    visited_types: set[int] = set()
    stack: list[tuple[XsdComplexType, str | None]] = [
        (global_el.type, global_el.name) for global_el in global_elements if isinstance(global_el.type, XsdComplexType)
    ]
    stack.extend((xsd_type, None) for xsd_type in schema.types.values() if isinstance(xsd_type, XsdComplexType))

    while stack:
        current_type, owner_el_name = stack.pop()
        type_id = id(current_type)
        if type_id in visited_types:
            continue
        visited_types.add(type_id)
        if current_type.model_group is None:
            continue
        for child in current_type.model_group.iter_elements():
            if not isinstance(child, XsdElement) or child.max_occurs == 0:
                continue
            ref_target = child.ref if child.ref is not None else child
            is_global_ref = child.ref is not None or id(ref_target) in global_element_ids
            if is_global_ref and ref_target.name in global_element_names and ref_target.name != owner_el_name:
                referenced.add(ref_target.name)
            if isinstance(child.type, XsdComplexType):
                stack.append((child.type, None))

    return referenced


def resolve_root_elements(
    schema: xmlschema.XMLSchema,
    root_tag: str | None = None,
    *,
    error_type: type[Exception] = ValueError,
    error_by_code: Mapping[str, type[Exception]] = frozendict(),
) -> list[XsdElement]:
    """Resolve the level-0 root ``XsdElement`` declarations in an XSD schema.

    When ``root_tag`` is omitted, root elements are deduced as global ``<xs:element>`` declarations
    not referenced as child elements inside any other complex type's content model (falling back to
    all global elements if all global elements participate in a mutual reference cycle).

    :param schema: loaded ``XMLSchema`` instance
    :type schema: xmlschema.XMLSchema
    :param root_tag: optional explicit root element name (local name or Clark notation)
    :type root_tag: str | None
    :param error_type: fallback exception class
    :type error_type: type[Exception]
    :param error_by_code: mapping of specification error codes to exception classes
    :type error_by_code: Mapping[str, type[Exception]]
    :return: list of root ``XsdElement`` declarations in schema order
    :rtype: list[XsdElement]
    """
    if root_tag is not None:
        return [resolve_target_element(schema, root_tag, error_type=error_type, error_by_code=error_by_code)]

    global_elements = [el for el in schema.elements.values() if isinstance(el, XsdElement)]
    if not global_elements:
        msg = "No global elements found in XSD schema"
        raise_xsd_code("E1", msg, error_type=error_type, error_by_code=error_by_code)

    referenced_names = _collect_referenced_global_element_names(schema)
    unreferenced = [el for el in global_elements if el.name not in referenced_names]
    return unreferenced or global_elements


def _advance_elements_one_level(
    schema: xmlschema.XMLSchema,
    elements: Sequence[XsdElement],
    *,
    error_type: type[Exception] = ValueError,
    error_by_code: Mapping[str, type[Exception]] = frozendict(),
) -> list[XsdElement]:
    """Return child ``XsdElement`` declarations reachable in one step from ``elements``.

    :param schema: loaded ``XMLSchema`` instance
    :type schema: xmlschema.XMLSchema
    :param elements: parent XSD element declarations at current depth
    :type elements: Sequence[XsdElement]
    :param error_type: fallback exception class
    :type error_type: type[Exception]
    :param error_by_code: mapping of specification error codes to exception classes
    :type error_by_code: Mapping[str, type[Exception]]
    :return: child XSD element declarations at the next depth
    :rtype: list[XsdElement]
    """
    next_elements: list[XsdElement] = []
    visited_types: set[int] = set()
    for parent_el in elements:
        parent_type = parent_el.type
        if not isinstance(parent_type, XsdComplexType) or id(parent_type) in visited_types:
            continue
        visited_types.add(id(parent_type))
        if parent_type.has_simple_content() or parent_type.mixed:
            continue
        next_elements.extend(
            collect_unique_children(schema, parent_type, error_type=error_type, error_by_code=error_by_code)
        )
    return next_elements


def _group_and_deduplicate_elements(
    elements: Sequence[XsdElement],
    *,
    error_type: type[Exception] = ValueError,
    error_by_code: Mapping[str, type[Exception]] = frozendict(),
) -> dict[str, XsdElement]:
    """Group ``elements`` by local name and deduplicate matching declarations.

    :param elements: XSD element declarations at a given depth
    :type elements: Sequence[XsdElement]
    :param error_type: fallback exception class
    :type error_type: type[Exception]
    :param error_by_code: mapping of specification error codes to exception classes
    :type error_by_code: Mapping[str, type[Exception]]
    :return: mapping of element local name to its deduplicated ``XsdElement``
    :rtype: dict[str, XsdElement]
    """
    by_name: dict[str, list[XsdElement]] = {}
    for el in elements:
        if el.local_name:
            by_name.setdefault(el.local_name, []).append(el)
    return {
        name: _deduplicate_target_elements(matches, name, error_type=error_type, error_by_code=error_by_code)
        for name, matches in by_name.items()
    }


def resolve_elements_at_level(
    schema: xmlschema.XMLSchema,
    level: int,
    *,
    root_tag: str | None = None,
    error_type: type[Exception] = ValueError,
    error_by_code: Mapping[str, type[Exception]] = frozendict(),
) -> dict[str, XsdElement]:
    """Deduce all ``XsdElement`` declarations that can appear at depth ``level`` in an XSD schema.

    :param schema: loaded ``XMLSchema`` instance
    :type schema: xmlschema.XMLSchema
    :param level: non-negative depth relative to the root element(s)
    :type level: int
    :param root_tag: optional explicit root element name to anchor the traversal
    :type root_tag: str | None
    :param error_type: fallback exception class
    :type error_type: type[Exception]
    :param error_by_code: mapping of specification error codes to exception classes
    :type error_by_code: Mapping[str, type[Exception]]
    :return: mapping of element local tag name to its validated ``XsdElement`` declaration at ``level``
    :rtype: dict[str, XsdElement]
    """
    if level < 0:
        msg = f"level must be >= 0, got {level}"
        raise ValueError(msg)

    current_elements = resolve_root_elements(
        schema,
        root_tag=root_tag,
        error_type=error_type,
        error_by_code=error_by_code,
    )
    for _ in range(level):
        current_elements = _advance_elements_one_level(
            schema,
            current_elements,
            error_type=error_type,
            error_by_code=error_by_code,
        )
        if not current_elements:
            break

    deduped = _group_and_deduplicate_elements(current_elements, error_type=error_type, error_by_code=error_by_code)
    if not deduped:
        msg = f"No XML elements found at level {level} in XSD schema"
        raise_xsd_code("E1", msg, error_type=error_type, error_by_code=error_by_code)
    return deduped
