"""Read XML Schema Definition (XSD) documents into the converter IR."""

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any, ClassVar, Final, Literal

import xmlschema
from frozendict import frozendict
from xmlschema.validators import (
    XsdAnyAttribute,
    XsdAnyElement,
    XsdAttribute,
    XsdComplexType,
    XsdElement,
    XsdGroup,
    XsdSimpleType,
    XsdType,
)

from cdm_data_loaders.converters.core.errors import ConversionError
from cdm_data_loaders.converters.core.guards import DEFAULT_MAX_RECURSION_DEPTH, validate_max_recursion_depth
from cdm_data_loaders.converters.core.ir import (
    SPEC_BINARY,
    SPEC_BOOL,
    SPEC_DATE,
    SPEC_FLOAT64,
    SPEC_INT8,
    SPEC_INT16,
    SPEC_INT32,
    SPEC_INT64,
    SPEC_STRING,
    SPEC_UINT8,
    SPEC_UINT16,
    SPEC_UINT32,
    SPEC_UINT64,
    NodeHints,
    NodeType,
    Provenance,
    SchemaDocument,
    TypedNode,
)
from cdm_data_loaders.converters.core.ir import Field as SchemaField
from cdm_data_loaders.utils.xml_utils import local_name as _local_name

XSD_NAMESPACE: Final[str] = "{http://www.w3.org/2001/XMLSchema}"
XSI_NAMESPACE: Final[str] = "{http://www.w3.org/2001/XMLSchema-instance}"
XSI_NIL: Final[str] = f"{XSI_NAMESPACE}nil"

FORBIDDEN_RECURSIVE_XSD_TYPES: Final[frozenset[str]] = frozenset(
    {
        "xs:date",
        "xs:dateTime",
        "xs:hexBinary",
        "xs:base64Binary",
    }
)

BUILTIN_LIST_ITEM_TYPES: Final[Mapping[str, str]] = frozendict(
    {
        "xs:NMTOKENS": "xs:NMTOKEN",
        "xs:IDREFS": "xs:IDREF",
        "xs:ENTITIES": "xs:ENTITY",
    }
)

XSD_SCALAR_SPEC: Final[Mapping[str, tuple[NodeType, NodeHints]]] = frozendict(
    {
        **dict.fromkeys(
            (
                "xs:string",
                "xs:normalizedString",
                "xs:token",
                "xs:language",
                "xs:Name",
                "xs:NCName",
                "xs:NMTOKEN",
                "xs:ID",
                "xs:IDREF",
                "xs:ENTITY",
                "xs:anyURI",
                "xs:QName",
                "xs:NOTATION",
                "xs:dateTime",
                "xs:time",
                "xs:duration",
                "xs:gYear",
                "xs:gYearMonth",
                "xs:gMonthDay",
                "xs:gDay",
                "xs:gMonth",
            ),
            SPEC_STRING,
        ),
        "xs:boolean": SPEC_BOOL,
        "xs:byte": SPEC_INT8,
        "xs:short": SPEC_INT16,
        "xs:int": SPEC_INT32,
        **dict.fromkeys(
            (
                "xs:long",
                "xs:integer",
                "xs:positiveInteger",
                "xs:nonNegativeInteger",
                "xs:negativeInteger",
                "xs:nonPositiveInteger",
            ),
            SPEC_INT64,
        ),
        "xs:unsignedByte": SPEC_UINT8,
        "xs:unsignedShort": SPEC_UINT16,
        "xs:unsignedInt": SPEC_UINT32,
        "xs:unsignedLong": SPEC_UINT64,
        **dict.fromkeys(("xs:float", "xs:double", "xs:decimal"), SPEC_FLOAT64),
        "xs:date": SPEC_DATE,
        **dict.fromkeys(("xs:base64Binary", "xs:hexBinary"), SPEC_BINARY),
    }
)


class XsdToPyArrowError(ConversionError):
    """Base class for XSD constructs that cannot be converted to a PyArrow schema.

    Each subclass carries a stable specification ``code`` and a short ``title``; its docstring states the reason
    the construct is rejected. The converter specification document is generated from these attributes.
    """

    code: ClassVar[str] = ""
    title: ClassVar[str] = ""


class XmlTagNotFoundError(XsdToPyArrowError):
    """The target element is not declared anywhere in the XSD."""

    code = "E1"
    title = "`xml_tag` not found"


class AmbiguousXmlTagError(XsdToPyArrowError):
    """Declarations of `xml_tag` at the shallowest matching depth have different types, so no single `pa.Schema` can be chosen."""

    code = "E2"
    title = "Ambiguous `xml_tag` type"


class NonStructXmlTagError(XsdToPyArrowError):
    """A `pa.Table` schema needs top-level columns: the type of `xml_tag` must yield at least one attribute, `#text`, or child element field."""

    code = "E3"
    title = "Non-struct top-level `xml_tag`"


class WildcardElementError(XsdToPyArrowError):
    """Arbitrary undeclared child elements cannot be represented as fixed `pa.struct` fields."""

    code = "E4"
    title = "Element wildcard (`xs:any`)"


class WildcardAttributeError(XsdToPyArrowError):
    """Arbitrary undeclared attributes cannot be represented as fixed `pa.struct` fields."""

    code = "E5"
    title = "Attribute wildcard (`xs:anyAttribute`)"


class AbstractComponentError(XsdToPyArrowError):
    """Concrete instances of an abstract element or type require `xsi:type` or substitution polymorphism, which has no single schema."""

    code = "E6"
    title = 'Abstract element or type (`abstract="true"`)'


class SubstitutionGroupError(XsdToPyArrowError):
    """Substituted elements introduce element names and types that are not declared in the parent content model."""

    code = "E7"
    title = "Substitution group"


class FieldNameCollisionError(XsdToPyArrowError):
    """An attribute (prefixed `_`) and a child element map to the same field name in one `pa.struct`."""

    code = "E8"
    title = "Attribute / child field name collision"


class RecursiveNonJsonTypeError(XsdToPyArrowError):
    """Subtrees truncated beyond `max_recursion_depth` are serialized with `json.dumps`, which cannot encode `datetime.date` or `bytes` without lossy conversion."""

    code = "E9"
    title = "Non-JSON-serializable scalar under recursion"


XSD_TO_PYARROW_ERRORS: Final[tuple[type[XsdToPyArrowError], ...]] = (
    XmlTagNotFoundError,
    AmbiguousXmlTagError,
    NonStructXmlTagError,
    WildcardElementError,
    WildcardAttributeError,
    AbstractComponentError,
    SubstitutionGroupError,
    FieldNameCollisionError,
    RecursiveNonJsonTypeError,
)


def load_schema(schema_path: str | Path) -> xmlschema.XMLSchema:
    """Load an XML schema document into an XMLSchema object.

    :param schema_path: path to the schema file
    :type schema_path: str | Path
    :return: loaded schema
    :rtype: xmlschema.XMLSchema
    """
    return xmlschema.XMLSchema(str(schema_path))


def _extract_doc_comment(component: object, fallback_type: object = None) -> str | None:
    """Extract whitespace-normalized ``<xs:documentation>`` text from an XSD component or its type."""
    candidates = [component]
    ref = getattr(component, "ref", None)
    if ref is not None and ref is not component:
        candidates.append(ref)
    if fallback_type is not None:
        candidates.append(fallback_type)

    for candidate in candidates:
        annotation = getattr(candidate, "annotation", None)
        docs = getattr(annotation, "documentation", None)
        if not docs:
            continue
        parts = [" ".join(doc.text.split()) for doc in docs if getattr(doc, "text", None) and doc.text.strip()]
        if parts:
            return " ".join(parts)
    return None


def _builtin_xsd_name(simple_type: XsdType) -> str:
    """Resolve an atomic simpleType or restriction to its built-in ``xs:<name>`` identifier."""
    cur: Any = simple_type
    while cur is not None:
        name = getattr(cur, "name", None)
        if isinstance(name, str) and name.startswith(XSD_NAMESPACE):
            return f"xs:{cur.local_name}"
        cur = getattr(cur, "base_type", None)
    return "xs:string"


def _unwrap_member_types(simple_type: XsdType) -> list[XsdSimpleType]:
    """Return the member types of an ``xs:union`` simpleType (unwrapping restrictions)."""
    cur: Any = simple_type
    while cur is not None:
        members = getattr(cur, "member_types", None)
        if members:
            return list(members)
        cur = getattr(cur, "base_type", None)
    return []


def _unwrap_item_type(simple_type: XsdType) -> XsdSimpleType | None:
    """Return the item type of an ``xs:list`` simpleType (unwrapping restrictions)."""
    cur: Any = simple_type
    while cur is not None:
        item_type = getattr(cur, "item_type", None)
        if item_type is not None:
            return item_type
        cur = getattr(cur, "base_type", None)
    return None


def _resolve_scalar_spec(simple_type: XsdType) -> tuple[NodeType, NodeHints, str]:
    """Map a non-list XSD simpleType (atomic or union) to ``(NodeType, NodeHints, xsd_type_str)``."""
    if simple_type.is_union():
        members = _unwrap_member_types(simple_type)
        member_specs = [_resolve_scalar_spec(m) for m in members]
        xsd_type_str = " | ".join(xsd_name for _, _, xsd_name in member_specs)
        unique_specs = {(nt, h) for nt, h, _ in member_specs}
        node_type, hints = next(iter(unique_specs)) if len(unique_specs) == 1 else SPEC_STRING
        return node_type, hints, xsd_type_str

    xsd_name = _builtin_xsd_name(simple_type)
    node_type, hints = XSD_SCALAR_SPEC.get(xsd_name, SPEC_STRING)
    return node_type, hints, xsd_name


def _simple_type_to_ir_node(
    simple_type: XsdType,
    path: tuple[str | int, ...],
    *,
    nullable: bool,
) -> TypedNode:
    """Map any XSD simpleType (atomic, union, or list) to an IR ``TypedNode``."""
    builtin_name = _builtin_xsd_name(simple_type)
    if builtin_name in BUILTIN_LIST_ITEM_TYPES or simple_type.is_list():
        if builtin_name in BUILTIN_LIST_ITEM_TYPES:
            item_nt, item_hints, item_xsd = *SPEC_STRING, BUILTIN_LIST_ITEM_TYPES[builtin_name]
        else:
            item_type = _unwrap_item_type(simple_type)
            item_nt, item_hints, item_xsd = (
                _resolve_scalar_spec(item_type) if item_type is not None else (*SPEC_STRING, "xs:string")
            )
        item_node = TypedNode(
            type=item_nt,
            nullable=True,
            hints=item_hints,
            provenance=Provenance("xsd", (*path, "item"), {"xsd_type": item_xsd}),
        )
        return TypedNode(
            type="array",
            nullable=nullable,
            items=item_node,
            provenance=Provenance("xsd", path),
        )

    node_type, hints, xsd_name = _resolve_scalar_spec(simple_type)
    return TypedNode(
        type=node_type,
        nullable=nullable,
        hints=hints,
        provenance=Provenance("xsd", path, {"xsd_type": xsd_name}),
    )


def _iter_simple_builtin_names(simple_type: XsdType) -> list[str]:
    """Return all constituent built-in ``xs:*`` type names for an atomic, list, or union simpleType."""
    builtin_name = _builtin_xsd_name(simple_type)
    if builtin_name in BUILTIN_LIST_ITEM_TYPES:
        return [BUILTIN_LIST_ITEM_TYPES[builtin_name]]
    if simple_type.is_list():
        item_type = _unwrap_item_type(simple_type)
        return _iter_simple_builtin_names(item_type) if item_type is not None else ["xs:string"]
    if simple_type.is_union():
        names: list[str] = []
        for member in _unwrap_member_types(simple_type):
            names.extend(_iter_simple_builtin_names(member))
        return names
    return [builtin_name]


@dataclass(frozen=True, slots=True)
class _TraversalContext:
    """Immutable traversal context for XSD schema generation and XML-to-dict conversion."""

    schema: xmlschema.XMLSchema
    type_counts: Mapping[int, int]
    max_recursion_depth: int | None
    struct_cache: dict[tuple[int, bool], Any] = field(default_factory=dict)


type LeafKind = Literal["simple", "mixed", "empty_marker"]


def _check_complex_type_forbidden_recursion(root_type: XsdType, current: XsdComplexType) -> list[XsdType]:
    """Validate attributes on ``current`` and return child ``XsdType``s to inspect for forbidden recursive scalars."""
    for attr in current.attributes.values():
        if isinstance(attr, XsdAttribute) and attr.type is not None:
            for xsd_name in _iter_simple_builtin_names(attr.type):
                if xsd_name in FORBIDDEN_RECURSIVE_XSD_TYPES:
                    msg = (
                        f"Unsupported XSD feature: recursive type {root_type.local_name or root_type.name!r} "
                        f"contains attribute @{attr.local_name} of type {xsd_name!r}"
                    )
                    raise RecursiveNonJsonTypeError(msg)

    if current.has_simple_content() and isinstance(current.content, XsdType):
        return [current.content]
    if not current.mixed and current.model_group is not None:
        return [
            child.type
            for child in current.model_group.iter_elements()
            if isinstance(child, XsdElement) and child.type is not None and child.max_occurs != 0
        ]
    return []


def _check_no_forbidden_types_under_recursion(root_type: XsdType) -> None:
    """Raise ValueError if a recursive type or any descendant type uses date, dateTime, or binary scalars."""
    visited: set[int] = set()
    stack: list[XsdType] = [root_type]

    while stack:
        current = stack.pop()
        if id(current) in visited:
            continue
        visited.add(id(current))

        if not isinstance(current, XsdComplexType):
            for xsd_name in _iter_simple_builtin_names(current):
                if xsd_name in FORBIDDEN_RECURSIVE_XSD_TYPES:
                    msg = (
                        f"Unsupported XSD feature: recursive type {root_type.local_name or root_type.name!r} "
                        f"contains {xsd_name!r}, which cannot be serialized to JSON when recursion is truncated"
                    )
                    raise RecursiveNonJsonTypeError(msg)
            continue

        stack.extend(_check_complex_type_forbidden_recursion(root_type, current))


def _group_min_occurs_by_name(group: XsdGroup, local_name: str) -> int:
    """Compute minimum occurrences of elements with ``local_name`` across a model group."""
    if group.min_occurs == 0 or group.max_occurs == 0:
        return 0
    target_group = group.ref if group.ref is not None else group
    counts: list[int] = []
    for item in target_group.content:
        if isinstance(item, XsdGroup):
            counts.append(_group_min_occurs_by_name(item, local_name))
        elif isinstance(item, XsdElement) and item.max_occurs != 0 and item.local_name == local_name:
            counts.append(item.min_occurs)
        else:
            counts.append(0)
    if not counts:
        return 0
    branch_min = min(counts) if target_group.model == "choice" else sum(counts)
    return group.min_occurs * branch_min


def _particle_max_occurs_by_name(item: object, local_name: str) -> int | None:
    """Compute maximum occurrences (None for unbounded) of ``local_name`` for a single group particle."""
    if isinstance(item, XsdGroup):
        return _group_max_occurs_by_name(item, local_name)
    if isinstance(item, XsdElement) and item.max_occurs != 0 and item.local_name == local_name:
        return item.max_occurs
    return 0


def _group_max_occurs_by_name(group: XsdGroup, local_name: str) -> int | None:
    """Compute maximum occurrences (None for unbounded) of elements with ``local_name`` across a model group."""
    if group.max_occurs == 0:
        return 0
    target_group = group.ref if group.ref is not None else group
    branch_maxes = [_particle_max_occurs_by_name(item, local_name) for item in target_group.content]
    if not branch_maxes:
        return 0
    if any(m is None for m in branch_maxes):
        inner_max: int | None = None
    elif target_group.model == "choice":
        inner_max = max(m for m in branch_maxes if m is not None)
    else:
        inner_max = sum(m for m in branch_maxes if m is not None)
    if inner_max == 0:
        return 0
    if inner_max is None or group.max_occurs is None:
        return None
    return group.max_occurs * inner_max


def _is_any_element(item: object) -> bool:
    """Return True if ``item`` is an ``xs:any`` wildcard particle."""
    return isinstance(item, XsdAnyElement)


def _is_any_attribute(attr: object) -> bool:
    """Return True if ``attr`` is an ``xs:anyAttribute`` wildcard declaration."""
    return isinstance(attr, XsdAnyAttribute)


def _validate_group_particles(schema: xmlschema.XMLSchema, group: XsdGroup) -> None:
    """Validate that a model group contains no wildcards, abstract elements/types, or substitution groups."""
    target_group = group.ref if group.ref is not None else group
    for item in target_group.content:
        if _is_any_element(item):
            msg = "Unsupported XSD feature: xs:any wildcard element is not supported"
            raise WildcardElementError(msg)
        if isinstance(item, XsdGroup):
            _validate_group_particles(schema, item)
        elif isinstance(item, XsdElement):
            target_el = item.ref if item.ref is not None else item
            if getattr(item, "abstract", False) or getattr(target_el, "abstract", False):
                msg = f"Unsupported XSD feature: abstract element {item.local_name!r} is not supported"
                raise AbstractComponentError(msg)
            if (
                item.name in schema.substitution_groups
                or target_el.name in schema.substitution_groups
                or getattr(item, "substitution_group", None) is not None
                or getattr(target_el, "substitution_group", None) is not None
            ):
                msg = f"Unsupported XSD feature: substitutionGroup on element {item.local_name!r} is not supported"
                raise SubstitutionGroupError(msg)
            if item.type is not None and getattr(item.type, "abstract", False):
                msg = f"Unsupported XSD feature: abstract type {item.type.local_name!r} is not supported"
                raise AbstractComponentError(msg)


def _collect_unique_children(schema: xmlschema.XMLSchema, complex_type: XsdComplexType) -> list[XsdElement]:
    """Validate and return unique child ``XsdElement`` declarations in content-model order."""
    model_group = complex_type.model_group
    if model_group is None:
        return []
    _validate_group_particles(schema, model_group)
    seen_names: set[str] = set()
    children: list[XsdElement] = []
    for child in model_group.iter_elements():
        if not isinstance(child, XsdElement):
            continue
        max_occurs = _group_max_occurs_by_name(model_group, child.local_name)
        if max_occurs == 0 or child.local_name in seen_names:
            continue
        seen_names.add(child.local_name)
        children.append(child)
    return children


def _collect_attributes(complex_type: XsdComplexType) -> list[XsdAttribute]:
    """Validate and return declared attributes on ``complex_type`` in declaration order."""
    attrs: list[XsdAttribute] = []
    for attr in complex_type.attributes.values():
        if _is_any_attribute(attr):
            msg = "Unsupported XSD feature: xs:anyAttribute wildcard is not supported"
            raise WildcardAttributeError(msg)
        if isinstance(attr, XsdAttribute) and getattr(attr, "use", None) != "prohibited":
            if attr.local_name is None:
                continue
            attrs.append(attr)
    return attrs


def _is_empty_marker_complex_type(complex_type: XsdComplexType) -> bool:
    """Return True if ``complex_type`` has no attributes, no child elements, not simpleContent, and not mixed."""
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
        isinstance(el, XsdElement) and _group_max_occurs_by_name(complex_type.model_group, el.local_name) != 0
        for el in complex_type.model_group.iter_elements()
    )


def _resolve_child_cardinality(
    model_group: XsdGroup,
    child_decl: XsdElement,
    *,
    parent_nillable: bool = False,
) -> tuple[bool, bool]:
    """Return ``(is_list, nullable)`` for ``child_decl`` within ``model_group``."""
    child_name = child_decl.local_name or ""
    max_occurs = _group_max_occurs_by_name(model_group, child_name)
    min_occurs = _group_min_occurs_by_name(model_group, child_name)
    is_list = max_occurs is None or max_occurs > 1
    nullable = parent_nillable or child_decl.nillable or min_occurs < 1
    return is_list, nullable


def _classify_leaf_element_type(elem_type: XsdType | None) -> tuple[LeafKind, XsdType | None] | None:
    """Classify ``elem_type`` as a non-struct leaf ``(kind, simple_type)`` or ``None`` if it maps to a struct."""
    if not isinstance(elem_type, XsdComplexType):
        return "simple", elem_type

    attrs = _collect_attributes(elem_type)
    if elem_type.has_simple_content() and not attrs:
        content_type = elem_type.content if isinstance(elem_type.content, XsdType) else None
        return "simple", content_type

    if elem_type.mixed and not attrs:
        return "mixed", None

    if _is_empty_marker_complex_type(elem_type):
        return "empty_marker", None

    return None


def _resolve_non_struct_element_node(
    elem_type: XsdType | None,
    path: tuple[str | int, ...],
    *,
    nullable: bool,
) -> TypedNode | None:
    """Return an IR ``TypedNode`` if ``elem_type`` maps to a leaf scalar rather than a struct."""
    leaf_info = _classify_leaf_element_type(elem_type)
    if leaf_info is None:
        return None
    kind, simple_type = leaf_info
    if kind == "simple":
        if simple_type is not None:
            return _simple_type_to_ir_node(simple_type, path, nullable=nullable)
        return TypedNode(
            type="string",
            nullable=nullable,
            hints=NodeHints(logical_type="string"),
            provenance=Provenance("xsd", path, {"xsd_type": "xs:string"}),
        )
    if kind == "mixed":
        return TypedNode(
            type="string",
            nullable=nullable,
            hints=NodeHints(logical_type="string"),
            provenance=Provenance("xsd", path, {"xsd_type": "xs:string"}),
        )
    return TypedNode(
        type="string",
        nullable=nullable,
        hints=NodeHints(logical_type="string"),
        provenance=Provenance("xsd", path),
    )


def _can_reach_any_type(start_type: XsdComplexType, target_type_ids: Mapping[int, int]) -> bool:
    """Return True if ``start_type`` or any descendant complexType has its ``id`` in ``target_type_ids``.

    :param start_type: starting XsdComplexType to inspect
    :type  start_type: XsdComplexType
    :param target_type_ids: mapping of active ancestor complexType object IDs
    :type  target_type_ids: Mapping[int, int]
    :return: True if any complexType in the subtree is in target_type_ids
    :rtype : bool
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


def _wrap_leaf_ir_field(  # noqa: PLR0913
    name: str,
    elem_type: XsdType | None,
    path: tuple[str | int, ...],
    *,
    comment: str | None,
    is_list: bool,
    nullable: bool,
    truncated_recursion: bool = False,
) -> SchemaField:
    """Create a scalar or list IR ``Field`` with ``xsd_type`` on leaf scalar nodes and ``comment`` on the outer field."""
    annotations = {"description": comment} if comment else {}
    leaf_path = (*path, "item") if is_list else path
    leaf_nullable = True if is_list else nullable
    trunc_meta = {"truncated_component_id": id(elem_type)} if (truncated_recursion and elem_type is not None) else {}

    if truncated_recursion:
        leaf_node = TypedNode(
            type="string",
            nullable=leaf_nullable,
            hints=NodeHints(logical_type="string"),
            provenance=Provenance("xsd", leaf_path, trunc_meta),
        )
    else:
        leaf_node = _resolve_non_struct_element_node(elem_type, leaf_path, nullable=leaf_nullable) or TypedNode(
            type="string",
            nullable=leaf_nullable,
            hints=NodeHints(logical_type="string"),
            provenance=Provenance("xsd", leaf_path),
        )

    if is_list:
        field_node = TypedNode(
            type="array",
            nullable=nullable,
            items=leaf_node,
            provenance=Provenance("xsd", path, trunc_meta),
        )
        prov_meta = trunc_meta
    else:
        field_node = leaf_node
        prov_meta = dict(leaf_node.provenance.metadata) if leaf_node.provenance is not None else {}

    return SchemaField(
        name=name,
        node=field_node,
        required=not nullable,
        annotations=annotations,
        provenance=Provenance("xsd", path, prov_meta),
    )


def _build_complex_struct_fields(
    ctx: _TraversalContext,
    complex_type: XsdComplexType,
    path: tuple[str | int, ...],
    *,
    parent_nillable: bool,
) -> tuple[SchemaField, ...]:
    """Build the ordered tuple of IR ``Field``s for a struct-mapped ``XsdComplexType``."""
    fields: list[SchemaField] = []
    attr_field_names: set[str] = set()

    for attr in _collect_attributes(complex_type):
        field_name = f"_{attr.local_name}"
        attr_field_names.add(field_name)
        attr_path = (*path, field_name)
        attr_type = attr.type
        nullable = parent_nillable or getattr(attr, "use", "optional") != "required"
        if attr_type is not None:
            node = _simple_type_to_ir_node(attr_type, attr_path, nullable=nullable)
        else:
            node = TypedNode(
                type="string",
                nullable=nullable,
                hints=NodeHints(logical_type="string"),
                provenance=Provenance("xsd", attr_path, {"xsd_type": "xs:string"}),
            )
        comment = _extract_doc_comment(attr, attr_type)
        annotations = {"description": comment} if comment else {}
        prov_meta = dict(node.provenance.metadata) if node.provenance is not None else {}
        fields.append(
            SchemaField(
                name=field_name,
                node=node,
                required=not nullable,
                annotations=annotations,
                provenance=Provenance("xsd", attr_path, prov_meta),
            )
        )

    if complex_type.has_simple_content() or complex_type.mixed:
        text_path = (*path, "#text")
        content_type = complex_type.content if complex_type.has_simple_content() else None
        if isinstance(content_type, XsdType):
            node = _simple_type_to_ir_node(content_type, text_path, nullable=True)
        else:
            node = TypedNode(
                type="string",
                nullable=True,
                hints=NodeHints(logical_type="string"),
                provenance=Provenance("xsd", text_path, {"xsd_type": "xs:string"}),
            )
        prov_meta = dict(node.provenance.metadata) if node.provenance is not None else {}
        fields.append(
            SchemaField(
                name="#text",
                node=node,
                required=False,
                provenance=Provenance("xsd", text_path, prov_meta),
            )
        )
        return tuple(fields)

    model_group = complex_type.model_group
    if model_group is None:
        return tuple(fields)

    for child in _collect_unique_children(ctx.schema, complex_type):
        child_name = child.local_name or ""
        if child_name in attr_field_names:
            msg = (
                f"Field name collision in {complex_type.local_name or 'complexType'!r}: "
                f"attribute and child element both map to {child_name!r}"
            )
            raise FieldNameCollisionError(msg)

        is_list, nullable = _resolve_child_cardinality(model_group, child, parent_nillable=parent_nillable)
        fields.append(
            _build_element_ir_field(
                ctx,
                child,
                (*path, child_name),
                is_list=is_list,
                nullable=nullable,
            )
        )

    return tuple(fields)


def _build_element_ir_field(
    ctx: _TraversalContext,
    element: XsdElement,
    path: tuple[str | int, ...],
    *,
    is_list: bool,
    nullable: bool,
) -> SchemaField:
    """Build an IR ``Field`` for a child ``XsdElement`` given its cardinality and nullability."""
    elem_type = element.type
    field_name = element.local_name or ""
    comment = _extract_doc_comment(element, elem_type)

    if _classify_leaf_element_type(elem_type) is not None:
        return _wrap_leaf_ir_field(
            field_name,
            elem_type,
            path,
            comment=comment,
            is_list=is_list,
            nullable=nullable,
        )

    elem_type = _require_complex_type(elem_type, field_name)
    type_id = id(elem_type)
    if type_id in ctx.type_counts:
        _check_no_forbidden_types_under_recursion(elem_type)

    next_count = ctx.type_counts.get(type_id, 0) + 1
    if ctx.max_recursion_depth is not None and next_count > ctx.max_recursion_depth:
        return _wrap_leaf_ir_field(
            field_name,
            elem_type,
            path,
            comment=comment,
            is_list=is_list,
            nullable=nullable,
            truncated_recursion=True,
        )

    cache_key = (type_id, element.nillable)
    is_cacheable = not _can_reach_any_type(elem_type, ctx.type_counts)
    cached = ctx.struct_cache.get(cache_key) if is_cacheable else None
    if isinstance(cached, TypedNode):
        struct_node = cached
    else:
        next_ctx = _TraversalContext(
            schema=ctx.schema,
            type_counts={**ctx.type_counts, type_id: next_count},
            max_recursion_depth=ctx.max_recursion_depth,
            struct_cache=ctx.struct_cache,
        )
        struct_fields = _build_complex_struct_fields(
            next_ctx,
            elem_type,
            path,
            parent_nillable=element.nillable,
        )
        required_names = tuple(f.name for f in struct_fields if f.required)
        struct_meta: dict[str, Any] = {
            "component_id": type_id,
            "nillable": bool(element.nillable),
        }
        if elem_type.local_name is not None:
            struct_meta["type_name"] = elem_type.local_name
        if not is_cacheable:
            struct_meta["inline_struct"] = True
        struct_node = TypedNode(
            type="object",
            nullable=False,
            properties=struct_fields,
            required_names=required_names,
            additional_properties=TypedNode(type="never", nullable=False),
            provenance=Provenance("xsd", path, struct_meta),
        )
        if is_cacheable:
            ctx.struct_cache[cache_key] = struct_node

    if is_list:
        item_struct_node = replace(struct_node, nullable=True)
        field_node = TypedNode(
            type="array",
            nullable=nullable,
            items=item_struct_node,
            provenance=Provenance("xsd", path),
        )
    else:
        field_node = replace(struct_node, nullable=nullable)

    annotations = {"description": comment} if comment else {}
    return SchemaField(
        name=field_name,
        node=field_node,
        required=not nullable,
        annotations=annotations,
        provenance=Provenance("xsd", path),
    )


def _resolve_target_element(schema: xmlschema.XMLSchema, xml_tag: str) -> XsdElement:
    """Resolve ``xml_tag`` (local name or Clark notation) to a unique ``XsdElement`` declaration."""
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
        return _deduplicate_target_elements(depth0, xml_tag)

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
            return _deduplicate_target_elements(matches, xml_tag)
        current_level = next_level

    msg = f"Element {xml_tag!r} not found in XSD schema"
    raise XmlTagNotFoundError(msg)


def _deduplicate_target_elements(matches: list[XsdElement], xml_tag: str) -> XsdElement:
    """Verify all matching ``XsdElement`` declarations at the resolution depth share a single ``XsdType``."""
    by_type: dict[int, XsdElement] = {id(el.type): el for el in matches}
    if len(by_type) > 1:
        msg = f"Ambiguous xml_tag {xml_tag!r}: found {len(by_type)} declarations with different types"
        raise AmbiguousXmlTagError(msg)
    target = next(iter(by_type.values()))
    target_ref = target.ref if target.ref is not None else target
    if getattr(target, "abstract", False) or getattr(target_ref, "abstract", False):
        msg = f"Unsupported XSD feature: abstract element {xml_tag!r} is not supported"
        raise AbstractComponentError(msg)
    if target.type is not None and getattr(target.type, "abstract", False):
        msg = f"Unsupported XSD feature: abstract type on {xml_tag!r} is not supported"
        raise AbstractComponentError(msg)
    return target


def _require_complex_type(elem_type: XsdType | None, xml_tag: str) -> XsdComplexType:
    """Ensure that the resolved target element has an ``XsdComplexType``.

    :param elem_type: XSD type to validate
    :type  elem_type: XsdType | None
    :param xml_tag: XML element tag name for error reporting
    :type  xml_tag: str
    :return: validated XsdComplexType
    :rtype : XsdComplexType
    """
    if not isinstance(elem_type, XsdComplexType):
        msg = f"Top-level xml_tag {xml_tag!r} must have a complex type with attributes or child elements"
        raise NonStructXmlTagError(msg)
    return elem_type


def _prepare_target_traversal(
    schema: xmlschema.XMLSchema,
    xml_tag: str,
    *,
    max_recursion_depth: int,
    struct_cache: dict[tuple[int, bool], Any] | None = None,
) -> tuple[XsdElement, XsdComplexType, _TraversalContext]:
    """Resolve ``xml_tag`` to its ``XsdElement``, validated ``XsdComplexType``, and initial ``_TraversalContext``.

    :param schema: loaded XMLSchema instance
    :type  schema: xmlschema.XMLSchema
    :param xml_tag: target XML element name
    :type  xml_tag: str
    :param max_recursion_depth: maximum structural recursion depth
    :type  max_recursion_depth: int
    :param struct_cache: optional struct component cache
    :type  struct_cache: dict[tuple[int, bool], Any] | None
    :return: tuple of (target_elem, elem_type, traversal_context)
    :rtype : tuple[XsdElement, XsdComplexType, _TraversalContext]
    """
    target_elem = _resolve_target_element(schema, xml_tag)
    elem_type = _require_complex_type(target_elem.type, xml_tag)
    ctx = _TraversalContext(
        schema=schema,
        type_counts={id(elem_type): 1},
        max_recursion_depth=max_recursion_depth,
        struct_cache=struct_cache if struct_cache is not None else {},
    )
    return target_elem, elem_type, ctx


def _build_element_struct_node(
    target_elem: XsdElement,
    xml_tag: str,
    ctx: _TraversalContext,
) -> TypedNode:
    """Validate that ``target_elem`` maps to a struct and return its root object ``TypedNode``."""
    elem_type = _require_complex_type(target_elem.type, xml_tag)
    if _is_empty_marker_complex_type(elem_type):
        msg = f"Top-level xml_tag {xml_tag!r} has an empty complex type with no attributes or child elements"
        raise NonStructXmlTagError(msg)

    attrs = _collect_attributes(elem_type)
    if (elem_type.has_simple_content() or elem_type.mixed) and not attrs:
        msg = f"Top-level xml_tag {xml_tag!r} must have a complex type that resolves to a struct"
        raise NonStructXmlTagError(msg)

    fields = _build_complex_struct_fields(
        ctx,
        elem_type,
        (),
        parent_nillable=target_elem.nillable,
    )
    if not fields:
        msg = f"Top-level xml_tag {xml_tag!r} must have a complex type with at least one field"
        raise NonStructXmlTagError(msg)

    required_names = tuple(f.name for f in fields if f.required)
    struct_meta: dict[str, Any] = {
        "component_id": id(elem_type),
        "nillable": bool(target_elem.nillable),
    }
    if elem_type.local_name is not None:
        struct_meta["type_name"] = elem_type.local_name
    return TypedNode(
        type="object",
        nullable=False,
        properties=fields,
        required_names=required_names,
        additional_properties=TypedNode(type="never", nullable=False),
        provenance=Provenance("xsd", (), struct_meta),
    )


def _collect_referenced_global_element_names(schema: xmlschema.XMLSchema) -> set[str]:
    """Return qualified names of global ``XsdElement``s referenced as children inside other complex types.

    :param schema: loaded XMLSchema instance
    :type  schema: xmlschema.XMLSchema
    :return: set of qualified element names referenced as non-self child elements
    :rtype : set[str]
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
) -> list[XsdElement]:
    """Resolve the level-0 root ``XsdElement`` declarations in an XSD schema.

    When ``root_tag`` is omitted, root elements are deduced as global ``<xs:element>`` declarations
    that are not referenced as child elements inside any other complex type's content model (falling
    back to all global elements if all global elements participate in a mutual reference cycle).

    :param schema: loaded XMLSchema instance
    :type schema: xmlschema.XMLSchema
    :param root_tag: optional explicit root element name (local name or Clark notation)
    :type root_tag: str | None
    :return: list of root XsdElement declarations in schema order
    :rtype: list[XsdElement]
    """
    if root_tag is not None:
        return [_resolve_target_element(schema, root_tag)]

    global_elements = [el for el in schema.elements.values() if isinstance(el, XsdElement)]
    if not global_elements:
        msg = "No global elements found in XSD schema"
        raise XmlTagNotFoundError(msg)

    referenced_names = _collect_referenced_global_element_names(schema)
    unreferenced = [el for el in global_elements if el.name not in referenced_names]
    return unreferenced or global_elements


def _advance_elements_one_level(
    schema: xmlschema.XMLSchema,
    elements: Sequence[XsdElement],
) -> list[XsdElement]:
    """Return child ``XsdElement`` declarations reachable in one step from ``elements``.

    :param schema: loaded XMLSchema instance
    :type schema: xmlschema.XMLSchema
    :param elements: parent XSD element declarations at current depth
    :type elements: Sequence[XsdElement]
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
        next_elements.extend(_collect_unique_children(schema, parent_type))
    return next_elements


def _group_and_deduplicate_elements(elements: Sequence[XsdElement]) -> dict[str, XsdElement]:
    """Group ``elements`` by local name and deduplicate matching declarations.

    :param elements: XSD element declarations at a given depth
    :type elements: Sequence[XsdElement]
    :return: mapping of element local name to its deduplicated XsdElement
    :rtype: dict[str, XsdElement]
    """
    by_name: dict[str, list[XsdElement]] = {}
    for el in elements:
        if el.local_name:
            by_name.setdefault(el.local_name, []).append(el)
    return {name: _deduplicate_target_elements(matches, name) for name, matches in by_name.items()}


def resolve_elements_at_level(
    schema: xmlschema.XMLSchema,
    level: int,
    *,
    root_tag: str | None = None,
) -> dict[str, XsdElement]:
    """Deduce all ``XsdElement`` declarations that can appear at depth ``level`` in an XSD schema.

    Depth ``0`` corresponds to the root element(s); depth ``1`` corresponds to direct children of
    the root element(s); depth ``2`` corresponds to grandchildren of the root element(s), and so on.

    :param schema: loaded XMLSchema instance
    :type schema: xmlschema.XMLSchema
    :param level: non-negative depth relative to the root element(s)
    :type level: int
    :param root_tag: optional explicit root element name to anchor the traversal
    :type root_tag: str | None
    :return: mapping of element local tag name to its validated XsdElement declaration at ``level``
    :rtype: dict[str, XsdElement]
    """
    if level < 0:
        msg = f"level must be >= 0, got {level}"
        raise ValueError(msg)

    current_elements = resolve_root_elements(schema, root_tag=root_tag)
    for _ in range(level):
        current_elements = _advance_elements_one_level(schema, current_elements)
        if not current_elements:
            break

    deduped = _group_and_deduplicate_elements(current_elements)
    if not deduped:
        msg = f"No XML elements found at level {level} in XSD schema"
        raise XmlTagNotFoundError(msg)
    return deduped


def _finish_element_doc(
    schema: xmlschema.XMLSchema,
    target_elem: XsdElement,
    tag_name: str,
    root_node: TypedNode,
) -> SchemaDocument:
    """Attach element documentation annotations to ``root_node`` and wrap in a ``SchemaDocument``."""
    comment = _extract_doc_comment(target_elem, target_elem.type)
    annotations = {"description": comment} if comment is not None else {}
    return SchemaDocument(
        root=replace(root_node, annotations=annotations),
        name=_local_name(tag_name),
        identifier=schema.target_namespace or None,
        annotations=annotations,
        provenance=Provenance("xsd", (tag_name,)),
    )


def _build_element_table_doc(
    schema: xmlschema.XMLSchema,
    target_elem: XsdElement,
    tag_name: str,
    *,
    max_recursion_depth: int,
    struct_cache: dict[tuple[int, bool], Any],
) -> SchemaDocument:
    """Build a ``SchemaDocument`` for ``target_elem`` (struct or single-column ``#text`` scalar)."""
    if isinstance(target_elem.type, XsdComplexType) and _is_empty_marker_complex_type(target_elem.type):
        msg = f"Top-level xml_tag {tag_name!r} has an empty complex type with no attributes or child elements"
        raise NonStructXmlTagError(msg)

    comment = _extract_doc_comment(target_elem, target_elem.type)

    if _classify_leaf_element_type(target_elem.type) is not None:
        text_field = _wrap_leaf_ir_field(
            "#text",
            target_elem.type,
            ("#text",),
            comment=comment,
            is_list=False,
            nullable=True,
        )
        root_node = TypedNode(
            type="object",
            nullable=False,
            properties=(text_field,),
            required_names=(),
            additional_properties=TypedNode(type="never", nullable=False),
            provenance=Provenance("xsd", (), {"scalar_leaf_table": True}),
        )
    else:
        elem_type = _require_complex_type(target_elem.type, tag_name)
        ctx = _TraversalContext(
            schema=schema,
            type_counts={id(elem_type): 1},
            max_recursion_depth=max_recursion_depth,
            struct_cache=struct_cache,
        )
        root_node = _build_element_struct_node(target_elem, tag_name, ctx)

    return _finish_element_doc(schema, target_elem, tag_name, root_node)


@dataclass(frozen=True, slots=True)
class XsdReader:
    """Build typed converter IR documents from XML Schema Definition (XSD) files.

    :param xml_tag: Default target XML element tag name when not supplied to :meth:`read`.
    :type xml_tag: str | None
    :param max_recursion_depth: Maximum times a recursive ``XsdComplexType`` may be expanded along an ancestor path.
    :type max_recursion_depth: int
    """

    xml_tag: str | None = None
    max_recursion_depth: int = DEFAULT_MAX_RECURSION_DEPTH

    def __post_init__(self) -> None:
        """Validate reader configuration."""
        validate_max_recursion_depth(self.max_recursion_depth)

    def read(
        self,
        source: xmlschema.XMLSchema | str | Path,
        xml_tag: str | None = None,
        *,
        struct_cache: dict[tuple[int, bool], Any] | None = None,
    ) -> SchemaDocument:
        """Read an ``xmlschema.XMLSchema`` element into a ``SchemaDocument``.

        :param source: Loaded ``XMLSchema`` instance or filesystem path to an XSD file.
        :type source: xmlschema.XMLSchema | str | Path
        :param xml_tag: Target XML element name (local name or Clark notation). Defaults to ``self.xml_tag`` or the sole root element.
        :type xml_tag: str | None
        :param struct_cache: Optional cache sharing IR struct ``TypedNode``s across calls.
        :type struct_cache: dict[tuple[int, bool], Any] | None
        :return: Structural IR document with ``source="xsd"`` provenance.
        :rtype: SchemaDocument
        :raises XsdToPyArrowError: If the schema or ``xml_tag`` violates ``E1``-``E9``.
        """
        schema = load_schema(source) if isinstance(source, str | Path) else source
        tag = xml_tag if xml_tag is not None else self.xml_tag
        if tag is None:
            roots = resolve_root_elements(schema)
            if len(roots) != 1 or roots[0].local_name is None:
                msg = "xml_tag must be specified when the XSD schema does not have a single root element"
                raise XmlTagNotFoundError(msg)
            tag = roots[0].local_name

        target_elem, _, ctx = _prepare_target_traversal(
            schema,
            tag,
            max_recursion_depth=self.max_recursion_depth,
            struct_cache=struct_cache,
        )
        root_struct = _build_element_struct_node(target_elem, tag, ctx)
        return _finish_element_doc(schema, target_elem, tag, root_struct)

    def read_at_level(
        self,
        source: xmlschema.XMLSchema | str | Path,
        level: int,
        *,
        root_tag: str | None = None,
    ) -> dict[str, SchemaDocument]:
        """Read all XML element types at depth ``level`` in an XSD schema into ``SchemaDocument``s.

        :param source: Loaded ``XMLSchema`` instance or filesystem path to an XSD file.
        :type source: xmlschema.XMLSchema | str | Path
        :param level: Non-negative depth relative to the root element(s).
        :type level: int
        :param root_tag: Optional explicit root element name to anchor the traversal.
        :type root_tag: str | None
        :return: Mapping of element local tag name to its ``SchemaDocument``.
        :rtype: dict[str, SchemaDocument]
        """
        schema = load_schema(source) if isinstance(source, str | Path) else source
        elements_at_level = resolve_elements_at_level(schema, level, root_tag=root_tag)
        struct_cache: dict[tuple[int, bool], Any] = {}
        return {
            tag_name: _build_element_table_doc(
                schema,
                target_elem,
                tag_name,
                max_recursion_depth=self.max_recursion_depth,
                struct_cache=struct_cache,
            )
            for tag_name, target_elem in elements_at_level.items()
        }
