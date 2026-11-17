"""Read XML Schema Definition (XSD) documents into the converter IR."""

from collections.abc import Mapping
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any, Final, NoReturn

import xmlschema
from frozendict import frozendict
from xmlschema.validators import (
    XsdComplexType,
    XsdElement,
    XsdType,
)

from cdm_data_loaders.converters.core.errors import ConversionError, raise_spec_error
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
from cdm_data_loaders.utils.recursion import DEFAULT_MAX_RECURSION_DEPTH, validate_max_recursion_depth
from cdm_data_loaders.utils.xml_utils import local_name as _local_name
from cdm_data_loaders.validation.xsd.particles import resolve_child_cardinality
from cdm_data_loaders.validation.xsd.scalars import (
    BUILTIN_LIST_ITEM_TYPES,
    XSD_SCALAR_KINDS,
    XsdScalarKind,
    builtin_xsd_name,
    extract_doc_comment,
    resolve_xsd_scalar_kind,
    unwrap_item_type,
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
)

SCALAR_KIND_TO_IR_SPEC: Final[Mapping[XsdScalarKind, tuple[NodeType, NodeHints]]] = frozendict(
    {
        "string": SPEC_STRING,
        "boolean": SPEC_BOOL,
        "int8": SPEC_INT8,
        "int16": SPEC_INT16,
        "int32": SPEC_INT32,
        "int64": SPEC_INT64,
        "uint8": SPEC_UINT8,
        "uint16": SPEC_UINT16,
        "uint32": SPEC_UINT32,
        "uint64": SPEC_UINT64,
        "float64": SPEC_FLOAT64,
        "date": SPEC_DATE,
        "binary": SPEC_BINARY,
    }
)

XSD_SCALAR_SPEC: Final[Mapping[str, tuple[NodeType, NodeHints]]] = frozendict(
    {xsd_name: SCALAR_KIND_TO_IR_SPEC[kind] for xsd_name, kind in XSD_SCALAR_KINDS.items()}
)


def _string_ir_node(
    path: tuple[str | int, ...],
    *,
    nullable: bool,
    metadata: Mapping[str, Any] | None = None,
) -> TypedNode:
    """Create a string IR ``TypedNode`` with ``source="xsd"`` provenance.

    :param path: IR provenance path.
    :type path: tuple[str | int, ...]
    :param nullable: Whether the node is nullable.
    :type nullable: bool
    :param metadata: Optional provenance metadata mapping.
    :type metadata: Mapping[str, Any] | None
    :return: String IR ``TypedNode``.
    :rtype: TypedNode
    """
    return TypedNode(
        type="string",
        nullable=nullable,
        hints=NodeHints(logical_type="string"),
        provenance=Provenance("xsd", path, metadata or {}),
    )


def _resolve_scalar_ir_spec(simple_type: XsdType) -> tuple[NodeType, NodeHints, str]:
    """Map a non-list XSD simpleType (atomic or union) to ``(NodeType, NodeHints, xsd_type_str)``.

    :param simple_type: atomic or union XSD simpleType
    :type simple_type: XsdType
    :return: tuple of ``(node_type, hints, xsd_type_str)``
    :rtype: tuple[NodeType, NodeHints, str]
    """
    scalar_kind, xsd_type_str = resolve_xsd_scalar_kind(simple_type)
    node_type, hints = SCALAR_KIND_TO_IR_SPEC[scalar_kind]
    return node_type, hints, xsd_type_str


def simple_type_to_ir_node(
    simple_type: XsdType,
    path: tuple[str | int, ...],
    *,
    nullable: bool,
) -> TypedNode:
    """Map any XSD simpleType (atomic, union, or list) to an IR ``TypedNode``.

    :param simple_type: XSD simpleType to lower
    :type simple_type: XsdType
    :param path: IR provenance path
    :type path: tuple[str | int, ...]
    :param nullable: whether the node is nullable
    :type nullable: bool
    :return: lowered IR ``TypedNode``
    :rtype: TypedNode
    """
    builtin_name = builtin_xsd_name(simple_type)
    if builtin_name in BUILTIN_LIST_ITEM_TYPES or simple_type.is_list():
        if builtin_name in BUILTIN_LIST_ITEM_TYPES:
            item_nt, item_hints, item_xsd = *SPEC_STRING, BUILTIN_LIST_ITEM_TYPES[builtin_name]
        else:
            item_type = unwrap_item_type(simple_type)
            item_nt, item_hints, item_xsd = (
                _resolve_scalar_ir_spec(item_type) if item_type is not None else (*SPEC_STRING, "xs:string")
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

    node_type, hints, xsd_name = _resolve_scalar_ir_spec(simple_type)
    return TypedNode(
        type=node_type,
        nullable=nullable,
        hints=hints,
        provenance=Provenance("xsd", path, {"xsd_type": xsd_name}),
    )


def _resolve_non_struct_element_node(
    ctx: TraversalContext,
    elem_type: XsdType | None,
    path: tuple[str | int, ...],
    *,
    nullable: bool,
) -> TypedNode | None:
    """Return an IR ``TypedNode`` if ``elem_type`` maps to a leaf scalar rather than a struct.

    :param ctx: active traversal context
    :type ctx: TraversalContext
    :param elem_type: XSD element type to inspect
    :type elem_type: XsdType | None
    :param path: IR provenance path
    :type path: tuple[str | int, ...]
    :param nullable: whether the leaf node is nullable
    :type nullable: bool
    :return: leaf ``TypedNode``, or ``None`` if ``elem_type`` maps to a struct
    :rtype: TypedNode | None
    """
    leaf_info = classify_leaf_element_type(elem_type, error_type=ctx.error_type, error_by_code=ctx.error_by_code)
    if leaf_info is None:
        return None
    kind, simple_type = leaf_info
    if kind == "simple":
        if simple_type is not None:
            return simple_type_to_ir_node(simple_type, path, nullable=nullable)
        return _string_ir_node(path, nullable=nullable, metadata={"xsd_type": "xs:string"})
    if kind == "mixed":
        return _string_ir_node(path, nullable=nullable, metadata={"xsd_type": "xs:string"})
    return _string_ir_node(path, nullable=nullable)


def _wrap_leaf_ir_field(
    ctx: TraversalContext,
    element: XsdElement,
    path: tuple[str | int, ...],
    *,
    is_list: bool,
    nullable: bool,
) -> SchemaField:
    """Create a non-truncated scalar or list IR ``Field`` for ``element`` using ``path[-1]`` as field name.

    :param ctx: active traversal context
    :type ctx: TraversalContext
    :param element: XSD element declaration
    :type element: XsdElement
    :param path: IR provenance path ending in the emitted field name
    :type path: tuple[str | int, ...]
    :param is_list: whether the field repeats as an array
    :type is_list: bool
    :param nullable: whether the field is nullable
    :type nullable: bool
    :return: lowered IR ``Field``
    :rtype: SchemaField
    """
    field_name = str(path[-1])
    elem_type = element.type
    comment = extract_doc_comment(element, elem_type)
    annotations = {"description": comment} if comment else {}
    leaf_path = (*path, "item") if is_list else path
    leaf_nullable = True if is_list else nullable
    leaf_node = _resolve_non_struct_element_node(ctx, elem_type, leaf_path, nullable=leaf_nullable) or _string_ir_node(
        leaf_path, nullable=leaf_nullable
    )

    if is_list:
        field_node = TypedNode(
            type="array",
            nullable=nullable,
            items=leaf_node,
            provenance=Provenance("xsd", path),
        )
        prov_meta: dict[str, Any] = {}
    else:
        field_node = leaf_node
        prov_meta = dict(leaf_node.provenance.metadata) if leaf_node.provenance is not None else {}

    return SchemaField(
        name=field_name,
        node=field_node,
        required=not nullable,
        annotations=annotations,
        provenance=Provenance("xsd", path, prov_meta),
    )


def _wrap_truncated_leaf_ir_field(
    element: XsdElement,
    path: tuple[str | int, ...],
    *,
    is_list: bool,
    nullable: bool,
) -> SchemaField:
    """Create a recursion-truncated JSON-string scalar or list IR ``Field`` for ``element``.

    :param element: recursive XSD element declaration at the depth cutoff
    :type element: XsdElement
    :param path: IR provenance path ending in the emitted field name
    :type path: tuple[str | int, ...]
    :param is_list: whether the field repeats as an array
    :type is_list: bool
    :param nullable: whether the field is nullable
    :type nullable: bool
    :return: lowered recursion-truncated IR ``Field``
    :rtype: SchemaField
    """
    field_name = str(path[-1])
    elem_type = element.type
    comment = extract_doc_comment(element, elem_type)
    annotations = {"description": comment} if comment else {}
    leaf_path = (*path, "item") if is_list else path
    leaf_nullable = True if is_list else nullable
    trunc_meta = {"truncated_component_id": id(elem_type)} if elem_type is not None else {}

    leaf_node = _string_ir_node(leaf_path, nullable=leaf_nullable, metadata=trunc_meta)
    field_node = (
        TypedNode(
            type="array",
            nullable=nullable,
            items=leaf_node,
            provenance=Provenance("xsd", path, trunc_meta),
        )
        if is_list
        else leaf_node
    )
    return SchemaField(
        name=field_name,
        node=field_node,
        required=not nullable,
        annotations=annotations,
        provenance=Provenance("xsd", path, trunc_meta),
    )


def _build_complex_struct_fields(
    ctx: TraversalContext,
    complex_type: XsdComplexType,
    path: tuple[str | int, ...],
    *,
    parent_nillable: bool,
) -> tuple[SchemaField, ...]:
    """Build the ordered tuple of IR ``Field``s for a struct-mapped ``XsdComplexType``.

    :param ctx: active traversal context
    :type ctx: TraversalContext
    :param complex_type: XSD complex type to lower
    :type complex_type: XsdComplexType
    :param path: IR provenance path
    :type path: tuple[str | int, ...]
    :param parent_nillable: whether an enclosing nillable element forces child fields nullable
    :type parent_nillable: bool
    :return: ordered tuple of lowered IR ``Field``s
    :rtype: tuple[SchemaField, ...]
    """
    fields: list[SchemaField] = []
    attr_field_names: set[str] = set()

    for attr in collect_attributes(complex_type, error_type=ctx.error_type, error_by_code=ctx.error_by_code):
        field_name = f"_{attr.local_name}"
        attr_field_names.add(field_name)
        attr_path = (*path, field_name)
        attr_type = attr.type
        nullable = parent_nillable or getattr(attr, "use", "optional") != "required"
        node = (
            simple_type_to_ir_node(attr_type, attr_path, nullable=nullable)
            if attr_type is not None
            else _string_ir_node(attr_path, nullable=nullable, metadata={"xsd_type": "xs:string"})
        )
        comment = extract_doc_comment(attr, attr_type)
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
        node = (
            simple_type_to_ir_node(content_type, text_path, nullable=True)
            if isinstance(content_type, XsdType)
            else _string_ir_node(text_path, nullable=True, metadata={"xsd_type": "xs:string"})
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

    for child in collect_unique_children(
        ctx.schema,
        complex_type,
        error_type=ctx.error_type,
        error_by_code=ctx.error_by_code,
    ):
        child_name = child.local_name or ""
        if child_name in attr_field_names:
            msg = (
                f"Field name collision in {complex_type.local_name or 'complexType'!r}: "
                f"attribute and child element both map to {child_name!r}"
            )
            ctx.raise_err("E8", msg)

        is_list, nullable = resolve_child_cardinality(model_group, child, parent_nillable=parent_nillable)
        fields.append(_build_element_ir_field(ctx, child, (*path, child_name), is_list=is_list, nullable=nullable))

    return tuple(fields)


def _build_element_ir_field(
    ctx: TraversalContext,
    element: XsdElement,
    path: tuple[str | int, ...],
    *,
    is_list: bool,
    nullable: bool,
) -> SchemaField:
    """Build an IR ``Field`` for a child ``XsdElement`` given its cardinality and nullability.

    :param ctx: active traversal context
    :type ctx: TraversalContext
    :param element: child XSD element declaration
    :type element: XsdElement
    :param path: IR provenance path ending in the child element name
    :type path: tuple[str | int, ...]
    :param is_list: whether the child element repeats as an array
    :type is_list: bool
    :param nullable: whether the child element is nullable
    :type nullable: bool
    :return: lowered IR ``Field``
    :rtype: SchemaField
    """
    elem_type = element.type
    field_name = element.local_name or ""
    comment = extract_doc_comment(element, elem_type)

    if classify_leaf_element_type(elem_type, error_type=ctx.error_type, error_by_code=ctx.error_by_code) is not None:
        return _wrap_leaf_ir_field(ctx, element, path, is_list=is_list, nullable=nullable)

    elem_type = require_complex_type(
        elem_type,
        field_name,
        error_type=ctx.error_type,
        error_by_code=ctx.error_by_code,
    )
    type_id = id(elem_type)
    if type_id in ctx.type_counts:
        check_no_forbidden_types_under_recursion(elem_type, ctx)

    next_count = ctx.type_counts.get(type_id, 0) + 1
    if ctx.max_recursion_depth is not None and next_count > ctx.max_recursion_depth:
        return _wrap_truncated_leaf_ir_field(element, path, is_list=is_list, nullable=nullable)

    cache_key = (type_id, element.nillable)
    is_cacheable = not can_reach_any_type(elem_type, ctx.type_counts)
    cached = ctx.struct_cache.get(cache_key) if is_cacheable else None
    if isinstance(cached, TypedNode):
        struct_node = cached
    else:
        next_ctx = ctx.with_type(type_id)
        struct_fields = _build_complex_struct_fields(next_ctx, elem_type, path, parent_nillable=element.nillable)
        required_names = tuple(f.name for f in struct_fields if f.required)
        struct_meta: dict[str, Any] = {"component_id": type_id, "nillable": bool(element.nillable)}
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

    field_node = (
        TypedNode(
            type="array",
            nullable=nullable,
            items=replace(struct_node, nullable=True),
            provenance=Provenance("xsd", path),
        )
        if is_list
        else replace(struct_node, nullable=nullable)
    )
    annotations = {"description": comment} if comment else {}
    return SchemaField(
        name=field_name,
        node=field_node,
        required=not nullable,
        annotations=annotations,
        provenance=Provenance("xsd", path),
    )


def _build_element_struct_node(
    target_elem: XsdElement,
    xml_tag: str,
    ctx: TraversalContext,
) -> TypedNode:
    """Validate that ``target_elem`` maps to a struct and return its root object ``TypedNode``.

    :param target_elem: resolved root/target ``XsdElement``
    :type target_elem: XsdElement
    :param xml_tag: XML element tag name for error diagnostics
    :type xml_tag: str
    :param ctx: active traversal context
    :type ctx: TraversalContext
    :return: root object ``TypedNode``
    :rtype: TypedNode
    """
    elem_type = require_complex_type(
        target_elem.type,
        xml_tag,
        error_type=ctx.error_type,
        error_by_code=ctx.error_by_code,
    )
    if is_empty_marker_complex_type(elem_type):
        msg = f"Top-level xml_tag {xml_tag!r} has an empty complex type with no attributes or child elements"
        ctx.raise_err("E3", msg)

    attrs = collect_attributes(elem_type, error_type=ctx.error_type, error_by_code=ctx.error_by_code)
    if (elem_type.has_simple_content() or elem_type.mixed) and not attrs:
        msg = f"Top-level xml_tag {xml_tag!r} must have a complex type that resolves to a struct"
        ctx.raise_err("E3", msg)

    fields = _build_complex_struct_fields(ctx, elem_type, (), parent_nillable=target_elem.nillable)
    if not fields:
        msg = f"Top-level xml_tag {xml_tag!r} must have a complex type with at least one field"
        ctx.raise_err("E3", msg)

    required_names = tuple(f.name for f in fields if f.required)
    struct_meta: dict[str, Any] = {"component_id": id(elem_type), "nillable": bool(target_elem.nillable)}
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


def _finish_element_doc(
    schema: xmlschema.XMLSchema,
    target_elem: XsdElement,
    tag_name: str,
    root_node: TypedNode,
) -> SchemaDocument:
    """Attach element documentation annotations to ``root_node`` and wrap in a ``SchemaDocument``.

    :param schema: loaded ``XMLSchema`` instance
    :type schema: xmlschema.XMLSchema
    :param target_elem: resolved ``XsdElement``
    :type target_elem: XsdElement
    :param tag_name: element tag name
    :type tag_name: str
    :param root_node: lowered root ``TypedNode``
    :type root_node: TypedNode
    :return: wrapped ``SchemaDocument``
    :rtype: SchemaDocument
    """
    comment = extract_doc_comment(target_elem, target_elem.type)
    annotations = {"description": comment} if comment is not None else {}
    return SchemaDocument(
        root=replace(root_node, annotations=annotations),
        name=_local_name(tag_name),
        identifier=schema.target_namespace or None,
        annotations=annotations,
        provenance=Provenance("xsd", (tag_name,)),
    )


def _build_element_table_doc(
    base_ctx: TraversalContext,
    target_elem: XsdElement,
    tag_name: str,
) -> SchemaDocument:
    """Build a ``SchemaDocument`` for ``target_elem`` (struct or single-column ``#text`` scalar).

    :param base_ctx: base traversal context carrying ``schema``, ``struct_cache``, and error handlers
    :type base_ctx: TraversalContext
    :param target_elem: resolved ``XsdElement`` at the target level
    :type target_elem: XsdElement
    :param tag_name: element local tag name
    :type tag_name: str
    :return: lowered ``SchemaDocument``
    :rtype: SchemaDocument
    """
    if isinstance(target_elem.type, XsdComplexType) and is_empty_marker_complex_type(target_elem.type):
        msg = f"Top-level xml_tag {tag_name!r} has an empty complex type with no attributes or child elements"
        base_ctx.raise_err("E3", msg)

    if (
        classify_leaf_element_type(
            target_elem.type,
            error_type=base_ctx.error_type,
            error_by_code=base_ctx.error_by_code,
        )
        is not None
    ):
        text_field = _wrap_leaf_ir_field(base_ctx, target_elem, ("#text",), is_list=False, nullable=True)
        root_node = TypedNode(
            type="object",
            nullable=False,
            properties=(text_field,),
            required_names=(),
            additional_properties=TypedNode(type="never", nullable=False),
            provenance=Provenance("xsd", (), {"scalar_leaf_table": True}),
        )
    else:
        elem_type = require_complex_type(
            target_elem.type,
            tag_name,
            error_type=base_ctx.error_type,
            error_by_code=base_ctx.error_by_code,
        )
        ctx = replace(base_ctx, type_counts={id(elem_type): 1})
        root_node = _build_element_struct_node(target_elem, tag_name, ctx)

    return _finish_element_doc(base_ctx.schema, target_elem, tag_name, root_node)


@dataclass(frozen=True, slots=True)
class XsdReader:
    """Build typed converter IR documents from XML Schema Definition (XSD) files.

    :param xml_tag: Default target XML element tag name when not supplied to :meth:`read`.
    :type xml_tag: str | None
    :param max_recursion_depth: Maximum times a recursive ``XsdComplexType`` may be expanded along an ancestor path.
    :type max_recursion_depth: int
    :param error_type: Base exception class raised when a construct cannot be converted.
    :type error_type: type[ConversionError]
    :param error_by_code: Mapping from specification error code (``E1``-``E9``) to exception class.
    :type error_by_code: Mapping[str, type[ConversionError]]
    """

    xml_tag: str | None = None
    max_recursion_depth: int = DEFAULT_MAX_RECURSION_DEPTH
    error_type: type[ConversionError] = ConversionError
    error_by_code: Mapping[str, type[ConversionError]] = field(default_factory=frozendict)

    def __post_init__(self) -> None:
        """Validate reader configuration."""
        validate_max_recursion_depth(self.max_recursion_depth)

    def _raise_err(self, code: str, msg: str) -> NoReturn:
        """Raise the configured exception subclass for ``code`` or fall back to ``error_type``.

        :param code: Specification error code (``E1``-``E9``).
        :type code: str
        :param msg: Diagnostic error message.
        :type msg: str
        :raises ConversionError: Always raised with the resolved error class.
        """
        return raise_spec_error(code, msg, error_type=self.error_type, error_by_code=self.error_by_code)

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
        :raises ConversionError: If the schema or ``xml_tag`` violates ``E1``-``E9``.
        """
        schema = load_schema(source) if isinstance(source, str | Path) else source
        tag = xml_tag if xml_tag is not None else self.xml_tag
        if tag is None:
            roots = resolve_root_elements(schema, error_type=self.error_type, error_by_code=self.error_by_code)
            if len(roots) != 1 or roots[0].local_name is None:
                msg = "xml_tag must be specified when the XSD schema does not have a single root element"
                self._raise_err("E1", msg)
            tag = roots[0].local_name

        target_elem, _, ctx = prepare_target_traversal(
            schema,
            tag,
            max_recursion_depth=self.max_recursion_depth,
            error_type=self.error_type,
            error_by_code=self.error_by_code,
        )
        if struct_cache is not None:
            ctx = replace(ctx, struct_cache=struct_cache)
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
        elements_at_level = resolve_elements_at_level(
            schema,
            level,
            root_tag=root_tag,
            error_type=self.error_type,
            error_by_code=self.error_by_code,
        )
        base_ctx = TraversalContext(
            schema=schema,
            type_counts={},
            max_recursion_depth=self.max_recursion_depth,
            struct_cache={},
            error_type=self.error_type,
            error_by_code=self.error_by_code,
        )
        return {
            tag_name: _build_element_table_doc(base_ctx, target_elem, tag_name)
            for tag_name, target_elem in elements_at_level.items()
        }
