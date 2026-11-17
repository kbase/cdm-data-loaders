"""IR and PyArrow component traversal and struct deduplication for Python schema codegen."""

from collections import Counter

import pyarrow as pa

from cdm_data_loaders.converters.core.ir import SchemaDocument, TypedNode
from cdm_data_loaders.converters.emitters.pyarrow_codegen.formatting import (
    field_spec_from_pa,
    render_field_call,
    render_struct_expr,
    render_type_expr,
)
from cdm_data_loaders.converters.emitters.pyarrow_codegen.naming import (
    ComponentKey,
    RenderedFieldSpec,
    StructComponentSpec,
    to_upper_snake,
)


def extract_component_meta(node: TypedNode) -> tuple[ComponentKey, str | None, bool]:
    """Extract component identity, local type name, and inline flag from ``node.provenance``.

    :param node: Object IR node representing a struct component.
    :type node: TypedNode
    :return: Tuple of ``((component_id, nillable), type_name, is_inline)``.
    :rtype: tuple[ComponentKey, str | None, bool]
    """
    meta = node.provenance.metadata if node.provenance is not None else {}
    comp_id = int(meta.get("component_id", id(node)))
    nillable = bool(meta.get("nillable", False))
    type_name = str(meta["type_name"]) if isinstance(meta.get("type_name"), str) else None
    is_inline = bool(meta.get("inline_struct", False))
    return (comp_id, nillable), type_name, is_inline


def unwrap_object_node(node: TypedNode | None) -> TypedNode | None:
    """Resolve ``node`` (or its first union branch) to an object ``TypedNode`` when applicable.

    :param node: Candidate IR node.
    :type node: TypedNode | None
    :return: Resolved object ``TypedNode`` or ``None``.
    :rtype: TypedNode | None
    """
    if node is None:
        return None
    if node.type == "object":
        return node
    for branches in (node.one_of, node.any_of):
        if branches:
            return unwrap_object_node(branches[0])
    return None


def unwrap_array_element_node(node: TypedNode | None) -> TypedNode | None:
    """Resolve the homogeneous element ``TypedNode`` of an array ``TypedNode``.

    :param node: Candidate array IR node.
    :type node: TypedNode | None
    :return: Element ``TypedNode`` or ``None``.
    :rtype: TypedNode | None
    """
    if node is None:
        return None
    if node.prefix_items:
        return node.prefix_items[0]
    if isinstance(node.items, tuple):
        return node.items[0] if node.items else None
    return node.items


def unwrap_map_value_node(node: TypedNode | None) -> TypedNode | None:
    """Resolve the value ``TypedNode`` of a map or dynamic-property object ``TypedNode``.

    :param node: Candidate map or object IR node.
    :type node: TypedNode | None
    :return: Value ``TypedNode`` or ``None``.
    :rtype: TypedNode | None
    """
    if node is None:
        return None
    if node.value_type is not None:
        return node.value_type
    if node.pattern_properties:
        return next(iter(node.pattern_properties.values()))
    if node.additional_properties is not None and node.additional_properties.type not in {"any", "never"}:
        return node.additional_properties
    return None


class IRComponentCollector:
    """Traverse IR ``SchemaDocument`` trees alongside emitted PyArrow types to collect reusable struct components."""

    def __init__(self) -> None:
        """Initialize the IR component collector."""
        self.components: dict[ComponentKey, StructComponentSpec] = {}
        self.order: list[ComponentKey] = []
        self.el_name_types: dict[str, set[int]] = {}

    def _scan_ir_node(self, node: TypedNode, el_name: str | None, visited: set[int]) -> None:
        """Record distinct component IDs associated with each element or field name.

        :param node: IR node to scan.
        :type node: TypedNode
        :param el_name: Element or field name associated with ``node``.
        :type el_name: str | None
        :param visited: Set of visited component IDs.
        :type visited: set[int]
        """
        prov_meta = node.provenance.metadata if node.provenance is not None else {}
        if "truncated_component_id" in prov_meta:
            trunc_id = int(prov_meta["truncated_component_id"])
            if el_name:
                self.el_name_types.setdefault(el_name, set()).add(trunc_id)
            return
        if node.type == "array":
            elem = unwrap_array_element_node(node)
            if elem is not None:
                self._scan_ir_node(elem, el_name, visited)
            return
        if node.type == "map":
            val = unwrap_map_value_node(node)
            if val is not None:
                self._scan_ir_node(val, el_name, visited)
            return
        obj_node = unwrap_object_node(node)
        if obj_node is None or prov_meta.get("scalar_leaf_table") or not obj_node.properties:
            return
        obj_meta = obj_node.provenance.metadata if obj_node.provenance is not None else {}
        comp_id = int(obj_meta.get("component_id", id(obj_node)))
        if el_name:
            self.el_name_types.setdefault(el_name, set()).add(comp_id)
        if comp_id in visited:
            return
        visited.add(comp_id)
        for child_field in obj_node.properties:
            self._scan_ir_node(child_field.node, child_field.name, visited)

    def register_document(self, tag_name: str, doc: SchemaDocument) -> None:
        """Pre-scan ``doc`` to count distinct struct components sharing each element or field name.

        :param tag_name: Table or root element name.
        :type tag_name: str
        :param doc: IR schema document to scan.
        :type doc: SchemaDocument
        """
        self._scan_ir_node(doc.root, tag_name, set())

    def assign_component_names(self) -> dict[ComponentKey, str]:
        """Assign unique ``*_STRUCT`` constant names to all collected components in post-order.

        :return: Mapping from ``ComponentKey`` to Python constant name.
        :rtype: dict[ComponentKey, str]
        """
        names: dict[ComponentKey, str] = {}
        used: set[str] = set()
        type_id_nillables: Counter[int] = Counter(tid for tid, _ in self.order)

        for key in self.order:
            tid, nillable = key
            spec = self.components[key]
            if spec.type_local_name is not None:
                base = to_upper_snake(spec.type_local_name, strip_type_suffix=True)
            elif len(self.el_name_types.get(spec.el_name, ())) > 1 and spec.parent_el_name:
                base = f"{to_upper_snake(spec.parent_el_name)}_{to_upper_snake(spec.el_name)}"
            else:
                base = to_upper_snake(spec.el_name)

            if nillable and type_id_nillables[tid] > 1:
                base = f"{base}_NILLABLE"

            candidate = f"{base}_STRUCT"
            if candidate in used and spec.parent_el_name:
                candidate = f"{to_upper_snake(spec.parent_el_name)}_{candidate}"
            suffix = 2
            orig_candidate = candidate
            while candidate in used:
                candidate = f"{orig_candidate[:-7]}_{suffix}_STRUCT"
                suffix += 1

            used.add(candidate)
            names[key] = candidate

        return names

    def collect_document(
        self,
        tag_name: str,
        doc: SchemaDocument,
        pa_schema: pa.Schema,
    ) -> ComponentKey | RenderedFieldSpec:
        """Collect components for ``doc`` and return its root ``ComponentKey`` or scalar ``#text`` field spec.

        :param tag_name: Table or root element name.
        :type tag_name: str
        :param doc: IR schema document.
        :type doc: SchemaDocument
        :param pa_schema: Emitted PyArrow schema for ``doc``.
        :type pa_schema: pa.Schema
        :return: ``ComponentKey`` for struct roots, or ``RenderedFieldSpec`` for scalar leaf tables.
        :rtype: ComponentKey | RenderedFieldSpec
        """
        root_meta = doc.root.provenance.metadata if doc.root.provenance is not None else {}
        if root_meta.get("scalar_leaf_table"):
            f0 = pa_schema.field(0)
            return field_spec_from_pa(f0, render_type_expr(f0.type))

        key, type_name, _ = extract_component_meta(doc.root)
        pa_struct = pa.struct(list(pa_schema))
        if key not in self.components:
            fields = self._build_struct_fields(doc.root, pa_struct, parent_el_name=tag_name)
            self.components[key] = StructComponentSpec(
                key=key,
                el_name=tag_name,
                parent_el_name=None,
                type_local_name=type_name,
                fields=tuple(fields),
                pa_struct=pa_struct,
            )
            self.order.append(key)
        return key

    def _build_struct_fields(
        self,
        obj_node: TypedNode,
        pa_struct: pa.StructType,
        *,
        parent_el_name: str,
    ) -> list[RenderedFieldSpec]:
        """Build field rendering specifications for ``obj_node`` and ``pa_struct``.

        :param obj_node: Object IR node corresponding to ``pa_struct``.
        :type obj_node: TypedNode
        :param pa_struct: Emitted PyArrow struct type.
        :type pa_struct: pa.StructType
        :param parent_el_name: Name of the enclosing element or field.
        :type parent_el_name: str
        :return: List of rendered field specifications.
        :rtype: list[RenderedFieldSpec]
        """
        specs: list[RenderedFieldSpec] = []
        ir_props = obj_node.properties
        for idx, pa_field in enumerate(pa_struct):
            ir_field = ir_props[idx] if idx < len(ir_props) else None
            ir_child_node = ir_field.node if ir_field is not None else None
            field_name = pa_field.name

            if isinstance(pa_field.type, pa.StructType):
                child_obj = unwrap_object_node(ir_child_node)
                struct_expr, inline_tuple = self._collect_struct_node(
                    child_obj,
                    pa_field.type,
                    el_name=field_name,
                    parent_el_name=parent_el_name,
                )
                specs.append(
                    field_spec_from_pa(
                        pa_field,
                        struct_expr,
                        inline_struct_fields=inline_tuple,
                        is_list=False,
                    )
                )
            elif isinstance(pa_field.type, pa.ListType) and isinstance(pa_field.type.value_field.type, pa.StructType):
                item_pa_field = pa_field.type.value_field
                elem_ir = unwrap_array_element_node(ir_child_node)
                child_obj = unwrap_object_node(elem_ir)
                struct_expr, inline_tuple = self._collect_struct_node(
                    child_obj,
                    item_pa_field.type,
                    el_name=field_name,
                    parent_el_name=parent_el_name,
                )
                item_spec = field_spec_from_pa(item_pa_field, struct_expr)
                specs.append(
                    field_spec_from_pa(
                        pa_field,
                        f"pa.list_({render_field_call(item_spec)})",
                        inline_struct_fields=inline_tuple,
                        is_list=True,
                        list_item_nullable=item_pa_field.nullable,
                    )
                )
            else:
                type_expr = self._render_nested_type_expr(
                    ir_child_node,
                    pa_field.type,
                    el_name=field_name,
                    parent_el_name=parent_el_name,
                )
                specs.append(field_spec_from_pa(pa_field, type_expr))
        return specs

    def _collect_struct_node(
        self,
        child_obj: TypedNode | None,
        pa_struct: pa.StructType,
        *,
        el_name: str,
        parent_el_name: str,
    ) -> tuple[str, tuple[RenderedFieldSpec, ...] | None]:
        """Collect a child struct component or render it inline when recursive.

        :param child_obj: Child object IR node, if available.
        :type child_obj: TypedNode | None
        :param pa_struct: Emitted PyArrow struct type.
        :type pa_struct: pa.StructType
        :param el_name: Element or field name for the struct.
        :type el_name: str
        :param parent_el_name: Name of the enclosing element or field.
        :type parent_el_name: str
        :return: Tuple of ``(type_expr, inline_struct_fields)``.
        :rtype: tuple[str, tuple[RenderedFieldSpec, ...] | None]
        """
        if child_obj is None or len(pa_struct) == 0:
            return render_type_expr(pa_struct), None

        source = child_obj.provenance.source if child_obj.provenance is not None else None
        cache_key, type_name, is_inline = extract_component_meta(child_obj)

        if (
            not is_inline
            and source != "xsd"
            and cache_key in self.components
            and self.components[cache_key].pa_struct != pa_struct
        ):
            is_inline = True

        if not is_inline:
            if cache_key not in self.components:
                child_fields = self._build_struct_fields(
                    child_obj,
                    pa_struct,
                    parent_el_name=el_name,
                )
                self.components[cache_key] = StructComponentSpec(
                    key=cache_key,
                    el_name=el_name,
                    parent_el_name=parent_el_name,
                    type_local_name=type_name,
                    fields=tuple(child_fields),
                    pa_struct=pa_struct,
                )
                self.order.append(cache_key)
            return f"__COMPONENT_{cache_key[0]}_{int(cache_key[1])}__", None

        inline_fields = self._build_struct_fields(
            child_obj,
            pa_struct,
            parent_el_name=el_name,
        )
        return render_struct_expr(inline_fields), tuple(inline_fields)

    def _render_nested_type_expr(
        self,
        ir_node: TypedNode | None,
        dtype: pa.DataType,
        *,
        el_name: str,
        parent_el_name: str,
    ) -> str:
        """Render a scalar, list, or map ``pa.DataType``, registering any nested struct components.

        :param ir_node: Corresponding IR node, if available.
        :type ir_node: TypedNode | None
        :param dtype: Emitted PyArrow data type.
        :type dtype: pa.DataType
        :param el_name: Element or field name.
        :type el_name: str
        :param parent_el_name: Name of the enclosing element or field.
        :type parent_el_name: str
        :return: Rendered Python constructor expression string.
        :rtype: str
        """
        if isinstance(dtype, pa.StructType):
            child_obj = unwrap_object_node(ir_node)
            struct_expr, _ = self._collect_struct_node(
                child_obj,
                dtype,
                el_name=el_name,
                parent_el_name=parent_el_name,
            )
            return struct_expr
        if isinstance(dtype, pa.ListType):
            item_field = dtype.value_field
            elem_ir = unwrap_array_element_node(ir_node)
            item_type_expr = self._render_nested_type_expr(
                elem_ir,
                item_field.type,
                el_name=el_name,
                parent_el_name=parent_el_name,
            )
            item_spec = field_spec_from_pa(item_field, item_type_expr)
            return f"pa.list_({render_field_call(item_spec)})"
        if isinstance(dtype, pa.MapType):
            key_expr = render_type_expr(dtype.key_field.type)
            val_field = dtype.item_field
            val_ir = unwrap_map_value_node(ir_node)
            val_type_expr = self._render_nested_type_expr(
                val_ir,
                val_field.type,
                el_name=el_name,
                parent_el_name=parent_el_name,
            )
            val_spec = field_spec_from_pa(val_field, val_type_expr)
            return f"pa.map_({key_expr}, {render_field_call(val_spec)})"
        return render_type_expr(dtype)
