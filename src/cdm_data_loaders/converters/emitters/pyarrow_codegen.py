"""Render static Python schema modules from converter IR documents and PyArrow types."""

import re
from collections import Counter
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import TYPE_CHECKING, Any, Final

import pyarrow as pa
from frozendict import frozendict
from pydantic import BaseModel, ConfigDict

from cdm_data_loaders.converters.core.ir import SchemaDocument, TypedNode

if TYPE_CHECKING:
    from cdm_data_loaders.converters.emitters.pyarrow import PyArrowEmitter

MAX_LINE_LENGTH: Final[int] = 120
TYPE_SUFFIX: Final[str] = "Type"

PA_SCALAR_EXPR: Final[Mapping[pa.DataType, str]] = frozendict(
    {
        pa.null(): "pa.null()",
        pa.string(): "pa.string()",
        pa.bool_(): "pa.bool_()",
        pa.int8(): "pa.int8()",
        pa.int16(): "pa.int16()",
        pa.int32(): "pa.int32()",
        pa.int64(): "pa.int64()",
        pa.uint8(): "pa.uint8()",
        pa.uint16(): "pa.uint16()",
        pa.uint32(): "pa.uint32()",
        pa.uint64(): "pa.uint64()",
        pa.float32(): "pa.float32()",
        pa.float64(): "pa.float64()",
        pa.date32(): "pa.date32()",
        pa.binary(): "pa.binary()",
    }
)

type ComponentKey = tuple[int, bool]


class _RenderedFieldSpec(BaseModel):
    """Specification for rendering a single ``pa.field(...)`` call."""

    model_config = ConfigDict(frozen=True)

    name: str
    type_expr: str
    nullable: bool
    metadata: Mapping[str, str] | None
    inline_struct_fields: tuple["_RenderedFieldSpec", ...] | None = None
    is_list: bool = False
    list_item_nullable: bool = True


class _StructComponentSpec(BaseModel):
    """Specification for a reusable top-level ``pa.StructType`` constant."""

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    key: ComponentKey
    el_name: str
    parent_el_name: str | None
    type_local_name: str | None
    fields: tuple[_RenderedFieldSpec, ...]
    pa_struct: Any = None


def _fmt_str(val: str) -> str:
    """Format a Python string literal using Ruff double-quote style.

    :param val: Raw string value.
    :type val: str
    :return: Quoted Python string literal.
    :rtype: str
    """
    if '"' in val:
        return repr(val)
    escaped = val.replace("\\", "\\\\").replace("\n", "\\n").replace("\r", "\\r").replace("\t", "\\t")
    return f'"{escaped}"'


def _to_upper_snake(name: str, *, strip_type_suffix: bool = False) -> str:
    """Convert a camelCase, PascalCase, or snake_case identifier to ``UPPER_SNAKE_CASE``.

    :param name: Raw type, model, or field name.
    :type name: str
    :param strip_type_suffix: Whether to strip a trailing ``Type`` suffix before conversion.
    :type strip_type_suffix: bool
    :return: ``UPPER_SNAKE_CASE`` identifier stem.
    :rtype: str
    """
    if strip_type_suffix and name.endswith(TYPE_SUFFIX) and len(name) > len(TYPE_SUFFIX):
        name = name[: -len(TYPE_SUFFIX)]
    step1 = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", name)
    step2 = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", step1)
    stem = re.sub(r"[^A-Za-z0-9]+", "_", step2).upper().strip("_")
    if not stem:
        return "FIELD"
    return f"F_{stem}" if stem[0].isdigit() else stem


def _decode_metadata(raw_meta: Mapping[str | bytes, str | bytes] | None) -> dict[str, str] | None:
    """Convert a PyArrow metadata mapping into a sorted ``str`` dictionary.

    :param raw_meta: Raw metadata dictionary with ``str`` or ``bytes`` keys and values.
    :type raw_meta: Mapping[str | bytes, str | bytes] | None
    :return: Sorted string dictionary, or ``None`` if ``raw_meta`` is empty.
    :rtype: dict[str, str] | None
    """
    if not raw_meta:
        return None
    decoded = {
        (k.decode("utf-8") if isinstance(k, bytes) else k): (v.decode("utf-8") if isinstance(v, bytes) else v)
        for k, v in raw_meta.items()
    }
    return dict(sorted(decoded.items()))


def _render_parametric_scalar_expr(dtype: pa.DataType) -> str:
    """Render a parametric PyArrow scalar ``DataType`` as a Python constructor expression.

    :param dtype: Parametric PyArrow scalar data type.
    :type dtype: pa.DataType
    :return: Python constructor expression string.
    :rtype: str
    :raises KeyError: If ``dtype`` is not a supported parametric scalar type.
    """
    if isinstance(dtype, pa.Decimal128Type | pa.Decimal256Type):
        fn = "decimal128" if isinstance(dtype, pa.Decimal128Type) else "decimal256"
        return f"pa.{fn}({dtype.precision}, {dtype.scale})"
    if isinstance(dtype, pa.TimestampType):
        return (
            f"pa.timestamp({_fmt_str(dtype.unit)}, tz={_fmt_str(dtype.tz)})"
            if dtype.tz is not None
            else f"pa.timestamp({_fmt_str(dtype.unit)})"
        )
    if isinstance(dtype, pa.Time64Type | pa.Time32Type):
        fn = "time64" if isinstance(dtype, pa.Time64Type) else "time32"
        return f"pa.{fn}({_fmt_str(dtype.unit)})"
    if isinstance(dtype, pa.FixedSizeBinaryType):
        return f"pa.binary({dtype.byte_width})"
    raise KeyError(dtype)


def _render_type_expr(dtype: pa.DataType) -> str:
    """Render a PyArrow ``DataType`` as a Python constructor expression.

    :param dtype: PyArrow data type.
    :type dtype: pa.DataType
    :return: Python constructor expression string.
    :rtype: str
    """
    if isinstance(dtype, pa.ListType):
        item_field = dtype.value_field
        item_expr = _render_field_call(
            _RenderedFieldSpec(
                name=item_field.name,
                type_expr=_render_type_expr(item_field.type),
                nullable=item_field.nullable,
                metadata=_decode_metadata(item_field.metadata),
            )
        )
        return f"pa.list_({item_expr})"
    if isinstance(dtype, pa.MapType):
        key_expr = _render_type_expr(dtype.key_field.type)
        val_field = dtype.item_field
        val_expr = _render_field_call(
            _RenderedFieldSpec(
                name=val_field.name,
                type_expr=_render_type_expr(val_field.type),
                nullable=val_field.nullable,
                metadata=_decode_metadata(val_field.metadata),
            )
        )
        return f"pa.map_({key_expr}, {val_expr})"
    if isinstance(dtype, pa.StructType):
        struct_fields = [
            _RenderedFieldSpec(
                name=f.name,
                type_expr=_render_type_expr(f.type),
                nullable=f.nullable,
                metadata=_decode_metadata(f.metadata),
            )
            for f in dtype
        ]
        return _render_struct_expr(struct_fields)
    if dtype in PA_SCALAR_EXPR:
        return PA_SCALAR_EXPR[dtype]
    return _render_parametric_scalar_expr(dtype)


def _format_metadata_single(metadata: Mapping[str, str]) -> str:
    """Format a metadata dictionary on a single line.

    :param metadata: Sorted string key/value metadata mapping.
    :type metadata: Mapping[str, str]
    :return: Single-line Python dictionary literal.
    :rtype: str
    """
    meta_items = ", ".join(f"{_fmt_str(k)}: {_fmt_str(v)}" for k, v in sorted(metadata.items()))
    return f"{{{meta_items}}}"


def _format_field_args_single(spec: _RenderedFieldSpec) -> str:
    """Format the comma-separated arguments of a ``pa.field(...)`` call on a single line.

    :param spec: Field rendering specification.
    :type spec: _RenderedFieldSpec
    :return: Comma-separated argument list string.
    :rtype: str
    """
    args = [_fmt_str(spec.name), spec.type_expr, f"nullable={spec.nullable!r}"]
    if spec.metadata:
        args.append(f"metadata={_format_metadata_single(spec.metadata)}")
    return ", ".join(args)


def _render_field_call(spec: _RenderedFieldSpec) -> str:
    """Render a single-line ``pa.field(...)`` expression from ``spec``.

    :param spec: Field rendering specification.
    :type spec: _RenderedFieldSpec
    :return: Single-line ``pa.field(...)`` call string.
    :rtype: str
    """
    return f"pa.field({_format_field_args_single(spec)})"


def _render_struct_expr(fields: Sequence[_RenderedFieldSpec]) -> str:
    """Render a single-line ``pa.struct([...])`` expression from ``fields``.

    :param fields: Sequence of field rendering specifications.
    :type fields: Sequence[_RenderedFieldSpec]
    :return: Single-line ``pa.struct([...])`` call string.
    :rtype: str
    """
    inner = ", ".join(_render_field_call(f) for f in fields)
    return f"pa.struct([{inner}])"


def _format_type_arg_indented(spec: _RenderedFieldSpec, *, arg_indent: int) -> str:
    """Format the ``type_expr`` argument of a multi-line ``pa.field(...)`` call.

    :param spec: Field rendering specification.
    :type spec: _RenderedFieldSpec
    :param arg_indent: Indentation of the ``pa.field`` arguments in spaces.
    :type arg_indent: int
    :return: Formatted type argument line(s) with trailing comma.
    :rtype: str
    """
    arg_pad = " " * arg_indent
    single = f"{arg_pad}{spec.type_expr},"
    if len(single) <= MAX_LINE_LENGTH or spec.inline_struct_fields is None:
        return single

    if spec.is_list:
        inner_body = "\n".join(
            _format_field_indented(f, indent=arg_indent + 16, trailing_comma=True) for f in spec.inline_struct_fields
        )
        return "\n".join(
            [
                f"{arg_pad}pa.list_(",
                f"{arg_pad}    pa.field(",
                f'{arg_pad}        "item",',
                f"{arg_pad}        pa.struct(",
                f"{arg_pad}            [",
                inner_body,
                f"{arg_pad}            ]",
                f"{arg_pad}        ),",
                f"{arg_pad}        nullable={spec.list_item_nullable!r},",
                f"{arg_pad}    )",
                f"{arg_pad}),",
            ]
        )

    inner_body = "\n".join(
        _format_field_indented(f, indent=arg_indent + 8, trailing_comma=True) for f in spec.inline_struct_fields
    )
    return "\n".join(
        [
            f"{arg_pad}pa.struct(",
            f"{arg_pad}    [",
            inner_body,
            f"{arg_pad}    ]",
            f"{arg_pad}),",
        ]
    )


def _format_field_indented(spec: _RenderedFieldSpec, *, indent: int, trailing_comma: bool) -> str:
    """Format a ``pa.field(...)`` call at ``indent`` spaces wrapped to ``MAX_LINE_LENGTH``.

    :param spec: Field rendering specification.
    :type spec: _RenderedFieldSpec
    :param indent: Leading indentation in spaces.
    :type indent: int
    :param trailing_comma: Whether to append a trailing comma after the closing parenthesis.
    :type trailing_comma: bool
    :return: Formatted ``pa.field(...)`` block.
    :rtype: str
    """
    comma = "," if trailing_comma else ""
    pad = " " * indent
    single = f"{pad}{_render_field_call(spec)}{comma}"
    if len(single) <= MAX_LINE_LENGTH:
        return single

    arg_pad = " " * (indent + 4)
    inner_single = f"{arg_pad}{_format_field_args_single(spec)}"
    if len(inner_single) <= MAX_LINE_LENGTH:
        return f"{pad}pa.field(\n{inner_single}\n{pad}){comma}"

    lines = [
        f"{pad}pa.field(",
        f"{arg_pad}{_fmt_str(spec.name)},",
        _format_type_arg_indented(spec, arg_indent=indent + 4),
        f"{arg_pad}nullable={spec.nullable!r},",
    ]
    if spec.metadata:
        meta_single = f"{arg_pad}metadata={_format_metadata_single(spec.metadata)},"
        if len(meta_single) <= MAX_LINE_LENGTH:
            lines.append(meta_single)
        elif len(spec.metadata) == 1:
            k, v = next(iter(spec.metadata.items()))
            lines.append(f"{arg_pad}metadata={{")
            lines.append(f"{arg_pad}    {_fmt_str(k)}: {_fmt_str(v)}")
            lines.append(f"{arg_pad}}},")
        else:
            lines.append(f"{arg_pad}metadata={{")
            for k, v in sorted(spec.metadata.items()):
                lines.append(f"{arg_pad}    {_fmt_str(k)}: {_fmt_str(v)},")
            lines.append(f"{arg_pad}}},")
    lines.append(f"{pad}){comma}")
    return "\n".join(lines)


def _format_struct_assignment(const_name: str, fields: Sequence[_RenderedFieldSpec]) -> str:
    """Format a top-level ``<NAME>_STRUCT: Final[pa.StructType] = pa.struct([...])`` assignment.

    :param const_name: Constant identifier for the struct component.
    :type const_name: str
    :param fields: Sequence of field rendering specifications.
    :type fields: Sequence[_RenderedFieldSpec]
    :return: Formatted Python assignment statement.
    :rtype: str
    """
    lhs = f"{const_name}: Final[pa.StructType] = "
    single = f"{lhs}{_render_struct_expr(fields)}"
    if len(single) <= MAX_LINE_LENGTH:
        return single

    if len(fields) == 1:
        one_single = f"    [{_render_field_call(fields[0])}]"
        if len(one_single) <= MAX_LINE_LENGTH:
            return f"{lhs}pa.struct(\n{one_single}\n)"
        body = _format_field_indented(fields[0], indent=8, trailing_comma=False)
        return f"{lhs}pa.struct(\n    [\n{body}\n    ]\n)"

    body = "\n".join(_format_field_indented(f, indent=8, trailing_comma=True) for f in fields)
    return f"{lhs}pa.struct(\n    [\n{body}\n    ]\n)"


def _replace_placeholders(expr: str, component_names: Mapping[ComponentKey, str]) -> str:
    """Replace component placeholders with their assigned Python constant names.

    :param expr: Expression containing ``__COMPONENT_<id>_<nillable>__`` placeholders.
    :type expr: str
    :param component_names: Mapping from ``ComponentKey`` to assigned constant name.
    :type component_names: Mapping[ComponentKey, str]
    :return: Expression with all placeholders replaced.
    :rtype: str
    """
    for (tid, nillable), const_name in component_names.items():
        token = f"__COMPONENT_{tid}_{int(nillable)}__"
        if token in expr:
            expr = expr.replace(token, const_name)
    return expr


def _resolve_field_placeholders(
    spec: _RenderedFieldSpec,
    component_names: Mapping[ComponentKey, str],
) -> _RenderedFieldSpec:
    """Resolve component placeholders in ``spec`` and any nested ``inline_struct_fields``.

    :param spec: Field rendering specification with placeholder tokens.
    :type spec: _RenderedFieldSpec
    :param component_names: Mapping from ``ComponentKey`` to assigned constant name.
    :type component_names: Mapping[ComponentKey, str]
    :return: Field rendering specification with resolved constant names.
    :rtype: _RenderedFieldSpec
    """
    resolved_inline = (
        tuple(_resolve_field_placeholders(inner, component_names) for inner in spec.inline_struct_fields)
        if spec.inline_struct_fields is not None
        else None
    )
    return _RenderedFieldSpec(
        name=spec.name,
        type_expr=_replace_placeholders(spec.type_expr, component_names),
        nullable=spec.nullable,
        metadata=spec.metadata,
        inline_struct_fields=resolved_inline,
        is_list=spec.is_list,
        list_item_nullable=spec.list_item_nullable,
    )


def _format_schema_exports(
    table_roots: Mapping[str, ComponentKey | _RenderedFieldSpec],
    component_names: Mapping[ComponentKey, str],
) -> list[str]:
    """Format the ``SCHEMA``, ``*_SCHEMA``, and ``SCHEMAS`` export statements for a schema module.

    :param table_roots: Mapping from table tag name to its root component key or scalar field spec.
    :type table_roots: Mapping[str, ComponentKey | _RenderedFieldSpec]
    :param component_names: Mapping from ``ComponentKey`` to assigned constant name.
    :type component_names: Mapping[ComponentKey, str]
    :return: Lines defining top-level ``pa.Schema`` constants and ``SCHEMAS``.
    :rtype: list[str]
    """
    lines: list[str] = []
    has_entry_table = "entry" in table_roots
    schema_var_by_tag: dict[str, str] = {}
    for tag_name, root_target in table_roots.items():
        schema_var = "SCHEMA" if has_entry_table and tag_name == "entry" else f"{_to_upper_snake(tag_name)}_SCHEMA"
        schema_var_by_tag[tag_name] = schema_var

        if isinstance(root_target, tuple):
            comp_const = component_names[root_target]
            lines.append(f"{schema_var}: Final[pa.Schema] = assemble_pyarrow_schema({comp_const})")
        else:
            field_expr = _render_field_call(root_target)
            single_schema = f"{schema_var}: Final[pa.Schema] = assemble_pyarrow_schema([{field_expr}])"
            if len(single_schema) <= MAX_LINE_LENGTH:
                lines.append(single_schema)
            else:
                lines.append(f"{schema_var}: Final[pa.Schema] = assemble_pyarrow_schema(\n    [{field_expr}]\n)")

    if not has_entry_table and len(table_roots) == 1:
        only_var = next(iter(schema_var_by_tag.values()))
        lines.append(f"SCHEMA: Final[pa.Schema] = {only_var}")

    entries_str = ", ".join(f"{_fmt_str(tag)}: {var}" for tag, var in schema_var_by_tag.items())
    schemas_single = f"SCHEMAS: Final[dict[str, pa.Schema]] = {{{entries_str}}}"
    if len(schemas_single) <= MAX_LINE_LENGTH:
        lines.append(schemas_single)
    else:
        items_block = "\n".join(f"    {_fmt_str(tag)}: {var}," for tag, var in schema_var_by_tag.items())
        lines.append(f"SCHEMAS: Final[dict[str, pa.Schema]] = {{\n{items_block}\n}}")
    lines.append("")
    return lines


def _unwrap_object_node(node: TypedNode | None) -> TypedNode | None:
    """Resolve ``node`` (or its first approximated union branch) to an object ``TypedNode`` if applicable.

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
            return _unwrap_object_node(branches[0])
    return None


def _unwrap_array_element_node(node: TypedNode | None) -> TypedNode | None:
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


def _unwrap_map_value_node(node: TypedNode | None) -> TypedNode | None:
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


class _IRComponentCollector:
    """Traverse IR ``SchemaDocument`` trees alongside emitted PyArrow types to collect reusable struct components."""

    def __init__(self) -> None:
        """Initialize the IR component collector."""
        self.components: dict[ComponentKey, _StructComponentSpec] = {}
        self.order: list[ComponentKey] = []
        self.el_name_types: dict[str, set[int]] = {}

    def _scan_ir_node(self, node: TypedNode, el_name: str | None, visited: set[int]) -> None:
        """Record distinct component IDs associated with each element or field name.

        :param node: IR node to scan.
        :type node: TypedNode
        :param el_name: Element or field name associated with ``node``.
        :type el_name: str | None
        :param visited: Set of already-visited component IDs.
        :type visited: set[int]
        """
        prov_meta = node.provenance.metadata if node.provenance is not None else {}
        if "truncated_component_id" in prov_meta:
            trunc_id = int(prov_meta["truncated_component_id"])
            if el_name:
                self.el_name_types.setdefault(el_name, set()).add(trunc_id)
            return
        if node.type == "array":
            elem = _unwrap_array_element_node(node)
            if elem is not None:
                self._scan_ir_node(elem, el_name, visited)
            return
        if node.type == "map":
            val = _unwrap_map_value_node(node)
            if val is not None:
                self._scan_ir_node(val, el_name, visited)
            return
        obj_node = _unwrap_object_node(node)
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
        """Pre-scan ``doc`` to count how many distinct struct components share each element or field name.

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
                base = _to_upper_snake(spec.type_local_name, strip_type_suffix=True)
            elif len(self.el_name_types.get(spec.el_name, ())) > 1 and spec.parent_el_name:
                base = f"{_to_upper_snake(spec.parent_el_name)}_{_to_upper_snake(spec.el_name)}"
            else:
                base = _to_upper_snake(spec.el_name)

            if nillable and type_id_nillables[tid] > 1:
                base = f"{base}_NILLABLE"

            candidate = f"{base}_STRUCT"
            if candidate in used and spec.parent_el_name:
                candidate = f"{_to_upper_snake(spec.parent_el_name)}_{candidate}"
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
    ) -> ComponentKey | _RenderedFieldSpec:
        """Collect components for ``doc`` and return its root ``ComponentKey`` or scalar ``#text`` field spec.

        :param tag_name: Table or root element name.
        :type tag_name: str
        :param doc: IR schema document.
        :type doc: SchemaDocument
        :param pa_schema: Emitted PyArrow schema for ``doc``.
        :type pa_schema: pa.Schema
        :return: ``ComponentKey`` for struct roots, or ``_RenderedFieldSpec`` for scalar leaf tables.
        :rtype: ComponentKey | _RenderedFieldSpec
        """
        root_meta = doc.root.provenance.metadata if doc.root.provenance is not None else {}
        if root_meta.get("scalar_leaf_table"):
            f0 = pa_schema.field(0)
            return _RenderedFieldSpec(
                name=f0.name,
                type_expr=_render_type_expr(f0.type),
                nullable=f0.nullable,
                metadata=_decode_metadata(f0.metadata),
            )

        comp_id = int(root_meta.get("component_id", id(doc.root)))
        nillable = bool(root_meta.get("nillable", False))
        type_name = str(root_meta["type_name"]) if isinstance(root_meta.get("type_name"), str) else None
        key: ComponentKey = (comp_id, nillable)
        pa_struct = pa.struct(list(pa_schema))
        if key not in self.components:
            fields = self._build_struct_fields(doc.root, pa_struct, parent_el_name=tag_name)
            self.components[key] = _StructComponentSpec(
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
    ) -> list[_RenderedFieldSpec]:
        """Build field rendering specifications for ``obj_node`` and ``pa_struct``.

        :param obj_node: Object IR node corresponding to ``pa_struct``.
        :type obj_node: TypedNode
        :param pa_struct: Emitted PyArrow struct type.
        :type pa_struct: pa.StructType
        :param parent_el_name: Name of the enclosing element or field.
        :type parent_el_name: str
        :return: List of rendered field specifications.
        :rtype: list[_RenderedFieldSpec]
        """
        specs: list[_RenderedFieldSpec] = []
        ir_props = obj_node.properties
        for idx, pa_field in enumerate(pa_struct):
            ir_field = ir_props[idx] if idx < len(ir_props) else None
            ir_child_node = ir_field.node if ir_field is not None else None
            field_name = pa_field.name

            if isinstance(pa_field.type, pa.StructType):
                child_obj = _unwrap_object_node(ir_child_node)
                struct_expr, inline_tuple = self._collect_struct_node(
                    child_obj,
                    pa_field.type,
                    el_name=field_name,
                    parent_el_name=parent_el_name,
                )
                specs.append(
                    _RenderedFieldSpec(
                        name=field_name,
                        type_expr=struct_expr,
                        nullable=pa_field.nullable,
                        metadata=_decode_metadata(pa_field.metadata),
                        inline_struct_fields=inline_tuple,
                        is_list=False,
                    )
                )
            elif isinstance(pa_field.type, pa.ListType) and isinstance(pa_field.type.value_field.type, pa.StructType):
                item_pa_field = pa_field.type.value_field
                elem_ir = _unwrap_array_element_node(ir_child_node)
                child_obj = _unwrap_object_node(elem_ir)
                struct_expr, inline_tuple = self._collect_struct_node(
                    child_obj,
                    item_pa_field.type,
                    el_name=field_name,
                    parent_el_name=parent_el_name,
                )
                item_spec = _RenderedFieldSpec(
                    name=item_pa_field.name,
                    type_expr=struct_expr,
                    nullable=item_pa_field.nullable,
                    metadata=_decode_metadata(item_pa_field.metadata),
                )
                specs.append(
                    _RenderedFieldSpec(
                        name=field_name,
                        type_expr=f"pa.list_({_render_field_call(item_spec)})",
                        nullable=pa_field.nullable,
                        metadata=_decode_metadata(pa_field.metadata),
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
                specs.append(
                    _RenderedFieldSpec(
                        name=field_name,
                        type_expr=type_expr,
                        nullable=pa_field.nullable,
                        metadata=_decode_metadata(pa_field.metadata),
                    )
                )
        return specs

    def _collect_struct_node(
        self,
        child_obj: TypedNode | None,
        pa_struct: pa.StructType,
        *,
        el_name: str,
        parent_el_name: str,
    ) -> tuple[str, tuple[_RenderedFieldSpec, ...] | None]:
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
        :rtype: tuple[str, tuple[_RenderedFieldSpec, ...] | None]
        """
        if child_obj is None or len(pa_struct) == 0:
            return _render_type_expr(pa_struct), None

        prov_meta = child_obj.provenance.metadata if child_obj.provenance is not None else {}
        source = child_obj.provenance.source if child_obj.provenance is not None else None
        comp_id = int(prov_meta.get("component_id", id(child_obj)))
        nillable = bool(prov_meta.get("nillable", False))
        type_name = str(prov_meta["type_name"]) if isinstance(prov_meta.get("type_name"), str) else None
        cache_key: ComponentKey = (comp_id, nillable)
        is_inline = bool(prov_meta.get("inline_struct", False))

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
                self.components[cache_key] = _StructComponentSpec(
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
        return _render_struct_expr(inline_fields), tuple(inline_fields)

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
            child_obj = _unwrap_object_node(ir_node)
            struct_expr, _ = self._collect_struct_node(
                child_obj,
                dtype,
                el_name=el_name,
                parent_el_name=parent_el_name,
            )
            return struct_expr
        if isinstance(dtype, pa.ListType):
            item_field = dtype.value_field
            elem_ir = _unwrap_array_element_node(ir_node)
            item_type_expr = self._render_nested_type_expr(
                elem_ir,
                item_field.type,
                el_name=el_name,
                parent_el_name=parent_el_name,
            )
            item_spec = _RenderedFieldSpec(
                name=item_field.name,
                type_expr=item_type_expr,
                nullable=item_field.nullable,
                metadata=_decode_metadata(item_field.metadata),
            )
            return f"pa.list_({_render_field_call(item_spec)})"
        if isinstance(dtype, pa.MapType):
            key_expr = _render_type_expr(dtype.key_field.type)
            val_field = dtype.item_field
            val_ir = _unwrap_map_value_node(ir_node)
            val_type_expr = self._render_nested_type_expr(
                val_ir,
                val_field.type,
                el_name=el_name,
                parent_el_name=parent_el_name,
            )
            val_spec = _RenderedFieldSpec(
                name=val_field.name,
                type_expr=val_type_expr,
                nullable=val_field.nullable,
                metadata=_decode_metadata(val_field.metadata),
            )
            return f"pa.map_({key_expr}, {_render_field_call(val_spec)})"
        return _render_type_expr(dtype)


def render_python_schema_module(
    emitter: "PyArrowEmitter",
    document: SchemaDocument | Mapping[str, SchemaDocument],
    *,
    docstring: str | None = None,
) -> str:
    """Render a complete, formatted Python schema module from one or more ``SchemaDocument`` instances.

    :param emitter: ``PyArrowEmitter`` instance used to resolve Arrow types and field metadata.
    :type emitter: PyArrowEmitter
    :param document: A single ``SchemaDocument`` or a mapping of table names to ``SchemaDocument``s.
    :type document: SchemaDocument | Mapping[str, SchemaDocument]
    :param docstring: Optional module docstring header.
    :type docstring: str | None
    :return: Formatted Python module source code defining ``*_STRUCT``, ``*_SCHEMA``, ``SCHEMA``, and ``SCHEMAS``.
    :rtype: str
    :raises ValueError: If ``document`` is an empty mapping.
    """
    if isinstance(document, SchemaDocument):
        tag_name = document.name or "root"
        docs_by_name: dict[str, SchemaDocument] = {tag_name: document}
        default_doc = (
            f"PyArrow schema and reusable struct components for {document.name}."
            if document.name
            else "PyArrow schema and reusable struct components."
        )
    else:
        docs_by_name = dict(document)
        if not docs_by_name:
            msg = "Cannot emit a Python schema module from an empty SchemaDocument mapping"
            raise ValueError(msg)
        default_doc = "PyArrow schemas and reusable struct components."

    module_doc = docstring if docstring is not None else default_doc
    pa_schemas = {name: emitter.emit(doc) for name, doc in docs_by_name.items()}

    collector = _IRComponentCollector()
    for name, doc in docs_by_name.items():
        collector.register_document(name, doc)

    table_roots: dict[str, ComponentKey | _RenderedFieldSpec] = {}
    for name, doc in docs_by_name.items():
        table_roots[name] = collector.collect_document(name, doc, pa_schemas[name])

    component_names = collector.assign_component_names()

    lines: list[str] = [
        f'"""{module_doc}"""',
        "",
        "from typing import Final",
        "",
        "import pyarrow as pa",
        "",
        "from cdm_data_loaders.converters.emitters.pyarrow import assemble_pyarrow_schema",
        "",
    ]

    for key in collector.order:
        const_name = component_names[key]
        comp_spec = collector.components[key]
        resolved_fields = [_resolve_field_placeholders(f, component_names) for f in comp_spec.fields]
        lines.append(_format_struct_assignment(const_name, resolved_fields))
        lines.append("")

    lines.extend(_format_schema_exports(table_roots, component_names))
    return "\n".join(lines)


def write_python_schema_file(
    emitter: "PyArrowEmitter",
    document: SchemaDocument | Mapping[str, SchemaDocument],
    output: str | Path,
    *,
    docstring: str | None = None,
) -> Path:
    """Render and write a static Python schema module to ``output``.

    :param emitter: ``PyArrowEmitter`` instance used to resolve Arrow types and field metadata.
    :type emitter: PyArrowEmitter
    :param document: A single ``SchemaDocument`` or a mapping of table names to ``SchemaDocument``s.
    :type document: SchemaDocument | Mapping[str, SchemaDocument]
    :param output: Destination file path for the generated Python module.
    :type output: str | Path
    :param docstring: Optional module docstring header.
    :type docstring: str | None
    :return: Resolved output ``Path``.
    :rtype: Path
    """
    out_path = Path(output)
    source = render_python_schema_module(emitter, document, docstring=docstring)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(source, encoding="utf-8")
    return out_path
