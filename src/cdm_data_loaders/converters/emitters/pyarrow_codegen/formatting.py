"""PyArrow expression and Ruff-compliant line-wrapping formatters for Python schema codegen."""

from collections.abc import Mapping, Sequence
from typing import Final

import pyarrow as pa
from frozendict import frozendict

from cdm_data_loaders.converters.emitters.pyarrow_codegen.naming import (
    MAX_LINE_LENGTH,
    ComponentKey,
    RenderedFieldSpec,
    decode_sorted_metadata,
    fmt_str,
    to_upper_snake,
)

PA_SCALAR_EXPR: Final[Mapping[pa.DataType, str]] = frozendict(
    {
        pa.null(): "pa.null()",
        pa.string(): "pa.string()",
        pa.large_string(): "pa.large_string()",
        pa.string_view(): "pa.string_view()",
        pa.bool_(): "pa.bool_()",
        pa.int8(): "pa.int8()",
        pa.int16(): "pa.int16()",
        pa.int32(): "pa.int32()",
        pa.int64(): "pa.int64()",
        pa.uint8(): "pa.uint8()",
        pa.uint16(): "pa.uint16()",
        pa.uint32(): "pa.uint32()",
        pa.uint64(): "pa.uint64()",
        pa.float16(): "pa.float16()",
        pa.float32(): "pa.float32()",
        pa.float64(): "pa.float64()",
        pa.date32(): "pa.date32()",
        pa.date64(): "pa.date64()",
        pa.binary(): "pa.binary()",
        pa.large_binary(): "pa.large_binary()",
        pa.binary_view(): "pa.binary_view()",
    }
)


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
            f"pa.timestamp({fmt_str(dtype.unit)}, tz={fmt_str(dtype.tz)})"
            if dtype.tz is not None
            else f"pa.timestamp({fmt_str(dtype.unit)})"
        )
    if isinstance(dtype, pa.Time64Type | pa.Time32Type):
        fn = "time64" if isinstance(dtype, pa.Time64Type) else "time32"
        return f"pa.{fn}({fmt_str(dtype.unit)})"
    if isinstance(dtype, pa.FixedSizeBinaryType):
        return f"pa.binary({dtype.byte_width})"
    if isinstance(dtype, pa.DictionaryType):
        idx_expr = render_scalar_type_expr(dtype.index_type)
        val_expr = render_scalar_type_expr(dtype.value_type)
        if dtype.ordered:
            return f"pa.dictionary({idx_expr}, {val_expr}, ordered=True)"
        return f"pa.dictionary({idx_expr}, {val_expr})"
    raise KeyError(dtype)


def render_scalar_type_expr(dtype: pa.DataType) -> str:
    """Render a scalar PyArrow ``DataType`` as a Python constructor expression.

    :param dtype: Scalar PyArrow data type.
    :type dtype: pa.DataType
    :return: Python constructor expression string.
    :rtype: str
    :raises KeyError: If ``dtype`` is not a supported scalar type.
    """
    if dtype in PA_SCALAR_EXPR:
        return PA_SCALAR_EXPR[dtype]
    return _render_parametric_scalar_expr(dtype)


def field_spec_from_pa(
    pa_field: pa.Field,
    type_expr: str,
    *,
    inline_struct_fields: tuple[RenderedFieldSpec, ...] | None = None,
    is_list: bool = False,
    list_item_nullable: bool = True,
) -> RenderedFieldSpec:
    """Build a ``RenderedFieldSpec`` from a ``pa.Field`` and its rendered type expression.

    :param pa_field: Source PyArrow field.
    :type pa_field: pa.Field
    :param type_expr: Rendered Python constructor expression for ``pa_field.type``.
    :type type_expr: str
    :param inline_struct_fields: Inline struct field specs when rendering an unhoisted struct.
    :type inline_struct_fields: tuple[RenderedFieldSpec, ...] | None
    :param is_list: Whether the field wraps a list of structs.
    :type is_list: bool
    :param list_item_nullable: Nullability of list elements when ``is_list`` is ``True``.
    :type list_item_nullable: bool
    :return: Rendered field specification.
    :rtype: RenderedFieldSpec
    """
    return RenderedFieldSpec(
        name=pa_field.name,
        type_expr=type_expr,
        nullable=pa_field.nullable,
        metadata=decode_sorted_metadata(pa_field.metadata),
        inline_struct_fields=inline_struct_fields,
        is_list=is_list,
        list_item_nullable=list_item_nullable,
    )


def render_type_expr(dtype: pa.DataType) -> str:
    """Render any PyArrow ``DataType`` as a Python constructor expression.

    :param dtype: PyArrow data type.
    :type dtype: pa.DataType
    :return: Python constructor expression string.
    :rtype: str
    """
    if isinstance(dtype, pa.ListType):
        item_field = dtype.value_field
        item_expr = render_field_call(field_spec_from_pa(item_field, render_type_expr(item_field.type)))
        return f"pa.list_({item_expr})"
    if isinstance(dtype, pa.MapType):
        key_expr = render_type_expr(dtype.key_field.type)
        val_field = dtype.item_field
        val_expr = render_field_call(field_spec_from_pa(val_field, render_type_expr(val_field.type)))
        return f"pa.map_({key_expr}, {val_expr})"
    if isinstance(dtype, pa.StructType):
        struct_fields = [field_spec_from_pa(f, render_type_expr(f.type)) for f in dtype]
        return render_struct_expr(struct_fields)
    return render_scalar_type_expr(dtype)


def _format_metadata_single(metadata: Mapping[str, str]) -> str:
    """Format a metadata dictionary on a single line.

    :param metadata: Sorted string key/value metadata mapping.
    :type metadata: Mapping[str, str]
    :return: Single-line Python dictionary literal.
    :rtype: str
    """
    meta_items = ", ".join(f"{fmt_str(k)}: {fmt_str(v)}" for k, v in sorted(metadata.items()))
    return f"{{{meta_items}}}"


def _format_field_args_single(spec: RenderedFieldSpec) -> str:
    """Format the comma-separated arguments of a ``pa.field(...)`` call on a single line.

    :param spec: Field rendering specification.
    :type spec: RenderedFieldSpec
    :return: Comma-separated argument list string.
    :rtype: str
    """
    args = [fmt_str(spec.name), spec.type_expr, f"nullable={spec.nullable!r}"]
    if spec.metadata:
        args.append(f"metadata={_format_metadata_single(spec.metadata)}")
    return ", ".join(args)


def render_field_call(spec: RenderedFieldSpec) -> str:
    """Render a single-line ``pa.field(...)`` expression from ``spec``.

    :param spec: Field rendering specification.
    :type spec: RenderedFieldSpec
    :return: Single-line ``pa.field(...)`` call string.
    :rtype: str
    """
    return f"pa.field({_format_field_args_single(spec)})"


def render_struct_expr(fields: Sequence[RenderedFieldSpec]) -> str:
    """Render a single-line ``pa.struct([...])`` expression from ``fields``.

    :param fields: Sequence of field rendering specifications.
    :type fields: Sequence[RenderedFieldSpec]
    :return: Single-line ``pa.struct([...])`` call string.
    :rtype: str
    """
    inner = ", ".join(render_field_call(f) for f in fields)
    return f"pa.struct([{inner}])"


def _format_type_arg_indented(spec: RenderedFieldSpec, *, arg_indent: int) -> str:
    """Format the ``type_expr`` argument of a multi-line ``pa.field(...)`` call.

    :param spec: Field rendering specification.
    :type spec: RenderedFieldSpec
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
            format_field_indented(f, indent=arg_indent + 16, trailing_comma=True) for f in spec.inline_struct_fields
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
        format_field_indented(f, indent=arg_indent + 8, trailing_comma=True) for f in spec.inline_struct_fields
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


def format_field_indented(spec: RenderedFieldSpec, *, indent: int, trailing_comma: bool) -> str:
    """Format a ``pa.field(...)`` call at ``indent`` spaces wrapped to ``MAX_LINE_LENGTH``.

    :param spec: Field rendering specification.
    :type spec: RenderedFieldSpec
    :param indent: Leading indentation in spaces.
    :type indent: int
    :param trailing_comma: Whether to append a trailing comma after the closing parenthesis.
    :type trailing_comma: bool
    :return: Formatted ``pa.field(...)`` block.
    :rtype: str
    """
    comma = "," if trailing_comma else ""
    pad = " " * indent
    single = f"{pad}{render_field_call(spec)}{comma}"
    if len(single) <= MAX_LINE_LENGTH:
        return single

    arg_pad = " " * (indent + 4)
    inner_single = f"{arg_pad}{_format_field_args_single(spec)}"
    if len(inner_single) <= MAX_LINE_LENGTH:
        return f"{pad}pa.field(\n{inner_single}\n{pad}){comma}"

    lines = [
        f"{pad}pa.field(",
        f"{arg_pad}{fmt_str(spec.name)},",
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
            lines.append(f"{arg_pad}    {fmt_str(k)}: {fmt_str(v)}")
            lines.append(f"{arg_pad}}},")
        else:
            lines.append(f"{arg_pad}metadata={{")
            for k, v in sorted(spec.metadata.items()):
                lines.append(f"{arg_pad}    {fmt_str(k)}: {fmt_str(v)},")
            lines.append(f"{arg_pad}}},")
    lines.append(f"{pad}){comma}")
    return "\n".join(lines)


def format_struct_assignment(const_name: str, fields: Sequence[RenderedFieldSpec]) -> str:
    """Format a top-level ``<NAME>_STRUCT: Final[pa.StructType] = pa.struct([...])`` assignment.

    :param const_name: Constant identifier for the struct component.
    :type const_name: str
    :param fields: Sequence of field rendering specifications.
    :type fields: Sequence[RenderedFieldSpec]
    :return: Formatted Python assignment statement.
    :rtype: str
    """
    lhs = f"{const_name}: Final[pa.StructType] = "
    single = f"{lhs}{render_struct_expr(fields)}"
    if len(single) <= MAX_LINE_LENGTH:
        return single

    if len(fields) == 1:
        one_single = f"    [{render_field_call(fields[0])}]"
        if len(one_single) <= MAX_LINE_LENGTH:
            return f"{lhs}pa.struct(\n{one_single}\n)"
        body = format_field_indented(fields[0], indent=8, trailing_comma=False)
        return f"{lhs}pa.struct(\n    [\n{body}\n    ]\n)"

    body = "\n".join(format_field_indented(f, indent=8, trailing_comma=True) for f in fields)
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


def resolve_field_placeholders(
    spec: RenderedFieldSpec,
    component_names: Mapping[ComponentKey, str],
) -> RenderedFieldSpec:
    """Resolve component placeholders in ``spec`` and any nested ``inline_struct_fields``.

    :param spec: Field rendering specification with placeholder tokens.
    :type spec: RenderedFieldSpec
    :param component_names: Mapping from ``ComponentKey`` to assigned constant name.
    :type component_names: Mapping[ComponentKey, str]
    :return: Field rendering specification with resolved constant names.
    :rtype: RenderedFieldSpec
    """
    resolved_inline = (
        tuple(resolve_field_placeholders(inner, component_names) for inner in spec.inline_struct_fields)
        if spec.inline_struct_fields is not None
        else None
    )
    return RenderedFieldSpec(
        name=spec.name,
        type_expr=_replace_placeholders(spec.type_expr, component_names),
        nullable=spec.nullable,
        metadata=spec.metadata,
        inline_struct_fields=resolved_inline,
        is_list=spec.is_list,
        list_item_nullable=spec.list_item_nullable,
    )


def format_schema_exports(
    table_roots: Mapping[str, ComponentKey | RenderedFieldSpec],
    component_names: Mapping[ComponentKey, str],
) -> list[str]:
    """Format the ``SCHEMA``, ``*_SCHEMA``, and ``SCHEMAS`` export statements for a schema module.

    :param table_roots: Mapping from table tag name to its root component key or scalar field spec.
    :type table_roots: Mapping[str, ComponentKey | RenderedFieldSpec]
    :param component_names: Mapping from ``ComponentKey`` to assigned constant name.
    :type component_names: Mapping[ComponentKey, str]
    :return: Lines defining top-level ``pa.Schema`` constants and ``SCHEMAS``.
    :rtype: list[str]
    """
    lines: list[str] = []
    has_entry_table = "entry" in table_roots
    schema_var_by_tag: dict[str, str] = {}
    for tag_name, root_target in table_roots.items():
        schema_var = "SCHEMA" if has_entry_table and tag_name == "entry" else f"{to_upper_snake(tag_name)}_SCHEMA"
        schema_var_by_tag[tag_name] = schema_var

        if isinstance(root_target, tuple):
            comp_const = component_names[root_target]
            lines.append(f"{schema_var}: Final[pa.Schema] = assemble_pyarrow_schema({comp_const})")
        else:
            field_expr = render_field_call(root_target)
            single_schema = f"{schema_var}: Final[pa.Schema] = assemble_pyarrow_schema([{field_expr}])"
            if len(single_schema) <= MAX_LINE_LENGTH:
                lines.append(single_schema)
            else:
                lines.append(f"{schema_var}: Final[pa.Schema] = assemble_pyarrow_schema(\n    [{field_expr}]\n)")

    if not has_entry_table and len(table_roots) == 1:
        only_var = next(iter(schema_var_by_tag.values()))
        lines.append(f"SCHEMA: Final[pa.Schema] = {only_var}")

    entries_str = ", ".join(f"{fmt_str(tag)}: {var}" for tag, var in schema_var_by_tag.items())
    schemas_single = f"SCHEMAS: Final[dict[str, pa.Schema]] = {{{entries_str}}}"
    if len(schemas_single) <= MAX_LINE_LENGTH:
        lines.append(schemas_single)
    else:
        items_block = "\n".join(f"    {fmt_str(tag)}: {var}," for tag, var in schema_var_by_tag.items())
        lines.append(f"SCHEMAS: Final[dict[str, pa.Schema]] = {{\n{items_block}\n}}")
    lines.append("")
    return lines
