"""Container, model-merge, and multi-branch union IR builders for Pydantic IR lowering."""

from collections.abc import Callable, Sequence
from dataclasses import replace
from typing import Any, Final, get_args, get_origin

from frozendict import frozendict
from pydantic import BaseModel

from cdm_data_loaders.converters.core.inference import nodes_same_shape, unify_scalar_specs
from cdm_data_loaders.converters.core.ir import Field as SchemaField
from cdm_data_loaders.converters.core.ir import NodeHints, Provenance, TypedNode
from cdm_data_loaders.converters.sources.pydantic.annotations import (
    MAP_TYPE_ARGS_LEN,
    ParsedAnnotation,
    allows_null,
    classify_parsed_kind,
    extract_model_field_specs,
    flatten_union_branches,
    is_none_type,
    is_union_origin,
    is_valid_string_map_key,
    is_variadic_unpack,
    unwrap_annotation,
)
from cdm_data_loaders.converters.sources.pydantic.model_synthesis import (
    ModelFieldSpec,
    SchemaBuildContext,
)
from cdm_data_loaders.converters.sources.pydantic.scalars import (
    check_no_forbidden_types_in_recursive_model,
    resolve_parsed_scalar,
)


def build_sequence_node(
    parsed: ParsedAnnotation,
    ctx: SchemaBuildContext,
    path: tuple[str | int, ...],
    *,
    nullable: bool,
) -> TypedNode:
    """Build an array ``TypedNode`` from a homogeneous sequence, set, or variadic tuple annotation.

    :param parsed: Unwrapped sequence annotation.
    :type parsed: ParsedAnnotation
    :param ctx: Active schema build context.
    :type ctx: SchemaBuildContext
    :param path: Provenance path tuple.
    :type path: tuple[str | int, ...]
    :param nullable: Whether the outer array is nullable.
    :type nullable: bool
    :return: Array IR ``TypedNode``.
    :rtype: TypedNode
    """
    args = get_args(parsed.annotation)
    if not args:
        msg = f"Untyped sequence annotation: {parsed.annotation!r}"
        ctx.raise_err("E4", msg)
    item_field = build_field_ir("item", args[0], ctx, (*path, "element"), required_in_parent=True)
    item_node = replace(item_field.node, annotations=item_field.annotations)
    return TypedNode(
        type="array",
        nullable=nullable,
        items=item_node,
        provenance=Provenance(source="pydantic", path=path),
    )


def build_tuple_node(
    parsed: ParsedAnnotation,
    ctx: SchemaBuildContext,
    path: tuple[str | int, ...],
    *,
    nullable: bool,
) -> TypedNode:
    """Build a positional object ``TypedNode`` (``item_0``, ``item_1``, ...) from a fixed-length tuple annotation.

    :param parsed: Unwrapped tuple annotation.
    :type parsed: ParsedAnnotation
    :param ctx: Active schema build context.
    :type ctx: SchemaBuildContext
    :param path: Provenance path tuple.
    :type path: tuple[str | int, ...]
    :param nullable: Whether the outer struct is nullable.
    :type nullable: bool
    :return: Positional struct IR ``TypedNode``.
    :rtype: TypedNode
    """
    args = get_args(parsed.annotation)
    if not args:
        msg = "Empty tuple tuple[()] cannot be converted to a PyArrow struct with zero fields"
        ctx.raise_err("E3", msg)
    if any(is_variadic_unpack(a) for a in args):
        msg = f"Open tuple mixing positional elements and variadic unpack is not supported: {parsed.annotation!r}"
        ctx.raise_err("E5", msg)

    fields = tuple(
        build_field_ir(f"item_{idx}", elem_ann, ctx, (*path, f"item_{idx}"), required_in_parent=True)
        for idx, elem_ann in enumerate(args)
    )
    required_names = tuple(f.name for f in fields if f.required)
    return TypedNode(
        type="object",
        nullable=nullable,
        properties=fields,
        required_names=required_names,
        additional_properties=TypedNode(type="never", nullable=False),
        provenance=Provenance(source="pydantic", path=path),
    )


def build_map_node(
    parsed: ParsedAnnotation,
    ctx: SchemaBuildContext,
    path: tuple[str | int, ...],
    *,
    nullable: bool,
) -> TypedNode:
    """Build a map ``TypedNode`` from a string-keyed mapping annotation.

    :param parsed: Unwrapped mapping annotation.
    :type parsed: ParsedAnnotation
    :param ctx: Active schema build context.
    :type ctx: SchemaBuildContext
    :param path: Provenance path tuple.
    :type path: tuple[str | int, ...]
    :param nullable: Whether the outer map is nullable.
    :type nullable: bool
    :return: Map IR ``TypedNode``.
    :rtype: TypedNode
    """
    args = get_args(parsed.annotation)
    if len(args) != MAP_TYPE_ARGS_LEN:
        msg = f"Mapping annotation requires key and value type arguments: {parsed.annotation!r}"
        ctx.raise_err("E4", msg)
    key_ann, val_ann = args
    if not is_valid_string_map_key(key_ann, ctx):
        msg = f"Map key type must resolve to str, got {key_ann!r}"
        ctx.raise_err("E6", msg)
    key_node = TypedNode(
        type="string",
        nullable=False,
        hints=NodeHints(logical_type="string"),
        provenance=Provenance(source="pydantic", path=(*path, "key")),
    )
    val_field = build_field_ir("value", val_ann, ctx, (*path, "value"), required_in_parent=True)
    val_node = replace(val_field.node, annotations=val_field.annotations)
    return TypedNode(
        type="map",
        nullable=nullable,
        key_type=key_node,
        value_type=val_node,
        provenance=Provenance(source="pydantic", path=path),
    )


CONTAINER_NODE_BUILDERS: Final[
    frozendict[
        str,
        Callable[..., TypedNode],
    ]
] = frozendict(
    {
        "sequence": build_sequence_node,
        "tuple": build_tuple_node,
        "map": build_map_node,
    }
)


def build_merged_models_struct(
    models: Sequence[type[BaseModel]],
    ctx: SchemaBuildContext,
    path: tuple[str | int, ...],
    *,
    nullable: bool,
    scalar_branches: Sequence[ParsedAnnotation] = (),
) -> TypedNode:
    """Build an object ``TypedNode`` from one or more ``BaseModel`` branches and optional scalar branches.

    :param models: Concrete ``BaseModel`` branches to merge.
    :type models: Sequence[type[BaseModel]]
    :param ctx: Active schema build context.
    :type ctx: SchemaBuildContext
    :param path: Provenance path tuple.
    :type path: tuple[str | int, ...]
    :param nullable: Whether the resulting object node is nullable.
    :type nullable: bool
    :param scalar_branches: Optional scalar union branches synthesized into ``_value``.
    :type scalar_branches: Sequence[ParsedAnnotation]
    :return: Merged object IR ``TypedNode``.
    :rtype: TypedNode
    """
    fields_by_name: dict[str, list[ModelFieldSpec]] = {}
    branch_non_null_sets: list[set[str]] = []

    for model in models:
        branch_specs = extract_model_field_specs(model, ctx)
        branch_non_null: set[str] = set()
        for spec in branch_specs:
            fields_by_name.setdefault(spec.output_name, []).append(spec)
            if spec.required_non_null:
                branch_non_null.add(spec.output_name)
        branch_non_null_sets.append(branch_non_null)

    common_non_null: set[str] = (
        set.intersection(*branch_non_null_sets) if (branch_non_null_sets and not scalar_branches) else set()
    )

    if scalar_branches and "_value" in fields_by_name:
        msg = "Field '_value' in model branch collides with synthesized '_value' field in scalar + model union"
        ctx.raise_err("E12", msg)

    ir_fields: list[SchemaField] = []
    for out_name, spec_list in fields_by_name.items():
        is_req = out_name in common_non_null
        descriptions = [s.description for s in spec_list if s.description]
        merged_desc = descriptions[0] if descriptions else None
        field_path = (*path, out_name)

        if len(spec_list) == 1:
            spec = spec_list[0]
            parsed_field, next_ctx = unwrap_annotation(
                spec.annotation,
                spec.context,
                field_info=spec.field_info,
                extra_description=merged_desc,
            )
            ir_fields.append(
                build_parsed_field_ir(
                    out_name,
                    parsed_field,
                    next_ctx,
                    field_path,
                    required_in_parent=is_req,
                )
            )
        else:
            branch_parsed_list = [
                unwrap_annotation(s.annotation, s.context, field_info=s.field_info) for s in spec_list
            ]
            field_nullable = (not is_req) or any(allows_null(s.annotation, s.context) for s in spec_list)
            node = build_multi_branch_node(branch_parsed_list, field_path, nullable=field_nullable)
            annotations = {"description": merged_desc} if merged_desc else {}
            prov_meta = dict(node.provenance.metadata) if node.provenance is not None else {}
            ir_fields.append(
                SchemaField(
                    name=out_name,
                    node=node,
                    required=not field_nullable,
                    annotations=annotations,
                    provenance=Provenance(source="pydantic", path=field_path, metadata=prov_meta),
                )
            )

    if scalar_branches:
        resolved_scalars = [resolve_parsed_scalar(sb, ctx) for sb in scalar_branches]
        val_node_type, val_hints, val_label = unify_scalar_specs(resolved_scalars)
        val_path = (*path, "_value")
        val_prov = Provenance(source="pydantic", path=val_path, metadata={"pydantic_type": val_label})
        val_node = TypedNode(type=val_node_type, nullable=True, hints=val_hints, provenance=val_prov)
        ir_fields.append(SchemaField(name="_value", node=val_node, required=False, provenance=val_prov))

    if not ir_fields:
        model_names = ", ".join(m.__name__ for m in models)
        msg = f"Model {model_names} has zero active fields and cannot be converted to a PyArrow struct"
        ctx.raise_err("E3", msg)

    fields_tuple = tuple(ir_fields)
    required_names = tuple(f.name for f in fields_tuple if f.required)
    prov_meta: dict[str, Any] = {}
    if len(models) == 1 and not scalar_branches:
        prov_meta["component_id"] = id(models[0])
        prov_meta["type_name"] = models[0].__name__
    if any(ctx.model_counts.get(m, 0) >= 1 for m in models):
        prov_meta["inline_struct"] = True
    return TypedNode(
        type="object",
        nullable=nullable,
        properties=fields_tuple,
        required_names=required_names,
        additional_properties=TypedNode(type="never", nullable=False),
        provenance=Provenance(source="pydantic", path=path, metadata=prov_meta),
    )


def partition_union_branches(
    non_null: Sequence[tuple[ParsedAnnotation, SchemaBuildContext]],
) -> tuple[list[type[BaseModel]], list[ParsedAnnotation], list[str]]:
    """Partition non-null union branches into models, scalars, and other container kinds.

    :param non_null: Non-null union branch pairs.
    :type non_null: Sequence[tuple[ParsedAnnotation, SchemaBuildContext]]
    :return: Tuple of ``(model_branches, scalar_branches, other_kinds)``.
    :rtype: tuple[list[type[BaseModel]], list[ParsedAnnotation], list[str]]
    """
    model_branches: list[type[BaseModel]] = []
    scalar_branches: list[ParsedAnnotation] = []
    other_kinds: list[str] = []

    for b_parsed, b_ctx in non_null:
        kind = classify_parsed_kind(b_parsed, b_ctx)
        if kind == "model":
            if b_parsed.annotation not in model_branches:
                model_branches.append(b_parsed.annotation)
        elif kind == "scalar":
            scalar_branches.append(b_parsed)
        else:
            other_kinds.append(kind)
    return model_branches, scalar_branches, other_kinds


def resolve_container_union_branches(
    non_null: Sequence[tuple[ParsedAnnotation, SchemaBuildContext]],
    partition: tuple[Sequence[type[BaseModel]], Sequence[ParsedAnnotation], Sequence[str]],
    path: tuple[str | int, ...],
    *,
    nullable: bool,
) -> TypedNode:
    """Resolve container branches in a union when shapes match, or raise ``E8``.

    :param non_null: Non-null union branch pairs.
    :type non_null: Sequence[tuple[ParsedAnnotation, SchemaBuildContext]]
    :param partition: Partitioned ``(model_branches, scalar_branches, other_kinds)``.
    :type partition: tuple[Sequence[type[BaseModel]], Sequence[ParsedAnnotation], Sequence[str]]
    :param path: Provenance path tuple.
    :type path: tuple[str | int, ...]
    :param nullable: Whether the union node is nullable.
    :type nullable: bool
    :return: Unified container ``TypedNode``.
    :rtype: TypedNode
    """
    model_branches, scalar_branches, other_kinds = partition
    container_nodes = [
        build_parsed_node(b, b_ctx, path, nullable=nullable)
        for b, b_ctx in non_null
        if classify_parsed_kind(b, b_ctx) not in {"model", "scalar"}
    ]
    if not model_branches and not scalar_branches and len(set(other_kinds)) == 1:
        first_node = container_nodes[0]
        if all(nodes_same_shape(n, first_node) for n in container_nodes[1:]):
            return first_node
    msg = (
        f"Incompatible union branch shapes: models={[m.__name__ for m in model_branches]}, "
        f"scalars={len(scalar_branches)}, containers={list(other_kinds)}"
    )
    first_ctx = non_null[0][1]
    raise first_ctx.error_by_code.get("E8", first_ctx.error_type)(msg)


def build_multi_branch_node(
    branches: Sequence[tuple[ParsedAnnotation, SchemaBuildContext]],
    path: tuple[str | int, ...],
    *,
    nullable: bool,
) -> TypedNode:
    """Resolve a list of unwrapped union branches into an IR ``TypedNode``.

    :param branches: Sequence of unwrapped union branch pairs.
    :type branches: Sequence[tuple[ParsedAnnotation, SchemaBuildContext]]
    :param path: Provenance path tuple.
    :type path: tuple[str | int, ...]
    :param nullable: Whether the resulting node is nullable.
    :type nullable: bool
    :return: Unified IR ``TypedNode``.
    :rtype: TypedNode
    """
    flat_branches: list[tuple[ParsedAnnotation, SchemaBuildContext]] = []
    for parsed, b_ctx in branches:
        flat_branches.extend(flatten_union_branches(parsed, b_ctx))

    non_null = [(b, b_ctx) for b, b_ctx in flat_branches if not is_none_type(b.annotation)]
    if not non_null:
        msg = "Standalone null-only union cannot be converted to a PyArrow field"
        branches[0][1].raise_err("E7", msg)

    if len(non_null) == 1:
        return build_parsed_node(non_null[0][0], non_null[0][1], path, nullable=nullable)

    partition = partition_union_branches(non_null)
    model_branches, scalar_branches, other_kinds = partition

    if other_kinds:
        return resolve_container_union_branches(non_null, partition, path, nullable=nullable)

    base_ctx = non_null[0][1]
    if scalar_branches and not model_branches:
        node_type, hints, pyd_type = unify_scalar_specs([resolve_parsed_scalar(sb, base_ctx) for sb in scalar_branches])
        return TypedNode(
            type=node_type,
            nullable=nullable,
            hints=hints,
            provenance=Provenance(source="pydantic", path=path, metadata={"pydantic_type": pyd_type}),
        )

    for m in model_branches:
        if base_ctx.model_counts.get(m, 0) >= 1:
            check_no_forbidden_types_in_recursive_model(m, base_ctx)

    active_models = [m for m in model_branches if base_ctx.model_counts.get(m, 0) < base_ctx.max_recursion_depth]
    if not active_models and not scalar_branches:
        return TypedNode(
            type="string",
            nullable=nullable,
            hints=NodeHints(logical_type="string"),
            provenance=Provenance(source="pydantic", path=path),
        )

    return build_merged_models_struct(
        active_models,
        base_ctx,
        path,
        nullable=nullable,
        scalar_branches=scalar_branches,
    )


def build_parsed_node(
    parsed: ParsedAnnotation,
    ctx: SchemaBuildContext,
    path: tuple[str | int, ...],
    *,
    nullable: bool,
) -> TypedNode:
    """Convert an unwrapped ``ParsedAnnotation`` into an IR ``TypedNode``.

    :param parsed: Unwrapped annotation.
    :type parsed: ParsedAnnotation
    :param ctx: Active schema build context.
    :type ctx: SchemaBuildContext
    :param path: Provenance path tuple.
    :type path: tuple[str | int, ...]
    :param nullable: Whether the resulting node is nullable.
    :type nullable: bool
    :return: Lowered IR ``TypedNode``.
    :rtype: TypedNode
    """
    tp = parsed.annotation
    if is_none_type(tp):
        msg = "Standalone NoneType field cannot be converted to a PyArrow column"
        ctx.raise_err("E7", msg)

    if is_union_origin(get_origin(tp)):
        return build_multi_branch_node([(parsed, ctx)], path, nullable=nullable)

    kind = classify_parsed_kind(parsed, ctx)
    if kind == "model":
        model_cls: type[BaseModel] = tp
        if ctx.model_counts.get(model_cls, 0) >= 1:
            check_no_forbidden_types_in_recursive_model(model_cls, ctx)
        if ctx.model_counts.get(model_cls, 0) >= ctx.max_recursion_depth:
            return TypedNode(
                type="string",
                nullable=nullable,
                hints=NodeHints(logical_type="string"),
                provenance=Provenance(source="pydantic", path=path),
            )
        return build_merged_models_struct([model_cls], ctx, path, nullable=nullable)
    builder = CONTAINER_NODE_BUILDERS.get(kind)
    if builder is not None:
        return builder(parsed, ctx, path, nullable=nullable)

    node_type, hints, pyd_type = resolve_parsed_scalar(parsed, ctx)
    return TypedNode(
        type=node_type,
        nullable=nullable,
        hints=hints,
        provenance=Provenance(source="pydantic", path=path, metadata={"pydantic_type": pyd_type}),
    )


def build_parsed_field_ir(
    name: str,
    parsed: ParsedAnnotation,
    ctx: SchemaBuildContext,
    path: tuple[str | int, ...],
    *,
    required_in_parent: bool,
) -> SchemaField:
    """Build an IR ``Field`` from an unwrapped ``ParsedAnnotation``.

    :param name: Output field name.
    :type name: str
    :param parsed: Unwrapped annotation with extracted metadata.
    :type parsed: ParsedAnnotation
    :param ctx: Active schema build context.
    :type ctx: SchemaBuildContext
    :param path: Provenance path tuple.
    :type path: tuple[str | int, ...]
    :param required_in_parent: Whether the field is required in its parent model or container.
    :type required_in_parent: bool
    :return: Lowered IR ``Field``.
    :rtype: SchemaField
    """
    nullable = (not required_in_parent) or allows_null(parsed.annotation, ctx)
    node = build_parsed_node(parsed, ctx, path, nullable=nullable)
    annotations = {"description": parsed.description} if parsed.description else {}
    prov_meta = dict(node.provenance.metadata) if node.provenance is not None else {}
    return SchemaField(
        name=name,
        node=node,
        required=not nullable,
        annotations=annotations,
        provenance=Provenance(source="pydantic", path=path, metadata=prov_meta),
    )


def build_field_ir(
    name: str,
    annotation: object,
    ctx: SchemaBuildContext,
    path: tuple[str | int, ...],
    *,
    required_in_parent: bool,
) -> SchemaField:
    """Unwrap ``annotation`` and build an IR ``Field`` with nullability and metadata.

    :param name: Output field name.
    :type name: str
    :param annotation: Raw field or container element annotation.
    :type annotation: object
    :param ctx: Active schema build context.
    :type ctx: SchemaBuildContext
    :param path: Provenance path tuple.
    :type path: tuple[str | int, ...]
    :param required_in_parent: Whether the field is required in its parent container.
    :type required_in_parent: bool
    :return: Lowered IR ``Field``.
    :rtype: SchemaField
    """
    parsed, next_ctx = unwrap_annotation(annotation, ctx)
    return build_parsed_field_ir(name, parsed, next_ctx, path, required_in_parent=required_in_parent)
