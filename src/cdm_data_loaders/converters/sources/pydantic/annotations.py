"""Annotation unwrapping, metadata extraction, and structural classification for Pydantic IR lowering."""

import operator
import re
import types
from collections import OrderedDict, defaultdict, deque
from collections.abc import (
    Callable,
    Collection,
    Iterable,
    Mapping,
    MutableMapping,
    MutableSequence,
    MutableSet,
    Sequence,
)
from collections.abc import Set as AbstractSet
from dataclasses import dataclass
from enum import StrEnum
from typing import (
    Annotated,
    Any,
    Final,
    ForwardRef,
    Literal,
    TypeAliasType,
    TypeVar,
    Union,
    Unpack,
    get_args,
    get_origin,
)

from pydantic import Base64Bytes, BaseModel, NaiveDatetime, RootModel
from pydantic.fields import FieldInfo
from pydantic_core import PydanticUndefined

from cdm_data_loaders.converters.sources.pydantic.model_synthesis import (
    ModelFieldSpec,
    SchemaBuildContext,
    coerce_structured_type_to_model,
    ensure_concrete_model,
    ensure_model_complete,
    extract_class_docstring,
)

VARIADIC_TUPLE_ARGS_LEN: Final[int] = 2
MAP_TYPE_ARGS_LEN: Final[int] = 2

NAIVE_DATETIME_MARKERS: Final[tuple[object, ...]] = (
    get_args(NaiveDatetime)[1:] if get_origin(NaiveDatetime) is Annotated else ()
)
BASE64_BYTES_MARKERS: Final[tuple[object, ...]] = (
    get_args(Base64Bytes)[1:] if get_origin(Base64Bytes) is Annotated else ()
)

SEQUENCE_ORIGINS: Final[frozenset[object]] = frozenset(
    {
        list,
        set,
        frozenset,
        deque,
        Sequence,
        MutableSequence,
        Iterable,
        Collection,
        AbstractSet,
        MutableSet,
    }
)

MAP_ORIGINS: Final[frozenset[object]] = frozenset(
    {
        dict,
        defaultdict,
        OrderedDict,
        Mapping,
        MutableMapping,
    }
)

CALLABLE_ORIGINS: Final[frozenset[object]] = frozenset(
    {
        Callable,
        get_origin(Callable[[], None]),
        re.Pattern,
    }
)


@dataclass(frozen=True, slots=True)
class ParsedAnnotation:
    """Unwrapped annotation paired with extracted Pydantic metadata."""

    annotation: Any
    description: str | None = None
    max_digits: int | None = None
    decimal_places: int | None = None
    is_naive_datetime: bool = False
    is_base64_bytes: bool = False


def is_union_origin(origin: object) -> bool:
    """Return ``True`` if ``origin`` represents a ``Union`` or ``types.UnionType``.

    :param origin: Typing origin object.
    :type origin: object
    :return: ``True`` if ``origin`` is ``Union`` or ``types.UnionType``.
    :rtype: bool
    """
    return origin is Union or origin is types.UnionType


def is_none_type(tp: object) -> bool:
    """Return ``True`` if ``tp`` is ``None`` or ``types.NoneType``.

    :param tp: Candidate type annotation.
    :type tp: object
    :return: ``True`` if ``tp`` represents ``None``.
    :rtype: bool
    """
    return tp is None or tp is type(None)


def substitute_typevars(tp: object, mapping: Mapping[object, object]) -> object:
    """Substitute ``TypeVar`` parameters in a parameterized generic type alias.

    :param tp: Generic type expression.
    :type tp: object
    :param mapping: Mapping from ``TypeVar`` to concrete type argument.
    :type mapping: Mapping[object, object]
    :return: Substituted type expression.
    :rtype: object
    """
    if isinstance(tp, TypeVar):
        return mapping.get(tp, tp)
    origin = get_origin(tp)
    args = get_args(tp)
    if not mapping or origin is None or not args:
        return tp
    if origin is Annotated:
        return Annotated[(substitute_typevars(args[0], mapping), *args[1:])]
    new_args = tuple(substitute_typevars(a, mapping) for a in args)
    if is_union_origin(origin):
        return operator.getitem(Union, new_args)
    result = tp
    if callable(getattr(origin, "__class_getitem__", None)) or hasattr(origin, "__getitem__"):
        try:
            result = operator.getitem(origin, new_args)
        except TypeError:
            result = tp
    return result


def collect_meta_items(meta_seq: Iterable[object]) -> list[object]:
    """Flatten metadata items, expanding nested ``FieldInfo.metadata`` lists.

    :param meta_seq: Iterable of annotation metadata items.
    :type meta_seq: Iterable[object]
    :return: Flattened list of metadata objects.
    :rtype: list[object]
    """
    items: list[object] = []
    for item in meta_seq:
        items.append(item)
        if isinstance(item, FieldInfo) and item.metadata:
            items.extend(item.metadata)
    return items


def unwrap_type_alias(curr: object, ctx: SchemaBuildContext) -> tuple[object, SchemaBuildContext] | None:
    """Unwrap one ``TypeAliasType`` or parameterized generic ``TypeAliasType`` step.

    :param curr: Candidate type annotation.
    :type curr: object
    :param ctx: Active schema build context.
    :type ctx: SchemaBuildContext
    :return: Tuple of ``(unwrapped_value, next_ctx)`` or ``None`` if ``curr`` is not a ``TypeAliasType``.
    :rtype: tuple[object, SchemaBuildContext] | None
    """
    if isinstance(curr, TypeAliasType):
        if curr in ctx.active_aliases:
            msg = f"Cyclic recursive type alias without a BaseModel boundary: {curr.__name__}"
            ctx.raise_err("E10", msg)
        return curr.__value__, ctx.with_alias(curr)
    origin = get_origin(curr)
    if isinstance(origin, TypeAliasType):
        if origin in ctx.active_aliases:
            msg = f"Cyclic recursive type alias without a BaseModel boundary: {origin.__name__}"
            ctx.raise_err("E10", msg)
        type_params = getattr(origin, "__type_params__", ())
        tv_map = dict(zip(type_params, get_args(curr), strict=False))
        return substitute_typevars(origin.__value__, tv_map), ctx.with_alias(origin)
    return None


def unwrap_step(
    curr: object,
    ctx: SchemaBuildContext,
    meta_items: list[object],
) -> tuple[object, SchemaBuildContext, bool]:
    """Perform a single unwrapping step on ``curr``, returning ``(next_type, next_ctx, changed)``.

    :param curr: Current type annotation.
    :type curr: object
    :param ctx: Active schema build context.
    :type ctx: SchemaBuildContext
    :param meta_items: Mutable list accumulating unwrapped metadata items.
    :type meta_items: list[object]
    :return: Tuple of ``(next_type, next_ctx, changed)``.
    :rtype: tuple[object, SchemaBuildContext, bool]
    """
    if isinstance(curr, str):
        curr = ForwardRef(curr)
    if isinstance(curr, ForwardRef):
        ref_name = curr.__forward_arg__
        if ref_name in ctx.module_ns:
            return ctx.module_ns[ref_name], ctx, True
        msg = f"Unresolved forward reference: {ref_name!r}"
        ctx.raise_err("E9", msg)
    if get_origin(curr) is Annotated:
        args = get_args(curr)
        meta_items.extend(collect_meta_items(args[1:]))
        return args[0], ctx, True
    alias_step = unwrap_type_alias(curr, ctx)
    if alias_step is not None:
        return alias_step[0], alias_step[1], True
    supertype = getattr(curr, "__supertype__", None)
    if supertype is not None:
        return supertype, ctx, True
    if isinstance(curr, type) and issubclass(curr, RootModel) and curr is not RootModel:
        ensure_concrete_model(curr, ctx)
        root_finfo = curr.model_fields["root"]
        meta_items.extend(collect_meta_items([root_finfo]))
        return root_finfo.annotation, ctx, True
    coerced = coerce_structured_type_to_model(curr, ctx)
    return coerced, ctx, coerced is not curr


def extract_parsed_metadata(
    curr: object,
    ctx: SchemaBuildContext,
    meta_items: Sequence[object],
    extra_description: str | None,
) -> tuple[str | None, int | None, int | None, bool, bool]:
    """Extract description, decimal constraints, and marker flags from unwrapped metadata items.

    :param curr: Unwrapped target annotation.
    :type curr: object
    :param ctx: Active schema build context.
    :type ctx: SchemaBuildContext
    :param meta_items: Collected metadata items.
    :type meta_items: Sequence[object]
    :param extra_description: Explicit fallback description override.
    :type extra_description: str | None
    :return: Tuple of ``(description, max_digits, decimal_places, is_naive_dt, is_b64_bytes)``.
    :rtype: tuple[str | None, int | None, int | None, bool, bool]
    """
    is_naive_dt = any(any(item is m or item == m for m in NAIVE_DATETIME_MARKERS) for item in meta_items)
    is_b64_bytes = any(any(item is m or item == m for m in BASE64_BYTES_MARKERS) for item in meta_items)
    description: str | None = extra_description
    max_digits: int | None = None
    decimal_places: int | None = None

    for item in meta_items:
        item_desc = getattr(item, "description", None)
        if description is None and isinstance(item_desc, str) and item_desc:
            description = item_desc
        item_digits = getattr(item, "max_digits", None)
        if max_digits is None and item_digits is not None:
            max_digits = int(item_digits)
        item_places = getattr(item, "decimal_places", None)
        if decimal_places is None and item_places is not None:
            decimal_places = int(item_places)

    if description is None:
        description = extract_class_docstring(curr)
        if description is None and is_union_origin(get_origin(curr)):
            non_null_args = [a for a in get_args(curr) if not is_none_type(a)]
            if len(non_null_args) == 1:
                sub_parsed, _ = unwrap_annotation(non_null_args[0], ctx)
                description = sub_parsed.description

    return description, max_digits, decimal_places, is_naive_dt, is_b64_bytes


def unwrap_annotation(
    annotation: object,
    ctx: SchemaBuildContext,
    field_info: FieldInfo | None = None,
    extra_description: str | None = None,
) -> tuple[ParsedAnnotation, SchemaBuildContext]:
    """Unwrap ``Annotated``, ``NewType``, ``TypeAliasType``, and ``RootModel`` while collecting metadata.

    :param annotation: Raw field or type annotation.
    :type annotation: object
    :param ctx: Active schema build context.
    :type ctx: SchemaBuildContext
    :param field_info: Optional Pydantic ``FieldInfo`` attached to the field.
    :type field_info: FieldInfo | None
    :param extra_description: Optional explicit description override.
    :type extra_description: str | None
    :return: Tuple of ``(ParsedAnnotation, updated_ctx)``.
    :rtype: tuple[ParsedAnnotation, SchemaBuildContext]
    """
    meta_items: list[object] = collect_meta_items([field_info]) if field_info is not None else []
    curr = annotation
    seen_naive = curr is NaiveDatetime or curr == NaiveDatetime
    seen_b64 = curr is Base64Bytes or curr == Base64Bytes

    while True:
        seen_naive = seen_naive or (curr is NaiveDatetime or curr == NaiveDatetime)
        seen_b64 = seen_b64 or (curr is Base64Bytes or curr == Base64Bytes)
        curr, ctx, changed = unwrap_step(curr, ctx, meta_items)
        if not changed:
            break

    desc, max_digits, dec_places, meta_naive, meta_b64 = extract_parsed_metadata(
        curr, ctx, meta_items, extra_description
    )
    return (
        ParsedAnnotation(
            annotation=curr,
            description=desc,
            max_digits=max_digits,
            decimal_places=dec_places,
            is_naive_datetime=seen_naive or meta_naive,
            is_base64_bytes=seen_b64 or meta_b64,
        ),
        ctx,
    )


def flatten_union_branches(
    parsed: ParsedAnnotation,
    ctx: SchemaBuildContext,
) -> list[tuple[ParsedAnnotation, SchemaBuildContext]]:
    """Flatten a union annotation into its unwrapped branch annotations.

    :param parsed: Unwrapped annotation to flatten.
    :type parsed: ParsedAnnotation
    :param ctx: Active schema build context.
    :type ctx: SchemaBuildContext
    :return: Ordered list of ``(ParsedAnnotation, SchemaBuildContext)`` branch pairs.
    :rtype: list[tuple[ParsedAnnotation, SchemaBuildContext]]
    """
    ann = parsed.annotation
    origin = get_origin(ann)
    if not is_union_origin(origin):
        return [(parsed, ctx)]
    branches: list[tuple[ParsedAnnotation, SchemaBuildContext]] = []
    for raw_arg in get_args(ann):
        sub_parsed, sub_ctx = unwrap_annotation(raw_arg, ctx)
        branches.extend(flatten_union_branches(sub_parsed, sub_ctx))
    return branches


def allows_null(annotation: object, ctx: SchemaBuildContext) -> bool:
    """Return ``True`` if ``annotation`` permits ``None``.

    :param annotation: Raw type annotation.
    :type annotation: object
    :param ctx: Active schema build context.
    :type ctx: SchemaBuildContext
    :return: ``True`` if ``None`` is valid for ``annotation``.
    :rtype: bool
    """
    parsed, next_ctx = unwrap_annotation(annotation, ctx)
    tp = parsed.annotation
    if is_none_type(tp):
        return True
    origin = get_origin(tp)
    if origin is Literal:
        return any(arg is None for arg in get_args(tp))
    if is_union_origin(origin):
        branches = flatten_union_branches(parsed, next_ctx)
        return any(allows_null(b.annotation, b_ctx) for b, b_ctx in branches)
    return False


def validate_non_bare_annotation(tp: object, origin: object, ctx: SchemaBuildContext) -> None:
    """Raise an error if ``tp`` is unconstrained, a bare container, or an unsupported callable/pattern.

    :param tp: Unwrapped type annotation.
    :type tp: object
    :param origin: Result of ``get_origin(tp)``.
    :type origin: object
    :param ctx: Active schema build context.
    :type ctx: SchemaBuildContext
    :raises ConversionError: On unconstrained/bare types (``E4``) or runtime callables/patterns (``E7``).
    """
    if tp is Any or tp is object or isinstance(tp, TypeVar):
        msg = f"Unconstrained field annotation: {tp!r}"
        ctx.raise_err("E4", msg)
    if origin in CALLABLE_ORIGINS or tp in CALLABLE_ORIGINS:
        msg = f"Unsupported runtime type annotation: {tp!r}"
        ctx.raise_err("E7", msg)
    if tp in SEQUENCE_ORIGINS or tp is tuple or tp in MAP_ORIGINS:
        msg = f"Bare untyped container {tp!r} requires type parameters"
        ctx.raise_err("E4", msg)


def classify_parsed_kind(parsed: ParsedAnnotation, ctx: SchemaBuildContext) -> str:
    """Classify an unwrapped non-union annotation into ``'null'``, ``'model'``, ``'sequence'``, ``'tuple'``, ``'map'``, or ``'scalar'``.

    :param parsed: Unwrapped annotation.
    :type parsed: ParsedAnnotation
    :param ctx: Active schema build context.
    :type ctx: SchemaBuildContext
    :return: Structural kind label.
    :rtype: str
    """
    tp = parsed.annotation
    if is_none_type(tp):
        return "null"

    origin = get_origin(tp)
    validate_non_bare_annotation(tp, origin, ctx)

    if origin in SEQUENCE_ORIGINS:
        return "sequence"
    if origin is tuple:
        args = get_args(tp)
        return "sequence" if (len(args) == VARIADIC_TUPLE_ARGS_LEN and args[1] is Ellipsis) else "tuple"
    if origin in MAP_ORIGINS:
        return "map"

    if isinstance(tp, type) and issubclass(tp, BaseModel):
        if tp in {BaseModel, RootModel}:
            msg = f"Bare {tp.__name__} cannot be used as a field type"
            ctx.raise_err("E1", msg)
        ensure_concrete_model(tp, ctx)
        return "model"

    return "scalar"


def is_valid_string_map_key(key_ann: object, ctx: SchemaBuildContext) -> bool:
    """Return ``True`` if ``key_ann`` resolves to a valid string key type for ``pa.map_``.

    :param key_ann: Map key type annotation.
    :type key_ann: object
    :param ctx: Active schema build context.
    :type ctx: SchemaBuildContext
    :return: ``True`` if ``key_ann`` resolves to ``str`` or string ``Literal``/``StrEnum``.
    :rtype: bool
    """
    parsed, next_ctx = unwrap_annotation(key_ann, ctx)
    tp = parsed.annotation
    if tp is Any or tp is object or isinstance(tp, TypeVar):
        msg = f"Unconstrained map key type: {tp!r}"
        ctx.raise_err("E4", msg)
    origin = get_origin(tp)
    if is_union_origin(origin):
        branches = flatten_union_branches(parsed, next_ctx)
        non_null = [b for b, _ in branches if not is_none_type(b.annotation)]
        return bool(non_null) and all(is_valid_string_map_key(b.annotation, next_ctx) for b in non_null)
    if origin is Literal:
        args = get_args(tp)
        return bool(args) and all(isinstance(a, str | StrEnum) for a in args)
    return isinstance(tp, type) and issubclass(tp, str)


def is_variadic_unpack(arg: object) -> bool:
    """Return ``True`` if ``arg`` is a PEP 646 ``*tuple[...]`` unpack inside a tuple type.

    :param arg: Tuple element type argument.
    :type arg: object
    :return: ``True`` if ``arg`` is a variadic unpack or ``Ellipsis``.
    :rtype: bool
    """
    origin = get_origin(arg)
    if origin is Unpack or type(arg).__name__ == "_UnpackGenericAlias":
        inner_args = get_args(arg)
        if not inner_args or inner_args[0] is tuple:
            return True
        inner = inner_args[0]
        if get_origin(inner) is tuple:
            t_args = get_args(inner)
            return len(t_args) == VARIADIC_TUPLE_ARGS_LEN and t_args[1] is Ellipsis
    return arg is Ellipsis


def extract_model_field_specs(
    model: type[BaseModel],
    ctx: SchemaBuildContext,
) -> list[ModelFieldSpec]:
    """Extract ordered active declared and computed fields from ``model``.

    :param model: Concrete Pydantic ``BaseModel`` subclass.
    :type model: type[BaseModel]
    :param ctx: Active schema build context.
    :type ctx: SchemaBuildContext
    :return: Ordered list of active ``ModelFieldSpec`` records.
    :rtype: list[ModelFieldSpec]
    """
    ensure_model_complete(model, ctx)
    if model.model_config.get("extra") == "allow":
        msg = (
            f"Model {model.__name__} declares extra='allow' on a fixed-field BaseModel; "
            "open extra fields cannot be represented in a closed PyArrow struct"
        )
        ctx.raise_err("E6", msg)
    model_ctx = ctx.with_model(model)
    specs: list[ModelFieldSpec] = []

    for field_name, finfo in model.model_fields.items():
        if finfo.exclude is True:
            continue
        out_name = (finfo.serialization_alias or finfo.alias or field_name) if ctx.by_alias else field_name
        ann: object = finfo.annotation if finfo.annotation is not None else Any
        allows_none = allows_null(ann, model_ctx) or (finfo.default is None and not finfo.is_required())
        parsed_ann, _ = unwrap_annotation(ann, model_ctx, field_info=finfo)
        specs.append(
            ModelFieldSpec(
                output_name=out_name,
                annotation=ann,
                field_info=finfo,
                description=parsed_ann.description,
                required_non_null=not allows_none,
                context=model_ctx,
            )
        )

    for comp_name, comp_info in model.model_computed_fields.items():
        if comp_info.repr is False and getattr(comp_info, "exclude", False):
            continue
        out_name = (comp_info.alias or comp_name) if ctx.by_alias else comp_name
        ret_type: object = comp_info.return_type
        if ret_type is PydanticUndefined or ret_type is None:
            fget = getattr(comp_info.wrapped_property, "fget", None)
            ret_type = fget.__annotations__.get("return", Any) if fget is not None else Any
        if ret_type is PydanticUndefined:
            ret_type = Any
        allows_none = allows_null(ret_type, model_ctx)
        specs.append(
            ModelFieldSpec(
                output_name=out_name,
                annotation=ret_type,
                field_info=None,
                description=comp_info.description,
                required_non_null=not allows_none,
                context=model_ctx,
            )
        )

    return specs
