"""Read Pydantic v2 BaseModel definitions into the converter IR."""

import inspect
import re
import sys
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
from dataclasses import MISSING, dataclass, field, is_dataclass, replace
from dataclasses import fields as dc_fields
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from enum import Enum, IntEnum, StrEnum
from ipaddress import (
    IPv4Address,
    IPv4Interface,
    IPv4Network,
    IPv6Address,
    IPv6Interface,
    IPv6Network,
)
from pathlib import PurePath
from typing import (
    Annotated,
    Any,
    Final,
    ForwardRef,
    Literal,
    NewType,
    NoReturn,
    NotRequired,
    Required,
    Self,
    TypeAliasType,
    TypeVar,
    Union,
    Unpack,
    get_args,
    get_origin,
    get_type_hints,
    is_typeddict,
)
from uuid import UUID

from frozendict import frozendict
from pydantic import (
    UUID1,
    UUID3,
    UUID4,
    UUID5,
    AnyUrl,
    AwareDatetime,
    Base64Bytes,
    Base64Str,
    BaseModel,
    DirectoryPath,
    EmailStr,
    Field,
    FilePath,
    FutureDate,
    FutureDatetime,
    HttpUrl,
    NaiveDatetime,
    NameEmail,
    NewPath,
    PastDate,
    PastDatetime,
    PydanticSchemaGenerationError,
    PydanticUndefinedAnnotation,
    RootModel,
    SecretBytes,
    SecretStr,
    create_model,
)
from pydantic.fields import FieldInfo
from pydantic.networks import IPvAnyAddress, IPvAnyInterface, IPvAnyNetwork
from pydantic_core import PydanticUndefined, Url

from cdm_data_loaders.converters.core.errors import ConversionError
from cdm_data_loaders.converters.core.guards import (
    DEFAULT_MAX_RECURSION_DEPTH,
    validate_max_recursion_depth,
)
from cdm_data_loaders.converters.core.ir import (
    SPEC_BINARY,
    SPEC_BOOL,
    SPEC_DATE,
    SPEC_FLOAT64,
    SPEC_INT64,
    SPEC_STRING,
    SPEC_TIME,
    SPEC_TS_NAIVE,
    SPEC_TS_UTC,
    NodeHints,
    NodeType,
    Provenance,
    SchemaDocument,
    TypedNode,
)
from cdm_data_loaders.converters.core.ir import (
    Field as SchemaField,
)
from cdm_data_loaders.converters.emitters._tabular_base import DECIMAL128_MAX_PRECISION

VARIADIC_TUPLE_ARGS_LEN: Final[int] = 2
MAP_TYPE_ARGS_LEN: Final[int] = 2

NAIVE_DATETIME_MARKERS: Final[tuple[Any, ...]] = (
    get_args(NaiveDatetime)[1:] if get_origin(NaiveDatetime) is Annotated else ()
)
BASE64_BYTES_MARKERS: Final[tuple[Any, ...]] = get_args(Base64Bytes)[1:] if get_origin(Base64Bytes) is Annotated else ()

SEQUENCE_ORIGINS: Final[frozenset[Any]] = frozenset(
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

MAP_ORIGINS: Final[frozenset[Any]] = frozenset(
    {
        dict,
        defaultdict,
        OrderedDict,
        Mapping,
        MutableMapping,
    }
)

CALLABLE_ORIGINS: Final[frozenset[Any]] = frozenset(
    {
        Callable,
        get_origin(Callable[[], None]),
        re.Pattern,
    }
)

IP_CLASSES: Final[tuple[type[Any], ...]] = (
    IPv4Address,
    IPv6Address,
    IPv4Network,
    IPv6Network,
    IPv4Interface,
    IPv6Interface,
    IPvAnyAddress,
    IPvAnyInterface,
    IPvAnyNetwork,
)


def _raise_code(
    code: str,
    msg: str,
    *,
    error_type: type[ConversionError] = ConversionError,
    error_by_code: Mapping[str, type[ConversionError]] = frozendict(),
    cause: BaseException | None = None,
) -> NoReturn:
    """Raise the configured ``ConversionError`` subclass for ``code`` or fall back to ``error_type``.

    :param code: Specification error code (``E1``-``E12``).
    :type code: str
    :param msg: Diagnostic error message.
    :type msg: str
    :param error_type: Fallback error class.
    :type error_type: type[ConversionError]
    :param error_by_code: Mapping from specification error code to exception class.
    :type error_by_code: Mapping[str, type[ConversionError]]
    :param cause: Optional underlying exception to chain via ``raise ... from cause``.
    :type cause: BaseException | None
    :raises ConversionError: Always raised with the resolved error class.
    """
    cls = error_by_code.get(code, error_type)
    if cause is not None:
        raise cls(msg) from cause
    raise cls(msg)


@dataclass(frozen=True, slots=True)
class _ParsedAnnotation:
    """Unwrapped annotation paired with extracted Pydantic metadata."""

    annotation: Any
    description: str | None = None
    max_digits: int | None = None
    decimal_places: int | None = None
    is_naive_datetime: bool = False
    is_base64_bytes: bool = False


@dataclass(frozen=True, slots=True)
class _SchemaBuildContext:
    """Immutable traversal context for Pydantic to IR schema generation."""

    model_counts: Mapping[type[BaseModel], int] = field(default_factory=frozendict)
    active_aliases: frozenset[Any] = field(default_factory=frozenset)
    max_recursion_depth: int = DEFAULT_MAX_RECURSION_DEPTH
    by_alias: bool = False
    module_ns: Mapping[str, Any] = field(default_factory=frozendict)
    error_type: type[ConversionError] = ConversionError
    error_by_code: Mapping[str, type[ConversionError]] = field(default_factory=frozendict)

    def raise_err(self, code: str, msg: str, *, cause: BaseException | None = None) -> NoReturn:
        """Raise the configured exception subclass for ``code`` or fall back to ``error_type``.

        :param code: Specification error code (``E1``-``E12``).
        :type code: str
        :param msg: Diagnostic error message.
        :type msg: str
        :param cause: Optional underlying exception to chain via ``raise ... from cause``.
        :type cause: BaseException | None
        :raises ConversionError: Always raised with the resolved error class.
        """
        _raise_code(
            code,
            msg,
            error_type=self.error_type,
            error_by_code=self.error_by_code,
            cause=cause,
        )

    def with_model(self, model: type[BaseModel]) -> Self:
        """Return a new context incrementing the expansion count for ``model`` and resetting alias cycle state."""
        mod = sys.modules.get(model.__module__)
        ns = frozendict(vars(mod)) if mod is not None else self.module_ns
        return type(self)(
            model_counts=frozendict({**self.model_counts, model: self.model_counts.get(model, 0) + 1}),
            active_aliases=frozenset(),
            max_recursion_depth=self.max_recursion_depth,
            by_alias=self.by_alias,
            module_ns=ns,
            error_type=self.error_type,
            error_by_code=self.error_by_code,
        )

    def with_alias(self, alias_obj: Any) -> Self:  # noqa: ANN401
        """Return a new context recording ``alias_obj`` in the active type-alias chain."""
        return type(self)(
            model_counts=self.model_counts,
            active_aliases=self.active_aliases | {alias_obj},
            max_recursion_depth=self.max_recursion_depth,
            by_alias=self.by_alias,
            module_ns=self.module_ns,
            error_type=self.error_type,
            error_by_code=self.error_by_code,
        )


def _ensure_model_complete(model: type[BaseModel], ctx: _SchemaBuildContext | None = None) -> None:
    """Ensure a Pydantic BaseModel subclass has resolved all forward references."""
    if not getattr(model, "__pydantic_complete__", True):
        mod = sys.modules.get(model.__module__)
        ns = vars(mod) if mod is not None else None
        try:
            model.model_rebuild(raise_errors=True, _types_namespace=ns)
        except (PydanticUndefinedAnnotation, NameError, TypeError) as exc:
            msg = f"Unresolved forward reference in model {model.__name__}: {exc}"
            if ctx is not None:
                ctx.raise_err("E9", msg, cause=exc)
            _raise_code("E9", msg, cause=exc)
        except PydanticSchemaGenerationError:
            return
    if not getattr(model, "__pydantic_complete__", True):
        msg = f"Model {model.__name__} remains incomplete after model_rebuild()"
        if ctx is not None:
            ctx.raise_err("E9", msg)
        _raise_code("E9", msg)


def _ensure_concrete_model(model: type[BaseModel], ctx: _SchemaBuildContext) -> None:
    """Ensure ``model`` has resolved forward references and has no unbound generic type parameters.

    :param model: Pydantic ``BaseModel`` subclass to validate.
    :type model: type[BaseModel]
    :param ctx: Active schema build context.
    :type ctx: _SchemaBuildContext
    :raises ConversionError: If forward references cannot be resolved or ``model`` has unbound generic type parameters.
    """
    _ensure_model_complete(model, ctx)
    generic_meta = getattr(model, "__pydantic_generic_metadata__", {})
    if generic_meta.get("parameters"):
        msg = (
            f"Unparameterized generic model {model.__name__} has unbound type parameters: {generic_meta['parameters']}"
        )
        ctx.raise_err("E4", msg)


def _validate_model_class(model: object, ctx: _SchemaBuildContext) -> type[BaseModel]:
    """Validate that ``model`` is a concrete Pydantic ``BaseModel`` subclass."""
    if not isinstance(model, type) or not issubclass(model, BaseModel) or model in {BaseModel, RootModel}:
        msg = f"Expected a concrete Pydantic BaseModel subclass, got {model!r}"
        ctx.raise_err("E1", msg)
    _ensure_concrete_model(model, ctx)
    return model


def _substitute_typevars(tp: Any, mapping: Mapping[Any, Any]) -> Any:  # noqa: ANN401
    """Substitute TypeVars in a parameterized generic type alias."""
    if isinstance(tp, TypeVar):
        return mapping.get(tp, tp)
    origin = get_origin(tp)
    args = get_args(tp)
    if not mapping or origin is None or not args:
        return tp
    if origin is Annotated:
        return Annotated[(_substitute_typevars(args[0], mapping), *args[1:])]
    new_args = tuple(_substitute_typevars(a, mapping) for a in args)
    if origin is types.UnionType or origin is Union:
        return Union[new_args]  # noqa: UP007
    try:
        return origin[new_args]
    except TypeError:
        return tp


def _collect_meta_items(meta_seq: Iterable[Any]) -> list[Any]:
    """Flatten metadata items, expanding nested FieldInfo.metadata lists."""
    items: list[Any] = []
    for item in meta_seq:
        items.append(item)
        if isinstance(item, FieldInfo) and item.metadata:
            items.extend(item.metadata)
    return items


def _clean_docstring(cls: type[Any]) -> str | None:
    """Return a cleaned class docstring if defined on ``cls``, excluding generated signature docstrings.

    :param cls: Class to inspect.
    :type cls: type[Any]
    :return: Cleaned docstring or ``None``.
    :rtype: str | None
    """
    raw_doc = cls.__dict__.get("__doc__")
    if not raw_doc or not isinstance(raw_doc, str):
        return None
    cleaned = inspect.cleandoc(raw_doc).strip()
    return None if (not cleaned or cleaned.startswith(f"{cls.__name__}(")) else cleaned


def _extract_class_docstring(tp: Any) -> str | None:  # noqa: ANN401
    """Extract a cleaned class docstring from a ``BaseModel`` or ``Enum`` subclass when defined on ``tp``."""
    if not isinstance(tp, type) or not (issubclass(tp, BaseModel) or issubclass(tp, Enum)):
        return None
    if tp in {BaseModel, RootModel, Enum, IntEnum, StrEnum}:
        return None
    return _clean_docstring(tp)


def _safe_type_hints(tp: type[Any], module_ns: Mapping[str, Any]) -> dict[str, Any]:
    """Resolve type hints for ``tp`` with fallback to ``__annotations__``."""
    try:
        return get_type_hints(tp, globalns=dict(module_ns), include_extras=True)
    except Exception:  # noqa: BLE001
        return dict(getattr(tp, "__annotations__", {}))


def _typeddict_to_model(tp: type[Any], ctx: _SchemaBuildContext) -> type[BaseModel]:
    """Convert a ``TypedDict`` class into a dynamic ``BaseModel`` subclass."""
    hints = _safe_type_hints(tp, ctx.module_ns)
    required_keys: frozenset[str] = getattr(tp, "__required_keys__", frozenset(hints.keys()))
    field_defs: dict[str, Any] = {}
    for fname, raw_ann in hints.items():
        origin = get_origin(raw_ann)
        if origin is Required:
            inner_ann, is_req = get_args(raw_ann)[0], True
        elif origin is NotRequired:
            inner_ann, is_req = get_args(raw_ann)[0], False
        else:
            inner_ann, is_req = raw_ann, fname in required_keys
        field_defs[fname] = (inner_ann, ... if is_req else None)
    model_cls = create_model(tp.__name__, __module__=tp.__module__, **field_defs)
    raw_doc = tp.__dict__.get("__doc__")
    model_cls.__doc__ = raw_doc if isinstance(raw_doc, str) else None
    return model_cls


def _dataclass_to_model(tp: type[Any], ctx: _SchemaBuildContext) -> type[BaseModel]:
    """Convert a stdlib ``@dataclass`` class into a dynamic ``BaseModel`` subclass."""
    hints = _safe_type_hints(tp, ctx.module_ns)
    field_defs: dict[str, Any] = {}
    for dc_f in dc_fields(tp):
        f_ann = hints.get(dc_f.name, dc_f.type)
        if dc_f.default is not MISSING:
            default_spec: Any = dc_f.default
        elif dc_f.default_factory is not MISSING:
            default_spec = Field(default_factory=dc_f.default_factory)
        else:
            default_spec = ...
        field_defs[dc_f.name] = (f_ann, default_spec)
    model_cls = create_model(tp.__name__, __module__=tp.__module__, **field_defs)
    model_cls.__doc__ = _clean_docstring(tp)
    return model_cls


def _coerce_structured_type_to_model(tp: Any, ctx: _SchemaBuildContext) -> Any:  # noqa: ANN401
    """Convert nested ``TypedDict`` or stdlib ``@dataclass`` classes into ``BaseModel`` subclasses."""
    if not isinstance(tp, type) or issubclass(tp, BaseModel):
        return tp
    if is_typeddict(tp):
        return _typeddict_to_model(tp, ctx)
    if is_dataclass(tp):
        return _dataclass_to_model(tp, ctx)
    return tp


def _unwrap_type_alias(curr: Any, ctx: _SchemaBuildContext) -> tuple[Any, _SchemaBuildContext] | None:  # noqa: ANN401
    """Unwrap one ``TypeAliasType`` or parameterized generic ``TypeAliasType`` step."""
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
        return _substitute_typevars(origin.__value__, tv_map), ctx.with_alias(origin)
    return None


def _unwrap_step(
    curr: Any,  # noqa: ANN401
    ctx: _SchemaBuildContext,
    meta_items: list[Any],
) -> tuple[Any, _SchemaBuildContext, bool]:
    """Perform a single unwrapping step on ``curr``, returning ``(next_type, next_ctx, changed)``."""
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
        meta_items.extend(_collect_meta_items(args[1:]))
        return args[0], ctx, True
    alias_step = _unwrap_type_alias(curr, ctx)
    if alias_step is not None:
        return alias_step[0], alias_step[1], True
    if isinstance(curr, NewType) or hasattr(curr, "__supertype__"):
        return curr.__supertype__, ctx, True
    if isinstance(curr, type) and issubclass(curr, RootModel) and curr is not RootModel:
        _ensure_concrete_model(curr, ctx)
        root_finfo = curr.model_fields["root"]
        meta_items.extend(_collect_meta_items([root_finfo]))
        return root_finfo.annotation, ctx, True
    coerced = _coerce_structured_type_to_model(curr, ctx)
    return coerced, ctx, coerced is not curr


def _extract_parsed_metadata(
    curr: Any,  # noqa: ANN401
    ctx: _SchemaBuildContext,
    meta_items: Sequence[Any],
    extra_description: str | None,
) -> tuple[str | None, int | None, int | None, bool, bool]:
    """Extract description, decimal constraints, and marker flags from unwrapped metadata items."""
    is_naive_dt = any(any(item is m or item == m for m in NAIVE_DATETIME_MARKERS) for item in meta_items)
    is_b64_bytes = any(any(item is m or item == m for m in BASE64_BYTES_MARKERS) for item in meta_items)
    description: str | None = extra_description
    max_digits: int | None = None
    decimal_places: int | None = None

    for item in meta_items:
        if description is None and getattr(item, "description", None):
            description = item.description
        if max_digits is None and getattr(item, "max_digits", None) is not None:
            max_digits = int(item.max_digits)
        if decimal_places is None and getattr(item, "decimal_places", None) is not None:
            decimal_places = int(item.decimal_places)

    if description is None:
        description = _extract_class_docstring(curr)
        if description is None and _is_union_origin(get_origin(curr)):
            non_null_args = [a for a in get_args(curr) if not _is_none_type(a)]
            if len(non_null_args) == 1:
                sub_parsed, _ = _unwrap_annotation(non_null_args[0], ctx)
                description = sub_parsed.description

    return description, max_digits, decimal_places, is_naive_dt, is_b64_bytes


def _unwrap_annotation(
    annotation: Any,  # noqa: ANN401
    ctx: _SchemaBuildContext,
    field_info: FieldInfo | None = None,
    extra_description: str | None = None,
) -> tuple[_ParsedAnnotation, _SchemaBuildContext]:
    """Unwrap ``Annotated``, ``NewType``, ``TypeAliasType``, and ``RootModel`` while collecting metadata."""
    meta_items: list[Any] = _collect_meta_items([field_info]) if field_info is not None else []
    curr = annotation
    seen_naive = curr is NaiveDatetime or curr == NaiveDatetime
    seen_b64 = curr is Base64Bytes or curr == Base64Bytes

    while True:
        seen_naive = seen_naive or (curr is NaiveDatetime or curr == NaiveDatetime)
        seen_b64 = seen_b64 or (curr is Base64Bytes or curr == Base64Bytes)
        curr, ctx, changed = _unwrap_step(curr, ctx, meta_items)
        if not changed:
            break

    desc, max_digits, dec_places, meta_naive, meta_b64 = _extract_parsed_metadata(
        curr, ctx, meta_items, extra_description
    )
    return (
        _ParsedAnnotation(
            annotation=curr,
            description=desc,
            max_digits=max_digits,
            decimal_places=dec_places,
            is_naive_datetime=seen_naive or meta_naive,
            is_base64_bytes=seen_b64 or meta_b64,
        ),
        ctx,
    )


def _is_union_origin(origin: Any) -> bool:  # noqa: ANN401
    """Return True if ``origin`` represents a ``Union`` or ``types.UnionType``."""
    return origin is Union or origin is types.UnionType


def _flatten_union_branches(
    parsed: _ParsedAnnotation,
    ctx: _SchemaBuildContext,
) -> list[tuple[_ParsedAnnotation, _SchemaBuildContext]]:
    """Flatten a union annotation into its unwrapped branch annotations."""
    ann = parsed.annotation
    origin = get_origin(ann)
    if not _is_union_origin(origin):
        return [(parsed, ctx)]
    branches: list[tuple[_ParsedAnnotation, _SchemaBuildContext]] = []
    for raw_arg in get_args(ann):
        sub_parsed, sub_ctx = _unwrap_annotation(raw_arg, ctx)
        branches.extend(_flatten_union_branches(sub_parsed, sub_ctx))
    return branches


def _is_none_type(tp: Any) -> bool:  # noqa: ANN401
    """Return True if ``tp`` is ``None`` or ``types.NoneType``."""
    return tp is None or tp is type(None)


def _allows_null(annotation: Any, ctx: _SchemaBuildContext) -> bool:  # noqa: ANN401
    """Return True if ``annotation`` permits ``None``."""
    parsed, next_ctx = _unwrap_annotation(annotation, ctx)
    tp = parsed.annotation
    if _is_none_type(tp):
        return True
    origin = get_origin(tp)
    if origin is Literal:
        return any(arg is None for arg in get_args(tp))
    if _is_union_origin(origin):
        branches = _flatten_union_branches(parsed, next_ctx)
        return any(_allows_null(b.annotation, b_ctx) for b, b_ctx in branches)
    return False


def _unify_scalar_specs(
    resolved: Sequence[tuple[NodeType, NodeHints, str]],
) -> tuple[NodeType, NodeHints, str]:
    """Unify multiple scalar ``(NodeType, NodeHints, pydantic_type)`` tuples according to ``R-UNION-SCALAR``."""
    unique_labels = list(dict.fromkeys(label for _, _, label in resolved))
    joined_label = " | ".join(unique_labels)

    unique_specs = list(dict.fromkeys((nt, h) for nt, h, _ in resolved))
    if len(unique_specs) == 1:
        return unique_specs[0][0], unique_specs[0][1], joined_label

    if all(h.logical_type == "decimal" for _, h in unique_specs):
        new_scale = max(h.scale or 0 for _, h in unique_specs)
        max_int_digits = max((h.precision or 0) - (h.scale or 0) for _, h in unique_specs)
        new_precision = max_int_digits + new_scale
        if new_precision <= DECIMAL128_MAX_PRECISION:
            return (
                "number",
                NodeHints(logical_type="decimal", precision=new_precision, scale=new_scale),
                joined_label,
            )
        return *SPEC_FLOAT64, joined_label

    if all(h.logical_type in {"long", "double", "decimal"} for _, h in unique_specs):
        return *SPEC_FLOAT64, joined_label

    if all(h.logical_type in {"timestamp-with-tz", "timestamp-without-tz"} for _, h in unique_specs):
        return *SPEC_TS_UTC, joined_label

    return *SPEC_STRING, joined_label


def _scalar_spec_for_runtime_value(
    val: object,
    error_msg: str,
    ctx: _SchemaBuildContext,
) -> tuple[NodeType, NodeHints, str]:
    """Map a runtime ``Literal`` or ``Enum`` member value to ``(NodeType, NodeHints, pydantic_type)``.

    :param val: Runtime scalar value.
    :type val: object
    :param error_msg: Diagnostic message if ``val`` is not a supported scalar primitive.
    :type error_msg: str
    :param ctx: Active schema build context.
    :type ctx: _SchemaBuildContext
    :return: Tuple of ``(NodeType, NodeHints, pydantic_type)``.
    :rtype: tuple[NodeType, NodeHints, str]
    :raises ConversionError: If ``val`` is not ``bool``, ``int``, ``float``, ``str``, or ``bytes``.
    """
    if isinstance(val, bool):
        return *SPEC_BOOL, "boolean"
    if isinstance(val, int):
        return *SPEC_INT64, "integer"
    if isinstance(val, float):
        return *SPEC_FLOAT64, "number"
    if isinstance(val, str):
        return *SPEC_STRING, "string"
    if isinstance(val, bytes):
        return *SPEC_BINARY, "binary"
    raise ctx.error_by_code.get("E7", ctx.error_type)(error_msg)


def _resolve_literal_scalar(tp: Any, ctx: _SchemaBuildContext) -> tuple[NodeType, NodeHints, str]:  # noqa: ANN401
    """Resolve a ``Literal[...]`` type to ``(NodeType, NodeHints, pydantic_type)``."""
    raw_values: list[Any] = []

    def _collect(node: Any) -> None:  # noqa: ANN401
        for arg in get_args(node):
            if get_origin(arg) is Literal:
                _collect(arg)
            else:
                raw_values.append(arg)

    _collect(tp)
    non_null = [v for v in raw_values if v is not None]
    if not non_null:
        msg = "Literal[None] has no non-null values and cannot be converted to a PyArrow column"
        ctx.raise_err("E7", msg)

    resolved = [
        _scalar_spec_for_runtime_value(
            val.value if isinstance(val, Enum) else val,
            f"Unsupported Literal member value: {val!r}",
            ctx,
        )
        for val in non_null
    ]
    return _unify_scalar_specs(resolved)


def _resolve_enum_scalar(enum_cls: type[Enum], ctx: _SchemaBuildContext) -> tuple[NodeType, NodeHints, str]:
    """Resolve an ``Enum`` subclass to ``(NodeType, NodeHints, pydantic_type)``."""
    if issubclass(enum_cls, StrEnum) or issubclass(enum_cls, str):
        return *SPEC_STRING, "string"
    if issubclass(enum_cls, IntEnum) or (issubclass(enum_cls, int) and not issubclass(enum_cls, bool)):
        return *SPEC_INT64, "integer"
    if issubclass(enum_cls, float):
        return *SPEC_FLOAT64, "number"

    members = list(enum_cls)
    if not members:
        msg = f"Empty Enum {enum_cls.__name__} has no members"
        ctx.raise_err("E7", msg)

    resolved = [
        _scalar_spec_for_runtime_value(
            member.value,
            f"Unsupported Enum member value {member.value!r} in {enum_cls.__name__}",
            ctx,
        )
        for member in members
    ]
    return _unify_scalar_specs(resolved)


DIRECT_SCALAR_MARKERS: Final[frozendict[Any, tuple[NodeType, NodeHints, str]]] = frozendict(
    {
        NaiveDatetime: (*SPEC_TS_NAIVE, "date-time-naive"),
        AwareDatetime: (*SPEC_TS_UTC, "date-time"),
        PastDatetime: (*SPEC_TS_UTC, "date-time"),
        FutureDatetime: (*SPEC_TS_UTC, "date-time"),
        PastDate: (*SPEC_DATE, "date"),
        FutureDate: (*SPEC_DATE, "date"),
        Base64Bytes: (*SPEC_BINARY, "binary (base64)"),
        UUID1: (*SPEC_STRING, "uuid"),
        UUID3: (*SPEC_STRING, "uuid"),
        UUID4: (*SPEC_STRING, "uuid"),
        UUID5: (*SPEC_STRING, "uuid"),
        FilePath: (*SPEC_STRING, "path"),
        DirectoryPath: (*SPEC_STRING, "path"),
        NewPath: (*SPEC_STRING, "path"),
        IPvAnyAddress: (*SPEC_STRING, "ip"),
        IPvAnyInterface: (*SPEC_STRING, "ip"),
        IPvAnyNetwork: (*SPEC_STRING, "ip"),
        HttpUrl: (*SPEC_STRING, "string"),
        Base64Str: (*SPEC_STRING, "string"),
    }
)

SUBCLASS_SCALAR_TABLE: Final[tuple[tuple[type[Any] | tuple[type[Any], ...], tuple[NodeType, NodeHints], str], ...]] = (
    (bool, SPEC_BOOL, "boolean"),
    (int, SPEC_INT64, "integer"),
    (float, SPEC_FLOAT64, "number"),
    (date, SPEC_DATE, "date"),
    (time, SPEC_TIME, "time"),
    (timedelta, SPEC_STRING, "duration"),
    (UUID, SPEC_STRING, "uuid"),
    (PurePath, SPEC_STRING, "path"),
    (IP_CLASSES, SPEC_STRING, "ip"),
    ((str, SecretStr, AnyUrl, Url, NameEmail, EmailStr), SPEC_STRING, "string"),
)


def _resolve_decimal_scalar(
    parsed: _ParsedAnnotation,
    ctx: _SchemaBuildContext,
) -> tuple[NodeType, NodeHints, str]:
    """Resolve a ``Decimal`` annotation and its ``max_digits`` / ``decimal_places`` metadata."""
    if parsed.max_digits is not None and not (1 <= parsed.max_digits <= DECIMAL128_MAX_PRECISION):
        msg = f"Unsupported Arrow decimal128 precision: max_digits={parsed.max_digits} (must be 1..38)"
        ctx.raise_err("E11", msg)
    if parsed.decimal_places is not None and (
        parsed.decimal_places < 0 or (parsed.max_digits is not None and parsed.decimal_places > parsed.max_digits)
    ):
        msg = (
            f"Unsupported Arrow decimal128 scale: max_digits={parsed.max_digits}, "
            f"decimal_places={parsed.decimal_places}"
        )
        ctx.raise_err("E11", msg)
    if parsed.max_digits is not None and parsed.decimal_places is not None:
        return (
            "number",
            NodeHints(logical_type="decimal", precision=parsed.max_digits, scale=parsed.decimal_places),
            "decimal",
        )
    return *SPEC_FLOAT64, "number"


def _resolve_class_scalar(
    tp: type[Any],
    parsed: _ParsedAnnotation,
    ctx: _SchemaBuildContext,
) -> tuple[NodeType, NodeHints, str] | None:
    """Resolve a class-based scalar ``tp`` to ``(NodeType, NodeHints, pydantic_type)`` or ``None``."""
    if issubclass(tp, Enum):
        return _resolve_enum_scalar(tp, ctx)
    if issubclass(tp, Decimal):
        return _resolve_decimal_scalar(parsed, ctx)
    if issubclass(tp, datetime):
        return (*SPEC_TS_NAIVE, "date-time-naive") if parsed.is_naive_datetime else (*SPEC_TS_UTC, "date-time")
    if issubclass(tp, bytes | bytearray | SecretBytes):
        return *SPEC_BINARY, ("binary (base64)" if parsed.is_base64_bytes else "binary")
    for target_cls, spec, label in SUBCLASS_SCALAR_TABLE:
        if issubclass(tp, target_cls):
            return *spec, label
    return None


def _resolve_parsed_scalar(
    parsed: _ParsedAnnotation,
    ctx: _SchemaBuildContext,
) -> tuple[NodeType, NodeHints, str]:
    """Resolve an unwrapped scalar ``_ParsedAnnotation`` to ``(NodeType, NodeHints, pydantic_type)``."""
    tp = parsed.annotation
    if _is_none_type(tp):
        msg = "Standalone NoneType cannot be converted to a PyArrow column"
        ctx.raise_err("E7", msg)
    if tp is Any or tp is object or isinstance(tp, TypeVar):
        msg = f"Unconstrained scalar type: {tp!r}"
        ctx.raise_err("E4", msg)

    if get_origin(tp) is Literal:
        return _resolve_literal_scalar(tp, ctx)

    try:
        direct = DIRECT_SCALAR_MARKERS.get(tp)
    except TypeError:
        direct = None
    if direct is not None:
        return direct

    if isinstance(tp, type):
        resolved = _resolve_class_scalar(tp, parsed, ctx)
        if resolved is not None:
            return resolved

    msg = f"Unsupported field type annotation: {tp!r}"
    raise ctx.error_by_code.get("E7", ctx.error_type)(msg)


def _validate_non_bare_annotation(tp: Any, origin: Any, ctx: _SchemaBuildContext) -> None:  # noqa: ANN401
    """Raise an error if ``tp`` is unconstrained, a bare container, or an unsupported callable/pattern."""
    if tp is Any or tp is object or isinstance(tp, TypeVar):
        msg = f"Unconstrained field annotation: {tp!r}"
        ctx.raise_err("E4", msg)
    if origin in CALLABLE_ORIGINS or tp in CALLABLE_ORIGINS:
        msg = f"Unsupported runtime type annotation: {tp!r}"
        ctx.raise_err("E7", msg)
    if tp in SEQUENCE_ORIGINS or tp is tuple or tp in MAP_ORIGINS:
        msg = f"Bare untyped container {tp!r} requires type parameters"
        ctx.raise_err("E4", msg)


def _classify_parsed_kind(parsed: _ParsedAnnotation, ctx: _SchemaBuildContext) -> str:
    """Classify an unwrapped non-union annotation into ``'null'``, ``'model'``, ``'sequence'``, ``'tuple'``, ``'map'``, or ``'scalar'``."""
    tp = parsed.annotation
    if _is_none_type(tp):
        return "null"

    origin = get_origin(tp)
    _validate_non_bare_annotation(tp, origin, ctx)

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
        _ensure_concrete_model(tp, ctx)
        return "model"

    return "scalar"


def _is_valid_string_map_key(key_ann: Any, ctx: _SchemaBuildContext) -> bool:  # noqa: ANN401
    """Return True if ``key_ann`` resolves to a valid string key type for ``pa.map_``."""
    parsed, next_ctx = _unwrap_annotation(key_ann, ctx)
    tp = parsed.annotation
    if tp is Any or tp is object or isinstance(tp, TypeVar):
        msg = f"Unconstrained map key type: {tp!r}"
        ctx.raise_err("E4", msg)
    origin = get_origin(tp)
    if _is_union_origin(origin):
        branches = _flatten_union_branches(parsed, next_ctx)
        non_null = [b for b, _ in branches if not _is_none_type(b.annotation)]
        return bool(non_null) and all(_is_valid_string_map_key(b.annotation, next_ctx) for b in non_null)
    if origin is Literal:
        args = get_args(tp)
        return bool(args) and all(isinstance(a, str | StrEnum) for a in args)
    return isinstance(tp, type) and issubclass(tp, str)


def _is_variadic_unpack(arg: Any) -> bool:  # noqa: ANN401
    """Return True if ``arg`` is a PEP 646 ``*tuple[...]`` unpack inside a tuple type."""
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


@dataclass(frozen=True, slots=True)
class _ModelFieldSpec:
    """Internal specification of an active field extracted from a ``BaseModel``."""

    output_name: str
    annotation: Any
    field_info: FieldInfo | None
    description: str | None
    required_non_null: bool
    context: _SchemaBuildContext


def _extract_model_field_specs(
    model: type[BaseModel],
    ctx: _SchemaBuildContext,
) -> list[_ModelFieldSpec]:
    """Extract ordered active declared and computed fields from ``model``."""
    _ensure_model_complete(model, ctx)
    if model.model_config.get("extra") == "allow":
        msg = (
            f"Model {model.__name__} declares extra='allow' on a fixed-field BaseModel; "
            "open extra fields cannot be represented in a closed PyArrow struct"
        )
        ctx.raise_err("E6", msg)
    model_ctx = ctx.with_model(model)
    specs: list[_ModelFieldSpec] = []

    for field_name, finfo in model.model_fields.items():
        if finfo.exclude is True:
            continue
        out_name = (finfo.serialization_alias or finfo.alias or field_name) if ctx.by_alias else field_name
        ann = finfo.annotation if finfo.annotation is not None else Any
        allows_none = _allows_null(ann, model_ctx) or (finfo.default is None and not finfo.is_required())
        parsed_ann, _ = _unwrap_annotation(ann, model_ctx, field_info=finfo)
        specs.append(
            _ModelFieldSpec(
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
        ret_type = comp_info.return_type
        if ret_type is PydanticUndefined or ret_type is None:
            fget = getattr(comp_info.wrapped_property, "fget", None)
            ret_type = fget.__annotations__.get("return", Any) if fget is not None else Any
        if ret_type is PydanticUndefined:
            ret_type = Any
        allows_none = _allows_null(ret_type, model_ctx)
        specs.append(
            _ModelFieldSpec(
                output_name=out_name,
                annotation=ret_type,
                field_info=None,
                description=comp_info.description,
                required_non_null=not allows_none,
                context=model_ctx,
            )
        )

    return specs


def _check_no_forbidden_types_in_recursive_model(
    model: type[BaseModel],
    ctx: _SchemaBuildContext,
    visited: set[type[BaseModel]] | None = None,
) -> None:
    """Raise ``E10`` if a recursive model cycle contains non-JSON-native scalar types."""
    if visited is None:
        visited = set()
    if model in visited:
        return
    visited.add(model)
    for spec in _extract_model_field_specs(model, ctx):
        _check_no_forbidden_types_in_annotation(spec.annotation, spec.context, visited)


def _check_no_forbidden_types_in_annotation(
    ann: Any,  # noqa: ANN401
    ctx: _SchemaBuildContext,
    visited: set[type[BaseModel]],
) -> None:
    """Inspect an annotation inside a recursive model cycle for non-JSON-native scalar types."""
    parsed, next_ctx = _unwrap_annotation(ann, ctx)
    tp = parsed.annotation
    if _is_none_type(tp):
        return
    origin = get_origin(tp)
    if _is_union_origin(origin):
        for branch_parsed, branch_ctx in _flatten_union_branches(parsed, next_ctx):
            _check_no_forbidden_types_in_annotation(branch_parsed.annotation, branch_ctx, visited)
        return
    kind = _classify_parsed_kind(parsed, next_ctx)
    if kind == "model":
        _check_no_forbidden_types_in_recursive_model(tp, next_ctx, visited)
        return
    if kind in {"sequence", "tuple", "map"}:
        for arg in get_args(tp):
            if arg is not Ellipsis:
                _check_no_forbidden_types_in_annotation(arg, next_ctx, visited)
        return
    _, hints, _ = _resolve_parsed_scalar(parsed, next_ctx)
    if hints.logical_type in {
        "date",
        "timestamp-with-tz",
        "timestamp-without-tz",
        "time",
        "decimal",
        "binary",
    }:
        msg = f"Non-JSON-serializable scalar type {hints.logical_type} in recursive model cycle"
        next_ctx.raise_err("E10", msg)


def _nodes_same_shape(a: TypedNode, b: TypedNode) -> bool:
    """Return True when two IR nodes emit identical PyArrow DataTypes."""
    if a.type != b.type or a.hints != b.hints:
        return False
    if a.type == "array":
        return (
            a.items is not None
            and b.items is not None
            and a.items.nullable == b.items.nullable
            and _nodes_same_shape(a.items, b.items)
        )
    if a.type == "map":
        return (
            a.key_type is not None
            and b.key_type is not None
            and a.value_type is not None
            and b.value_type is not None
            and a.value_type.nullable == b.value_type.nullable
            and _nodes_same_shape(a.key_type, b.key_type)
            and _nodes_same_shape(a.value_type, b.value_type)
        )
    if a.type == "object":
        return len(a.properties) == len(b.properties) and all(
            fa.name == fb.name and fa.required == fb.required and _nodes_same_shape(fa.node, fb.node)
            for fa, fb in zip(a.properties, b.properties, strict=True)
        )
    return True


def _build_sequence_node(
    parsed: _ParsedAnnotation,
    ctx: _SchemaBuildContext,
    path: tuple[str | int, ...],
    *,
    nullable: bool,
) -> TypedNode:
    """Build an array ``TypedNode`` from a homogeneous sequence, set, or variadic tuple annotation."""
    args = get_args(parsed.annotation)
    if not args:
        msg = f"Untyped sequence annotation: {parsed.annotation!r}"
        ctx.raise_err("E4", msg)
    item_field = _build_field_ir("item", args[0], ctx, (*path, "element"), required_in_parent=True)
    item_node = replace(item_field.node, annotations=item_field.annotations)
    return TypedNode(
        type="array",
        nullable=nullable,
        items=item_node,
        provenance=Provenance("pydantic", path),
    )


def _build_tuple_node(
    parsed: _ParsedAnnotation,
    ctx: _SchemaBuildContext,
    path: tuple[str | int, ...],
    *,
    nullable: bool,
) -> TypedNode:
    """Build a positional object ``TypedNode`` (``item_0``, ``item_1``, ...) from a fixed-length tuple annotation."""
    args = get_args(parsed.annotation)
    if not args:
        msg = "Empty tuple tuple[()] cannot be converted to a PyArrow struct with zero fields"
        ctx.raise_err("E3", msg)
    if any(_is_variadic_unpack(a) for a in args):
        msg = f"Open tuple mixing positional elements and variadic unpack is not supported: {parsed.annotation!r}"
        ctx.raise_err("E5", msg)

    fields = tuple(
        _build_field_ir(f"item_{idx}", elem_ann, ctx, (*path, f"item_{idx}"), required_in_parent=True)
        for idx, elem_ann in enumerate(args)
    )
    required_names = tuple(f.name for f in fields if f.required)
    return TypedNode(
        type="object",
        nullable=nullable,
        properties=fields,
        required_names=required_names,
        additional_properties=TypedNode(type="never", nullable=False),
        provenance=Provenance("pydantic", path),
    )


def _build_map_node(
    parsed: _ParsedAnnotation,
    ctx: _SchemaBuildContext,
    path: tuple[str | int, ...],
    *,
    nullable: bool,
) -> TypedNode:
    """Build a map ``TypedNode`` from a string-keyed mapping annotation."""
    args = get_args(parsed.annotation)
    if len(args) != MAP_TYPE_ARGS_LEN:
        msg = f"Mapping annotation requires key and value type arguments: {parsed.annotation!r}"
        ctx.raise_err("E4", msg)
    key_ann, val_ann = args
    if not _is_valid_string_map_key(key_ann, ctx):
        msg = f"Map key type must resolve to str, got {key_ann!r}"
        ctx.raise_err("E6", msg)
    key_node = TypedNode(
        type="string",
        nullable=False,
        hints=NodeHints(logical_type="string"),
        provenance=Provenance("pydantic", (*path, "key")),
    )
    val_field = _build_field_ir("value", val_ann, ctx, (*path, "value"), required_in_parent=True)
    val_node = replace(val_field.node, annotations=val_field.annotations)
    return TypedNode(
        type="map",
        nullable=nullable,
        key_type=key_node,
        value_type=val_node,
        provenance=Provenance("pydantic", path),
    )


def _build_merged_models_struct(
    models: Sequence[type[BaseModel]],
    ctx: _SchemaBuildContext,
    path: tuple[str | int, ...],
    *,
    nullable: bool,
    scalar_branches: Sequence[_ParsedAnnotation] = (),
) -> TypedNode:
    """Build an object ``TypedNode`` from one or more ``BaseModel`` branches and optional scalar branches."""
    fields_by_name: dict[str, list[_ModelFieldSpec]] = {}
    branch_non_null_sets: list[set[str]] = []

    for model in models:
        branch_specs = _extract_model_field_specs(model, ctx)
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
            ir_fields.append(
                _build_field_ir(
                    out_name,
                    spec.annotation,
                    spec.context,
                    field_path,
                    field_info=spec.field_info,
                    extra_description=merged_desc,
                    required_in_parent=is_req,
                )
            )
        else:
            branch_parsed_list = [
                _unwrap_annotation(s.annotation, s.context, field_info=s.field_info) for s in spec_list
            ]
            field_nullable = (not is_req) or any(_allows_null(s.annotation, s.context) for s in spec_list)
            node = _build_multi_branch_node(branch_parsed_list, field_path, nullable=field_nullable)
            annotations = {"description": merged_desc} if merged_desc else {}
            prov_meta = dict(node.provenance.metadata) if node.provenance is not None else {}
            ir_fields.append(
                SchemaField(
                    name=out_name,
                    node=node,
                    required=not field_nullable,
                    annotations=annotations,
                    provenance=Provenance("pydantic", field_path, prov_meta),
                )
            )

    if scalar_branches:
        resolved_scalars = [_resolve_parsed_scalar(sb, ctx) for sb in scalar_branches]
        val_node_type, val_hints, val_label = _unify_scalar_specs(resolved_scalars)
        val_path = (*path, "_value")
        val_prov = Provenance("pydantic", val_path, {"pydantic_type": val_label})
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
        provenance=Provenance("pydantic", path, prov_meta),
    )


def _partition_union_branches(
    non_null: Sequence[tuple[_ParsedAnnotation, _SchemaBuildContext]],
) -> tuple[list[type[BaseModel]], list[_ParsedAnnotation], list[str]]:
    """Partition non-null union branches into models, scalars, and other container kinds."""
    model_branches: list[type[BaseModel]] = []
    scalar_branches: list[_ParsedAnnotation] = []
    other_kinds: list[str] = []

    for b_parsed, b_ctx in non_null:
        kind = _classify_parsed_kind(b_parsed, b_ctx)
        if kind == "model":
            if b_parsed.annotation not in model_branches:
                model_branches.append(b_parsed.annotation)
        elif kind == "scalar":
            scalar_branches.append(b_parsed)
        else:
            other_kinds.append(kind)
    return model_branches, scalar_branches, other_kinds


def _resolve_container_union_branches(
    non_null: Sequence[tuple[_ParsedAnnotation, _SchemaBuildContext]],
    partition: tuple[Sequence[type[BaseModel]], Sequence[_ParsedAnnotation], Sequence[str]],
    path: tuple[str | int, ...],
    *,
    nullable: bool,
) -> TypedNode:
    """Resolve container branches in a union or raise ``E8``."""
    model_branches, scalar_branches, other_kinds = partition
    container_nodes = [
        _build_parsed_node(b, b_ctx, path, nullable=nullable)
        for b, b_ctx in non_null
        if _classify_parsed_kind(b, b_ctx) not in {"model", "scalar"}
    ]
    if not model_branches and not scalar_branches and len(set(other_kinds)) == 1:
        first_node = container_nodes[0]
        if all(_nodes_same_shape(n, first_node) for n in container_nodes[1:]):
            return first_node
    msg = (
        f"Incompatible union branch shapes: models={[m.__name__ for m in model_branches]}, "
        f"scalars={len(scalar_branches)}, containers={list(other_kinds)}"
    )
    first_ctx = non_null[0][1]
    raise first_ctx.error_by_code.get("E8", first_ctx.error_type)(msg)


def _build_multi_branch_node(
    branches: Sequence[tuple[_ParsedAnnotation, _SchemaBuildContext]],
    path: tuple[str | int, ...],
    *,
    nullable: bool,
) -> TypedNode:
    """Resolve a list of unwrapped union branches into an IR ``TypedNode``."""
    flat_branches: list[tuple[_ParsedAnnotation, _SchemaBuildContext]] = []
    for parsed, b_ctx in branches:
        flat_branches.extend(_flatten_union_branches(parsed, b_ctx))

    non_null = [(b, b_ctx) for b, b_ctx in flat_branches if not _is_none_type(b.annotation)]
    if not non_null:
        msg = "Standalone null-only union cannot be converted to a PyArrow field"
        branches[0][1].raise_err("E7", msg)

    if len(non_null) == 1:
        return _build_parsed_node(non_null[0][0], non_null[0][1], path, nullable=nullable)

    partition = _partition_union_branches(non_null)
    model_branches, scalar_branches, other_kinds = partition

    if other_kinds:
        return _resolve_container_union_branches(
            non_null,
            partition,
            path,
            nullable=nullable,
        )

    base_ctx = non_null[0][1]
    if scalar_branches and not model_branches:
        node_type, hints, pyd_type = _unify_scalar_specs(
            [_resolve_parsed_scalar(sb, base_ctx) for sb in scalar_branches]
        )
        return TypedNode(
            type=node_type,
            nullable=nullable,
            hints=hints,
            provenance=Provenance("pydantic", path, {"pydantic_type": pyd_type}),
        )

    for m in model_branches:
        if base_ctx.model_counts.get(m, 0) >= 1:
            _check_no_forbidden_types_in_recursive_model(m, base_ctx)

    active_models = [m for m in model_branches if base_ctx.model_counts.get(m, 0) < base_ctx.max_recursion_depth]
    if not active_models and not scalar_branches:
        return TypedNode(
            type="string",
            nullable=nullable,
            hints=NodeHints(logical_type="string"),
            provenance=Provenance("pydantic", path),
        )

    return _build_merged_models_struct(
        active_models,
        base_ctx,
        path,
        nullable=nullable,
        scalar_branches=scalar_branches,
    )


CONTAINER_NODE_BUILDERS: Final[
    frozendict[
        str,
        Callable[..., TypedNode],
    ]
] = frozendict(
    {
        "sequence": _build_sequence_node,
        "tuple": _build_tuple_node,
        "map": _build_map_node,
    }
)


def _build_parsed_node(
    parsed: _ParsedAnnotation,
    ctx: _SchemaBuildContext,
    path: tuple[str | int, ...],
    *,
    nullable: bool,
) -> TypedNode:
    """Convert an unwrapped ``_ParsedAnnotation`` into an IR ``TypedNode``."""
    tp = parsed.annotation
    if _is_none_type(tp):
        msg = "Standalone NoneType field cannot be converted to a PyArrow column"
        ctx.raise_err("E7", msg)

    if _is_union_origin(get_origin(tp)):
        return _build_multi_branch_node([(parsed, ctx)], path, nullable=nullable)

    kind = _classify_parsed_kind(parsed, ctx)
    if kind == "model":
        model_cls: type[BaseModel] = tp
        if ctx.model_counts.get(model_cls, 0) >= 1:
            _check_no_forbidden_types_in_recursive_model(model_cls, ctx)
        if ctx.model_counts.get(model_cls, 0) >= ctx.max_recursion_depth:
            return TypedNode(
                type="string",
                nullable=nullable,
                hints=NodeHints(logical_type="string"),
                provenance=Provenance("pydantic", path),
            )
        return _build_merged_models_struct([model_cls], ctx, path, nullable=nullable)
    builder = CONTAINER_NODE_BUILDERS.get(kind)
    if builder is not None:
        return builder(parsed, ctx, path, nullable=nullable)

    node_type, hints, pyd_type = _resolve_parsed_scalar(parsed, ctx)
    return TypedNode(
        type=node_type,
        nullable=nullable,
        hints=hints,
        provenance=Provenance("pydantic", path, {"pydantic_type": pyd_type}),
    )


def _build_field_ir(  # noqa: PLR0913
    name: str,
    annotation: Any,  # noqa: ANN401
    ctx: _SchemaBuildContext,
    path: tuple[str | int, ...],
    *,
    field_info: FieldInfo | None = None,
    extra_description: str | None = None,
    required_in_parent: bool,
) -> SchemaField:
    """Build an IR ``Field`` with nullability and metadata from a Pydantic field or element annotation."""
    parsed, next_ctx = _unwrap_annotation(
        annotation,
        ctx,
        field_info=field_info,
        extra_description=extra_description,
    )
    nullable = (not required_in_parent) or _allows_null(parsed.annotation, next_ctx)
    node = _build_parsed_node(parsed, next_ctx, path, nullable=nullable)
    annotations = {"description": parsed.description} if parsed.description else {}
    prov_meta = dict(node.provenance.metadata) if node.provenance is not None else {}
    return SchemaField(
        name=name,
        node=node,
        required=not nullable,
        annotations=annotations,
        provenance=Provenance("pydantic", path, prov_meta),
    )


@dataclass(frozen=True, slots=True)
class PydanticReader:
    """Build typed converter IR documents from Pydantic v2 BaseModel classes.

    :param max_recursion_depth: Maximum times a recursive ``BaseModel`` may be expanded along an ancestor path.
    :type max_recursion_depth: int
    :param by_alias: When ``True``, use field aliases rather than Python attribute names.
    :type by_alias: bool
    :param error_type: Base exception class raised when a construct cannot be converted.
    :type error_type: type[ConversionError]
    :param error_by_code: Mapping from specification error code (``E1``-``E12``) to exception class.
    :type error_by_code: Mapping[str, type[ConversionError]]
    """

    max_recursion_depth: int = DEFAULT_MAX_RECURSION_DEPTH
    by_alias: bool = False
    error_type: type[ConversionError] = ConversionError
    error_by_code: Mapping[str, type[ConversionError]] = field(default_factory=frozendict)

    def __post_init__(self) -> None:
        """Validate reader configuration."""
        validate_max_recursion_depth(self.max_recursion_depth)
        if type(self.by_alias) is not bool:
            msg = "by_alias must be a boolean"
            raise TypeError(msg)

    def _raise_err(self, code: str, msg: str) -> NoReturn:
        """Raise the configured exception subclass for ``code`` or fall back to ``error_type``.

        :param code: Specification error code (``E1``-``E12``).
        :type code: str
        :param msg: Diagnostic error message.
        :type msg: str
        :raises ConversionError: Always raised with the resolved error class.
        """
        _raise_code(code, msg, error_type=self.error_type, error_by_code=self.error_by_code)

    def read(self, source: type[BaseModel]) -> SchemaDocument:
        """Read a Pydantic ``BaseModel`` subclass into a ``SchemaDocument``.

        :param source: Pydantic v2 ``BaseModel`` subclass.
        :type source: type[BaseModel]
        :return: Structural IR document with ``source="pydantic"`` provenance.
        :rtype: SchemaDocument
        :raises ConversionError: If the model or any of its fields violates ``E1``-``E12``.
        """
        init_ctx = _SchemaBuildContext(
            max_recursion_depth=self.max_recursion_depth,
            by_alias=self.by_alias,
            error_type=self.error_type,
            error_by_code=self.error_by_code,
        )
        validated_model = _validate_model_class(source, init_ctx)
        mod = sys.modules.get(validated_model.__module__)
        module_ns = vars(mod) if mod is not None else {}

        ctx = _SchemaBuildContext(
            model_counts={},
            active_aliases=frozenset(),
            max_recursion_depth=self.max_recursion_depth,
            by_alias=self.by_alias,
            module_ns=module_ns,
            error_type=self.error_type,
            error_by_code=self.error_by_code,
        )

        if issubclass(validated_model, RootModel):
            root_finfo = validated_model.model_fields["root"]
            parsed, next_ctx = _unwrap_annotation(root_finfo.annotation, ctx, field_info=root_finfo)
            origin = get_origin(parsed.annotation)
            if _is_union_origin(origin):
                branches = _flatten_union_branches(parsed, next_ctx)
                non_null = [(b, b_ctx) for b, b_ctx in branches if not _is_none_type(b.annotation)]
                if not non_null or any(_classify_parsed_kind(b, b_ctx) != "model" for b, b_ctx in non_null):
                    msg = (
                        f"RootModel union must consist exclusively of BaseModel branches, got {root_finfo.annotation!r}"
                    )
                    self._raise_err("E2", msg)
                root_struct = _build_merged_models_struct(
                    [b.annotation for b, _ in non_null],
                    next_ctx,
                    (),
                    nullable=False,
                )
            elif _classify_parsed_kind(parsed, next_ctx) != "model":
                msg = f"RootModel must wrap a BaseModel or union of BaseModels, got {parsed.annotation!r}"
                self._raise_err("E2", msg)
            else:
                root_struct = _build_merged_models_struct([parsed.annotation], next_ctx, (), nullable=False)
        else:
            root_struct = _build_merged_models_struct([validated_model], ctx, (), nullable=False)

        doc = _clean_docstring(source)
        annotations = {"description": doc} if doc is not None else {}
        root = replace(root_struct, annotations=annotations)
        qualname = source.__qualname__.replace(".<locals>.", ".")
        return SchemaDocument(
            root=root,
            name=source.__name__,
            identifier=f"urn:pydantic:{source.__module__}.{qualname}",
            annotations=annotations,
            provenance=Provenance("pydantic", (source.__name__,)),
        )

    def read_scalar_annotation(self, annotation: object) -> TypedNode:
        """Read a scalar Python or Pydantic type annotation into an IR ``TypedNode``.

        :param annotation: Scalar type annotation.
        :type annotation: object
        :return: Structural IR scalar node with ``pydantic_type`` provenance metadata.
        :rtype: TypedNode
        :raises ConversionError: If ``annotation`` is not a supported scalar type.
        """
        ctx = _SchemaBuildContext(
            max_recursion_depth=self.max_recursion_depth,
            by_alias=self.by_alias,
            error_type=self.error_type,
            error_by_code=self.error_by_code,
        )
        parsed, next_ctx = _unwrap_annotation(annotation, ctx)
        origin = get_origin(parsed.annotation)
        if _is_union_origin(origin):
            branches = _flatten_union_branches(parsed, next_ctx)
            non_null = [(b, b_ctx) for b, b_ctx in branches if not _is_none_type(b.annotation)]
            if not non_null:
                msg = "Union contains only NoneType branches"
                self._raise_err("E7", msg)
            node_type, hints, pyd_type = _unify_scalar_specs(
                [_resolve_parsed_scalar(b, b_ctx) for b, b_ctx in non_null]
            )
        else:
            node_type, hints, pyd_type = _resolve_parsed_scalar(parsed, next_ctx)
        return TypedNode(
            type=node_type,
            nullable=_allows_null(annotation, ctx),
            hints=hints,
            provenance=Provenance("pydantic", (), {"pydantic_type": pyd_type}),
        )
