"""Convert Pydantic v2 BaseModel schemas to PyArrow schemas and coerce records for Arrow/Parquet."""

import base64
import inspect
import json
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
from dataclasses import MISSING, dataclass, field, is_dataclass
from dataclasses import fields as dc_fields
from datetime import UTC, date, datetime, time, timedelta
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
    ClassVar,
    Final,
    ForwardRef,
    Literal,
    NewType,
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

import pyarrow as pa
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
    ConfigDict,
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
    PositiveInt,
    PydanticSchemaGenerationError,
    PydanticUndefinedAnnotation,
    RootModel,
    SecretBytes,
    SecretStr,
    TypeAdapter,
    create_model,
)
from pydantic.fields import FieldInfo
from pydantic.networks import IPvAnyAddress, IPvAnyInterface, IPvAnyNetwork
from pydantic_core import PydanticUndefined, Url

from cdm_data_loaders.converters.core.errors import ConversionError
from cdm_data_loaders.converters.emitters._tabular_base import DECIMAL128_MAX_PRECISION

DEFAULT_MAX_RECURSION_DEPTH: Final[int] = 2
VARIADIC_TUPLE_ARGS_LEN: Final[int] = 2
MAP_TYPE_ARGS_LEN: Final[int] = 2

TIMEDELTA_ADAPTER: Final[TypeAdapter[timedelta]] = TypeAdapter(timedelta)

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


class PydanticToPyArrowError(ConversionError):
    """Raised when a Pydantic model construct cannot be converted to a PyArrow schema.

    Subclasses raised by :func:`generate_pyarrow_schema` carry a stable specification ``code`` and a short
    ``title``; their docstrings state why the construct is rejected. The converter specification document is
    generated from these attributes.
    """

    code: ClassVar[str] = ""
    title: ClassVar[str] = ""


class InvalidPydanticModelError(PydanticToPyArrowError):
    """Converter requires a concrete Pydantic `BaseModel` subclass as input."""

    code = "E1"
    title = "Input is not a concrete Pydantic `BaseModel` subclass"


class NonStructRootError(PydanticToPyArrowError):
    """Root of a PyArrow schema must be a `struct` (a table has named columns, not a bare scalar, list, or map)."""

    code = "E2"
    title = "Non-struct `RootModel`"


class EmptyModelError(PydanticToPyArrowError):
    """Parquet cannot persist a `struct` with zero child fields."""

    code = "E3"
    title = "Empty model or tuple (zero fields)"


class UnconstrainedFieldError(PydanticToPyArrowError):
    """PyArrow requires a concrete static column type and has no dynamic `any` or untyped container column."""

    code = "E4"
    title = "Unconstrained field (`Any`, `object`, untyped container, or unbound `TypeVar`)"


class OpenTupleError(PydanticToPyArrowError):
    """Positional tuple structs require a fixed element count so every position maps to a deterministic field."""

    code = "E5"
    title = "Open tuple mixing positional and variadic elements"


class NonStringMapKeyError(PydanticToPyArrowError):
    """Fixed-field structs cannot mix open `extra='allow'` maps, and Arrow maps require string keys for JSONL round-tripping."""

    code = "E6"
    title = "Open `extra='allow'` model or non-string mapping key type"


class UnsupportedFieldTypeError(PydanticToPyArrowError):
    """Non-serializable Python runtime types and null-only fields have no valid Arrow column representation."""

    code = "E7"
    title = "Unsupported field type or null-only field"


class IncompatibleUnionBranchesError(PydanticToPyArrowError):
    """PyArrow cannot merge lists, maps, and structs into a single column without an opaque union type."""

    code = "E8"
    title = "Incompatible structural union branches"


class UnresolvedForwardRefError(PydanticToPyArrowError):
    """Schema generation requires all forward type annotations to be resolved to concrete types."""

    code = "E9"
    title = "Unresolvable forward reference"


class RecursiveNonJsonTypeError(PydanticToPyArrowError):
    """Truncated recursive subtrees are serialized with `json.dumps`, which cannot encode `datetime.date` or `bytes`; cyclic type aliases without a `BaseModel` boundary cannot be finitely unrolled."""

    code = "E10"
    title = "Non-JSON-serializable type in a recursive model cycle or cyclic type alias"


CyclicTypeAliasError = RecursiveNonJsonTypeError


class InvalidDecimalConstraintsError(PydanticToPyArrowError):
    """Arrow `decimal128` requires `1 <= max_digits <= 38` and `0 <= decimal_places <= max_digits`."""

    code = "E11"
    title = "Invalid or out-of-range `Decimal` precision/scale constraints"


NullOnlyFieldError = UnsupportedFieldTypeError


class ValueFieldCollisionError(PydanticToPyArrowError):
    """The synthesized `_value` field for scalar union branches would collide with the model's declared `_value` field."""

    code = "E12"
    title = "`_value` collision in a scalar + model union"


PYDANTIC_TO_PYARROW_ERRORS: Final[tuple[type[PydanticToPyArrowError], ...]] = (
    InvalidPydanticModelError,
    NonStructRootError,
    EmptyModelError,
    UnconstrainedFieldError,
    OpenTupleError,
    NonStringMapKeyError,
    UnsupportedFieldTypeError,
    IncompatibleUnionBranchesError,
    UnresolvedForwardRefError,
    RecursiveNonJsonTypeError,
    InvalidDecimalConstraintsError,
    ValueFieldCollisionError,
)


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
    """Immutable traversal context for Pydantic to PyArrow schema generation."""

    model_counts: Mapping[type[BaseModel], int] = field(default_factory=frozendict)
    active_aliases: frozenset[Any] = field(default_factory=frozenset)
    max_recursion_depth: int = DEFAULT_MAX_RECURSION_DEPTH
    by_alias: bool = False
    module_ns: Mapping[str, Any] = field(default_factory=frozendict)

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
        )

    def with_alias(self, alias_obj: Any) -> Self:  # noqa: ANN401
        """Return a new context recording ``alias_obj`` in the active type-alias chain."""
        return type(self)(
            model_counts=self.model_counts,
            active_aliases=self.active_aliases | {alias_obj},
            max_recursion_depth=self.max_recursion_depth,
            by_alias=self.by_alias,
            module_ns=self.module_ns,
        )


def _ensure_model_complete(model: type[BaseModel]) -> None:
    """Ensure a Pydantic BaseModel subclass has resolved all forward references."""
    if not getattr(model, "__pydantic_complete__", True):
        mod = sys.modules.get(model.__module__)
        ns = vars(mod) if mod is not None else None
        try:
            model.model_rebuild(raise_errors=True, _types_namespace=ns)
        except (PydanticUndefinedAnnotation, NameError, TypeError) as exc:
            msg = f"Unresolved forward reference in model {model.__name__}: {exc}"
            raise UnresolvedForwardRefError(msg) from exc
        except PydanticSchemaGenerationError:
            return
    if not getattr(model, "__pydantic_complete__", True):
        msg = f"Model {model.__name__} remains incomplete after model_rebuild()"
        raise UnresolvedForwardRefError(msg)


def _validate_model_class(model: object) -> type[BaseModel]:
    """Validate that ``model`` is a concrete Pydantic ``BaseModel`` subclass."""
    if not isinstance(model, type) or not issubclass(model, BaseModel) or model in {BaseModel, RootModel}:
        msg = f"Expected a concrete Pydantic BaseModel subclass, got {model!r}"
        raise InvalidPydanticModelError(msg)
    _ensure_model_complete(model)
    generic_meta = getattr(model, "__pydantic_generic_metadata__", {})
    if generic_meta.get("parameters"):
        msg = (
            f"Unparameterized generic model {model.__name__} has unbound type parameters: {generic_meta['parameters']}"
        )
        raise UnconstrainedFieldError(msg)
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


def _extract_class_docstring(tp: Any) -> str | None:  # noqa: ANN401
    """Extract a cleaned class docstring from a ``BaseModel`` or ``Enum`` subclass when explicitly defined."""
    if not isinstance(tp, type) or not (issubclass(tp, BaseModel) or issubclass(tp, Enum)):
        return None
    if tp in {BaseModel, RootModel, Enum, IntEnum, StrEnum}:
        return None
    raw_doc = tp.__dict__.get("__doc__")
    if not raw_doc or not isinstance(raw_doc, str):
        return None
    cleaned = inspect.cleandoc(raw_doc).strip()
    return None if (not cleaned or cleaned.startswith(f"{tp.__name__}(")) else cleaned


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
    raw_doc = tp.__dict__.get("__doc__")
    is_custom_doc = isinstance(raw_doc, str) and not raw_doc.strip().startswith(f"{tp.__name__}(")
    model_cls.__doc__ = raw_doc if is_custom_doc else None
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
            raise CyclicTypeAliasError(msg)
        return curr.__value__, ctx.with_alias(curr)
    origin = get_origin(curr)
    if isinstance(origin, TypeAliasType):
        if origin in ctx.active_aliases:
            msg = f"Cyclic recursive type alias without a BaseModel boundary: {origin.__name__}"
            raise CyclicTypeAliasError(msg)
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
        raise UnresolvedForwardRefError(msg)
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
        _ensure_model_complete(curr)
        if getattr(curr, "__pydantic_generic_metadata__", {}).get("parameters"):
            msg = f"Unparameterized generic RootModel {curr.__name__}"
            raise UnconstrainedFieldError(msg)
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


def _unify_scalar_types(resolved: Sequence[tuple[pa.DataType, str]]) -> tuple[pa.DataType, str]:
    """Unify multiple scalar ``(pa.DataType, pydantic_type)`` pairs according to ``R-UNION-SCALAR``."""
    unique_labels = list(dict.fromkeys(label for _, label in resolved))
    joined_label = " | ".join(unique_labels)

    unique_types: list[pa.DataType] = []
    for dtype, _ in resolved:
        if not any(dtype.equals(existing) for existing in unique_types):
            unique_types.append(dtype)

    if len(unique_types) == 1:
        return unique_types[0], joined_label

    if all(pa.types.is_decimal(t) for t in unique_types):
        new_scale = max(t.scale for t in unique_types)
        max_int_digits = max(t.precision - t.scale for t in unique_types)
        new_precision = max_int_digits + new_scale
        if new_precision <= DECIMAL128_MAX_PRECISION:
            return pa.decimal128(new_precision, new_scale), joined_label
        return pa.float64(), joined_label

    if all(pa.types.is_integer(t) or pa.types.is_floating(t) or pa.types.is_decimal(t) for t in unique_types):
        return pa.float64(), joined_label

    if all(pa.types.is_timestamp(t) for t in unique_types):
        return pa.timestamp("us", tz="UTC"), joined_label

    return pa.string(), joined_label


def _resolve_literal_scalar(tp: Any) -> tuple[pa.DataType, str]:  # noqa: ANN401
    """Resolve a ``Literal[...]`` type to ``(pa.DataType, pydantic_type)``."""
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
        raise NullOnlyFieldError(msg)

    resolved: list[tuple[pa.DataType, str]] = []
    for val in non_null:
        raw_val = val.value if isinstance(val, Enum) else val
        if isinstance(raw_val, bool):
            resolved.append((pa.bool_(), "boolean"))
        elif isinstance(raw_val, int):
            resolved.append((pa.int64(), "integer"))
        elif isinstance(raw_val, float):
            resolved.append((pa.float64(), "number"))
        elif isinstance(raw_val, str):
            resolved.append((pa.string(), "string"))
        elif isinstance(raw_val, bytes):
            resolved.append((pa.binary(), "binary"))
        else:
            msg = f"Unsupported Literal member value: {val!r}"
            raise UnsupportedFieldTypeError(msg)
    return _unify_scalar_types(resolved)


def _resolve_enum_scalar(enum_cls: type[Enum]) -> tuple[pa.DataType, str]:
    """Resolve an ``Enum`` subclass to ``(pa.DataType, pydantic_type)``."""
    if issubclass(enum_cls, StrEnum) or issubclass(enum_cls, str):
        return pa.string(), "string"
    if issubclass(enum_cls, IntEnum) or (issubclass(enum_cls, int) and not issubclass(enum_cls, bool)):
        return pa.int64(), "integer"
    if issubclass(enum_cls, float):
        return pa.float64(), "number"

    members = list(enum_cls)
    if not members:
        msg = f"Empty Enum {enum_cls.__name__} has no members"
        raise UnsupportedFieldTypeError(msg)

    resolved: list[tuple[pa.DataType, str]] = []
    for member in members:
        val = member.value
        if isinstance(val, bool):
            resolved.append((pa.bool_(), "boolean"))
        elif isinstance(val, int):
            resolved.append((pa.int64(), "integer"))
        elif isinstance(val, float):
            resolved.append((pa.float64(), "number"))
        elif isinstance(val, str):
            resolved.append((pa.string(), "string"))
        elif isinstance(val, bytes):
            resolved.append((pa.binary(), "binary"))
        else:
            msg = f"Unsupported Enum member value {val!r} in {enum_cls.__name__}"
            raise UnsupportedFieldTypeError(msg)
    return _unify_scalar_types(resolved)


DIRECT_SCALAR_MARKERS: Final[frozendict[Any, tuple[pa.DataType, str]]] = frozendict(
    {
        NaiveDatetime: (pa.timestamp("us"), "date-time-naive"),
        AwareDatetime: (pa.timestamp("us", tz="UTC"), "date-time"),
        PastDatetime: (pa.timestamp("us", tz="UTC"), "date-time"),
        FutureDatetime: (pa.timestamp("us", tz="UTC"), "date-time"),
        PastDate: (pa.date32(), "date"),
        FutureDate: (pa.date32(), "date"),
        Base64Bytes: (pa.binary(), "binary (base64)"),
        UUID1: (pa.string(), "uuid"),
        UUID3: (pa.string(), "uuid"),
        UUID4: (pa.string(), "uuid"),
        UUID5: (pa.string(), "uuid"),
        FilePath: (pa.string(), "path"),
        DirectoryPath: (pa.string(), "path"),
        NewPath: (pa.string(), "path"),
        IPvAnyAddress: (pa.string(), "ip"),
        IPvAnyInterface: (pa.string(), "ip"),
        IPvAnyNetwork: (pa.string(), "ip"),
        AnyUrl: (pa.string(), "string"),
        HttpUrl: (pa.string(), "string"),
        EmailStr: (pa.string(), "string"),
        NameEmail: (pa.string(), "string"),
        Base64Str: (pa.string(), "string"),
        Url: (pa.string(), "string"),
    }
)

SUBCLASS_SCALAR_TABLE: Final[tuple[tuple[type[Any] | tuple[type[Any], ...], pa.DataType, str], ...]] = (
    (bool, pa.bool_(), "boolean"),
    (int, pa.int64(), "integer"),
    (float, pa.float64(), "number"),
    (date, pa.date32(), "date"),
    (time, pa.time64("us"), "time"),
    (timedelta, pa.string(), "duration"),
    (UUID, pa.string(), "uuid"),
    (PurePath, pa.string(), "path"),
    (IP_CLASSES, pa.string(), "ip"),
    ((str, SecretStr, AnyUrl, Url, NameEmail, EmailStr), pa.string(), "string"),
)


def _resolve_decimal_scalar(parsed: _ParsedAnnotation) -> tuple[pa.DataType, str]:
    """Resolve a ``Decimal`` annotation and its ``max_digits`` / ``decimal_places`` metadata."""
    if parsed.max_digits is not None and not (1 <= parsed.max_digits <= DECIMAL128_MAX_PRECISION):
        msg = f"Unsupported Arrow decimal128 precision: max_digits={parsed.max_digits} (must be 1..38)"
        raise InvalidDecimalConstraintsError(msg)
    if parsed.decimal_places is not None and (
        parsed.decimal_places < 0 or (parsed.max_digits is not None and parsed.decimal_places > parsed.max_digits)
    ):
        msg = (
            f"Unsupported Arrow decimal128 scale: max_digits={parsed.max_digits}, "
            f"decimal_places={parsed.decimal_places}"
        )
        raise InvalidDecimalConstraintsError(msg)
    if parsed.max_digits is not None and parsed.decimal_places is not None:
        return pa.decimal128(parsed.max_digits, parsed.decimal_places), "decimal"
    return pa.float64(), "number"


def _resolve_class_scalar(tp: type[Any], parsed: _ParsedAnnotation) -> tuple[pa.DataType, str] | None:
    """Resolve a class-based scalar ``tp`` to ``(pa.DataType, pydantic_type)`` or ``None``."""
    if issubclass(tp, Enum):
        return _resolve_enum_scalar(tp)
    if issubclass(tp, Decimal):
        return _resolve_decimal_scalar(parsed)
    if issubclass(tp, datetime):
        return (
            (pa.timestamp("us"), "date-time-naive")
            if parsed.is_naive_datetime
            else (pa.timestamp("us", tz="UTC"), "date-time")
        )
    if issubclass(tp, bytes | bytearray | SecretBytes):
        return pa.binary(), ("binary (base64)" if parsed.is_base64_bytes else "binary")
    for target_cls, dtype, label in SUBCLASS_SCALAR_TABLE:
        if issubclass(tp, target_cls):
            return dtype, label
    return None


def _resolve_parsed_scalar(parsed: _ParsedAnnotation) -> tuple[pa.DataType, str]:
    """Resolve an unwrapped scalar ``_ParsedAnnotation`` to ``(pa.DataType, pydantic_type)``."""
    tp = parsed.annotation
    if _is_none_type(tp):
        msg = "Standalone NoneType cannot be converted to a PyArrow column"
        raise NullOnlyFieldError(msg)
    if tp is Any or tp is object or isinstance(tp, TypeVar):
        msg = f"Unconstrained scalar type: {tp!r}"
        raise UnconstrainedFieldError(msg)

    if get_origin(tp) is Literal:
        return _resolve_literal_scalar(tp)

    try:
        direct = DIRECT_SCALAR_MARKERS.get(tp)
    except TypeError:
        direct = None
    if direct is not None:
        return direct

    if isinstance(tp, type):
        resolved = _resolve_class_scalar(tp, parsed)
        if resolved is not None:
            return resolved

    msg = f"Unsupported field type annotation: {tp!r}"
    raise UnsupportedFieldTypeError(msg)


def resolve_scalar_type(annotation: Any) -> tuple[pa.DataType, str]:  # noqa: ANN401
    """Map a scalar Python / Pydantic type annotation to its PyArrow DataType and ``pydantic_type`` label.

    :param annotation: scalar type annotation (e.g. ``int``, ``AwareDatetime``, ``Annotated[Decimal, Field(...)]``)
    :type annotation: Any
    :return: ``(pa.DataType, pydantic_type)``
    :rtype: tuple[pa.DataType, str]
    :raises PydanticToPyArrowError: if ``annotation`` is not a supported scalar type
    """
    ctx = _SchemaBuildContext()
    parsed, next_ctx = _unwrap_annotation(annotation, ctx)
    origin = get_origin(parsed.annotation)
    if _is_union_origin(origin):
        branches = _flatten_union_branches(parsed, next_ctx)
        non_null = [(b, b_ctx) for b, b_ctx in branches if not _is_none_type(b.annotation)]
        if not non_null:
            msg = "Union contains only NoneType branches"
            raise NullOnlyFieldError(msg)
        return _unify_scalar_types([_resolve_parsed_scalar(b) for b, _ in non_null])
    return _resolve_parsed_scalar(parsed)


def _validate_non_bare_annotation(tp: Any, origin: Any) -> None:  # noqa: ANN401
    """Raise an error if ``tp`` is unconstrained, a bare container, or an unsupported callable/pattern."""
    if tp is Any or tp is object or isinstance(tp, TypeVar):
        msg = f"Unconstrained field annotation: {tp!r}"
        raise UnconstrainedFieldError(msg)
    if origin in CALLABLE_ORIGINS or tp in CALLABLE_ORIGINS:
        msg = f"Unsupported runtime type annotation: {tp!r}"
        raise UnsupportedFieldTypeError(msg)
    if tp in SEQUENCE_ORIGINS or tp is tuple or tp in MAP_ORIGINS:
        msg = f"Bare untyped container {tp!r} requires type parameters"
        raise UnconstrainedFieldError(msg)


def _classify_parsed_kind(parsed: _ParsedAnnotation) -> str:
    """Classify an unwrapped non-union annotation into ``'null'``, ``'model'``, ``'sequence'``, ``'tuple'``, ``'map'``, or ``'scalar'``."""
    tp = parsed.annotation
    if _is_none_type(tp):
        return "null"

    origin = get_origin(tp)
    _validate_non_bare_annotation(tp, origin)

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
            raise InvalidPydanticModelError(msg)
        _ensure_model_complete(tp)
        if getattr(tp, "__pydantic_generic_metadata__", {}).get("parameters"):
            msg = f"Unparameterized generic model {tp.__name__} has unbound type parameters"
            raise UnconstrainedFieldError(msg)
        return "model"

    return "scalar"


def _is_valid_string_map_key(key_ann: Any, ctx: _SchemaBuildContext) -> bool:  # noqa: ANN401
    """Return True if ``key_ann`` resolves to a valid string key type for ``pa.map_``."""
    parsed, next_ctx = _unwrap_annotation(key_ann, ctx)
    tp = parsed.annotation
    if tp is Any or tp is object or isinstance(tp, TypeVar):
        msg = f"Unconstrained map key type: {tp!r}"
        raise UnconstrainedFieldError(msg)
    origin = get_origin(tp)
    if _is_union_origin(origin):
        branches = _flatten_union_branches(parsed, next_ctx)
        non_null = [b for b, _ in branches if not _is_none_type(b.annotation)]
        return bool(non_null) and all(_is_valid_string_map_key(b.annotation, next_ctx) for b in non_null)
    if origin is Literal:
        args = get_args(tp)
        return bool(args) and all(isinstance(a, str | StrEnum) for a in args)
    return isinstance(tp, type) and issubclass(tp, str)


def _build_sequence_type(parsed: _ParsedAnnotation, ctx: _SchemaBuildContext) -> pa.ListType:
    """Build a ``pa.ListType`` from a homogeneous sequence, set, or variadic tuple annotation."""
    args = get_args(parsed.annotation)
    if not args:
        msg = f"Untyped sequence annotation: {parsed.annotation!r}"
        raise UnconstrainedFieldError(msg)
    item_ann = args[0]
    item_field = _build_field("item", item_ann, ctx, required_in_parent=True)
    return pa.list_(item_field)


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


def _build_tuple_type(parsed: _ParsedAnnotation, ctx: _SchemaBuildContext) -> pa.StructType:
    """Build a positional ``pa.StructType`` (``item_0``, ``item_1``, ...) from a fixed-length tuple annotation."""
    args = get_args(parsed.annotation)
    if not args:
        msg = "Empty tuple tuple[()] cannot be converted to a PyArrow struct with zero fields"
        raise EmptyModelError(msg)
    if any(_is_variadic_unpack(a) for a in args):
        msg = f"Open tuple mixing positional elements and variadic unpack is not supported: {parsed.annotation!r}"
        raise OpenTupleError(msg)

    fields = [_build_field(f"item_{idx}", elem_ann, ctx, required_in_parent=True) for idx, elem_ann in enumerate(args)]
    return pa.struct(fields)


def _build_map_type(parsed: _ParsedAnnotation, ctx: _SchemaBuildContext) -> pa.MapType:
    """Build a ``pa.MapType`` from a string-keyed mapping annotation."""
    args = get_args(parsed.annotation)
    if len(args) != MAP_TYPE_ARGS_LEN:
        msg = f"Mapping annotation requires key and value type arguments: {parsed.annotation!r}"
        raise UnconstrainedFieldError(msg)
    key_ann, val_ann = args
    if not _is_valid_string_map_key(key_ann, ctx):
        msg = f"Map key type must resolve to str, got {key_ann!r}"
        raise NonStringMapKeyError(msg)
    val_field = _build_field("value", val_ann, ctx, required_in_parent=True)
    return pa.map_(pa.string(), val_field)


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
    _ensure_model_complete(model)
    if model.model_config.get("extra") == "allow":
        msg = (
            f"Model {model.__name__} declares extra='allow' on a fixed-field BaseModel; "
            "open extra fields cannot be represented in a closed PyArrow struct"
        )
        raise NonStringMapKeyError(msg)
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
    """Raise ``RecursiveNonJsonTypeError`` (``E10``) if a recursive model cycle contains non-JSON-native scalar types."""
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
    """Recursively inspect an annotation inside a recursive model cycle for non-JSON-native scalar types."""
    parsed, next_ctx = _unwrap_annotation(ann, ctx)
    tp = parsed.annotation
    if _is_none_type(tp):
        return
    origin = get_origin(tp)
    if _is_union_origin(origin):
        for branch_parsed, branch_ctx in _flatten_union_branches(parsed, next_ctx):
            _check_no_forbidden_types_in_annotation(branch_parsed.annotation, branch_ctx, visited)
        return
    kind = _classify_parsed_kind(parsed)
    if kind == "model":
        _check_no_forbidden_types_in_recursive_model(tp, next_ctx, visited)
        return
    if kind in {"sequence", "tuple", "map"}:
        for arg in get_args(tp):
            if arg is not Ellipsis:
                _check_no_forbidden_types_in_annotation(arg, next_ctx, visited)
        return
    dtype, _ = _resolve_parsed_scalar(parsed)
    if (
        pa.types.is_date(dtype)
        or pa.types.is_timestamp(dtype)
        or pa.types.is_time(dtype)
        or pa.types.is_decimal(dtype)
        or pa.types.is_binary(dtype)
    ):
        msg = f"Non-JSON-serializable scalar type {dtype} in recursive model cycle"
        raise RecursiveNonJsonTypeError(msg)


def _build_merged_models_struct(
    models: Sequence[type[BaseModel]],
    ctx: _SchemaBuildContext,
    *,
    scalar_branches: Sequence[_ParsedAnnotation] = (),
) -> pa.StructType:
    """Build a ``pa.StructType`` from one or more ``BaseModel`` branches and optional scalar branches."""
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
        raise ValueFieldCollisionError(msg)

    pa_fields: list[pa.Field] = []
    for out_name, spec_list in fields_by_name.items():
        is_req = out_name in common_non_null
        descriptions = [s.description for s in spec_list if s.description]
        merged_desc = descriptions[0] if descriptions else None

        if len(spec_list) == 1:
            spec = spec_list[0]
            pa_fields.append(
                _build_field(
                    out_name,
                    spec.annotation,
                    spec.context,
                    field_info=spec.field_info,
                    extra_description=merged_desc,
                    required_in_parent=is_req,
                )
            )
        else:
            branch_parsed_list = [
                _unwrap_annotation(s.annotation, s.context, field_info=s.field_info) for s in spec_list
            ]
            data_type, pyd_type = _build_multi_branch_type(branch_parsed_list)
            nullable = (not is_req) or any(_allows_null(s.annotation, s.context) for s in spec_list)
            metadata: dict[str, str] = {}
            if merged_desc:
                metadata["comment"] = merged_desc
            if pyd_type:
                metadata["pydantic_type"] = pyd_type
            pa_fields.append(pa.field(out_name, data_type, nullable=nullable, metadata=metadata or None))

    if scalar_branches:
        resolved_scalars = [_resolve_parsed_scalar(sb) for sb in scalar_branches]
        val_type, val_label = _unify_scalar_types(resolved_scalars)
        pa_fields.append(pa.field("_value", val_type, nullable=True, metadata={"pydantic_type": val_label}))

    if not pa_fields:
        model_names = ", ".join(m.__name__ for m in models)
        msg = f"Model {model_names} has zero active fields and cannot be converted to a PyArrow struct"
        raise EmptyModelError(msg)

    return pa.struct(pa_fields)


def _resolve_container_union_branches(
    non_null: Sequence[tuple[_ParsedAnnotation, _SchemaBuildContext]],
    model_branches: Sequence[type[BaseModel]],
    scalar_branches: Sequence[_ParsedAnnotation],
    other_kinds: Sequence[str],
) -> tuple[pa.DataType, str | None]:
    """Resolve container branches in a union or raise ``IncompatibleUnionBranchesError``."""
    container_types = [
        _build_parsed_type(b, b_ctx) for b, b_ctx in non_null if _classify_parsed_kind(b) not in {"model", "scalar"}
    ]
    if not model_branches and not scalar_branches and len(set(other_kinds)) == 1:
        first_type, first_label = container_types[0]
        if all(t.equals(first_type) for t, _ in container_types[1:]):
            return first_type, first_label
    msg = (
        f"Incompatible union branch shapes: models={[m.__name__ for m in model_branches]}, "
        f"scalars={len(scalar_branches)}, containers={list(other_kinds)}"
    )
    raise IncompatibleUnionBranchesError(msg)


def _partition_union_branches(
    non_null: Sequence[tuple[_ParsedAnnotation, _SchemaBuildContext]],
) -> tuple[list[type[BaseModel]], list[_ParsedAnnotation], list[str]]:
    """Partition non-null union branches into models, scalars, and other container kinds."""
    model_branches: list[type[BaseModel]] = []
    scalar_branches: list[_ParsedAnnotation] = []
    other_kinds: list[str] = []

    for b_parsed, _ in non_null:
        kind = _classify_parsed_kind(b_parsed)
        if kind == "model":
            if b_parsed.annotation not in model_branches:
                model_branches.append(b_parsed.annotation)
        elif kind == "scalar":
            scalar_branches.append(b_parsed)
        else:
            other_kinds.append(kind)
    return model_branches, scalar_branches, other_kinds


def _build_multi_branch_type(
    branches: Sequence[tuple[_ParsedAnnotation, _SchemaBuildContext]],
) -> tuple[pa.DataType, str | None]:
    """Resolve a list of unwrapped union branches into ``(pa.DataType, pydantic_type | None)``."""
    flat_branches: list[tuple[_ParsedAnnotation, _SchemaBuildContext]] = []
    for parsed, b_ctx in branches:
        flat_branches.extend(_flatten_union_branches(parsed, b_ctx))

    non_null = [(b, b_ctx) for b, b_ctx in flat_branches if not _is_none_type(b.annotation)]
    if not non_null:
        msg = "Standalone null-only union cannot be converted to a PyArrow field"
        raise NullOnlyFieldError(msg)

    if len(non_null) == 1:
        return _build_parsed_type(*non_null[0])

    model_branches, scalar_branches, other_kinds = _partition_union_branches(non_null)

    if other_kinds:
        return _resolve_container_union_branches(non_null, model_branches, scalar_branches, other_kinds)

    if scalar_branches and not model_branches:
        return _unify_scalar_types([_resolve_parsed_scalar(sb) for sb in scalar_branches])

    base_ctx = non_null[0][1]
    for m in model_branches:
        if base_ctx.model_counts.get(m, 0) >= 1:
            _check_no_forbidden_types_in_recursive_model(m, base_ctx)

    active_models = [m for m in model_branches if base_ctx.model_counts.get(m, 0) < base_ctx.max_recursion_depth]
    if not active_models and not scalar_branches:
        return pa.string(), None

    return _build_merged_models_struct(active_models, base_ctx, scalar_branches=scalar_branches), None


CONTAINER_TYPE_BUILDERS: Final[frozendict[str, Callable[[_ParsedAnnotation, _SchemaBuildContext], pa.DataType]]] = (
    frozendict(
        {
            "sequence": _build_sequence_type,
            "tuple": _build_tuple_type,
            "map": _build_map_type,
        }
    )
)


def _build_parsed_type(
    parsed: _ParsedAnnotation,
    ctx: _SchemaBuildContext,
) -> tuple[pa.DataType, str | None]:
    """Convert an unwrapped ``_ParsedAnnotation`` into ``(pa.DataType, pydantic_type | None)``."""
    tp = parsed.annotation
    if _is_none_type(tp):
        msg = "Standalone NoneType field cannot be converted to a PyArrow column"
        raise NullOnlyFieldError(msg)

    if _is_union_origin(get_origin(tp)):
        return _build_multi_branch_type([(parsed, ctx)])

    kind = _classify_parsed_kind(parsed)
    if kind == "model":
        model_cls: type[BaseModel] = tp
        if ctx.model_counts.get(model_cls, 0) >= 1:
            _check_no_forbidden_types_in_recursive_model(model_cls, ctx)
        if ctx.model_counts.get(model_cls, 0) >= ctx.max_recursion_depth:
            return pa.string(), None
        return _build_merged_models_struct([model_cls], ctx), None
    builder = CONTAINER_TYPE_BUILDERS.get(kind)
    if builder is not None:
        return builder(parsed, ctx), None
    return _resolve_parsed_scalar(parsed)


def _build_field(  # noqa: PLR0913
    name: str,
    annotation: Any,  # noqa: ANN401
    ctx: _SchemaBuildContext,
    *,
    field_info: FieldInfo | None = None,
    extra_description: str | None = None,
    required_in_parent: bool,
) -> pa.Field:
    """Build a ``pa.Field`` with nullability and metadata from a Pydantic field or element annotation."""
    parsed, next_ctx = _unwrap_annotation(
        annotation,
        ctx,
        field_info=field_info,
        extra_description=extra_description,
    )
    nullable = (not required_in_parent) or _allows_null(parsed.annotation, next_ctx)
    data_type, pyd_type = _build_parsed_type(parsed, next_ctx)

    metadata: dict[str, str] = {}
    if parsed.description:
        metadata["comment"] = parsed.description
    if pyd_type:
        metadata["pydantic_type"] = pyd_type
    return pa.field(name, data_type, nullable=nullable, metadata=metadata or None)


def generate_pyarrow_schema(
    model: type[BaseModel],
    *,
    max_recursion_depth: int = DEFAULT_MAX_RECURSION_DEPTH,
    by_alias: bool = False,
) -> pa.Schema:
    """Generate a deterministic PyArrow Schema from a Pydantic v2 ``BaseModel`` subclass.

    :param model: concrete Pydantic ``BaseModel`` subclass
    :type model: type[BaseModel]
    :param max_recursion_depth: maximum expansion depth for recursive ``BaseModel`` references
    :type max_recursion_depth: int
    :param by_alias: when ``True``, use ``serialization_alias`` or ``alias`` for field names
    :type by_alias: bool
    :return: PyArrow schema with top-level fields corresponding to model fields
    :rtype: pa.Schema
    :raises PydanticToPyArrowError: if ``model`` is invalid or uses an unsupported construct
    """
    validated_model = _validate_model_class(model)
    mod = sys.modules.get(validated_model.__module__)
    module_ns = vars(mod) if mod is not None else {}

    ctx = _SchemaBuildContext(
        model_counts={},
        active_aliases=frozenset(),
        max_recursion_depth=max_recursion_depth,
        by_alias=by_alias,
        module_ns=module_ns,
    )

    if issubclass(validated_model, RootModel):
        root_finfo = validated_model.model_fields["root"]
        parsed, next_ctx = _unwrap_annotation(root_finfo.annotation, ctx, field_info=root_finfo)
        origin = get_origin(parsed.annotation)
        if _is_union_origin(origin):
            branches = _flatten_union_branches(parsed, next_ctx)
            non_null = [(b, b_ctx) for b, b_ctx in branches if not _is_none_type(b.annotation)]
            if not non_null or any(_classify_parsed_kind(b) != "model" for b, _ in non_null):
                msg = f"RootModel union must consist exclusively of BaseModel branches, got {root_finfo.annotation!r}"
                raise NonStructRootError(msg)
            struct_type = _build_merged_models_struct([b.annotation for b, _ in non_null], next_ctx)
            return pa.schema(list(struct_type))
        if _classify_parsed_kind(parsed) != "model":
            msg = f"RootModel must wrap a BaseModel or union of BaseModels, got {parsed.annotation!r}"
            raise NonStructRootError(msg)
        struct_type = _build_merged_models_struct([parsed.annotation], next_ctx)
        return pa.schema(list(struct_type))

    struct_type = _build_merged_models_struct([validated_model], ctx)
    return pa.schema(list(struct_type))


def _extract_model_dict(instance: BaseModel, *, by_alias: bool = False) -> dict[str, Any]:
    """Extract active fields and computed fields from a ``BaseModel`` instance into a dictionary."""
    if isinstance(instance, RootModel):
        inner = instance.root
        if isinstance(inner, BaseModel):
            return _extract_model_dict(inner, by_alias=by_alias)
        if isinstance(inner, Mapping):
            return dict(inner)
        return {"_value": inner}

    model_cls = type(instance)
    result: dict[str, Any] = {}
    fields_set = instance.model_fields_set

    for field_name, finfo in model_cls.model_fields.items():
        if finfo.exclude is True:
            continue
        val = getattr(instance, field_name)
        if field_name in fields_set or val is not None:
            out_name = (finfo.serialization_alias or finfo.alias or field_name) if by_alias else field_name
            result[out_name] = val

    for comp_name, comp_info in model_cls.model_computed_fields.items():
        if comp_info.repr is False and getattr(comp_info, "exclude", False):
            continue
        val = getattr(instance, comp_name)
        if val is not None:
            out_name = (comp_info.alias or comp_name) if by_alias else comp_name
            result[out_name] = val

    return result


def _extract_dataclass_dict(instance: Any) -> dict[str, Any]:  # noqa: ANN401
    """Extract non-None fields from a stdlib dataclass instance into a dictionary."""
    result: dict[str, Any] = {}
    for dc_f in dc_fields(instance):
        val = getattr(instance, dc_f.name)
        if val is not None:
            result[dc_f.name] = val
    return result


def _comparable_sort_key(value: Any) -> tuple[bool, Any]:  # noqa: ANN401
    """Return a deterministic sort key for set elements during record coercion."""
    return (value is None, value if isinstance(value, int | float | str | bytes) else str(value))


def _to_jsonable_scalar(value: Any) -> Any:  # noqa: ANN401
    """Convert non-container Python scalar objects to JSON-serializable scalars."""
    if isinstance(value, date | datetime | time):
        return value.isoformat()
    if isinstance(value, timedelta):
        return TIMEDELTA_ADAPTER.dump_python(value, mode="json")
    if isinstance(value, bytes | bytearray):
        return bytes(value).decode("utf-8", errors="replace")
    if isinstance(value, (Decimal, UUID, PurePath, AnyUrl, Url, NameEmail, *IP_CLASSES)):
        return str(value)
    return value


def _to_jsonable_recursive(value: Any, *, by_alias: bool = False) -> Any:  # noqa: ANN401
    """Convert nested BaseModels and Python objects into JSON-serializable structures for recursion cutoffs."""
    while isinstance(value, RootModel | Enum | SecretStr | SecretBytes):
        if isinstance(value, RootModel):
            value = value.root
        elif isinstance(value, Enum):
            value = value.value
        else:
            value = value.get_secret_value()

    if isinstance(value, BaseModel) or (is_dataclass(value) and not isinstance(value, type)):
        raw_dict = (
            _extract_model_dict(value, by_alias=by_alias)
            if isinstance(value, BaseModel)
            else _extract_dataclass_dict(value)
        )
        return {k: _to_jsonable_recursive(v, by_alias=by_alias) for k, v in raw_dict.items()}
    if isinstance(value, Mapping):
        return {str(k): _to_jsonable_recursive(v, by_alias=by_alias) for k, v in value.items()}
    if isinstance(value, set | frozenset):
        items = [_to_jsonable_recursive(v, by_alias=by_alias) for v in value]
        return sorted(items, key=_comparable_sort_key)
    if isinstance(value, list | tuple | deque):
        return [_to_jsonable_recursive(v, by_alias=by_alias) for v in value]
    return _to_jsonable_scalar(value)


def _coerce_timestamp(value: Any, data_type: pa.TimestampType) -> Any:  # noqa: ANN401
    """Coerce a ``datetime`` or ISO-8601 string to match a tz-aware or tz-naive ``pa.TimestampType``."""
    parsed = datetime.fromisoformat(value) if isinstance(value, str) else value
    if not isinstance(parsed, datetime):
        return value
    if data_type.tz is not None:
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=UTC)
        return parsed.astimezone(UTC)
    if parsed.tzinfo is not None:
        return parsed.astimezone(UTC).replace(tzinfo=None)
    return parsed


def _coerce_time(value: Any) -> Any:  # noqa: ANN401
    """Coerce a ``time`` or ISO-8601 string to a naive ``datetime.time`` for ``pa.time64('us')``."""
    parsed = time.fromisoformat(value) if isinstance(value, str) else value
    if isinstance(parsed, time) and parsed.tzinfo is not None:
        return parsed.replace(tzinfo=None)
    return parsed


def _coerce_decimal(value: Any, data_type: pa.Decimal128Type) -> Decimal:  # noqa: ANN401
    """Coerce a ``Decimal``, ``int``, ``float``, or ``str`` to a quantized ``Decimal``."""
    dec = value if isinstance(value, Decimal) else Decimal(str(value))
    quantum = Decimal(10) ** (-data_type.scale)
    return dec.quantize(quantum)


def _coerce_binary(value: Any, metadata: dict[bytes, bytes] | None) -> bytes:  # noqa: ANN401
    """Coerce ``bytes``, ``bytearray``, or ``str`` into ``bytes``."""
    if isinstance(value, bytes):
        return value
    if isinstance(value, bytearray | memoryview):
        return bytes(value)
    if isinstance(value, str):
        if metadata and b"pydantic_type" in metadata and b"base64" in metadata[b"pydantic_type"]:
            return base64.b64decode(value)
        return value.encode("utf-8")
    return bytes(value)


def _coerce_date(value: Any) -> Any:  # noqa: ANN401
    """Coerce a ``date``, ``datetime``, or ISO-8601 string to a ``datetime.date``."""
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, str):
        return date.fromisoformat(value)
    return value


def _coerce_numeric(value: Any, data_type: pa.DataType) -> Any:  # noqa: ANN401
    """Coerce an int, float, Decimal, or numeric string to match an integer or floating PyArrow DataType."""
    if pa.types.is_floating(data_type):
        if isinstance(value, Decimal | int | str) and not isinstance(value, bool):
            return float(value)
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    if isinstance(value, int | str) and not isinstance(value, bool):
        return int(value)
    return value


def _coerce_string(value: Any, *, by_alias: bool = False) -> str:  # noqa: ANN401
    """Coerce any Python value into its ``pa.string()`` representation."""
    if isinstance(value, str):
        return str(value)
    if isinstance(value, timedelta):
        return str(TIMEDELTA_ADAPTER.dump_python(value, mode="json"))
    if isinstance(value, BaseModel | Mapping | list | tuple | set | frozenset | deque | bool) or (
        is_dataclass(value) and not isinstance(value, type)
    ):
        return json.dumps(_to_jsonable_recursive(value, by_alias=by_alias))
    if isinstance(value, date | datetime | time):
        return value.isoformat()
    if isinstance(value, bytes | bytearray):
        return bytes(value).decode("utf-8")
    return str(value)


def _coerce_temporal(value: Any, data_type: pa.DataType) -> Any:  # noqa: ANN401
    """Coerce a date, time, or timestamp value to match a temporal PyArrow DataType."""
    if pa.types.is_date(data_type):
        return _coerce_date(value)
    if pa.types.is_timestamp(data_type):
        return _coerce_timestamp(value, data_type)
    return _coerce_time(value)


def _coerce_scalar_for_pyarrow(
    value: Any,  # noqa: ANN401
    data_type: pa.DataType,
    metadata: dict[bytes, bytes] | None = None,
    *,
    by_alias: bool = False,
) -> Any:  # noqa: ANN401
    """Coerce a Python or JSON-parsed scalar value to match a PyArrow scalar DataType."""
    if isinstance(value, Enum):
        value = value.value
    if isinstance(value, SecretStr | SecretBytes):
        value = value.get_secret_value()

    if pa.types.is_date(data_type) or pa.types.is_timestamp(data_type) or pa.types.is_time(data_type):
        return _coerce_temporal(value, data_type)
    if pa.types.is_decimal(data_type):
        return _coerce_decimal(value, data_type)
    if pa.types.is_binary(data_type):
        return _coerce_binary(value, metadata)
    if pa.types.is_floating(data_type) or pa.types.is_integer(data_type):
        return _coerce_numeric(value, data_type)
    if pa.types.is_string(data_type) or pa.types.is_large_string(data_type):
        return _coerce_string(value, by_alias=by_alias)
    return value


def _coerce_struct_for_pyarrow(
    value: Any,  # noqa: ANN401
    data_type: pa.StructType,
    *,
    by_alias: bool = False,
) -> Any:  # noqa: ANN401
    """Coerce a ``BaseModel``, ``Mapping``, positional tuple, or scalar union value to match a ``pa.StructType``."""
    if isinstance(value, RootModel):
        value = value.root
    if isinstance(value, BaseModel):
        value = _extract_model_dict(value, by_alias=by_alias)
    elif is_dataclass(value) and not isinstance(value, type):
        value = _extract_dataclass_dict(value)
    if isinstance(value, Mapping):
        return {
            field.name: _coerce_value_for_pyarrow(
                value[field.name],
                field.type,
                field.metadata,
                by_alias=by_alias,
            )
            for field in data_type
            if field.name in value
        }
    if isinstance(value, list | tuple) and len(data_type) > 0 and data_type[0].name == "item_0":
        return {
            field.name: _coerce_value_for_pyarrow(
                value[idx],
                field.type,
                field.metadata,
                by_alias=by_alias,
            )
            for idx, field in enumerate(data_type)
            if idx < len(value) and value[idx] is not None
        }
    value_field = next((field for field in data_type if field.name == "_value"), None)
    if value_field is not None:
        return {
            "_value": _coerce_value_for_pyarrow(
                value,
                value_field.type,
                value_field.metadata,
                by_alias=by_alias,
            )
        }
    return value


def _coerce_list_for_pyarrow(
    value: Iterable[Any],
    data_type: pa.ListType | pa.LargeListType,
    *,
    by_alias: bool = False,
) -> list[Any]:
    """Coerce a sequence or set into a list matching ``data_type``."""
    item_field = data_type.value_field
    coerced = [
        _coerce_value_for_pyarrow(item, item_field.type, item_field.metadata, by_alias=by_alias) for item in value
    ]
    if isinstance(value, set | frozenset):
        return sorted(coerced, key=_comparable_sort_key)
    return coerced


def _coerce_value_for_pyarrow(
    value: Any,  # noqa: ANN401
    data_type: pa.DataType,
    metadata: dict[bytes, bytes] | None = None,
    *,
    by_alias: bool = False,
) -> Any:  # noqa: ANN401
    """Recursively coerce a Python or JSON-parsed value to match the target PyArrow DataType."""
    if isinstance(value, RootModel):
        value = value.root
    if value is None:
        return None
    if pa.types.is_struct(data_type):
        return _coerce_struct_for_pyarrow(value, data_type, by_alias=by_alias)
    if (pa.types.is_list(data_type) or pa.types.is_large_list(data_type)) and isinstance(
        value, list | tuple | set | frozenset | deque
    ):
        return _coerce_list_for_pyarrow(value, data_type, by_alias=by_alias)
    if pa.types.is_map(data_type) and isinstance(value, Mapping):
        key_field = data_type.key_field
        item_field = data_type.item_field
        return {
            _coerce_value_for_pyarrow(k, key_field.type, key_field.metadata, by_alias=by_alias): (
                _coerce_value_for_pyarrow(v, item_field.type, item_field.metadata, by_alias=by_alias)
            )
            for k, v in value.items()
        }
    return _coerce_scalar_for_pyarrow(value, data_type, metadata, by_alias=by_alias)


def coerce_record_for_pyarrow(
    record: BaseModel | Mapping[str, Any],
    schema: pa.Schema,
    *,
    model: type[BaseModel] | None = None,
    by_alias: bool = False,
) -> dict[str, Any]:
    """Coerce a Pydantic ``BaseModel`` instance or record mapping to match a PyArrow schema.

    When ``record`` is a ``BaseModel`` instance (or ``model`` is supplied to validate a mapping),
    non-null default values and ``@computed_field`` properties are populated automatically.
    Only fields declared in ``schema`` are retained and coerced recursively so
    ``pyarrow.Table.from_pylist`` can construct a table matching ``schema``.

    :param record: Pydantic ``BaseModel`` instance or record dictionary
    :type record: BaseModel | Mapping[str, Any]
    :param schema: target PyArrow schema for the record's table
    :type schema: pa.Schema
    :param model: optional ``BaseModel`` class used to validate ``record`` when ``record`` is a mapping
    :type model: type[BaseModel] | None
    :param by_alias: when ``True``, extract model fields using ``serialization_alias`` or ``alias``
    :type by_alias: bool
    :return: coerced record dictionary ready for ``DictBuffer`` or ``pa.Table.from_pylist``
    :rtype: dict[str, Any]
    """
    if isinstance(record, BaseModel):
        source_map: Mapping[str, Any] = _extract_model_dict(record, by_alias=by_alias)
    elif model is not None:
        validated = model.model_validate(dict(record))
        source_map = _extract_model_dict(validated, by_alias=by_alias)
    else:
        source_map = record

    return {
        field.name: _coerce_value_for_pyarrow(
            source_map[field.name],
            field.type,
            field.metadata,
            by_alias=by_alias,
        )
        for field in schema
        if field.name in source_map
    }


coerce_model_for_pyarrow = coerce_record_for_pyarrow


class PydanticToPyArrow(BaseModel):
    """Facade converting Pydantic v2 ``BaseModel`` classes to PyArrow schemas and empty tables."""

    model_config = ConfigDict(arbitrary_types_allowed=True, frozen=True)

    max_recursion_depth: PositiveInt = Field(default=DEFAULT_MAX_RECURSION_DEPTH)
    by_alias: bool = False

    def convert(self, model: type[BaseModel]) -> pa.Schema:
        """Convert a Pydantic ``BaseModel`` subclass to a ``pa.Schema``.

        :param model: concrete Pydantic ``BaseModel`` subclass
        :type model: type[BaseModel]
        :return: converted PyArrow schema
        :rtype: pa.Schema
        """
        return generate_pyarrow_schema(
            model,
            max_recursion_depth=self.max_recursion_depth,
            by_alias=self.by_alias,
        )

    def convert_to_table(self, model: type[BaseModel]) -> pa.Table:
        """Convert a Pydantic ``BaseModel`` subclass to a zero-row ``pa.Table``.

        :param model: concrete Pydantic ``BaseModel`` subclass
        :type model: type[BaseModel]
        :return: zero-row PyArrow table with the converted schema
        :rtype: pa.Table
        """
        return self.convert(model).empty_table()
