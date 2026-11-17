"""Scalar type resolution and recursive cycle JSON-serializability checks for Pydantic IR lowering."""

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
from typing import Any, Final, Literal, TypeVar, get_args, get_origin
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
    FilePath,
    FutureDate,
    FutureDatetime,
    HttpUrl,
    NaiveDatetime,
    NameEmail,
    NewPath,
    PastDate,
    PastDatetime,
    SecretBytes,
    SecretStr,
)
from pydantic.networks import IPvAnyAddress, IPvAnyInterface, IPvAnyNetwork
from pydantic_core import Url

from cdm_data_loaders.converters.core.constants import DECIMAL128_MAX_PRECISION
from cdm_data_loaders.converters.core.guards import FORBIDDEN_RECURSIVE_LOGICAL_TYPES
from cdm_data_loaders.converters.core.inference import unify_scalar_specs
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
)
from cdm_data_loaders.converters.sources.pydantic.annotations import (
    ParsedAnnotation,
    classify_parsed_kind,
    extract_model_field_specs,
    flatten_union_branches,
    is_none_type,
    is_union_origin,
    unwrap_annotation,
)
from cdm_data_loaders.converters.sources.pydantic.model_synthesis import SchemaBuildContext

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

DIRECT_SCALAR_MARKERS: Final[frozendict[object, tuple[NodeType, NodeHints, str]]] = frozendict(
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


def scalar_spec_for_runtime_value(
    val: object,
    error_msg: str,
    ctx: SchemaBuildContext,
) -> tuple[NodeType, NodeHints, str]:
    """Map a runtime ``Literal`` or ``Enum`` member value to ``(NodeType, NodeHints, pydantic_type)``.

    :param val: Runtime scalar value.
    :type val: object
    :param error_msg: Diagnostic message if ``val`` is not a supported scalar primitive.
    :type error_msg: str
    :param ctx: Active schema build context.
    :type ctx: SchemaBuildContext
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


def resolve_literal_scalar(tp: object, ctx: SchemaBuildContext) -> tuple[NodeType, NodeHints, str]:
    """Resolve a ``Literal[...]`` type to ``(NodeType, NodeHints, pydantic_type)``.

    :param tp: ``Literal[...]`` type annotation.
    :type tp: object
    :param ctx: Active schema build context.
    :type ctx: SchemaBuildContext
    :return: Tuple of ``(NodeType, NodeHints, pydantic_type)``.
    :rtype: tuple[NodeType, NodeHints, str]
    """
    raw_values: list[object] = []

    def _collect(node: object) -> None:
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
        scalar_spec_for_runtime_value(
            val.value if isinstance(val, Enum) else val,
            f"Unsupported Literal member value: {val!r}",
            ctx,
        )
        for val in non_null
    ]
    return unify_scalar_specs(resolved)


def resolve_enum_scalar(enum_cls: type[Enum], ctx: SchemaBuildContext) -> tuple[NodeType, NodeHints, str]:
    """Resolve an ``Enum`` subclass to ``(NodeType, NodeHints, pydantic_type)``.

    :param enum_cls: ``Enum`` subclass to resolve.
    :type enum_cls: type[Enum]
    :param ctx: Active schema build context.
    :type ctx: SchemaBuildContext
    :return: Tuple of ``(NodeType, NodeHints, pydantic_type)``.
    :rtype: tuple[NodeType, NodeHints, str]
    """
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
        scalar_spec_for_runtime_value(
            member.value,
            f"Unsupported Enum member value {member.value!r} in {enum_cls.__name__}",
            ctx,
        )
        for member in members
    ]
    return unify_scalar_specs(resolved)


def resolve_decimal_scalar(
    parsed: ParsedAnnotation,
    ctx: SchemaBuildContext,
) -> tuple[NodeType, NodeHints, str]:
    """Resolve a ``Decimal`` annotation and its ``max_digits`` / ``decimal_places`` metadata.

    :param parsed: Unwrapped ``Decimal`` annotation with constraint metadata.
    :type parsed: ParsedAnnotation
    :param ctx: Active schema build context.
    :type ctx: SchemaBuildContext
    :return: Tuple of ``(NodeType, NodeHints, pydantic_type)``.
    :rtype: tuple[NodeType, NodeHints, str]
    """
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


def resolve_class_scalar(
    tp: type[Any],
    parsed: ParsedAnnotation,
    ctx: SchemaBuildContext,
) -> tuple[NodeType, NodeHints, str] | None:
    """Resolve a class-based scalar ``tp`` to ``(NodeType, NodeHints, pydantic_type)`` or ``None``.

    :param tp: Python class annotation.
    :type tp: type[Any]
    :param parsed: Unwrapped annotation carrying metadata markers.
    :type parsed: ParsedAnnotation
    :param ctx: Active schema build context.
    :type ctx: SchemaBuildContext
    :return: Tuple of ``(NodeType, NodeHints, pydantic_type)`` or ``None``.
    :rtype: tuple[NodeType, NodeHints, str] | None
    """
    if issubclass(tp, Enum):
        return resolve_enum_scalar(tp, ctx)
    if issubclass(tp, Decimal):
        return resolve_decimal_scalar(parsed, ctx)
    if issubclass(tp, datetime):
        return (*SPEC_TS_NAIVE, "date-time-naive") if parsed.is_naive_datetime else (*SPEC_TS_UTC, "date-time")
    if issubclass(tp, bytes | bytearray | SecretBytes):
        return *SPEC_BINARY, ("binary (base64)" if parsed.is_base64_bytes else "binary")
    for target_cls, spec, label in SUBCLASS_SCALAR_TABLE:
        if issubclass(tp, target_cls):
            return *spec, label
    return None


def resolve_parsed_scalar(
    parsed: ParsedAnnotation,
    ctx: SchemaBuildContext,
) -> tuple[NodeType, NodeHints, str]:
    """Resolve an unwrapped scalar ``ParsedAnnotation`` to ``(NodeType, NodeHints, pydantic_type)``.

    :param parsed: Unwrapped scalar annotation.
    :type parsed: ParsedAnnotation
    :param ctx: Active schema build context.
    :type ctx: SchemaBuildContext
    :return: Tuple of ``(NodeType, NodeHints, pydantic_type)``.
    :rtype: tuple[NodeType, NodeHints, str]
    """
    tp = parsed.annotation
    if is_none_type(tp):
        msg = "Standalone NoneType cannot be converted to a PyArrow column"
        ctx.raise_err("E7", msg)
    if tp is Any or tp is object or isinstance(tp, TypeVar):
        msg = f"Unconstrained scalar type: {tp!r}"
        ctx.raise_err("E4", msg)

    if get_origin(tp) is Literal:
        return resolve_literal_scalar(tp, ctx)

    try:
        direct = DIRECT_SCALAR_MARKERS.get(tp)
    except TypeError:
        direct = None
    if direct is not None:
        return direct

    if isinstance(tp, type):
        resolved = resolve_class_scalar(tp, parsed, ctx)
        if resolved is not None:
            return resolved

    msg = f"Unsupported field type annotation: {tp!r}"
    raise ctx.error_by_code.get("E7", ctx.error_type)(msg)


def check_no_forbidden_types_in_recursive_model(
    model: type[BaseModel],
    ctx: SchemaBuildContext,
    visited: set[type[BaseModel]] | None = None,
) -> None:
    """Raise ``E10`` if a recursive model cycle contains non-JSON-native scalar types.

    :param model: Recursive ``BaseModel`` subclass to inspect.
    :type model: type[BaseModel]
    :param ctx: Active schema build context.
    :type ctx: SchemaBuildContext
    :param visited: Visited model set preventing infinite traversal.
    :type visited: set[type[BaseModel]] | None
    """
    if visited is None:
        visited = set()
    if model in visited:
        return
    visited.add(model)
    for spec in extract_model_field_specs(model, ctx):
        check_no_forbidden_types_in_annotation(spec.annotation, spec.context, visited)


def check_no_forbidden_types_in_annotation(
    ann: object,
    ctx: SchemaBuildContext,
    visited: set[type[BaseModel]],
) -> None:
    """Inspect an annotation inside a recursive model cycle for non-JSON-native scalar types.

    :param ann: Field or container element annotation.
    :type ann: object
    :param ctx: Active schema build context.
    :type ctx: SchemaBuildContext
    :param visited: Visited model set preventing infinite traversal.
    :type visited: set[type[BaseModel]]
    """
    parsed, next_ctx = unwrap_annotation(ann, ctx)
    tp = parsed.annotation
    if is_none_type(tp):
        return
    origin = get_origin(tp)
    if is_union_origin(origin):
        for branch_parsed, branch_ctx in flatten_union_branches(parsed, next_ctx):
            check_no_forbidden_types_in_annotation(branch_parsed.annotation, branch_ctx, visited)
        return
    kind = classify_parsed_kind(parsed, next_ctx)
    if kind == "model":
        check_no_forbidden_types_in_recursive_model(tp, next_ctx, visited)
        return
    if kind in {"sequence", "tuple", "map"}:
        for arg in get_args(tp):
            if arg is not Ellipsis:
                check_no_forbidden_types_in_annotation(arg, next_ctx, visited)
        return
    _, hints, _ = resolve_parsed_scalar(parsed, next_ctx)
    if hints.logical_type in FORBIDDEN_RECURSIVE_LOGICAL_TYPES:
        msg = f"Non-JSON-serializable scalar type {hints.logical_type} in recursive model cycle"
        next_ctx.raise_err("E10", msg)
