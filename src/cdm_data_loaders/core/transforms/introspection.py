"""Signature and type-annotation reflection for pluggable table transform callables."""

import functools
import inspect
from collections.abc import Callable
from types import UnionType
from typing import Annotated, Any, Final, TypeAliasType, Union, get_args, get_origin, get_type_hints

import pyarrow as pa

from cdm_data_loaders.core.registry import TransformContext

CONTEXT_POSITIONAL_ARITY: Final[int] = 2
CONTEXT_PARAM_NAMES: Final[frozenset[str]] = frozenset({"ctx", "context"})


def _unwrap_partial_target(fn: Callable[..., Any]) -> Callable[..., Any]:
    """Unwrap nested ``functools.partial`` wrappers to the underlying callable.

    :param fn: callable or ``functools.partial`` wrapper
    :type fn: Callable[..., Any]
    :return: underlying callable object
    :rtype: Callable[..., Any]
    """
    target: Callable[..., Any] = fn
    while isinstance(target, functools.partial):
        target = target.func
    return target


def _safe_type_hints(fn: Callable[..., Any]) -> dict[str, Any]:
    """Resolve type hints for ``fn`` when possible, returning an empty dict on unresolvable locals.

    :param fn: callable to inspect
    :type fn: Callable[..., Any]
    :return: mapping of parameter names to resolved annotations
    :rtype: dict[str, Any]
    """
    target = _unwrap_partial_target(fn)
    try:
        return get_type_hints(target)
    except (TypeError, NameError, AttributeError, ValueError):
        if not inspect.isfunction(target) and not inspect.ismethod(target) and callable(target):
            try:
                return get_type_hints(type(target).__call__)
            except (TypeError, NameError, AttributeError, ValueError):
                return {}
        return {}


def _annotation_capabilities(ann: object) -> tuple[bool, bool]:
    """Inspect a first-parameter type annotation and return ``(accepts_pyarrow_table, accepts_record_dict)``.

    :param ann: parameter type annotation
    :type ann: object
    :return: pair of booleans ``(accepts_pyarrow_table, accepts_record_dict)``
    :rtype: tuple[bool, bool]
    """
    if isinstance(ann, TypeAliasType):
        return _annotation_capabilities(ann.__value__)
    origin = get_origin(ann)
    if origin is Annotated:
        args = get_args(ann)
        return _annotation_capabilities(args[0]) if args else (False, True)
    if origin in (UnionType, Union):
        sub_caps = [_annotation_capabilities(arg) for arg in get_args(ann) if arg is not type(None)]
        return (any(cap[0] for cap in sub_caps), any(cap[1] for cap in sub_caps)) if sub_caps else (False, True)
    if ann is pa.Table:
        return (True, False)
    if isinstance(ann, str):
        has_table = "Table" in ann
        has_dict = "dict" in ann or "Mapping" in ann
        return (has_table, has_dict or not has_table)
    return (False, True)


def _annotation_is_transform_context(ann: object) -> bool:
    """Return whether ``ann`` refers to :class:`TransformContext` directly or inside a union or ``Annotated``.

    :param ann: parameter type annotation
    :type ann: object
    :return: ``True`` if ``TransformContext`` appears in ``ann``
    :rtype: bool
    """
    if ann is TransformContext:
        return True
    if isinstance(ann, TypeAliasType):
        return _annotation_is_transform_context(ann.__value__)
    origin = get_origin(ann)
    if origin is Annotated:
        args = get_args(ann)
        return _annotation_is_transform_context(args[0]) if args else False
    if origin in (UnionType, Union):
        return any(_annotation_is_transform_context(arg) for arg in get_args(ann))
    if isinstance(ann, str):
        return "TransformContext" in ann
    return False


def inspect_transform_signature(transform: Callable[..., Any]) -> tuple[bool, bool, bool]:
    """Determine ``(accepts_pyarrow, accepts_dict, takes_context)`` for ``transform``.

    :param transform: table transform callable
    :type transform: Callable[..., Any]
    :return: tuple ``(accepts_pyarrow_table, accepts_record_dict, takes_transform_context)``
    :rtype: tuple[bool, bool, bool]
    """
    accepts_attr = getattr(transform, "accepts_pyarrow", None)
    if isinstance(accepts_attr, bool):
        return (accepts_attr, True, True)

    positional_kinds = (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD)
    positional_params = [
        param for param in inspect.signature(transform).parameters.values() if param.kind in positional_kinds
    ]
    hints = _safe_type_hints(transform)

    if positional_params:
        first_param = positional_params[0]
        first_ann = hints.get(first_param.name, first_param.annotation)
        accepts_arrow, accepts_dict = _annotation_capabilities(first_ann)
    else:
        accepts_arrow, accepts_dict = (False, True)

    takes_ctx = False
    if len(positional_params) >= CONTEXT_POSITIONAL_ARITY:
        second_param = positional_params[1]
        second_ann = hints.get(second_param.name, second_param.annotation)
        takes_ctx = (
            second_param.default is inspect.Parameter.empty
            or _annotation_is_transform_context(second_ann)
            or second_param.name in CONTEXT_PARAM_NAMES
        )

    return (accepts_arrow, accepts_dict, takes_ctx)


def accepts_pyarrow_table(transform: Callable[..., Any] | None) -> bool:
    """Return whether ``transform`` declares support for :class:`pyarrow.Table` batch inputs.

    :param transform: optional table transform callable
    :type transform: Callable[..., Any] | None
    :return: ``True`` if ``transform`` accepts a ``pyarrow.Table`` as its first argument
    :rtype: bool
    """
    if transform is None:
        return False
    return inspect_transform_signature(transform)[0]


def accepts_record_dict(transform: Callable[..., Any] | None) -> bool:
    """Return whether ``transform`` accepts ``dict[str, Any]`` single-record inputs.

    :param transform: optional table transform callable
    :type transform: Callable[..., Any] | None
    :return: ``True`` if ``transform`` accepts a ``dict[str, Any]`` as its first argument
    :rtype: bool
    """
    if transform is None:
        return True
    return inspect_transform_signature(transform)[1]
