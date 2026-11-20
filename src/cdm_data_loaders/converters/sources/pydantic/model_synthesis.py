"""Model validation, docstring extraction, and dynamic TypedDict/dataclass synthesis for Pydantic IR lowering."""

import inspect
import sys
from collections.abc import Mapping
from dataclasses import MISSING, field, is_dataclass, replace
from dataclasses import fields as dc_fields
from enum import Enum, IntEnum, StrEnum
from typing import (
    Annotated,
    Any,
    NoReturn,
    NotRequired,
    Required,
    Self,
    get_args,
    get_origin,
    get_type_hints,
    is_typeddict,
)

from frozendict import frozendict
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    GetPydanticSchema,
    InstanceOf,
    PydanticSchemaGenerationError,
    PydanticUndefinedAnnotation,
    RootModel,
    StrictBool,
    StrictInt,
    StrictStr,
    create_model,
)
from pydantic.dataclasses import dataclass as pydantic_dataclass
from pydantic.fields import FieldInfo
from pydantic_core import core_schema

from cdm_data_loaders.converters.core.errors import ConversionError, ErrorCodeMap, raise_spec_error
from cdm_data_loaders.utils.recursion import DEFAULT_MAX_RECURSION_DEPTH, StrictPositiveRecursionDepth

type _ModuleNamespace = Annotated[
    Mapping[str, Any],
    GetPydanticSchema(lambda _tp, _handler: core_schema.is_instance_schema(Mapping)),
]


@pydantic_dataclass(frozen=True, slots=True, config=ConfigDict(arbitrary_types_allowed=True, extra="forbid"))
class SchemaBuildContext:
    """Immutable traversal context for Pydantic to IR schema generation."""

    model_counts: Mapping[type[BaseModel], StrictInt] = field(default_factory=frozendict)
    active_aliases: frozenset[Any] = field(default_factory=frozenset)
    max_recursion_depth: StrictPositiveRecursionDepth = DEFAULT_MAX_RECURSION_DEPTH
    by_alias: StrictBool = False
    module_ns: _ModuleNamespace = field(default_factory=frozendict)
    error_type: type[ConversionError] = field(default_factory=lambda: ConversionError)
    error_by_code: ErrorCodeMap = field(default_factory=frozendict)

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
        return raise_spec_error(
            code,
            msg,
            error_type=self.error_type,
            error_by_code=self.error_by_code,
            cause=cause,
        )

    def with_model(self, model: type[BaseModel]) -> Self:
        """Return a new context incrementing the expansion count for ``model`` and resetting alias cycle state.

        :param model: ``BaseModel`` subclass being entered.
        :type model: type[BaseModel]
        :return: Updated traversal context.
        :rtype: Self
        """
        mod = sys.modules.get(model.__module__)
        ns = frozendict(vars(mod)) if mod is not None else self.module_ns
        return replace(
            self,
            model_counts=frozendict({**self.model_counts, model: self.model_counts.get(model, 0) + 1}),
            active_aliases=frozenset(),
            module_ns=ns,
        )

    def with_alias(self, alias_obj: object) -> Self:
        """Return a new context recording ``alias_obj`` in the active type-alias chain.

        :param alias_obj: ``TypeAliasType`` instance being expanded.
        :type alias_obj: object
        :return: Updated traversal context.
        :rtype: Self
        """
        return replace(self, active_aliases=self.active_aliases | {alias_obj})


@pydantic_dataclass(frozen=True, slots=True, config=ConfigDict(extra="forbid"))
class ModelFieldSpec:
    """Internal specification of an active field extracted from a ``BaseModel``."""

    output_name: StrictStr
    annotation: Any
    field_info: InstanceOf[FieldInfo] | None
    description: StrictStr | None
    required_non_null: StrictBool
    context: InstanceOf[SchemaBuildContext]


def ensure_model_complete(model: type[BaseModel], ctx: SchemaBuildContext | None = None) -> None:
    """Ensure a Pydantic ``BaseModel`` subclass has resolved all forward references.

    :param model: Pydantic ``BaseModel`` subclass to rebuild if incomplete.
    :type model: type[BaseModel]
    :param ctx: Optional schema build context for error dispatch.
    :type ctx: SchemaBuildContext | None
    :raises ConversionError: If forward references cannot be resolved (``E9``).
    """
    if not getattr(model, "__pydantic_complete__", True):
        mod = sys.modules.get(model.__module__)
        ns = vars(mod) if mod is not None else None
        try:
            model.model_rebuild(raise_errors=True, _types_namespace=ns)
        except (PydanticUndefinedAnnotation, NameError, TypeError) as exc:
            msg = f"Unresolved forward reference in model {model.__name__}: {exc}"
            if ctx is not None:
                ctx.raise_err("E9", msg, cause=exc)
            raise_spec_error("E9", msg, cause=exc)
        except PydanticSchemaGenerationError:
            return
    if not getattr(model, "__pydantic_complete__", True):
        msg = f"Model {model.__name__} remains incomplete after model_rebuild()"
        if ctx is not None:
            ctx.raise_err("E9", msg)
        raise_spec_error("E9", msg)


def ensure_concrete_model(model: type[BaseModel], ctx: SchemaBuildContext) -> None:
    """Ensure ``model`` has resolved forward references and has no unbound generic type parameters.

    :param model: Pydantic ``BaseModel`` subclass to validate.
    :type model: type[BaseModel]
    :param ctx: Active schema build context.
    :type ctx: SchemaBuildContext
    :raises ConversionError: If forward references cannot be resolved or ``model`` has unbound generic type parameters.
    """
    ensure_model_complete(model, ctx)
    generic_meta = getattr(model, "__pydantic_generic_metadata__", {})
    if generic_meta.get("parameters"):
        msg = (
            f"Unparameterized generic model {model.__name__} has unbound type parameters: {generic_meta['parameters']}"
        )
        ctx.raise_err("E4", msg)


def validate_model_class(model: object, ctx: SchemaBuildContext) -> type[BaseModel]:
    """Validate that ``model`` is a concrete Pydantic ``BaseModel`` subclass.

    :param model: Candidate model class.
    :type model: object
    :param ctx: Active schema build context.
    :type ctx: SchemaBuildContext
    :return: Validated ``BaseModel`` subclass.
    :rtype: type[BaseModel]
    :raises ConversionError: If ``model`` is not a concrete ``BaseModel`` subclass.
    """
    if not isinstance(model, type) or not issubclass(model, BaseModel) or model in {BaseModel, RootModel}:
        msg = f"Expected a concrete Pydantic BaseModel subclass, got {model!r}"
        ctx.raise_err("E1", msg)
    ensure_concrete_model(model, ctx)
    return model


def clean_docstring(cls: type[Any]) -> str | None:
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


def extract_class_docstring(tp: object) -> str | None:
    """Extract a cleaned class docstring from a ``BaseModel`` or ``Enum`` subclass when defined on ``tp``.

    :param tp: Candidate Python type.
    :type tp: object
    :return: Cleaned docstring or ``None``.
    :rtype: str | None
    """
    if not isinstance(tp, type) or not (issubclass(tp, BaseModel) or issubclass(tp, Enum)):
        return None
    if tp in {BaseModel, RootModel, Enum, IntEnum, StrEnum}:
        return None
    return clean_docstring(tp)


def safe_type_hints(tp: type[Any], module_ns: Mapping[str, Any]) -> dict[str, Any]:
    """Resolve type hints for ``tp`` with fallback to ``__annotations__``.

    :param tp: Structured class (``TypedDict`` or dataclass) to inspect.
    :type tp: type[Any]
    :param module_ns: Module namespace dictionary for forward-reference evaluation.
    :type module_ns: Mapping[str, Any]
    :return: Mapping of attribute name to resolved annotation.
    :rtype: dict[str, Any]
    """
    try:
        return get_type_hints(tp, globalns=dict(module_ns), include_extras=True)
    except (NameError, TypeError, AttributeError, ValueError):
        return dict(getattr(tp, "__annotations__", {}))


def typeddict_to_model(tp: type[Any], ctx: SchemaBuildContext) -> type[BaseModel]:
    """Convert a ``TypedDict`` class into a dynamic ``BaseModel`` subclass.

    :param tp: ``TypedDict`` class to convert.
    :type tp: type[Any]
    :param ctx: Active schema build context.
    :type ctx: SchemaBuildContext
    :return: Synthesized ``BaseModel`` subclass.
    :rtype: type[BaseModel]
    """
    hints = safe_type_hints(tp, ctx.module_ns)
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


def dataclass_to_model(tp: type[Any], ctx: SchemaBuildContext) -> type[BaseModel]:
    """Convert a stdlib ``@dataclass`` class into a dynamic ``BaseModel`` subclass.

    :param tp: Dataclass type to convert.
    :type tp: type[Any]
    :param ctx: Active schema build context.
    :type ctx: SchemaBuildContext
    :return: Synthesized ``BaseModel`` subclass.
    :rtype: type[BaseModel]
    """
    hints = safe_type_hints(tp, ctx.module_ns)
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
    model_cls.__doc__ = clean_docstring(tp)
    return model_cls


def coerce_structured_type_to_model(tp: object, ctx: SchemaBuildContext) -> object:
    """Convert nested ``TypedDict`` or stdlib ``@dataclass`` classes into ``BaseModel`` subclasses.

    :param tp: Candidate type annotation.
    :type tp: object
    :param ctx: Active schema build context.
    :type ctx: SchemaBuildContext
    :return: Synthesized ``BaseModel`` subclass or ``tp`` unchanged.
    :rtype: object
    """
    if not isinstance(tp, type) or issubclass(tp, BaseModel):
        return tp
    if is_typeddict(tp):
        return typeddict_to_model(tp, ctx)
    if is_dataclass(tp):
        return dataclass_to_model(tp, ctx)
    return tp
