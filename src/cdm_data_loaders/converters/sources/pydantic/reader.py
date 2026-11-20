"""Public ``PydanticReader`` lowering Pydantic v2 ``BaseModel`` classes into the converter IR."""

import sys
from dataclasses import replace
from typing import NoReturn, get_origin

from frozendict import frozendict
from pydantic import BaseModel, ConfigDict, Field, RootModel, StrictBool

from cdm_data_loaders.converters.core.errors import ConversionError, ErrorCodeMap, raise_spec_error
from cdm_data_loaders.converters.core.inference import unify_scalar_specs
from cdm_data_loaders.converters.core.ir import Provenance, SchemaDocument, TypedNode
from cdm_data_loaders.converters.sources.pydantic.annotations import (
    allows_null,
    classify_parsed_kind,
    flatten_union_branches,
    is_none_type,
    is_union_origin,
    unwrap_annotation,
)
from cdm_data_loaders.converters.sources.pydantic.model_synthesis import (
    SchemaBuildContext,
    clean_docstring,
    validate_model_class,
)
from cdm_data_loaders.converters.sources.pydantic.scalars import resolve_parsed_scalar
from cdm_data_loaders.converters.sources.pydantic.unions import build_merged_models_struct
from cdm_data_loaders.utils.recursion import DEFAULT_MAX_RECURSION_DEPTH, StrictPositiveRecursionDepth


class PydanticReader(BaseModel):
    """Build typed converter IR documents from Pydantic v2 ``BaseModel`` classes.

    :param max_recursion_depth: Maximum times a recursive ``BaseModel`` may be expanded along an ancestor path.
    :type max_recursion_depth: int
    :param by_alias: When ``True``, use field aliases rather than Python attribute names.
    :type by_alias: bool
    :param error_type: Base exception class raised when a construct cannot be converted.
    :type error_type: type[ConversionError]
    :param error_by_code: Mapping from specification error code (``E1``-``E12``) to exception class.
    :type error_by_code: ErrorCodeMap
    """

    model_config = ConfigDict(arbitrary_types_allowed=True, extra="forbid", frozen=True)

    max_recursion_depth: StrictPositiveRecursionDepth = DEFAULT_MAX_RECURSION_DEPTH
    by_alias: StrictBool = False
    error_type: type[ConversionError] = Field(default_factory=lambda: ConversionError)
    error_by_code: ErrorCodeMap = Field(default_factory=frozendict)

    def _raise_err(self, code: str, msg: str) -> NoReturn:
        """Raise the configured exception subclass for ``code`` or fall back to ``error_type``.

        :param code: Specification error code (``E1``-``E12``).
        :type code: str
        :param msg: Diagnostic error message.
        :type msg: str
        :raises ConversionError: Always raised with the resolved error class.
        """
        return raise_spec_error(code, msg, error_type=self.error_type, error_by_code=self.error_by_code)

    def read(self, source: type[BaseModel]) -> SchemaDocument:
        """Read a Pydantic ``BaseModel`` subclass into a ``SchemaDocument``.

        :param source: Pydantic v2 ``BaseModel`` subclass.
        :type source: type[BaseModel]
        :return: Structural IR document with ``source="pydantic"`` provenance.
        :rtype: SchemaDocument
        :raises ConversionError: If the model or any of its fields violates ``E1``-``E12``.
        """
        init_ctx = SchemaBuildContext(
            max_recursion_depth=self.max_recursion_depth,
            by_alias=self.by_alias,
            error_type=self.error_type,
            error_by_code=self.error_by_code,
        )
        validated_model = validate_model_class(source, init_ctx)
        mod = sys.modules.get(validated_model.__module__)
        module_ns = vars(mod) if mod is not None else {}

        ctx = SchemaBuildContext(
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
            parsed, next_ctx = unwrap_annotation(root_finfo.annotation, ctx, field_info=root_finfo)
            origin = get_origin(parsed.annotation)
            if is_union_origin(origin):
                branches = flatten_union_branches(parsed, next_ctx)
                non_null = [(b, b_ctx) for b, b_ctx in branches if not is_none_type(b.annotation)]
                if not non_null or any(classify_parsed_kind(b, b_ctx) != "model" for b, b_ctx in non_null):
                    msg = (
                        f"RootModel union must consist exclusively of BaseModel branches, got {root_finfo.annotation!r}"
                    )
                    self._raise_err("E2", msg)
                root_struct = build_merged_models_struct(
                    [b.annotation for b, _ in non_null],
                    next_ctx,
                    (),
                    nullable=False,
                )
            elif classify_parsed_kind(parsed, next_ctx) != "model":
                msg = f"RootModel must wrap a BaseModel or union of BaseModels, got {parsed.annotation!r}"
                self._raise_err("E2", msg)
            else:
                root_struct = build_merged_models_struct([parsed.annotation], next_ctx, (), nullable=False)
        else:
            root_struct = build_merged_models_struct([validated_model], ctx, (), nullable=False)

        doc = clean_docstring(source)
        annotations = {"description": doc} if doc is not None else {}
        root = replace(root_struct, annotations=annotations)
        qualname = source.__qualname__.replace(".<locals>.", ".")
        return SchemaDocument(
            root=root,
            name=source.__name__,
            identifier=f"urn:pydantic:{source.__module__}.{qualname}",
            annotations=annotations,
            provenance=Provenance(source="pydantic", path=(source.__name__,)),
        )

    def read_scalar_annotation(self, annotation: object) -> TypedNode:
        """Read a scalar Python or Pydantic type annotation into an IR ``TypedNode``.

        :param annotation: Scalar type annotation.
        :type annotation: object
        :return: Structural IR scalar node with ``pydantic_type`` provenance metadata.
        :rtype: TypedNode
        :raises ConversionError: If ``annotation`` is not a supported scalar type.
        """
        ctx = SchemaBuildContext(
            max_recursion_depth=self.max_recursion_depth,
            by_alias=self.by_alias,
            error_type=self.error_type,
            error_by_code=self.error_by_code,
        )
        parsed, next_ctx = unwrap_annotation(annotation, ctx)
        origin = get_origin(parsed.annotation)
        if is_union_origin(origin):
            branches = flatten_union_branches(parsed, next_ctx)
            non_null = [(b, b_ctx) for b, b_ctx in branches if not is_none_type(b.annotation)]
            if not non_null:
                msg = "Union contains only NoneType branches"
                self._raise_err("E7", msg)
            node_type, hints, pyd_type = unify_scalar_specs([resolve_parsed_scalar(b, b_ctx) for b, b_ctx in non_null])
        else:
            node_type, hints, pyd_type = resolve_parsed_scalar(parsed, next_ctx)
        return TypedNode(
            type=node_type,
            nullable=allows_null(annotation, ctx),
            hints=hints,
            provenance=Provenance(source="pydantic", path=(), metadata={"pydantic_type": pyd_type}),
        )
