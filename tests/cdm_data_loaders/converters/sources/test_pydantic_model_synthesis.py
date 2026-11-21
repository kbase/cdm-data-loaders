"""Unit tests for cdm_data_loaders.converters.sources.pydantic.model_synthesis."""

from dataclasses import dataclass, field
from enum import Enum, IntEnum, StrEnum
from typing import Any, ForwardRef, NotRequired, Required, TypedDict

import pytest
from frozendict import frozendict
from pydantic import BaseModel, PydanticSchemaGenerationError, RootModel, create_model

from cdm_data_loaders.converters.core.errors import ConversionError
from cdm_data_loaders.converters.sources.pydantic.model_synthesis import (
    SchemaBuildContext,
    clean_docstring,
    coerce_structured_type_to_model,
    dataclass_to_model,
    ensure_concrete_model,
    ensure_model_complete,
    extract_class_docstring,
    safe_type_hints,
    typeddict_to_model,
    validate_model_class,
)


class CustomConvError(ConversionError):
    """Custom conversion error for context tests."""


class SampleModel(BaseModel):
    """Sample model doc."""

    x: int


class SampleEnum(Enum):
    """Sample enum doc."""

    A = "a"


class SampleIntEnum(IntEnum):
    """Sample int enum doc."""

    ONE = 1


class SampleStrEnum(StrEnum):
    """Sample str enum doc."""

    FOO = "foo"


class _NoDocChildModel(SampleModel):
    y: str


class SampleTypedDict(TypedDict, total=False):
    """TypedDict doc."""

    req_field: Required[int]
    opt_field: NotRequired[str]
    plain_opt: float


class _TotalTypedDict(TypedDict):
    a: int


@dataclass
class SampleDataclass:
    """Dataclass doc.

    More details.
    """

    req: int
    with_default: str = "abc"
    with_factory: list[int] = field(default_factory=list)


@dataclass
class _AutoDocDataclass:
    x: int


class _UnresolvedHints:
    pass


_UnresolvedHints.__annotations__ = {"x": "MissingHintType"}


def _error_ctx() -> SchemaBuildContext:
    return SchemaBuildContext(
        error_by_code=frozendict({"E1": CustomConvError, "E4": CustomConvError, "E9": CustomConvError})
    )


@pytest.mark.parametrize(
    ("code", "message", "cause", "expected_exc"),
    [
        pytest.param("E4", "bad generic", ValueError("boom"), CustomConvError, id="mapped-code-with-cause"),
        pytest.param("E99", "other", None, ConversionError, id="unmapped-code-fallback"),
    ],
)
def test_schema_build_context_raise_err(
    code: str,
    message: str,
    cause: Exception | None,
    expected_exc: type[ConversionError],
) -> None:
    """Verify SchemaBuildContext.raise_err selects mapped or fallback errors and preserves cause."""
    ctx = SchemaBuildContext(
        max_recursion_depth=5,
        by_alias=True,
        error_type=ConversionError,
        error_by_code=frozendict({"E4": CustomConvError}),
    )

    with pytest.raises(expected_exc, match=message) as exc_info:
        ctx.raise_err(code, message, cause=cause)
    assert exc_info.value.__cause__ is cause


def test_schema_build_context_with_alias_and_with_model() -> None:
    """Verify with_alias and with_model update active aliases, model counts, and module namespace immutably."""
    ctx = SchemaBuildContext(
        max_recursion_depth=5,
        by_alias=True,
        error_type=ConversionError,
        error_by_code=frozendict({"E4": CustomConvError}),
    )

    alias_ctx = ctx.with_alias("AliasObj")
    assert alias_ctx.active_aliases == frozenset({"AliasObj"})
    assert alias_ctx.max_recursion_depth == ctx.max_recursion_depth
    assert alias_ctx.by_alias is True
    assert alias_ctx.error_by_code == ctx.error_by_code

    model_ctx = alias_ctx.with_model(SampleModel)
    assert model_ctx.model_counts[SampleModel] == 1
    assert model_ctx.active_aliases == frozenset()
    assert model_ctx.max_recursion_depth == ctx.max_recursion_depth
    assert model_ctx.by_alias is True
    assert model_ctx.error_by_code == ctx.error_by_code
    assert "SampleModel" in model_ctx.module_ns

    model_ctx2 = model_ctx.with_model(SampleModel)
    expected_count = 2
    assert model_ctx2.model_counts[SampleModel] == expected_count


def test_schema_build_context_with_model_missing_module_preserves_ns() -> None:
    """Verify with_model keeps the existing module_ns when the model module is not in sys.modules."""

    class MissingModModel(BaseModel):
        a: int

    MissingModModel.__module__ = "nonexistent_mod_xyz_123"
    custom_ns = frozendict({"sentinel": 42})
    ctx_with_ns = SchemaBuildContext(module_ns=custom_ns)

    assert ctx_with_ns.with_model(MissingModModel).module_ns == custom_ns


def test_validate_model_class_valid() -> None:
    """Verify validate_model_class returns a concrete BaseModel subclass unchanged."""
    assert validate_model_class(SampleModel, _error_ctx()) is SampleModel


@pytest.mark.parametrize(
    "invalid",
    [
        pytest.param(123, id="non-type"),
        pytest.param(dict, id="non-model-class"),
        pytest.param(BaseModel, id="bare-base-model"),
        pytest.param(RootModel, id="bare-root-model"),
    ],
)
def test_validate_model_class_invalid_raises(invalid: object) -> None:
    """Verify validate_model_class rejects non-model types and abstract Pydantic base classes."""
    with pytest.raises(CustomConvError):
        validate_model_class(invalid, _error_ctx())


def test_ensure_model_complete_rebuilds_incomplete_model() -> None:
    """Verify ensure_model_complete rebuilds an incomplete model with resolvable forward references."""

    class IncompleteModel(BaseModel):
        ref: "SampleModel"

    IncompleteModel.__pydantic_complete__ = False
    ensure_model_complete(IncompleteModel, _error_ctx())

    assert IncompleteModel.__pydantic_complete__ is True


@pytest.mark.parametrize(
    ("ctx", "expected_exc"),
    [
        pytest.param(_error_ctx(), CustomConvError, id="with-context"),
        pytest.param(None, ConversionError, id="without-context"),
    ],
)
def test_ensure_model_complete_unresolved_forward_ref_raises(
    ctx: SchemaBuildContext | None,
    expected_exc: type[ConversionError],
) -> None:
    """Verify ensure_model_complete raises on unresolved forward references."""
    broken_ref_model = create_model("BrokenRefModel", bad=(ForwardRef("DoesNotExistXYZ"), ...))

    with pytest.raises(expected_exc, match="Unresolved forward reference"):
        ensure_model_complete(broken_ref_model, ctx)


def test_ensure_model_complete_ignores_schema_generation_error(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify ensure_model_complete ignores PydanticSchemaGenerationError during model_rebuild."""

    class SchemaGenErrModel(BaseModel):
        x: int

    def _raise_schema_gen(_cls: type[BaseModel], **_kwargs: object) -> bool:
        msg = "skip"
        raise PydanticSchemaGenerationError(msg)

    SchemaGenErrModel.__pydantic_complete__ = False
    monkeypatch.setattr(SchemaGenErrModel, "model_rebuild", classmethod(_raise_schema_gen))

    ensure_model_complete(SchemaGenErrModel, _error_ctx())


@pytest.mark.parametrize(
    ("ctx", "expected_exc"),
    [
        pytest.param(_error_ctx(), CustomConvError, id="with-context"),
        pytest.param(None, ConversionError, id="without-context"),
    ],
)
def test_ensure_model_complete_still_incomplete_raises(
    monkeypatch: pytest.MonkeyPatch,
    ctx: SchemaBuildContext | None,
    expected_exc: type[ConversionError],
) -> None:
    """Verify ensure_model_complete raises when model_rebuild returns False."""

    class StillIncompleteModel(BaseModel):
        x: int

    def _return_false(_cls: type[BaseModel], **_kwargs: object) -> bool:
        return False

    StillIncompleteModel.__pydantic_complete__ = False
    monkeypatch.setattr(StillIncompleteModel, "model_rebuild", classmethod(_return_false))

    with pytest.raises(expected_exc, match="remains incomplete"):
        ensure_model_complete(StillIncompleteModel, ctx)


def test_ensure_concrete_model_unparameterized_generic_raises() -> None:
    """Verify ensure_concrete_model rejects unparameterized generic models."""

    class GenericBox[T](BaseModel):
        val: T

    with pytest.raises(CustomConvError, match="Unparameterized generic model"):
        ensure_concrete_model(GenericBox, _error_ctx())


@pytest.mark.parametrize(
    ("cls", "expected"),
    [
        pytest.param(SampleModel, "Sample model doc.", id="model-with-doc"),
        pytest.param(_NoDocChildModel, None, id="subclass-without-own-doc"),
        pytest.param(_AutoDocDataclass, None, id="auto-generated-dataclass-doc"),
        pytest.param(SampleDataclass, "Dataclass doc.\n\nMore details.", id="multiline-dataclass-doc"),
    ],
)
def test_clean_docstring_variants(cls: type[Any], expected: str | None) -> None:
    """Verify clean_docstring strips indentation and ignores inherited or auto-generated docstrings."""
    assert clean_docstring(cls) == expected


@pytest.mark.parametrize(
    ("target", "expected"),
    [
        pytest.param("not a type", None, id="non-type"),
        pytest.param(dict, None, id="builtin-dict"),
        pytest.param(BaseModel, None, id="base-model"),
        pytest.param(RootModel, None, id="root-model"),
        pytest.param(Enum, None, id="enum-base"),
        pytest.param(IntEnum, None, id="int-enum-base"),
        pytest.param(StrEnum, None, id="str-enum-base"),
        pytest.param(SampleModel, "Sample model doc.", id="sample-model"),
        pytest.param(SampleEnum, "Sample enum doc.", id="sample-enum"),
        pytest.param(SampleIntEnum, "Sample int enum doc.", id="sample-int-enum"),
        pytest.param(SampleStrEnum, "Sample str enum doc.", id="sample-str-enum"),
    ],
)
def test_extract_class_docstring_variants(target: object, expected: str | None) -> None:
    """Verify extract_class_docstring returns user docstrings and skips non-types and base classes."""
    assert extract_class_docstring(target) == expected


@pytest.mark.parametrize(
    ("cls", "expected"),
    [
        pytest.param(_UnresolvedHints, {"x": "MissingHintType"}, id="unresolved-annotations-fallback"),
        pytest.param(_TotalTypedDict, {"a": int}, id="resolved-typeddict"),
    ],
)
def test_safe_type_hints_variants(cls: type[Any], expected: dict[str, Any]) -> None:
    """Verify safe_type_hints resolves valid annotations and falls back to raw annotations on failure."""
    assert safe_type_hints(cls, {}) == expected


@pytest.mark.parametrize(
    ("td_cls", "expected_doc", "expected_required"),
    [
        pytest.param(
            SampleTypedDict,
            "TypedDict doc.",
            {"req_field": True, "opt_field": False, "plain_opt": False},
            id="partial-typeddict-with-doc",
        ),
        pytest.param(
            _TotalTypedDict,
            None,
            {"a": True},
            id="total-typeddict-without-doc",
        ),
    ],
)
def test_typeddict_to_model_variants(
    td_cls: type[Any],
    expected_doc: str | None,
    expected_required: dict[str, bool],
) -> None:
    """Verify typeddict_to_model preserves docstrings and per-field requiredness."""
    model = typeddict_to_model(td_cls, SchemaBuildContext())

    assert model.__doc__ == expected_doc
    assert {name: f.is_required() for name, f in model.model_fields.items()} == expected_required


def test_dataclass_to_model_preserves_doc_and_defaults() -> None:
    """Verify dataclass_to_model preserves docstrings, required fields, defaults, and default factories."""
    dc_model = dataclass_to_model(SampleDataclass, SchemaBuildContext())

    assert dc_model.__doc__ == "Dataclass doc.\n\nMore details."
    assert dc_model.model_fields["req"].is_required() is True
    assert dc_model.model_fields["with_default"].default == "abc"
    assert dc_model.model_fields["with_factory"].default_factory is list


@pytest.mark.parametrize(
    ("source_type", "expect_synthesized_model"),
    [
        pytest.param(int | str, False, id="union-passthrough"),
        pytest.param(SampleModel, False, id="model-passthrough"),
        pytest.param(int, False, id="scalar-passthrough"),
        pytest.param(SampleTypedDict, True, id="typeddict-synthesized"),
        pytest.param(SampleDataclass, True, id="dataclass-synthesized"),
    ],
)
def test_coerce_structured_type_to_model_variants(source_type: object, expect_synthesized_model: bool) -> None:
    """Verify coerce_structured_type_to_model synthesizes models for TypedDict and dataclass only."""
    result = coerce_structured_type_to_model(source_type, SchemaBuildContext())

    if expect_synthesized_model:
        assert isinstance(result, type)
        assert issubclass(result, BaseModel)
    else:
        assert result == source_type
