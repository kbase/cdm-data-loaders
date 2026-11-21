"""Unit tests for Pydantic annotation unwrapping and classification in cdm_data_loaders.converters.sources.pydantic.annotations."""

import re
from collections.abc import Callable
from decimal import Decimal
from enum import StrEnum
from typing import Annotated, Any, Final, Literal, NewType, TypeVar, Unpack

import pytest
from pydantic import Base64Bytes, BaseModel, ConfigDict, Field, NaiveDatetime, RootModel, computed_field

from cdm_data_loaders.converters.core.errors import ConversionError
from cdm_data_loaders.converters.sources.pydantic.annotations import (
    ParsedAnnotation,
    allows_null,
    classify_parsed_kind,
    extract_model_field_specs,
    is_valid_string_map_key,
    is_variadic_unpack,
    substitute_typevars,
    unwrap_annotation,
)
from cdm_data_loaders.converters.sources.pydantic.model_synthesis import SchemaBuildContext

type MyBox[T] = list[T]
type SimpleAlias = Annotated[int, Field(description="from alias")]
UserId = NewType("UserId", str)
SAMPLE_TYPEVAR: Final = TypeVar("SAMPLE_TYPEVAR")


class _Color(StrEnum):
    RED = "red"


class _DocModel(BaseModel):
    """Docstring from model."""

    x: int


class _RootStr(RootModel[str]):
    pass


class _ExtraAllowModel(BaseModel):
    model_config = ConfigDict(extra="allow")
    x: int


class _RichModel(BaseModel):
    kept: int = Field(alias="keptAlias", serialization_alias="keptSer", description="kept desc")
    hidden: str = Field(default="x", exclude=True)
    opt: str | None = None

    @computed_field(alias="compAlias", description="comp desc")  # type: ignore[prop-decorator]
    @property
    def total(self) -> int:
        return self.kept


class _AliasFallbackModel(BaseModel):
    a_field: int = Field(alias="aAlias")
    default_val: int = 5
    default_none: int = None  # type: ignore[assignment]

    @computed_field(alias="cAlias")  # type: ignore[prop-decorator]
    @property
    def comp_opt(self) -> str | None:
        return None


def _ctx(*, by_alias: bool = False, module_ns: dict[str, Any] | None = None) -> SchemaBuildContext:
    return SchemaBuildContext(
        model_counts={},
        max_recursion_depth=3,
        by_alias=by_alias,
        error_type=ConversionError,
        error_by_code={},
        module_ns=module_ns or {},
    )


@pytest.mark.parametrize(
    ("annotation", "mapping", "expected"),
    [
        pytest.param(SAMPLE_TYPEVAR, {SAMPLE_TYPEVAR: int}, int, id="direct-typevar"),
        pytest.param(Annotated[SAMPLE_TYPEVAR, "m"], {SAMPLE_TYPEVAR: str}, Annotated[str, "m"], id="annotated"),
        pytest.param(SAMPLE_TYPEVAR | None, {SAMPLE_TYPEVAR: int}, int | None, id="union"),
        pytest.param(list[SAMPLE_TYPEVAR], {SAMPLE_TYPEVAR: float}, list[float], id="generic-list"),
    ],
)
def test_substitute_typevars_replaces_parameters(
    annotation: object,
    mapping: dict[TypeVar, Any],
    expected: object,
) -> None:
    """Verify substitute_typevars replaces TypeVars across direct, annotated, union, and generic forms."""
    assert substitute_typevars(annotation, mapping) == expected


@pytest.mark.parametrize(
    ("annotation", "expected_attrs"),
    [
        pytest.param(MyBox[int], {"annotation": list[int]}, id="generic-type-alias"),
        pytest.param(SimpleAlias, {"annotation": int, "description": "from alias"}, id="annotated-type-alias"),
        pytest.param("TargetStr", {"annotation": str}, id="forward-ref"),
        pytest.param(UserId, {"annotation": str}, id="new-type"),
        pytest.param(_RootStr, {"annotation": str}, id="root-model"),
        pytest.param(
            Annotated[Decimal, Field(max_digits=10, decimal_places=3)],
            {"max_digits": 10, "decimal_places": 3},
            id="decimal-constraints",
        ),
        pytest.param(NaiveDatetime, {"is_naive_datetime": True}, id="naive-datetime"),
        pytest.param(Base64Bytes, {"is_base64_bytes": True}, id="base64-bytes"),
        pytest.param(_DocModel | None, {"description": "Docstring from model."}, id="optional-model-docstring"),
    ],
)
def test_unwrap_annotation_variants(annotation: object, expected_attrs: dict[str, Any]) -> None:
    """Verify unwrap_annotation resolves aliases, forward refs, NewType, RootModel, and metadata."""
    parsed, _ = unwrap_annotation(annotation, _ctx(module_ns={"TargetStr": str}))
    for attr, expected in expected_attrs.items():
        assert getattr(parsed, attr) == expected


def test_unwrap_annotation_missing_forward_ref_raises() -> None:
    """Verify unwrap_annotation raises ConversionError for unresolved forward references."""
    with pytest.raises(ConversionError):
        unwrap_annotation("MissingRef", _ctx(module_ns={"TargetStr": str}))


@pytest.mark.parametrize(
    ("annotation", "expected"),
    [
        pytest.param(None, True, id="none"),
        pytest.param(Literal.__getitem__((None, "a")), True, id="literal-with-none"),
        pytest.param(int | None, True, id="optional-int"),
        pytest.param(str, False, id="non-null-str"),
    ],
)
def test_allows_null_variants(annotation: object, expected: bool) -> None:
    """Verify allows_null detects nullable annotations and rejects non-nullable ones."""
    assert allows_null(annotation, _ctx()) is expected


@pytest.mark.parametrize(
    ("annotation", "expected_kind"),
    [
        pytest.param(None, "null", id="null"),
        pytest.param(list[int], "sequence", id="list"),
        pytest.param(tuple[int, ...], "sequence", id="variadic-tuple"),
        pytest.param(tuple[int, str], "tuple", id="fixed-tuple"),
        pytest.param(dict[str, int], "map", id="dict-map"),
        pytest.param(_DocModel, "model", id="model"),
        pytest.param(int, "scalar", id="scalar"),
    ],
)
def test_classify_parsed_kind_valid(annotation: object, expected_kind: str) -> None:
    """Verify classify_parsed_kind classifies supported annotations into IR kinds."""
    assert classify_parsed_kind(ParsedAnnotation(annotation), _ctx()) == expected_kind


@pytest.mark.parametrize(
    "bad_annotation",
    [
        pytest.param(Any, id="any"),
        pytest.param(object, id="object"),
        pytest.param(TypeVar("U"), id="typevar"),
        pytest.param(Callable[[], None], id="callable"),
        pytest.param(re.Pattern, id="regex-pattern"),
        pytest.param(list, id="untyped-list"),
        pytest.param(tuple, id="untyped-tuple"),
        pytest.param(dict, id="untyped-dict"),
        pytest.param(BaseModel, id="bare-base-model"),
        pytest.param(RootModel, id="bare-root-model"),
    ],
)
def test_classify_parsed_kind_unsupported_raises(bad_annotation: object) -> None:
    """Verify classify_parsed_kind raises ConversionError for unsupported annotations."""
    with pytest.raises(ConversionError):
        classify_parsed_kind(ParsedAnnotation(bad_annotation), _ctx())


@pytest.mark.parametrize(
    ("key_type", "expected"),
    [
        pytest.param(str, True, id="str"),
        pytest.param(Literal["a", _Color.RED], True, id="literal-str-and-strenum"),
        pytest.param(_Color | Literal["b"] | None, True, id="strenum-literal-optional-union"),
        pytest.param(int, False, id="int"),
        pytest.param(None, False, id="none"),
        pytest.param(Annotated[None, "a"] | Annotated[None, "b"], False, id="annotated-none-union"),
        pytest.param(Literal[1, "a"], False, id="mixed-literal"),
    ],
)
def test_is_valid_string_map_key_variants(key_type: object, expected: bool) -> None:
    """Verify is_valid_string_map_key accepts string-like keys and rejects non-string keys."""
    assert is_valid_string_map_key(key_type, _ctx()) is expected


@pytest.mark.parametrize(
    "bad_key",
    [
        pytest.param(Any, id="any"),
        pytest.param(object, id="object"),
        pytest.param(TypeVar("K"), id="typevar"),
    ],
)
def test_is_valid_string_map_key_unsupported_raises(bad_key: object) -> None:
    """Verify is_valid_string_map_key raises ConversionError for unsupported key types."""
    with pytest.raises(ConversionError):
        is_valid_string_map_key(bad_key, _ctx())


@pytest.mark.parametrize(
    ("annotation", "expected"),
    [
        pytest.param(Ellipsis, True, id="ellipsis"),
        pytest.param(Unpack[tuple[int, ...]], True, id="unpack-variadic-tuple"),
        pytest.param(Unpack[tuple], True, id="unpack-untyped-tuple"),
        pytest.param(Unpack[tuple[int, str]], False, id="unpack-fixed-tuple"),
        pytest.param(int, False, id="scalar-int"),
    ],
)
def test_is_variadic_unpack_variants(annotation: object, expected: bool) -> None:
    """Verify is_variadic_unpack distinguishes variadic tuple unpacks from fixed types."""
    assert is_variadic_unpack(annotation) is expected


def test_extract_model_field_specs_extra_allow_raises() -> None:
    """Verify extract_model_field_specs rejects models with extra='allow'."""
    with pytest.raises(ConversionError):
        extract_model_field_specs(_ExtraAllowModel, _ctx())


def test_extract_model_field_specs_excluded_aliases_and_computed() -> None:
    """Verify extract_model_field_specs omits excluded fields and resolves serialization and computed aliases."""
    specs = extract_model_field_specs(_RichModel, _ctx(by_alias=True))
    by_name = {s.output_name: s for s in specs}

    assert "hidden" not in by_name
    assert by_name["keptSer"].required_non_null is True
    assert by_name["keptSer"].description == "kept desc"
    assert by_name["opt"].required_non_null is False
    assert by_name["compAlias"].annotation is int
    assert by_name["compAlias"].description == "comp desc"


@pytest.mark.parametrize(
    ("by_alias", "expected_names", "expected_required"),
    [
        pytest.param(
            False,
            {"a_field", "default_val", "default_none", "comp_opt"},
            {"a_field": True, "default_val": True, "default_none": False, "comp_opt": False},
            id="python-names",
        ),
        pytest.param(
            True,
            {"aAlias", "default_val", "default_none", "cAlias"},
            {"aAlias": True, "default_val": True, "default_none": False, "cAlias": False},
            id="alias-names",
        ),
    ],
)
def test_extract_model_field_specs_alias_mode_and_defaults(
    by_alias: bool,
    expected_names: set[str],
    expected_required: dict[str, bool],
) -> None:
    """Verify extract_model_field_specs honors by_alias and marks default=None fields nullable."""
    specs = extract_model_field_specs(_AliasFallbackModel, _ctx(by_alias=by_alias))
    by_name = {s.output_name: s for s in specs}

    assert set(by_name) == expected_names
    for name, req in expected_required.items():
        assert by_name[name].required_non_null is req
    first_spec = next(iter(by_name.values()))
    assert first_spec.context.model_counts[_AliasFallbackModel] == 1
