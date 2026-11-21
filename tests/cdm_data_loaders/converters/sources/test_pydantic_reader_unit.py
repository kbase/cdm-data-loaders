"""Unit tests for cdm_data_loaders.converters.sources.pydantic.reader.PydanticReader."""

from typing import Annotated, Any

import pytest
from frozendict import frozendict
from pydantic import BaseModel, Field, RootModel, ValidationError

from cdm_data_loaders.converters.core.errors import ConversionError
from cdm_data_loaders.converters.core.ir import Provenance
from cdm_data_loaders.converters.sources.pydantic.reader import PydanticReader


class _ReaderConvError(ConversionError):
    """Custom error for PydanticReader error_by_code tests."""


class _ModelA(BaseModel):
    a_field: int = Field(alias="aAlias")


class _ModelB(BaseModel):
    b_field: str


class _RootSingle(RootModel[_ModelA]):
    """Root single doc."""


class _RootUnion(RootModel[_ModelA | _ModelB | None]):
    pass


def _reader() -> PydanticReader:
    return PydanticReader(by_alias=True, error_by_code=frozendict({"E2": _ReaderConvError, "E7": _ReaderConvError}))


def test_pydantic_reader_invalid_by_alias_raises() -> None:
    """Verify PydanticReader rejects non-boolean by_alias configuration values."""
    with pytest.raises(ValidationError):
        PydanticReader(by_alias="yes")  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "bad_root",
    [
        pytest.param(RootModel[int], id="scalar-root"),
        pytest.param(RootModel[int | None], id="optional-scalar-root"),
        pytest.param(RootModel[None], id="none-root"),
    ],
)
def test_pydantic_reader_read_invalid_root_model_raises(bad_root: type[RootModel[Any]]) -> None:
    """Verify PydanticReader.read rejects RootModel types that do not wrap model objects."""
    with pytest.raises(_ReaderConvError, match="RootModel"):
        _reader().read(bad_root)


def test_pydantic_reader_read_root_model_single() -> None:
    """Verify PydanticReader.read lowers a single-model RootModel with aliases, docstrings, and provenance."""
    doc = _reader().read(_RootSingle)

    assert doc.name == "_RootSingle"
    assert doc.identifier == f"urn:pydantic:{_RootSingle.__module__}._RootSingle"
    assert doc.annotations == {"description": "Root single doc."}
    assert doc.root.annotations == {"description": "Root single doc."}
    assert doc.root.nullable is False
    assert doc.root.properties[0].name == "aAlias"
    assert doc.provenance == Provenance("pydantic", ("_RootSingle",))


def test_pydantic_reader_read_root_model_union() -> None:
    """Verify PydanticReader.read merges fields across a union RootModel."""
    doc = _reader().read(_RootUnion)

    assert doc.annotations == {}
    assert doc.root.annotations == {}
    assert {f.name for f in doc.root.properties} == {"aAlias", "b_field"}


def test_pydantic_reader_read_local_model_strips_locals_from_identifier() -> None:
    """Verify PydanticReader.read omits .<locals>. segments from locally defined model identifiers."""

    class _LocalModel(BaseModel):
        x: int

    doc = _reader().read(_LocalModel)

    assert doc.identifier is not None
    assert ".<locals>." not in doc.identifier


@pytest.mark.parametrize(
    ("annotation", "expected_type", "expected_nullable", "expected_provenance"),
    [
        pytest.param(
            int,
            "integer",
            False,
            Provenance("pydantic", (), {"pydantic_type": "integer"}),
            id="int",
        ),
        pytest.param(
            int | str | None,
            "string",
            True,
            None,
            id="optional-scalar-union",
        ),
    ],
)
def test_pydantic_reader_read_scalar_annotation_valid(
    annotation: object,
    expected_type: str,
    expected_nullable: bool,
    expected_provenance: Provenance | None,
) -> None:
    """Verify read_scalar_annotation resolves scalar types, unions, and nullability."""
    reader = PydanticReader(error_by_code=frozendict({"E7": _ReaderConvError}))
    node = reader.read_scalar_annotation(annotation)

    assert node.type == expected_type
    assert node.nullable is expected_nullable
    if expected_provenance is not None:
        assert node.provenance == expected_provenance


def test_pydantic_reader_read_scalar_annotation_none_only_raises() -> None:
    """Verify read_scalar_annotation rejects unions containing only NoneType."""
    reader = PydanticReader(error_by_code=frozendict({"E7": _ReaderConvError}))

    with pytest.raises(_ReaderConvError, match="only NoneType"):
        reader.read_scalar_annotation(Annotated[None, "a"] | Annotated[None, "b"])
