"""Unit tests for Pydantic union and container IR builders in cdm_data_loaders.converters.sources.pydantic.unions."""

from typing import Any

import pytest
from pydantic import BaseModel, Field

from cdm_data_loaders.converters.core.errors import ConversionError
from cdm_data_loaders.converters.core.ir import NodeHints, Provenance, TypedNode
from cdm_data_loaders.converters.sources.pydantic.annotations import unwrap_annotation
from cdm_data_loaders.converters.sources.pydantic.model_synthesis import SchemaBuildContext
from cdm_data_loaders.converters.sources.pydantic.unions import (
    build_field_ir,
    build_merged_models_struct,
    build_multi_branch_node,
    build_parsed_node,
)


class _ModelA(BaseModel):
    shared: int = Field(description="from A")
    only_a: str


class _ModelB(BaseModel):
    shared: float
    only_b: bool


class _EmptyModel(BaseModel):
    pass


class _ExplicitValueField(BaseModel):
    val: str = Field(alias="_value")


class _LeafA(BaseModel):
    a: int


class _LeafB(BaseModel):
    b: str


def _ctx(
    *,
    max_recursion_depth: int = 3,
    model_counts: dict[type[BaseModel], int] | None = None,
    by_alias: bool = False,
) -> SchemaBuildContext:
    return SchemaBuildContext(
        model_counts=model_counts or {},
        max_recursion_depth=max_recursion_depth,
        by_alias=by_alias,
        error_type=ConversionError,
        error_by_code={},
    )


def test_build_field_ir_sequence() -> None:
    """Verify build_field_ir lowers sequence annotations with element provenance."""
    seq_field = build_field_ir("tags", list[str], _ctx(), ("tags",), required_in_parent=True)

    assert seq_field.node.type == "array"
    assert seq_field.node.nullable is False
    assert seq_field.node.provenance == Provenance("pydantic", ("tags",))
    assert seq_field.node.items is not None
    assert seq_field.node.items.provenance == Provenance("pydantic", ("tags", "element"), {"pydantic_type": "string"})


def test_build_field_ir_tuple() -> None:
    """Verify build_field_ir lowers fixed tuples to closed structs with indexed fields."""
    tup_field = build_field_ir("pair", tuple[str, int], _ctx(), ("pair",), required_in_parent=True)

    assert tup_field.node.type == "object"
    assert tup_field.node.required_names == ("item_0", "item_1")
    assert tup_field.node.additional_properties == TypedNode(type="never", nullable=False)
    assert tup_field.node.provenance == Provenance("pydantic", ("pair",))
    assert tup_field.node.properties[0].provenance == Provenance(
        "pydantic", ("pair", "item_0"), {"pydantic_type": "string"}
    )


def test_build_field_ir_map() -> None:
    """Verify build_field_ir lowers string-keyed dict annotations to map nodes with key and value provenance."""
    map_field = build_field_ir("counts", dict[str, int], _ctx(), ("counts",), required_in_parent=False)

    assert map_field.node.type == "map"
    assert map_field.node.nullable is True
    assert map_field.node.provenance == Provenance("pydantic", ("counts",))
    assert map_field.node.key_type == TypedNode(
        type="string",
        nullable=False,
        hints=NodeHints(logical_type="string"),
        provenance=Provenance("pydantic", ("counts", "key")),
    )
    assert map_field.node.value_type is not None
    assert map_field.node.value_type.provenance == Provenance(
        "pydantic", ("counts", "value"), {"pydantic_type": "integer"}
    )


@pytest.mark.parametrize(
    "bad_ann",
    [
        pytest.param(list, id="untyped-list"),
        pytest.param(tuple[()], id="empty-tuple"),
        pytest.param(dict, id="untyped-dict"),
        pytest.param(dict[int, str], id="non-str-dict-key"),
        pytest.param(None, id="none"),
    ],
)
def test_build_field_ir_invalid_container_raises(bad_ann: object) -> None:
    """Verify build_field_ir raises ConversionError for invalid container annotations."""
    with pytest.raises(ConversionError):
        build_field_ir("bad", bad_ann, _ctx(), ("bad",), required_in_parent=True)


def test_build_merged_models_struct_merges_two_models() -> None:
    """Verify build_merged_models_struct intersects required fields and widens shared field types."""
    merged = build_merged_models_struct([_ModelA, _ModelB], _ctx(), ("u",), nullable=False)
    by_name = {f.name: f for f in merged.properties}

    assert merged.required_names == ("shared",)
    assert merged.additional_properties == TypedNode(type="never", nullable=False)
    assert merged.provenance == Provenance("pydantic", ("u",), {})
    assert by_name["shared"].annotations == {"description": "from A"}
    assert by_name["shared"].provenance == Provenance(
        "pydantic", ("u", "shared"), {"pydantic_type": "integer | number"}
    )
    assert by_name["only_a"].required is False


def test_build_merged_models_struct_with_scalar_branches() -> None:
    """Verify build_merged_models_struct synthesizes a nullable _value field for scalar union branches."""
    scalar_branch, s_ctx = unwrap_annotation(str, _ctx())
    mixed = build_merged_models_struct([_ModelA], s_ctx, ("m",), nullable=True, scalar_branches=[scalar_branch])
    by_name = {f.name: f for f in mixed.properties}

    assert mixed.required_names == ()
    assert by_name["_value"].required is False
    assert by_name["_value"].node.nullable is True
    assert by_name["_value"].provenance == Provenance("pydantic", ("m", "_value"), {"pydantic_type": "string"})


def test_build_merged_models_struct_inline_struct_on_recursion() -> None:
    """Verify build_merged_models_struct marks recursive model expansions with inline_struct provenance."""
    inline_ctx = _ctx(model_counts={_ModelA: 1}, max_recursion_depth=2)
    inlined = build_merged_models_struct([_ModelA], inline_ctx, ("rec",), nullable=False)

    assert inlined.provenance == Provenance(
        "pydantic", ("rec",), {"component_id": id(_ModelA), "type_name": "_ModelA", "inline_struct": True}
    )


def test_build_parsed_node_recursion_cutoff() -> None:
    """Verify build_parsed_node truncates recursive models to a string node at max_recursion_depth."""
    cutoff_ctx = _ctx(model_counts={_ModelA: 1}, max_recursion_depth=1)
    parsed_a, p_ctx = unwrap_annotation(_ModelA, cutoff_ctx)
    cutoff_node = build_parsed_node(parsed_a, p_ctx, ("cut",), nullable=True)

    assert cutoff_node == TypedNode(
        type="string",
        nullable=True,
        hints=NodeHints(logical_type="string"),
        provenance=Provenance("pydantic", ("cut",)),
    )


@pytest.mark.parametrize(
    ("models", "by_alias", "with_scalar"),
    [
        pytest.param([_EmptyModel], False, False, id="empty-model"),
        pytest.param([_ExplicitValueField], True, True, id="value-alias-collision"),
    ],
)
def test_build_merged_models_struct_invalid_raises(
    models: list[type[BaseModel]],
    by_alias: bool,
    with_scalar: bool,
) -> None:
    """Verify build_merged_models_struct rejects empty models and _value alias collisions."""
    ctx = _ctx(by_alias=by_alias)
    scalar_branches = [unwrap_annotation(str, ctx)[0]] if with_scalar else None

    with pytest.raises(ConversionError):
        build_merged_models_struct(models, ctx, ("err",), nullable=False, scalar_branches=scalar_branches)


def test_build_multi_branch_node_matching_containers() -> None:
    """Verify build_multi_branch_node unifies compatible sequence container branches."""
    ctx = _ctx()
    branches = [unwrap_annotation(list[str], ctx), unwrap_annotation(tuple[str, ...], ctx)]

    unified = build_multi_branch_node(branches, ("c",), nullable=False)

    assert unified.type == "array"


@pytest.mark.parametrize(
    ("annotations", "path", "nullable", "expected_type", "expected_provenance"),
    [
        pytest.param(
            (int, None),
            ("opt_i",),
            True,
            "integer",
            None,
            id="single-non-null-with-none",
        ),
        pytest.param(
            (int, float),
            ("num",),
            False,
            "number",
            Provenance("pydantic", ("num",), {"pydantic_type": "integer | number"}),
            id="pure-scalar-union",
        ),
    ],
)
def test_build_multi_branch_node_scalar_variants(
    annotations: tuple[Any, ...],
    path: tuple[str, ...],
    nullable: bool,
    expected_type: str,
    expected_provenance: Provenance | None,
) -> None:
    """Verify build_multi_branch_node lowers optional single-scalar and multi-scalar unions."""
    ctx = _ctx()
    branches = [unwrap_annotation(ann, ctx) for ann in annotations]

    node = build_multi_branch_node(branches, path, nullable=nullable)

    assert node.type == expected_type
    assert node.nullable is nullable
    if expected_provenance is not None:
        assert node.provenance == expected_provenance


def test_build_multi_branch_node_model_and_scalar() -> None:
    """Verify build_multi_branch_node merges model and scalar branches into an object with _value."""
    ctx = _ctx()
    branches = [unwrap_annotation(_LeafA, ctx), unwrap_annotation(str, ctx)]

    node = build_multi_branch_node(branches, ("mix",), nullable=True)

    assert node.type == "object"
    assert node.nullable is True
    assert {f.name for f in node.properties} == {"a", "_value"}


def test_build_multi_branch_node_recursion_cutoff() -> None:
    """Verify build_multi_branch_node falls back to string when all model branches hit recursion cutoff."""
    rec_ctx = _ctx(model_counts={_LeafA: 1, _LeafB: 1}, max_recursion_depth=1)
    branches = [unwrap_annotation(_LeafA, rec_ctx), unwrap_annotation(_LeafB, rec_ctx)]

    node = build_multi_branch_node(branches, ("rec_u",), nullable=True)

    assert node == TypedNode(
        type="string",
        nullable=True,
        hints=NodeHints(logical_type="string"),
        provenance=Provenance("pydantic", ("rec_u",)),
    )


@pytest.mark.parametrize(
    ("annotations", "nullable"),
    [
        pytest.param((list[str], dict[str, int]), False, id="mismatched-containers"),
        pytest.param((None,), True, id="null-only-union"),
    ],
)
def test_build_multi_branch_node_invalid_raises(annotations: tuple[Any, ...], nullable: bool) -> None:
    """Verify build_multi_branch_node rejects mismatched container branches and null-only unions."""
    ctx = _ctx()
    branches = [unwrap_annotation(ann, ctx) for ann in annotations]

    with pytest.raises(ConversionError):
        build_multi_branch_node(branches, ("bad",), nullable=nullable)
