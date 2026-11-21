"""Unit tests for cdm_data_loaders.converters.emitters.pyarrow_codegen.collector."""

from typing import Final

import pyarrow as pa
import pytest

from cdm_data_loaders.converters.core.ir import Field, Provenance, SchemaDocument, TypedNode
from cdm_data_loaders.converters.emitters.pyarrow_codegen.collector import (
    IRComponentCollector,
    extract_component_meta,
    unwrap_array_element_node,
    unwrap_map_value_node,
    unwrap_object_node,
)
from cdm_data_loaders.converters.emitters.pyarrow_codegen.naming import RenderedFieldSpec

PLAIN_OBJECT_NODE: Final[TypedNode] = TypedNode("object")
ELEM_STRING_NODE: Final[TypedNode] = TypedNode("string")
MAP_VALUE_INT_NODE: Final[TypedNode] = TypedNode("integer")
BAD_NAME_OBJECT_NODE: Final[TypedNode] = TypedNode("object", provenance=Provenance("xsd", (), {"type_name": 123}))


@pytest.mark.parametrize(
    ("node", "expected"),
    [
        pytest.param(
            PLAIN_OBJECT_NODE,
            ((id(PLAIN_OBJECT_NODE), False), None, False),
            id="plain-object-falls-back-to-node-id",
        ),
        pytest.param(
            TypedNode(
                "object",
                provenance=Provenance(
                    "xsd",
                    (),
                    {"component_id": 7, "nillable": True, "type_name": "ItemType", "inline_struct": True},
                ),
            ),
            ((7, True), "ItemType", True),
            id="provenance-metadata-present",
        ),
        pytest.param(
            BAD_NAME_OBJECT_NODE,
            ((id(BAD_NAME_OBJECT_NODE), False), None, False),
            id="non-string-type-name-ignored",
        ),
    ],
)
def test_extract_component_meta_variants(
    node: TypedNode,
    expected: tuple[tuple[int, bool], str | None, bool],
) -> None:
    """Verify extract_component_meta extracts component key, type name, and inline flag."""
    assert extract_component_meta(node) == expected


@pytest.mark.parametrize(
    ("node", "expected"),
    [
        pytest.param(None, None, id="none"),
        pytest.param(TypedNode("string"), None, id="scalar"),
        pytest.param(PLAIN_OBJECT_NODE, PLAIN_OBJECT_NODE, id="direct-object"),
        pytest.param(TypedNode("any", one_of=(PLAIN_OBJECT_NODE,)), PLAIN_OBJECT_NODE, id="one-of-object"),
        pytest.param(TypedNode("any", any_of=(PLAIN_OBJECT_NODE,)), PLAIN_OBJECT_NODE, id="any-of-object"),
    ],
)
def test_unwrap_object_node_variants(node: TypedNode | None, expected: TypedNode | None) -> None:
    """Verify unwrap_object_node unwraps direct and combinator-wrapped object nodes."""
    assert unwrap_object_node(node) is expected


@pytest.mark.parametrize(
    ("node", "expected"),
    [
        pytest.param(None, None, id="none"),
        pytest.param(TypedNode("array", prefix_items=(ELEM_STRING_NODE,)), ELEM_STRING_NODE, id="prefix-items"),
        pytest.param(TypedNode("array", items=(ELEM_STRING_NODE,)), ELEM_STRING_NODE, id="items-tuple"),
        pytest.param(TypedNode("array", items=()), None, id="empty-items-tuple"),
        pytest.param(TypedNode("array", items=ELEM_STRING_NODE), ELEM_STRING_NODE, id="single-items-node"),
    ],
)
def test_unwrap_array_element_node_variants(node: TypedNode | None, expected: TypedNode | None) -> None:
    """Verify unwrap_array_element_node extracts the element node from array variants."""
    assert unwrap_array_element_node(node) is expected


@pytest.mark.parametrize(
    ("node", "expected"),
    [
        pytest.param(None, None, id="none"),
        pytest.param(TypedNode("map", value_type=MAP_VALUE_INT_NODE), MAP_VALUE_INT_NODE, id="map-value-type"),
        pytest.param(
            TypedNode("object", pattern_properties={"^k": MAP_VALUE_INT_NODE}),
            MAP_VALUE_INT_NODE,
            id="pattern-properties",
        ),
        pytest.param(
            TypedNode("object", additional_properties=MAP_VALUE_INT_NODE),
            MAP_VALUE_INT_NODE,
            id="additional-properties",
        ),
        pytest.param(
            TypedNode("object", additional_properties=TypedNode("any")),
            None,
            id="additional-properties-any",
        ),
        pytest.param(
            TypedNode("object", additional_properties=TypedNode("never")),
            None,
            id="additional-properties-never",
        ),
    ],
)
def test_unwrap_map_value_node_variants(node: TypedNode | None, expected: TypedNode | None) -> None:
    """Verify unwrap_map_value_node extracts value nodes for maps and map-like objects."""
    assert unwrap_map_value_node(node) is expected


def test_ir_component_collector_collect_document_scalar_leaf_table() -> None:
    """Verify collect_document returns a RenderedFieldSpec directly for scalar leaf tables."""
    collector = IRComponentCollector()
    scalar_doc = SchemaDocument(root=TypedNode("object", provenance=Provenance("xsd", (), {"scalar_leaf_table": True})))

    scalar_spec = collector.collect_document("note", scalar_doc, pa.schema([pa.field("#text", pa.string())]))

    assert isinstance(scalar_spec, RenderedFieldSpec)
    assert scalar_spec.name == "#text"


def test_ir_component_collector_scan_collect_and_assign_names() -> None:
    """Verify IRComponentCollector handles truncated IDs, maps, nested lists, collisions, and inline fallback."""
    collector = IRComponentCollector()

    trunc_node = TypedNode("string", provenance=Provenance("xsd", (), {"truncated_component_id": 99}))
    leaf_obj = TypedNode(
        "object",
        properties=(Field("v", TypedNode("integer")),),
        provenance=Provenance("json-schema", (), {"component_id": 10}),
    )
    leaf_obj_same_key_diff_struct = TypedNode(
        "object",
        properties=(Field("v", TypedNode("string")),),
        provenance=Provenance("json-schema", (), {"component_id": 10}),
    )
    cyclic_obj = TypedNode(
        "object",
        properties=(Field("name", TypedNode("string")),),
        provenance=Provenance("xsd", (), {"component_id": 20}),
    )
    # self-referential properties via another node sharing component_id=20
    cyclic_holder = TypedNode(
        "object",
        properties=(
            Field("self_ref", cyclic_obj),
            Field("trunc", trunc_node),
            Field("nested_map", TypedNode("map", value_type=leaf_obj)),
            Field("nested_list", TypedNode("array", items=TypedNode("array", items=leaf_obj_same_key_diff_struct))),
            Field("empty_obj", TypedNode("object")),
        ),
        provenance=Provenance("xsd", (), {"component_id": 20}),
    )
    doc = SchemaDocument(root=cyclic_holder)
    collector.register_document("root", doc)
    trunc_id = 99
    assert trunc_id in collector.el_name_types["trunc"]
    assert "empty_obj" not in collector.el_name_types

    pa_leaf_int = pa.struct([pa.field("v", pa.int64())])
    pa_leaf_str = pa.struct([pa.field("v", pa.string())])
    pa_schema = pa.schema(
        [
            pa.field("self_ref", pa.struct([pa.field("name", pa.string())])),
            pa.field("trunc", pa.string()),
            pa.field("nested_map", pa.map_(pa.string(), pa_leaf_int)),
            pa.field("nested_list", pa.list_(pa.list_(pa_leaf_str))),
            pa.field("empty_obj", pa.struct([])),
        ]
    )
    root_key = collector.collect_document("root", doc, pa_schema)
    assert root_key == (20, False)

    # nillable=True on component_id=10 triggers _NILLABLE suffix
    nillable_leaf = TypedNode(
        "object",
        properties=(Field("v", TypedNode("integer")),),
        provenance=Provenance("xsd", (), {"component_id": 10, "nillable": True}),
    )
    collector.collect_document("nested_map", SchemaDocument(root=nillable_leaf), pa.schema([pa.field("v", pa.int64())]))

    # duplicate root name without parent triggers _2_STRUCT suffix
    dup_root = TypedNode(
        "object",
        properties=(Field("v", TypedNode("integer")),),
        provenance=Provenance("xsd", (), {"component_id": 30}),
    )
    collector.collect_document("nested_map", SchemaDocument(root=dup_root), pa.schema([pa.field("v", pa.int64())]))

    names = collector.assign_component_names()
    assert names[(10, True)] == "NESTED_MAP_NILLABLE_STRUCT"
    assert names[(30, False)] == "NESTED_MAP_2_STRUCT"
    assert "__COMPONENT_10_0__" in next(
        f.type_expr for f in collector.components[(20, False)].fields if f.name == "nested_map"
    )
    # differing pa_struct for same json-schema component_id=10 renders inline
    nested_list_expr = next(f.type_expr for f in collector.components[(20, False)].fields if f.name == "nested_list")
    assert "pa.struct([" in nested_list_expr
