"""Unit tests for StrictJsonSchemaReader IR construction, provenance, annotations, and error handling."""

from typing import Any, Final

import pytest

from cdm_data_loaders.converters.core.errors import ConversionError
from cdm_data_loaders.converters.core.ir import Field, NodeHints, Provenance, TypedNode
from cdm_data_loaders.converters.sources.json_schema.strict_reader import StrictJsonSchemaReader

DIALECT: Final[str] = "https://json-schema.org/draft/2020-12/schema"


def test_strict_json_schema_reader_read_node_scalar() -> None:
    """Verify StrictJsonSchemaReader.read_node lowers a scalar fragment with constraints and provenance."""
    scalar = StrictJsonSchemaReader().read_node({"type": "integer", "minimum": 1, "description": "pos int"})

    assert scalar.type == "integer"
    assert scalar.nullable is False
    assert scalar.annotations == {"description": "pos int"}
    assert scalar.constraints["minimum"] == 1
    assert scalar.provenance == Provenance("json-schema", (), {"jsonschema_type": "integer"})


def test_strict_json_schema_reader_read_node_array() -> None:
    """Verify StrictJsonSchemaReader.read_node lowers an array fragment with item provenance."""
    arr = StrictJsonSchemaReader().read_node({"type": "array", "items": {"type": "string"}, "description": "tags"})

    assert arr.type == "array"
    assert arr.hints == NodeHints(logical_type="list")
    assert arr.annotations == {"description": "tags"}
    assert arr.provenance == Provenance("json-schema", ())
    assert arr.items is not None
    assert arr.items.provenance == Provenance("json-schema", ("items",), {"jsonschema_type": "string"})


def test_strict_json_schema_reader_read_node_tuple() -> None:
    """Verify StrictJsonSchemaReader.read_node lowers a closed prefixItems tuple to a struct."""
    tup = StrictJsonSchemaReader().read_node(
        {
            "type": "array",
            "prefixItems": [{"type": "string"}, {"type": "integer"}],
            "minItems": 1,
            "items": False,
            "description": "pair",
        }
    )

    assert tup.type == "object"
    assert tup.hints == NodeHints(logical_type="struct")
    assert tup.required_names == ("item_0",)
    assert tup.additional_properties == TypedNode(type="never", nullable=False)
    assert tup.annotations == {"description": "pair"}
    assert tup.provenance == Provenance("json-schema", ())
    assert tup.properties[0] == Field(
        "item_0",
        TypedNode(
            type="string",
            declared_type="string",
            nullable=False,
            hints=NodeHints(logical_type="string"),
            provenance=Provenance("json-schema", ("prefixItems", 0), {"jsonschema_type": "string"}),
        ),
        required=True,
        provenance=Provenance("json-schema", ("prefixItems", 0), {"jsonschema_type": "string"}),
    )


def test_strict_json_schema_reader_read_node_map() -> None:
    """Verify StrictJsonSchemaReader.read_node lowers an object map fragment with key and value provenance."""
    mp = StrictJsonSchemaReader().read_node(
        {
            "type": "object",
            "additionalProperties": {"type": "integer"},
            "patternProperties": {"^x_": {"type": "integer"}},
            "description": "counts",
        }
    )

    assert mp.type == "map"
    assert mp.hints == NodeHints(logical_type="map")
    assert mp.annotations == {"description": "counts"}
    assert mp.provenance == Provenance("json-schema", ())
    assert mp.key_type == TypedNode(
        type="string",
        nullable=False,
        hints=NodeHints(logical_type="string"),
        provenance=Provenance("json-schema", (), {"jsonschema_type": "string"}),
    )
    assert mp.value_type is not None
    assert mp.value_type.provenance == Provenance(
        "json-schema", ("additionalProperties",), {"jsonschema_type": "integer"}
    )


def test_strict_json_schema_reader_read_refs_unions_and_cutoff() -> None:
    """Verify StrictJsonSchemaReader.read lowers $defs refs, sibling constraints, unions, and recursion cutoffs."""
    expected_min = 5
    schema: dict[str, Any] = {
        "$schema": DIALECT,
        "$id": "urn:test:strict",
        "description": "root doc",
        "$defs": {
            "PosInt": {"type": "integer", "description": "from def"},
            "Node": {
                "type": "object",
                "required": ["label"],
                "properties": {
                    "label": {"type": "string"},
                    "next": {"$ref": "#/$defs/Node", "description": "child node"},
                },
            },
        },
        "type": "object",
        "required": ["n", "u_scalar", "u_mixed"],
        "properties": {
            "n": {"$ref": "#/$defs/PosInt", "minimum": expected_min},
            "u_scalar": {"anyOf": [{"type": "integer"}, {"type": "number"}], "description": "num"},
            "u_mixed": {
                "oneOf": [
                    {"type": "string"},
                    {"type": "object", "required": ["k"], "properties": {"k": {"type": "string"}}},
                ]
            },
            "node": {"$ref": "#/$defs/Node"},
        },
    }
    doc = StrictJsonSchemaReader(max_recursion_depth=1).read(schema)
    assert doc.dialect == DIALECT
    assert doc.identifier == "urn:test:strict"
    assert doc.annotations == {"description": "root doc"}
    assert doc.provenance == Provenance("json-schema")

    by_name = {f.name: f for f in doc.root.properties}
    assert by_name["n"].node.constraints["minimum"] == expected_min
    assert by_name["n"].node.annotations == {"description": "from def"}

    assert by_name["u_scalar"].node.type == "number"
    assert by_name["u_scalar"].node.annotations == {"description": "num"}
    assert by_name["u_scalar"].node.provenance == Provenance(
        "json-schema", ("properties", "u_scalar"), {"jsonschema_type": "integer | number"}
    )

    mixed_fields = {f.name: f for f in by_name["u_mixed"].node.properties}
    assert mixed_fields["k"].required is False
    assert mixed_fields["_value"].required is False
    assert mixed_fields["_value"].provenance == Provenance(
        "json-schema", ("properties", "u_mixed", "_value"), {"jsonschema_type": "string"}
    )

    node_props = {f.name: f for f in by_name["node"].node.properties}
    assert node_props["next"].node.type == "string"
    assert node_props["next"].node.annotations == {"description": "child node"}
    assert node_props["next"].node.provenance == Provenance("json-schema", ("properties", "node", "properties", "next"))


@pytest.mark.parametrize(
    "fragment",
    [
        pytest.param(
            {"type": "array", "prefixItems": [{"type": "string"}], "items": {"type": "string"}}, id="open-tuple"
        ),
        pytest.param({"type": "array", "prefixItems": [], "items": False}, id="empty-prefixItems"),
        pytest.param({"anyOf": [{"type": "null"}]}, id="null-only-anyOf"),
        pytest.param(
            {"anyOf": [{"type": "string"}, {"type": "array", "items": {"type": "string"}}]},
            id="incompatible-anyOf-array-scalar",
        ),
        pytest.param({"type": "object", "properties": {}}, id="empty-object"),
        pytest.param(
            {
                "oneOf": [
                    {"type": "string"},
                    {"type": "object", "properties": {"_value": {"type": "string"}}},
                ]
            },
            id="value-collision-in-mixed-union",
        ),
    ],
)
def test_strict_json_schema_reader_read_node_unsupported_raises(fragment: dict[str, Any]) -> None:
    """Verify StrictJsonSchemaReader.read_node raises ConversionError on unsupported strict constructs."""
    with pytest.raises(ConversionError):
        StrictJsonSchemaReader().read_node(fragment)


def test_strict_json_schema_reader_read_inline_struct_siblings_and_object_union() -> None:
    """Verify StrictJsonSchemaReader.read handles inline_struct, object siblings, and multi-object unions."""
    expected_max = 10
    expected_min = 2
    schema: dict[str, Any] = {
        "$schema": DIALECT,
        "$defs": {
            "BoundedInt": {"type": "integer", "maximum": expected_max},
            "Node": {
                "type": "object",
                "required": ["label"],
                "properties": {
                    "label": {"type": "string"},
                    "next": {"$ref": "#/$defs/Node"},
                },
            },
            "PartA": {"type": "object", "required": ["a"], "properties": {"a": {"type": "integer"}}},
            "PartB": {"type": "object", "required": ["b"], "properties": {"b": {"type": "string"}}},
        },
        "type": "object",
        "properties": {
            "bounded": {"$ref": "#/$defs/BoundedInt", "minimum": expected_min},
            "ext_node": {"$ref": "#/$defs/Node", "properties": {"extra": {"type": "boolean"}}},
            "node_d2": {"$ref": "#/$defs/Node"},
            "opt_str": {"anyOf": [{"type": "string"}, {"type": "null"}]},
            "obj_union": {"oneOf": [{"$ref": "#/$defs/PartA"}, {"$ref": "#/$defs/PartB"}]},
        },
    }
    doc = StrictJsonSchemaReader(max_recursion_depth=2).read(schema)
    by_name = {f.name: f for f in doc.root.properties}

    assert by_name["bounded"].node.constraints == {"minimum": expected_min, "maximum": expected_max}

    ext_props = {f.name for f in by_name["ext_node"].node.properties}
    assert ext_props == {"label", "next", "extra"}

    node_lvl1 = by_name["node_d2"].node
    assert node_lvl1.provenance is not None
    assert node_lvl1.provenance.metadata["type_name"] == "Node"
    assert node_lvl1.provenance.metadata["component_id"] == hash(("jsonschema_ref", "Node"))
    assert "inline_struct" not in node_lvl1.provenance.metadata

    node_lvl2 = next(f for f in node_lvl1.properties if f.name == "next").node
    assert node_lvl2.type == "object"
    assert node_lvl2.provenance is not None
    assert node_lvl2.provenance.metadata["inline_struct"] is True

    assert by_name["opt_str"].node.type == "string"
    assert by_name["opt_str"].node.nullable is True

    obj_u = by_name["obj_union"].node
    assert obj_u.type == "object"
    assert {f.name for f in obj_u.properties} == {"a", "b"}
    assert obj_u.required_names == ()
