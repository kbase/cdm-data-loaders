"""End-to-end tests for pyiceberg_to_linkml.

Chains a real pyiceberg SQL catalog (file-backed SQLite) through
dump_catalog_schemas and validates the emitted documents.
"""

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest
from linkml.linter.linter import Linter
from linkml_runtime.linkml_model.meta import ClassDefinition, SchemaDefinition, SlotDefinition
from linkml_runtime.loaders import yaml_loader
from pyiceberg.catalog import Catalog
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, StringType, StructType

from cdm_data_loaders.converters.pyiceberg_catalog_to_linkml import (
    IcebergToLinkMLSettings,
    dump_catalog_schemas,
)
from tests.cdm_data_loaders.converters.conftest import CATALOG_NAME


@pytest.mark.parametrize("group_by_namespace", [False, True], ids=["per-table", "per-namespace"])
def test_dump_catalog_schemas_pass_yaml_models(
    catalog: Catalog, catalog_env: Path, simple_schema: Schema, wide_schema: Schema, group_by_namespace: bool
) -> None:
    """Write YAML schemas with valid LinkML models and a shared timezone-aware timestamp."""
    catalog.create_namespace("ns")
    catalog.create_table(("ns", "simple"), schema=simple_schema)
    catalog.create_table(("ns", "wide"), schema=wide_schema)

    out_dir = catalog_env / "schemas"
    settings = IcebergToLinkMLSettings(  # pyright: ignore[reportCallIssue]
        catalog=CATALOG_NAME, output_dir=str(out_dir), group_by_namespace=group_by_namespace
    )
    dump_catalog_schemas(settings)

    files = sorted(out_dir.iterdir())
    assert [f.name for f in files] == (
        ["ns.schema.yaml"] if group_by_namespace else ["ns.simple.schema.yaml", "ns.wide.schema.yaml"]
    )

    simple_doc = yaml_loader.load(str(files[0]), target_class=SchemaDefinition)
    assert simple_doc.name == ("ns" if group_by_namespace else "simple")
    assert simple_doc.id == ("urn:iceberg:ns" if group_by_namespace else "urn:iceberg:ns.simple")
    assert {"simple": simple_doc.classes["simple"]} == {
        "simple": ClassDefinition(
            name="simple",
            attributes={
                "id": SlotDefinition(name="id", range="integer", required=True, description="primary key"),
                "name": SlotDefinition(name="name", range="string", description="display name"),
            },
        )
    }

    wide_doc = simple_doc if group_by_namespace else yaml_loader.load(str(files[1]), target_class=SchemaDefinition)
    assert set(wide_doc.classes) == (
        {"simple", "wide", "wide_attrs_map", "wide_address"}
        if group_by_namespace
        else {"wide", "wide_attrs_map", "wide_address"}
    )
    assert wide_doc.classes["wide"].attributes == {
        "id": SlotDefinition(name="id", range="integer", required=True, description="primary key"),
        "name": SlotDefinition(name="name", range="string", required=True),
        "count": SlotDefinition(name="count", range="integer"),
        "ratio": SlotDefinition(name="ratio", range="float"),
        "as_of": SlotDefinition(name="as_of", range="datetime"),
        "born_on": SlotDefinition(name="born_on", range="date"),
        "rate": SlotDefinition(name="rate", range="decimal"),
        "tags": SlotDefinition(name="tags", range="string", multivalued=True),
        "attrs": SlotDefinition(name="attrs", range="wide_attrs_map", multivalued=True),
        "address": SlotDefinition(name="address", range="wide_address"),
    }
    assert wide_doc.classes["wide_attrs_map"].attributes == {
        "key": SlotDefinition(name="key", range="string", required=True),
        "value": SlotDefinition(name="value", range="float"),
    }
    assert wide_doc.classes["wide_address"].attributes == {
        "street": SlotDefinition(name="street", range="string", required=True),
        "city": SlotDefinition(name="city", range="string"),
    }
    generated_at = simple_doc.annotations["generated_at"].value
    assert wide_doc.annotations["generated_at"].value == generated_at
    assert datetime.fromisoformat(generated_at).tzinfo == UTC

    for schema_path in files:
        with pytest.raises(json.JSONDecodeError):
            json.loads(schema_path.read_text(encoding="utf-8"))
        assert list(Linter.validate_schema(str(schema_path))) == []


def test_dump_catalog_schemas_pass_namespace_isolation_and_collisions(
    catalog: Catalog, catalog_env: Path, simple_schema: Schema
) -> None:
    """Keep namespaces separate and retain colliding table and nested class definitions."""
    nested_schema = Schema(NestedField(1, "child", StructType(NestedField(2, "value", StringType()))))
    for namespace in ("first", "second"):
        catalog.create_namespace(namespace)
        catalog.create_table((namespace, "record"), schema=nested_schema, properties={"comment": "Table description."})
    catalog.create_table(("first", "record_child"), schema=simple_schema)
    catalog.create_table(("first", "a-b"), schema=simple_schema)
    catalog.create_table(("first", "a_b"), schema=nested_schema)
    out_dir = catalog_env / "schemas"
    settings = IcebergToLinkMLSettings(  # pyright: ignore[reportCallIssue]
        catalog=CATALOG_NAME, output_dir=str(out_dir), group_by_namespace=True
    )
    dump_catalog_schemas(settings)
    assert sorted(file.name for file in out_dir.iterdir()) == ["first.schema.yaml", "second.schema.yaml"]
    first = yaml_loader.load(str(out_dir / "first.schema.yaml"), target_class=SchemaDefinition)
    second = yaml_loader.load(str(out_dir / "second.schema.yaml"), target_class=SchemaDefinition)
    assert set(first.classes) == {"a_b", "a_b_2", "a_b_2_child", "record", "record_child", "record_child_2"}
    assert set(second.classes) == {"record", "record_child"}
    assert first.classes["a_b"].attributes == first.classes["record_child_2"].attributes
    assert first.classes["a_b_2"].attributes["child"].range == "a_b_2_child"
    assert first.classes["a_b_2_child"].attributes == first.classes["record_child"].attributes
    assert first.classes["record_child"].attributes == {
        "value": SlotDefinition(name="value", range="string"),
    }
    for namespace, document in (("first", first), ("second", second)):
        assert document.id == f"urn:iceberg:{namespace}"
        assert document.name == namespace
        assert document.classes["record"].description == "Table description."
        assert document.classes["record"].attributes == {"child": SlotDefinition(name="child", range="record_child")}
        assert list(Linter.validate_schema(str(out_dir / f"{namespace}.schema.yaml"))) == []
    assert first.annotations["generated_at"] == second.annotations["generated_at"]
