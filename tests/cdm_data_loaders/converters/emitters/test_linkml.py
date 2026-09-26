"""LinkML schema emission from typed converter documents."""

from pathlib import Path

import pytest
import yaml
from linkml.linter.linter import Linter

from cdm_data_loaders.converters.emitters.linkml import LinkMLEmitter, LinkMLEmitterError
from cdm_data_loaders.converters.readers.json_schema import JsonSchemaReader


def test_emit_pass_nested_enum_array_constraints() -> None:
    """Objects, enums, arrays, descriptions, and scalar constraints emit LinkML definitions."""
    document = JsonSchemaReader().read(
        {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "$id": "https://example.org/record",
            "title": "record",
            "description": "A record.",
            "type": "object",
            "properties": {
                "id": {"type": "integer", "minimum": 1},
                "status": {"enum": ["active", "retired"]},
                "tags": {"type": "array", "items": {"type": "string", "pattern": "^[a-z]+$"}},
                "address": {
                    "type": "object",
                    "description": "Mailing address.",
                    "properties": {"city": {"type": "string"}},
                    "required": ["city"],
                },
            },
            "required": ["id", "status"],
        }
    )

    assert LinkMLEmitter().emit(document) == {
        "id": "https://example.org/record",
        "name": "record",
        "prefixes": {"record": "https://example.org/record#", "linkml": "https://w3id.org/linkml/"},
        "default_prefix": "record",
        "imports": ["linkml:types"],
        "description": "A record.",
        "classes": {
            "record": {
                "attributes": {
                    "id": {"range": "integer", "minimum_value": 1, "required": True},
                    "status": {"range": "record_status_enum", "required": True},
                    "tags": {"range": "string", "pattern": "^[a-z]+$", "multivalued": True},
                    "address": {"range": "record_address", "description": "Mailing address."},
                }
            },
            "record_address": {
                "attributes": {"city": {"range": "string", "required": True}},
                "description": "Mailing address.",
            },
        },
        "enums": {"record_status_enum": {"permissible_values": {"active": None, "retired": None}}},
    }


def test_emit_pass_linkml_metamodel_validation(tmp_path: Path) -> None:
    """Emitted schemas conform to the LinkML metamodel."""
    document = JsonSchemaReader().read(
        {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "object",
            "properties": {
                "id": {"type": "integer", "minimum": 1},
                "status": {"enum": ["active", "retired"]},
            },
            "required": ["id"],
        }
    )
    schema_path = tmp_path / "record.yaml"
    schema_path.write_text(yaml.safe_dump(LinkMLEmitter().emit(document)), encoding="utf-8")

    assert list(Linter.validate_schema(str(schema_path))) == []


@pytest.mark.parametrize(
    "property_schema",
    [
        {"type": ["string", "integer"]},
        {"anyOf": [{"type": "string"}, {"type": "integer"}]},
        {"type": "array", "items": [{"type": "string"}, {"type": "integer"}]},
        {"enum": [1, 2]},
        {"type": "null"},
    ],
    ids=["union-type", "any-of", "tuple-array", "numeric-enum", "null"],
)
def test_emit_fail_unsupported_node(property_schema: dict[str, object]) -> None:
    """Unsupported JSON Schema constructs fail rather than silently changing their meaning."""
    document = JsonSchemaReader().read(
        {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "object",
            "properties": {"value": property_schema},
        }
    )

    with pytest.raises(LinkMLEmitterError):
        LinkMLEmitter().emit(document)
