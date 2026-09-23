"""Closed extension contracts and custom registry isolation."""

from decimal import Decimal
from typing import Any

import pytest
from jsonschema.exceptions import SchemaError

from cdm_data_loaders.converters.extensions import (
    DEFAULT_EXTENSIONS,
    ExtensionError,
    ExtensionRegistry,
    Extensions,
    ExtensionSpec,
)
from cdm_data_loaders.converters.ir import TypedNode
from cdm_data_loaders.readers.jsonschema_xsv.xsv_validator.custom_metaschema import X_XSV_CONFIG_SCHEMA


@pytest.mark.parametrize(
    "values",
    [
        {"x-dlt": {"data_type": "decimal", "precision": 20, "scale": 2, "timezone": None}},
        {"x-iceberg": {"field_id": 1, "required": False, "initial_default": [None, Decimal("1.20")]}},
        {"x-xsv-config": {"x-delimiter": "\t", "x-null-cols": ["a"]}},
        {"x-file-glob": "*.tsv", "x-delimiter": ",", "x-dlt-prefix": "id:", "x-dlt-split": ";", "x-pii": True},
        {},
    ],
    ids=["dlt", "iceberg-default", "xsv", "discovered", "empty"],
)
def test_extensions_pass_builtin(values: dict[str, Any]) -> None:
    """Builtin namespaces validate exact internal key spellings."""
    extensions = Extensions(values)
    assert list(extensions) == list(values)
    assert len(extensions) == len(values)
    assert TypedNode(type="any", extensions=extensions).extensions is extensions


@pytest.mark.parametrize(
    "values",
    [
        {"title": "bad"},
        {"x-unknown": None},
        {"x-pii": "yes"},
        {"x-dlt": {"bogus": True}},
        {"x-iceberg": {"bogus": 1}},
        {"x-xsv-config": {}},
        {"x-delimiter": "::"},
        {"x-dlt": []},
    ],
    ids=["namespace", "unknown", "wrong-type", "dlt-key", "iceberg-key", "empty-xsv", "delimiter", "array"],
)
def test_extensions_fail_invalid(values: dict[str, Any]) -> None:
    """Unknown keys and invalid payloads fail on ordinary node construction."""
    with pytest.raises(ExtensionError):
        TypedNode(type="any", extensions=values)


def test_extension_registry_pass_custom_copy_isolation() -> None:
    """Custom arrays and null are allowed only under an explicit schema."""
    schema = {"type": "array", "items": {"type": ["string", "null"]}}
    registry = DEFAULT_EXTENSIONS.register(ExtensionSpec("x-custom", schema))
    values = {"x-custom": ["data", None]}
    extensions = Extensions(values, registry)
    schema["items"]["type"].append("number")
    values["x-custom"].append("changed")
    assert extensions["x-custom"] == ("data", None)
    with pytest.raises(ExtensionError):
        registry.validate({"x-custom": [1]})
    with pytest.raises(TypeError):
        extensions.payload["x-custom"] = ()
    custom_dlt = registry.extend_payload("x-dlt", {"x-custom": {"type": ["array", "null"]}})
    assert custom_dlt.validate({"x-dlt": {"x-custom": None}}) == {"x-dlt": {"x-custom": None}}
    with pytest.raises(ExtensionError):
        DEFAULT_EXTENSIONS.validate({"x-dlt": {"x-custom": None}})


@pytest.mark.parametrize("namespace", ["", "x-", "ordinary"], ids=["empty", "prefix-only", "no-prefix"])
def test_extension_spec_fail_namespace(namespace: str) -> None:
    """Only named x-prefixed namespaces can be registered."""
    with pytest.raises(ExtensionError):
        ExtensionSpec(namespace, schema=True)


def test_extension_registry_fail_duplicates() -> None:
    """Duplicate declarations require explicit replacement."""
    spec = ExtensionSpec("x-test", schema=True)
    registry = ExtensionRegistry((spec,))
    with pytest.raises(ExtensionError):
        ExtensionRegistry((spec, spec))
    with pytest.raises(ExtensionError):
        registry.register(spec)
    assert registry.register(spec, replace=True) == registry


def test_extension_spec_fail_invalid_schemas_and_payload_extensions() -> None:
    """Invalid schemas, wrong registry entries and payload collisions fail early."""
    with pytest.raises(ExtensionError, match="validation schemas"):
        ExtensionSpec("x-bad", 1)
    with pytest.raises(SchemaError):
        ExtensionSpec("x-bad", {"type": "bogus"})
    with pytest.raises(TypeError, match="entries"):
        ExtensionRegistry(({},))
    with pytest.raises(ExtensionError, match="Not an object"):
        DEFAULT_EXTENSIONS.extend_payload("x-pii", {"extra": True})
    with pytest.raises(ExtensionError, match="already registered"):
        DEFAULT_EXTENSIONS.extend_payload("x-dlt", {"data_type": True})
    with pytest.raises(ExtensionError, match="Unregistered"):
        DEFAULT_EXTENSIONS.extend_payload("x-missing", {"extra": True})


def test_extensions_pass_exact_xsv_schema_and_mapping_api() -> None:
    """The exact existing XSV schema is reused, and mapping methods remain callable."""
    spec = next(spec for spec in DEFAULT_EXTENSIONS.specs if spec.namespace == "x-xsv-config")
    extensions = Extensions({"x-pii": True})
    assert list(extensions.values()) == [True]
    assert list(extensions.items()) == [("x-pii", True)]
    assert spec.schema["additionalProperties"] == X_XSV_CONFIG_SCHEMA["additionalProperties"]
    assert tuple(spec.schema["properties"]) == tuple(X_XSV_CONFIG_SCHEMA["properties"])
    with pytest.raises(KeyError):
        extensions["x-missing"]
    with pytest.raises(TypeError):
        spec.schema["properties"]["x-delimiter"]["type"] = "integer"


@pytest.mark.parametrize(
    "hint",
    [
        "primary_key",
        "unique",
        "foreign_key",
        "sort",
        "cluster",
        "partition",
        "merge_key",
        "row_key",
        "root_key",
        "variant",
        "timezone",
    ],
    ids=["primary", "unique", "foreign", "sort", "cluster", "partition", "merge", "row", "root", "variant", "timezone"],
)
def test_extensions_pass_dlt_boolean_hints(hint: str) -> None:
    """Each observed dlt column hint is explicitly registered as a boolean."""
    assert Extensions({"x-dlt": {hint: True}})["x-dlt"] == {hint: True}
    with pytest.raises(ExtensionError):
        Extensions({"x-dlt": {hint: "yes"}})
