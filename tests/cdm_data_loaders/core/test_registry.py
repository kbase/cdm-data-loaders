"""Unit tests for registry models and the load_registry_dict loader."""

from collections.abc import Callable
from types import MappingProxyType
from typing import Any
from uuid import uuid4

import pyarrow as pa
import pytest
from jsonschema.exceptions import SchemaError
from pydantic import BaseModel, ValidationError

from cdm_data_loaders.core.registry import (
    ArrowSchemaRegistry,
    ModelRegistry,
    RejectedFreezerMixin,
    SchemaRegistry,
    TransformRegistry,
    load_registry_dict,
)
from cdm_data_loaders.readers.jsonschema_xsv.xsv_validator.schema_utils import ValidatedSchema
from tests.helpers import register_temp_module

WIDGET_PA_SCHEMA = pa.schema([pa.field("id", pa.string())])


def _identity_widget_transform(record: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    """Route a record to the widget table."""
    return {"widget": [record]}


@pytest.mark.parametrize(
    "registry_cls",
    [
        pytest.param(SchemaRegistry, id="schema-registry"),
        pytest.param(ModelRegistry, id="model-registry"),
        pytest.param(TransformRegistry, id="transform-registry"),
        pytest.param(ArrowSchemaRegistry, id="arrow-schema-registry"),
    ],
)
def test_registry_pass_copies_and_freezes_mapping(
    widget_schema: dict[str, Any],
    widget_model: type[BaseModel],
    registry_cls: type[RejectedFreezerMixin],
) -> None:
    """Every Registry class strips keys, copies the input dict, and freezes registry as a MappingProxyType."""
    raw_by_cls: dict[type[RejectedFreezerMixin], Any] = {
        SchemaRegistry: widget_schema,
        ModelRegistry: widget_model,
        TransformRegistry: _identity_widget_transform,
        ArrowSchemaRegistry: WIDGET_PA_SCHEMA,
    }
    expected_by_cls: dict[type[RejectedFreezerMixin], Any] = {
        SchemaRegistry: ValidatedSchema(jsonschema=widget_schema),
        ModelRegistry: widget_model,
        TransformRegistry: _identity_widget_transform,
        ArrowSchemaRegistry: WIDGET_PA_SCHEMA,
    }
    raw_val = raw_by_cls[registry_cls]
    expected_val = expected_by_cls[registry_cls]

    source_map = {"  widget \n": raw_val}
    registry_obj = registry_cls(registry=source_map)  # pyright: ignore[reportCallIssue]
    source_map["other"] = raw_val

    assert isinstance(registry_obj.registry, MappingProxyType)  # pyright: ignore[reportAttributeAccessIssue]
    assert registry_obj.registry == {"widget": expected_val}  # pyright: ignore[reportAttributeAccessIssue]
    with pytest.raises(TypeError):
        registry_obj.registry["injected"] = expected_val  # type: ignore[index]
    assert "injected" not in registry_obj.registry  # pyright: ignore[reportAttributeAccessIssue]


@pytest.mark.parametrize(
    ("transform_map", "match"),
    [
        pytest.param("not_a_mapping", "Input should be a valid dictionary", id="not_a_mapping"),
        pytest.param({}, "Dictionary should have at least 1 item", id="empty_dict"),
        pytest.param({"": _identity_widget_transform}, "String should have at least 1 character", id="empty_key"),
        pytest.param(
            {"   ": _identity_widget_transform},
            "String should have at least 1 character",
            id="whitespace_key",
        ),
        pytest.param({123: _identity_widget_transform}, "Input should be a valid string", id="non_string_key"),
        pytest.param({"items": "not_a_callable"}, "Input should be callable", id="non_callable_value"),
    ],
)
def test_transform_registry_fail_invalid_transform_fn(
    transform_map: Any,  # noqa: ANN401
    match: str,
) -> None:
    """TransformRegistry validates registry structure, keys, and callable transform values."""
    with pytest.raises(ValidationError, match=match):
        TransformRegistry(registry=transform_map)


@pytest.mark.parametrize(
    ("schema_map", "match"),
    [
        pytest.param("not_a_mapping", "Input should be a valid dictionary", id="not_a_mapping"),
        pytest.param({}, "Dictionary should have at least 1 item", id="empty_dict"),
        pytest.param({"": pa.schema([])}, "String should have at least 1 character", id="empty_key"),
        pytest.param({"   ": pa.schema([])}, "String should have at least 1 character", id="whitespace_key"),
        pytest.param({123: pa.schema([])}, "Input should be a valid string", id="non_string_key"),
        pytest.param({"items": "not_a_schema"}, "Input should be an instance of Schema", id="non_schema_value"),
    ],
)
def test_arrow_schema_registry_fail_invalid_schema_map(
    schema_map: Any,  # noqa: ANN401
    match: str,
) -> None:
    """ArrowSchemaRegistry validates registry structure and types."""
    with pytest.raises(ValidationError, match=match):
        ArrowSchemaRegistry(registry=schema_map)


@pytest.mark.parametrize(
    ("attribute_present", "value", "exc_type", "match"),
    [
        pytest.param(
            False,
            None,
            TypeError,
            r"must be a dictionary, not NoneType",
            id="attribute_missing_entirely",
        ),
        pytest.param(True, {}, ValidationError, "Dictionary should have at least 1 item", id="empty_dict"),
        pytest.param(
            True,
            ["widget"],
            TypeError,
            r"must be a dictionary, not list",
            id="wrong_type_list_instead_of_dict",
        ),
        pytest.param(
            True,
            {"": BaseModel},
            ValidationError,
            "String should have at least 1 character",
            id="empty_key",
        ),
        pytest.param(
            True,
            {"   ": BaseModel},
            ValidationError,
            "String should have at least 1 character",
            id="whitespace_only_key",
        ),
        pytest.param(
            True,
            {"widget": "not_a_model"},
            ValidationError,
            "Input should be a subclass of BaseModel",
            id="non_model_value",
        ),
    ],
)
def test_load_registry_dict_fail_invalid_entity_models_registry(
    monkeypatch: pytest.MonkeyPatch,
    attribute_present: bool,
    value: object,
    exc_type: type[Exception],
    match: str,
) -> None:
    """load_registry_dict raises TypeError or ValidationError for a missing, empty, or invalid ENTITY_MODELS."""
    attrs = {"ENTITY_MODELS": value} if attribute_present else {}
    module_name = register_temp_module(monkeypatch, f"_test_invalid_registry_{id(value)}", **attrs)
    with pytest.raises(exc_type, match=match):
        load_registry_dict(module_name, "ENTITY_MODELS")


@pytest.mark.parametrize(
    "attribute_name",
    ["ENTITY_SCHEMAS", "ENTITY_MODELS", "TABLE_TRANSFORMS", "OUTPUT_SCHEMAS"],
    ids=["entity-schemas", "entity-models", "table-transforms", "output-schemas"],
)
def test_load_registry_dict_pass_returns_mapping(
    monkeypatch: pytest.MonkeyPatch,
    widget_schema: dict[str, Any],
    widget_model: type[BaseModel],
    attribute_name: str,
) -> None:
    """load_registry_dict returns the corresponding registry model with stripped keys from the imported module."""
    pa_schema = pa.schema([pa.field("id", pa.string())])
    raw_by_attr: dict[str, dict[str, Any]] = {
        "ENTITY_SCHEMAS": {"  widget  ": widget_schema},
        "ENTITY_MODELS": {"  widget \n": widget_model},
        "TABLE_TRANSFORMS": {"  widget  ": _identity_widget_transform},
        "OUTPUT_SCHEMAS": {"  widget  ": pa_schema},
    }
    expected_by_attr = {
        "ENTITY_SCHEMAS": SchemaRegistry(registry={"widget": ValidatedSchema(jsonschema=widget_schema)}),
        "ENTITY_MODELS": ModelRegistry(registry={"widget": widget_model}),
        "TABLE_TRANSFORMS": TransformRegistry(registry={"widget": _identity_widget_transform}),
        "OUTPUT_SCHEMAS": ArrowSchemaRegistry(registry={"widget": pa_schema}),
    }
    mod_name = register_temp_module(
        monkeypatch,
        name=f"_test_load_registry_dict_{attribute_name.lower()}",
        **{attribute_name: raw_by_attr[attribute_name]},
    )
    assert load_registry_dict(mod_name, attribute_name) == expected_by_attr[attribute_name]


@pytest.mark.parametrize(
    "attribute_name",
    ["ENTITY_SCHEMAS", "ENTITY_MODELS", "TABLE_TRANSFORMS", "OUTPUT_SCHEMAS"],
    ids=["entity-schemas", "entity-models", "table-transforms", "output-schemas"],
)
def test_load_registry_dict_fail_unsupported_attribute(
    monkeypatch: pytest.MonkeyPatch,
    widget_schema: dict[str, Any],
    widget_model: type[BaseModel],
    attribute_name: str,
) -> None:
    """load_registry_dict raises ValueError when attribute_name is not a supported registry attribute."""
    val_by_attr: dict[str, Any] = {
        "ENTITY_SCHEMAS": widget_schema,
        "ENTITY_MODELS": widget_model,
        "TABLE_TRANSFORMS": _identity_widget_transform,
        "OUTPUT_SCHEMAS": pa.schema([pa.field("id", pa.string())]),
    }
    registry_dict = {"widget": val_by_attr[attribute_name]}
    module_name = register_temp_module(
        monkeypatch,
        name=f"_test_load_registry_dict_unsupported_{attribute_name.lower()}",
        **{attribute_name: registry_dict, "CUSTOM_REG": registry_dict},
    )
    with pytest.raises(ValueError, match=r"^Unsupported registry attribute: CUSTOM_REG$"):
        load_registry_dict(module_name, "CUSTOM_REG")


@pytest.mark.parametrize(
    ("attribute_name", "cls_name"),
    [
        pytest.param("ENTITY_SCHEMAS", "SchemaRegistry", id="entity-schemas"),
        pytest.param("ENTITY_MODELS", "ModelRegistry", id="entity-models"),
        pytest.param("TABLE_TRANSFORMS", "TransformRegistry", id="table-transforms"),
        pytest.param("OUTPUT_SCHEMAS", "ArrowSchemaRegistry", id="output-schemas"),
    ],
)
@pytest.mark.parametrize(
    "table_name",
    [
        pytest.param("_rejected", id="exact"),
        pytest.param("  \t_rejected", id="leading-whitespace"),
        pytest.param("_rejected \n", id="trailing-whitespace"),
        pytest.param("\n\n _rejected  ", id="both-whitespace"),
    ],
)
def test_load_registry_dict_fail_reserved_rejected_table_name(
    monkeypatch: pytest.MonkeyPatch,
    widget_schema: dict[str, Any],
    widget_model: type[BaseModel],
    attribute_name: str,
    cls_name: str,
    table_name: str,
) -> None:
    """load_registry_dict rejects the reserved _rejected table name and whitespace variants across all registry types."""
    val_by_attr: dict[str, Any] = {
        "ENTITY_SCHEMAS": widget_schema,
        "ENTITY_MODELS": widget_model,
        "TABLE_TRANSFORMS": _identity_widget_transform,
        "OUTPUT_SCHEMAS": pa.schema([pa.field("id", pa.string())]),
    }
    module_name = register_temp_module(
        monkeypatch,
        name=f"_test_load_registry_dict_rejected_{attribute_name.lower()}",
        **{attribute_name: {table_name: val_by_attr[attribute_name]}},
    )
    expected_msg = (
        f"{cls_name} cannot use reserved table name '_rejected'; '_rejected' is reserved for pipeline error records."
    )
    with pytest.raises(ValidationError, match=expected_msg):
        load_registry_dict(module_name, attribute_name)


def test_load_registry_dict_pass_entity_schemas(
    entity_schemas_factory: Callable[[object], str], widget_schema: dict[str, Any]
) -> None:
    """load_registry_dict returns a SchemaRegistry with stripped keys for valid schemas across supported drafts."""
    registry = {
        "  widget ": widget_schema,
        "older": {"$schema": "http://json-schema.org/draft-04/schema#", "type": "object"},
    }
    expected = SchemaRegistry(
        registry={
            "widget": ValidatedSchema(jsonschema=widget_schema),
            "older": ValidatedSchema(
                jsonschema={"$schema": "http://json-schema.org/draft-04/schema#", "type": "object"}
            ),
        }
    )

    assert load_registry_dict(entity_schemas_factory(registry), "ENTITY_SCHEMAS") == expected


@pytest.mark.parametrize(
    ("registry", "exc_type", "message"),
    [
        pytest.param(None, TypeError, r"must be a dictionary, not NoneType", id="none"),
        pytest.param({}, ValidationError, "Dictionary should have at least 1 item", id="empty"),
        pytest.param([], TypeError, r"must be a dictionary, not list", id="list"),
        pytest.param({"": {}}, ValidationError, "String should have at least 1 character", id="empty-table"),
        pytest.param({"   ": {}}, ValidationError, "String should have at least 1 character", id="whitespace-table"),
        pytest.param({1: {}}, ValidationError, "Input should be a valid string", id="non-string-table"),
        pytest.param({"widget": None}, ValidationError, "Input should be a valid dictionary", id="null-schema"),
        pytest.param({"widget": True}, ValidationError, "Input should be a valid dictionary", id="boolean-schema"),
        pytest.param({"widget": ["a", "b"]}, ValidationError, "Input should be a valid dictionary", id="list-schema"),
        pytest.param(
            {"widget": "not a schema"}, ValidationError, "Input should be a valid dictionary", id="string-schema"
        ),
        pytest.param({"widget": 42}, ValidationError, "Input should be a valid dictionary", id="int-schema"),
        pytest.param({"widget": {}}, ValidationError, r"missing the \$schema keyword", id="missing-draft"),
        pytest.param(
            {"widget": {"type": "object"}}, ValidationError, r"missing the \$schema keyword", id="object-missing-draft"
        ),
        pytest.param({"widget": {"$schema": None}}, ValidationError, r"missing the \$schema keyword", id="null-draft"),
        pytest.param({"widget": {"$schema": []}}, ValidationError, r"missing the \$schema keyword", id="list-draft"),
        pytest.param({"widget": {"$schema": 123}}, ValidationError, r"missing the \$schema keyword", id="int-draft"),
        pytest.param({"widget": {"$schema": True}}, ValidationError, r"missing the \$schema keyword", id="bool-draft"),
        pytest.param({"widget": {"$schema": {}}}, ValidationError, r"missing the \$schema keyword", id="dict-draft"),
        pytest.param({"widget": {"$schema": ""}}, ValidationError, "Unsupported JSON Schema draft", id="empty-draft"),
        pytest.param(
            {"widget": {"$schema": "not-a-uri"}},
            ValidationError,
            "Unsupported JSON Schema draft",
            id="invalid-uri-draft",
        ),
        pytest.param(
            {"widget": {"$schema": "https://example.invalid/schema"}},
            ValidationError,
            "Unsupported JSON Schema draft",
            id="unknown-draft",
        ),
        pytest.param(
            {"widget": {"$schema": "https://json-schema.org/draft/2099-12/schema"}},
            ValidationError,
            "Unsupported JSON Schema draft",
            id="future-unsupported-draft",
        ),
    ],
)
def test_load_registry_dict_fail_invalid_entity_schemas_registry(
    entity_schemas_factory: Callable[[object], str],
    registry: object,
    exc_type: type[Exception],
    message: str,
) -> None:
    """load_registry_dict raises TypeError or ValidationError for malformed ENTITY_SCHEMAS and unsupported drafts."""
    with pytest.raises(exc_type, match=message):
        load_registry_dict(entity_schemas_factory(registry), "ENTITY_SCHEMAS")


@pytest.mark.parametrize(
    "attribute_name",
    ["ENTITY_SCHEMAS", "ENTITY_MODELS", "TABLE_TRANSFORMS", "OUTPUT_SCHEMAS"],
    ids=["entity-schemas", "entity-models", "table-transforms", "output-schemas"],
)
def test_load_registry_dict_fail_missing_registry(
    monkeypatch: pytest.MonkeyPatch,
    attribute_name: str,
) -> None:
    """load_registry_dict raises TypeError when the module lacks the requested registry dictionary attribute."""
    module_name = register_temp_module(monkeypatch, "_test_missing_registry")
    with pytest.raises(TypeError, match=rf"{module_name}\.{attribute_name} must be a dictionary, not NoneType"):
        load_registry_dict(module_name, attribute_name)


@pytest.mark.parametrize(
    "attribute_name",
    ["ENTITY_SCHEMAS", "ENTITY_MODELS", "TABLE_TRANSFORMS", "OUTPUT_SCHEMAS"],
    ids=["entity-schemas", "entity-models", "table-transforms", "output-schemas"],
)
def test_load_registry_dict_fail_missing_module(attribute_name: str) -> None:
    """load_registry_dict raises ModuleNotFoundError for a nonexistent module across all registry attributes."""
    with pytest.raises(ModuleNotFoundError):
        load_registry_dict(f"_missing_registry_{uuid4().hex}", attribute_name)


def test_load_registry_dict_fail_invalid_entity_schema(
    entity_schemas_factory: Callable[[object], str], widget_schema: dict[str, Any]
) -> None:
    """load_registry_dict raises SchemaError when an ENTITY_SCHEMAS entry fails JSON Schema meta-validation."""
    module_name = entity_schemas_factory(
        {"first": {**widget_schema, "type": "invalid"}, "second": {**widget_schema, "required": 7}}
    )
    with pytest.raises(SchemaError, match="'invalid' is not valid under any of the given schemas"):
        load_registry_dict(module_name, "ENTITY_SCHEMAS")


@pytest.mark.parametrize(
    ("attribute_present", "table_transforms", "exc_type", "match"),
    [
        pytest.param(
            False,
            None,
            TypeError,
            r"_test_invalid_transforms_mod_[0-9a-f]+\.TABLE_TRANSFORMS must be a dictionary, not NoneType",
            id="attribute_missing",
        ),
        pytest.param(True, {}, ValidationError, "Dictionary should have at least 1 item", id="empty_dict"),
        pytest.param(
            True,
            {"   ": _identity_widget_transform},
            ValidationError,
            "String should have at least 1 character",
            id="whitespace_key",
        ),
        pytest.param(
            True,
            {"items": "not_a_transform"},
            ValidationError,
            "Input should be callable",
            id="non_callable_value",
        ),
    ],
)
def test_load_registry_dict_fail_invalid_table_transforms(
    monkeypatch: pytest.MonkeyPatch,
    attribute_present: bool,
    table_transforms: object,
    exc_type: type[Exception],
    match: str,
) -> None:
    """load_registry_dict raises TypeError or ValidationError for missing, empty, or invalid TABLE_TRANSFORMS."""
    attrs = {"TABLE_TRANSFORMS": table_transforms} if attribute_present else {}
    mod_name = register_temp_module(monkeypatch, "_test_invalid_transforms_mod", **attrs)
    with pytest.raises(exc_type, match=match):
        load_registry_dict(mod_name, "TABLE_TRANSFORMS")


@pytest.mark.parametrize(
    ("attribute_present", "output_schemas", "exc_type", "match"),
    [
        pytest.param(
            False,
            None,
            TypeError,
            r"_test_invalid_output_schemas_mod_[0-9a-f]+\.OUTPUT_SCHEMAS must be a dictionary, not NoneType",
            id="attribute_missing",
        ),
        pytest.param(True, {}, ValidationError, "Dictionary should have at least 1 item", id="empty_dict"),
        pytest.param(
            True,
            {"   ": WIDGET_PA_SCHEMA},
            ValidationError,
            "String should have at least 1 character",
            id="whitespace_key",
        ),
        pytest.param(
            True,
            {"items": "not_a_schema"},
            ValidationError,
            "Input should be an instance of Schema",
            id="non_schema_value",
        ),
    ],
)
def test_load_registry_dict_fail_invalid_output_schemas(
    monkeypatch: pytest.MonkeyPatch,
    attribute_present: bool,
    output_schemas: object,
    exc_type: type[Exception],
    match: str,
) -> None:
    """load_registry_dict raises TypeError or ValidationError for missing, empty, or invalid OUTPUT_SCHEMAS."""
    attrs = {"OUTPUT_SCHEMAS": output_schemas} if attribute_present else {}
    mod_name = register_temp_module(monkeypatch, "_test_invalid_output_schemas_mod", **attrs)
    with pytest.raises(exc_type, match=match):
        load_registry_dict(mod_name, "OUTPUT_SCHEMAS")
