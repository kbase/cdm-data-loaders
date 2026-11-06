"""Tests for `cdm_data_loaders.pipelines.jsonlines.settings`."""

from collections.abc import Callable
from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock

import pyarrow as pa
import pytest
from pydantic import BaseModel, ValidationError

import cdm_data_loaders.pipelines.jsonlines.settings as jsonlines_settings_module
from cdm_data_loaders.converters.jsonschema_to_pyarrow import JSONSchemaToPyArrowError
from cdm_data_loaders.converters.pydantic_to_pyarrow import PydanticToPyArrow
from cdm_data_loaders.core.fields import (
    DEFAULT_JSONL_FILE_GLOB,
    FILE_GLOB,
    TABLE_NAME,
    LoaderFileFormatEnum,
)
from cdm_data_loaders.core.registry import (
    ArrowSchemaRegistry,
    ModelRegistry,
    SchemaRegistry,
    TransformRegistry,
)
from cdm_data_loaders.core.settings import CdmDataLoadersBase
from cdm_data_loaders.pipelines.jsonlines.settings import (
    ENTITY_MODELS_MODULE,
    ENTITY_SCHEMAS_MODULE,
    EXTRACT_PIPELINE_NAME,
    JSONSCHEMA_PIPELINE_NAME,
    LOCAL_CLI_SHORTCUTS,
    PYDANTIC_PIPELINE_NAME,
    JsonlExtractSettings,
    JsonlIngestSettings,
    JsonlJsonschemaIngestSettings,
    JsonlPydanticIngestSettings,
    build_output_schema_registry,
    convert_schema_to_pyarrow,
)
from cdm_data_loaders.readers.jsonschema_xsv.xsv_validator.schema_utils import ValidatedSchema
from cdm_data_loaders.utils.jsonschema.dereferencer import DereferencingError
from tests.cdm_data_loaders.conftest import check_settings, cts_expected
from tests.cdm_data_loaders.pipelines.jsonlines.integration.dataset_report_schemas import (
    ENTITY_SCHEMAS as DATASET_ENTITY_SCHEMAS,
)
from tests.helpers import (
    assert_cli_field_roundtrips,
    assert_no_cli_clashes,
    build_cli_arg_specs,
    register_temp_module,
)


class EmptyEntityModel(BaseModel):
    """Pydantic model with zero fields that fails PydanticToPyArrow with EmptyModelError."""


class UnconstrainedEntityModel(BaseModel):
    """Pydantic model with an unconstrained Any field that fails PydanticToPyArrow with UnconstrainedFieldError."""

    payload: Any


class GadgetEntityModel(BaseModel):
    """Valid Pydantic model used alongside widget_model in multi-table tests."""

    gadget_id: str
    tags: list[str] | None = None


GADGET_SCHEMA: dict[str, Any] = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "required": ["gadget_id"],
    "properties": {
        "gadget_id": {"type": "string"},
        "tags": {"type": ["array", "null"], "items": {"type": "string"}},
    },
}

BROKEN_REF_SCHEMA: dict[str, Any] = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "properties": {"item": {"$ref": "#/$defs/Missing"}},
}

NON_OBJECT_ROOT_SCHEMA: dict[str, Any] = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "string",
}

WIDGET_MAIN_PA_SCHEMA = pa.schema([pa.field("widget_id", pa.string(), nullable=False)])
WIDGET_TAG_PA_SCHEMA = pa.schema(
    [pa.field("widget_id", pa.string(), nullable=False), pa.field("tag", pa.string(), nullable=False)]
)
OVERRIDE_PA_SCHEMA = pa.schema([pa.field("payload_json", pa.string(), nullable=False)])


def _identity_widget_transform(row: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    """Return the input row routed to the widget table."""
    return {"widget": [row]}


def _split_widget_transform(row: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    """Split a widget row across widget_main and widget_tag tables."""
    return {"widget_main": [row], "widget_tag": []}


def _make_validation_registry(
    kind: str,
    widget_model: type[BaseModel],
    widget_schema: dict[str, Any],
    *,
    include_unconvertible: bool = False,
) -> tuple[ModelRegistry | SchemaRegistry, dict[str, pa.Schema]]:
    """Build a ModelRegistry or SchemaRegistry and its expected converted PyArrow schemas."""
    if kind == "pydantic":
        converter = PydanticToPyArrow()
        models: dict[str, type[BaseModel]] = {"widget": widget_model, "gadget": GadgetEntityModel}
        if include_unconvertible:
            models["raw"] = UnconstrainedEntityModel
        expected = {
            "widget": converter.convert(widget_model),
            "gadget": converter.convert(GadgetEntityModel),
        }
        return ModelRegistry(registry=models), expected

    schemas: dict[str, dict[str, Any]] = {"widget": widget_schema, "gadget": GADGET_SCHEMA}
    if include_unconvertible:
        schemas["raw"] = NON_OBJECT_ROOT_SCHEMA
    expected = {
        "widget": convert_schema_to_pyarrow(ValidatedSchema(jsonschema=widget_schema)),
        "gadget": convert_schema_to_pyarrow(ValidatedSchema(jsonschema=GADGET_SCHEMA)),
    }
    return SchemaRegistry(registry=schemas), expected


def test_convert_schema_to_pyarrow_pass_flat_and_nested_schema(widget_schema: dict[str, Any]) -> None:
    """Validated JSON Schemas convert to PyArrow schemas with expected types and nullability."""
    pa_schema = convert_schema_to_pyarrow(ValidatedSchema(jsonschema=widget_schema))

    assert pa_schema == pa.schema(
        [
            pa.field("widget_id", pa.string(), nullable=False, metadata={"jsonschema_type": "string"}),
            pa.field("count", pa.int64(), nullable=False, metadata={"jsonschema_type": "integer"}),
            pa.field("collected", pa.date32(), nullable=True, metadata={"jsonschema_type": "string (date)"}),
            pa.field("label", pa.string(), nullable=True, metadata={"jsonschema_type": "string"}),
            pa.field(
                "nested",
                pa.struct(
                    [
                        pa.field(
                            "tags",
                            pa.list_(
                                pa.field("item", pa.string(), nullable=False, metadata={"jsonschema_type": "string"})
                            ),
                            nullable=True,
                        )
                    ]
                ),
                nullable=True,
            ),
        ]
    )


def test_convert_schema_to_pyarrow_pass_dereferences_root_and_allof_refs() -> None:
    """Schemas using root $ref, nested definitions, and allOf dereference while preserving draft before PyArrow conversion."""
    root_ref_schema: dict[str, Any] = {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$ref": "#/$defs/RootItem",
        "$defs": {
            "Base": {
                "type": "object",
                "required": ["id"],
                "properties": {"id": {"type": "string"}},
            },
            "RootItem": {
                "allOf": [
                    {"$ref": "#/$defs/Base"},
                    {
                        "type": "object",
                        "properties": {"score": {"type": "number", "multipleOf": 0.01}},
                    },
                ]
            },
        },
    }

    pa_schema = convert_schema_to_pyarrow(ValidatedSchema(jsonschema=root_ref_schema))
    assert pa_schema == pa.schema(
        [
            pa.field("id", pa.string(), nullable=False, metadata={"jsonschema_type": "string"}),
            pa.field("score", pa.float64(), nullable=True, metadata={"jsonschema_type": "number"}),
        ]
    )


def test_convert_schema_to_pyarrow_pass_dataset_entity_schema() -> None:
    """The dataset entity schema with nested $defs dereferences and converts to PyArrow."""
    dataset_pa_schema = convert_schema_to_pyarrow(ValidatedSchema(jsonschema=DATASET_ENTITY_SCHEMAS["dataset"]))
    assert "accession" in dataset_pa_schema.names
    assert dataset_pa_schema.field("accession").type == pa.string()


@pytest.mark.parametrize(
    ("schema", "exc_type"),
    [
        pytest.param(
            {
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": "object",
                "properties": {"item": {"$ref": "#/$defs/Missing"}},
            },
            DereferencingError,
            id="unresolvable-ref",
        ),
        pytest.param(
            {
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "$ref": "#/$defs/Loop",
                "$defs": {"Loop": {"$ref": "#/$defs/Loop"}},
            },
            DereferencingError,
            id="circular-ref",
        ),
        pytest.param(
            {
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": "string",
            },
            JSONSchemaToPyArrowError,
            id="non-object-root",
        ),
        pytest.param(
            {
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": ["object", "null"],
                "properties": {"id": {"type": "string"}},
            },
            JSONSchemaToPyArrowError,
            id="nullable-object-root",
        ),
        pytest.param(
            {
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": "object",
                "additionalProperties": {"type": "string"},
            },
            JSONSchemaToPyArrowError,
            id="root-map-without-properties",
        ),
        pytest.param(
            {
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": "object",
                "properties": {},
            },
            JSONSchemaToPyArrowError,
            id="empty-properties",
        ),
        pytest.param(
            {
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": "object",
            },
            JSONSchemaToPyArrowError,
            id="missing-properties",
        ),
        pytest.param(
            {
                "type": "object",
                "properties": {"id": {"type": "string"}},
            },
            JSONSchemaToPyArrowError,
            id="missing-schema-draft",
        ),
    ],
)
def test_convert_schema_to_pyarrow_fail_invalid_schemas(schema: dict[str, Any], exc_type: type[Exception]) -> None:
    """Dereferencing and PyArrow conversion failures raise their documented exception types."""
    validated = (
        ValidatedSchema(jsonschema=schema)
        if "$schema" in schema
        else ValidatedSchema.model_construct(jsonschema=schema)
    )
    with pytest.raises(exc_type):
        convert_schema_to_pyarrow(validated)


@pytest.mark.parametrize("kind", ["pydantic", "jsonschema"], ids=["pydantic", "jsonschema"])
@pytest.mark.parametrize(
    "scenario",
    ["converts_all", "transform_with_overrides", "transform_without_override_returns_none"],
    ids=["converts-all", "transform-with-overrides", "transform-without-override-returns-none"],
)
def test_build_output_schema_registry_pass_converts_and_applies_overrides(
    kind: str,
    scenario: str,
    widget_model: type[BaseModel],
    widget_schema: dict[str, Any],
) -> None:
    """build_output_schema_registry converts non-overridden tables, skips transformed tables, and conserves output_schemas."""
    if scenario == "converts_all":
        registry, expected_converted = _make_validation_registry(kind, widget_model, widget_schema)
        table_transforms = None
        output_schemas = None
        expected: ArrowSchemaRegistry | None = ArrowSchemaRegistry(registry=expected_converted)
    elif scenario == "transform_with_overrides":
        registry, expected_converted = _make_validation_registry(
            kind,
            widget_model,
            widget_schema,
            include_unconvertible=True,
        )
        table_transforms = TransformRegistry(registry={"widget": _split_widget_transform})
        output_schemas = ArrowSchemaRegistry(
            registry={
                "widget_main": WIDGET_MAIN_PA_SCHEMA,
                "widget_tag": WIDGET_TAG_PA_SCHEMA,
                "raw": OVERRIDE_PA_SCHEMA,
            }
        )
        expected = ArrowSchemaRegistry(
            registry={
                "widget_main": WIDGET_MAIN_PA_SCHEMA,
                "widget_tag": WIDGET_TAG_PA_SCHEMA,
                "raw": OVERRIDE_PA_SCHEMA,
                "gadget": expected_converted["gadget"],
            }
        )
    else:
        registry = (
            ModelRegistry(registry={"widget": UnconstrainedEntityModel})
            if kind == "pydantic"
            else SchemaRegistry(registry={"widget": NON_OBJECT_ROOT_SCHEMA})
        )
        table_transforms = TransformRegistry(registry={"widget": _identity_widget_transform})
        output_schemas = None
        expected = None

    assert (
        build_output_schema_registry(
            registry,
            table_transforms=table_transforms,
            output_schemas=output_schemas,
        )
        == expected
    )


@pytest.mark.parametrize("kind", ["pydantic", "jsonschema"], ids=["pydantic", "jsonschema"])
def test_build_output_schema_registry_fail_aggregates_conversion_errors(
    kind: str,
    widget_model: type[BaseModel],
    widget_schema: dict[str, Any],
) -> None:
    """build_output_schema_registry aggregates all failing tables into one RuntimeError and omits valid tables."""
    registry: ModelRegistry | SchemaRegistry = (
        ModelRegistry(
            registry={
                "valid_widget": widget_model,
                "bad_first": EmptyEntityModel,
                "bad_second": UnconstrainedEntityModel,
            }
        )
        if kind == "pydantic"
        else SchemaRegistry(
            registry={
                "valid_widget": widget_schema,
                "bad_first": BROKEN_REF_SCHEMA,
                "bad_second": NON_OBJECT_ROOT_SCHEMA,
            }
        )
    )
    with pytest.raises(RuntimeError, match="converting schemas to PyArrow") as exc_info:
        build_output_schema_registry(registry)
    message = str(exc_info.value)
    assert "bad_first:" in message
    assert "bad_second:" in message
    assert "valid_widget:" not in message


@pytest.mark.parametrize(
    "settings_cls",
    [
        JsonlIngestSettings,
        JsonlExtractSettings,
        JsonlPydanticIngestSettings,
        JsonlJsonschemaIngestSettings,
    ],
    ids=["ingest", "extract", "pydantic", "jsonschema"],
)
def test_jsonlines_settings_pass_no_cli_clashes(settings_cls: type[CdmDataLoadersBase]) -> None:
    """Every JSONL settings class has no CLI argument or shortcut collisions."""
    assert_no_cli_clashes(settings_cls)


def test_jsonl_ingest_settings_pass_defaults(test_defaults: dict[str, Any]) -> None:
    """JsonlIngestSettings defaults file_glob to '*.jsonl*' and loader_file_format to parquet."""
    settings = JsonlIngestSettings(**test_defaults, dataset_name="test_ds")
    check_settings(
        settings,
        cts_expected(
            **test_defaults,
            dataset_name="test_ds",
            loader_file_format=LoaderFileFormatEnum.PARQUET,
            file_glob=DEFAULT_JSONL_FILE_GLOB,
        ),
    )


def test_jsonl_extract_settings_pass_defaults(test_defaults: dict[str, Any]) -> None:
    """JsonlExtractSettings sets defaults, cli_prog_name, and leaves transform/schema registries None."""
    settings = JsonlExtractSettings(**test_defaults, dataset_name="test_ds", table_name="widget")
    check_settings(
        settings,
        cts_expected(
            **test_defaults,
            dataset_name="test_ds",
            loader_file_format=LoaderFileFormatEnum.PARQUET,
            file_glob=DEFAULT_JSONL_FILE_GLOB,
            table_name="widget",
            transforms_module=None,
            output_schemas_module=None,
        ),
    )
    assert settings.model_config.get("cli_prog_name") == EXTRACT_PIPELINE_NAME
    assert settings.table_transforms is None
    assert settings.schema_map is None


@pytest.mark.parametrize("kind", ["pydantic", "jsonschema"], ids=["pydantic", "jsonschema"])
def test_validated_jsonl_settings_pass_defaults_and_registries(
    kind: str,
    monkeypatch: pytest.MonkeyPatch,
    test_defaults: dict[str, Any],
    widget_model: type[BaseModel],
    widget_schema: dict[str, Any],
) -> None:
    """JsonlPydanticIngestSettings and JsonlJsonschemaIngestSettings load their entity registry and schema_map on init."""
    if kind == "pydantic":
        mod_name = register_temp_module(monkeypatch, "_test_pydantic_models", ENTITY_MODELS={"widget": widget_model})
        settings: JsonlPydanticIngestSettings | JsonlJsonschemaIngestSettings = JsonlPydanticIngestSettings(
            **test_defaults,
            dataset_name="test_ds",
            entity_models=mod_name,
        )
        expected_prog = PYDANTIC_PIPELINE_NAME
        expected_field = {ENTITY_MODELS_MODULE: mod_name}
        expected_pa_schema = PydanticToPyArrow().convert(widget_model)
        assert settings.model_registry == ModelRegistry(registry={"widget": widget_model})
    else:
        mod_name = register_temp_module(monkeypatch, "_test_json_schemas", ENTITY_SCHEMAS={"widget": widget_schema})
        settings = JsonlJsonschemaIngestSettings(
            **test_defaults,
            dataset_name="test_ds",
            entity_schemas=mod_name,
        )
        expected_prog = JSONSCHEMA_PIPELINE_NAME
        expected_field = {ENTITY_SCHEMAS_MODULE: mod_name}
        validated_widget = ValidatedSchema(jsonschema=widget_schema)
        expected_pa_schema = convert_schema_to_pyarrow(validated_widget)
        assert settings.schema_registry == SchemaRegistry(registry={"widget": validated_widget})

    check_settings(
        settings,
        cts_expected(
            **test_defaults,
            dataset_name="test_ds",
            loader_file_format=LoaderFileFormatEnum.PARQUET,
            file_glob=DEFAULT_JSONL_FILE_GLOB,
            transforms_module=None,
            output_schemas_module=None,
            **expected_field,
        ),
    )
    assert settings.model_config.get("cli_prog_name") == expected_prog
    assert settings.table_transforms is None
    assert settings.schema_map == ArrowSchemaRegistry(registry={"widget": expected_pa_schema})


@pytest.mark.parametrize(
    ("settings_cls", "field_name", "shortcut"),
    [
        pytest.param(JsonlExtractSettings, FILE_GLOB, LOCAL_CLI_SHORTCUTS[FILE_GLOB], id="extract-file_glob"),
        pytest.param(JsonlExtractSettings, TABLE_NAME, LOCAL_CLI_SHORTCUTS[TABLE_NAME], id="extract-table_name"),
        pytest.param(JsonlPydanticIngestSettings, FILE_GLOB, LOCAL_CLI_SHORTCUTS[FILE_GLOB], id="pydantic-file_glob"),
        pytest.param(
            JsonlPydanticIngestSettings,
            ENTITY_MODELS_MODULE,
            LOCAL_CLI_SHORTCUTS[ENTITY_MODELS_MODULE],
            id="pydantic-entity_models",
        ),
        pytest.param(
            JsonlJsonschemaIngestSettings, FILE_GLOB, LOCAL_CLI_SHORTCUTS[FILE_GLOB], id="jsonschema-file_glob"
        ),
        pytest.param(
            JsonlJsonschemaIngestSettings,
            ENTITY_SCHEMAS_MODULE,
            LOCAL_CLI_SHORTCUTS[ENTITY_SCHEMAS_MODULE],
            id="jsonschema-entity_schemas",
        ),
    ],
)
def test_jsonlines_settings_pass_cli_field_roundtrips(
    monkeypatch: pytest.MonkeyPatch,
    test_defaults: dict[str, Any],
    widget_model: type[BaseModel],
    widget_schema: dict[str, Any],
    settings_cls: type[CdmDataLoadersBase],
    field_name: str,
    shortcut: str,
) -> None:
    """Every JSONL settings CLI field and its short flag (-g, -t, -m) roundtrips to the same value."""
    default_mod = register_temp_module(
        monkeypatch,
        "_test_cli_default_mod",
        ENTITY_MODELS={"widget": widget_model},
        ENTITY_SCHEMAS={"widget": widget_schema},
    )
    alt_mod = register_temp_module(
        monkeypatch,
        "_test_cli_alt_mod",
        ENTITY_MODELS={"gadget": GadgetEntityModel},
        ENTITY_SCHEMAS={"gadget": GADGET_SCHEMA},
    )
    required: dict[str, Any] = {**test_defaults, "dataset_name": "test_ds"}
    if settings_cls is JsonlExtractSettings:
        required[TABLE_NAME] = "widget"
    elif settings_cls is JsonlPydanticIngestSettings:
        required[ENTITY_MODELS_MODULE] = default_mod
    elif settings_cls is JsonlJsonschemaIngestSettings:
        required[ENTITY_SCHEMAS_MODULE] = default_mod

    raw_values = {
        FILE_GLOB: "*.jsonl.gz",
        TABLE_NAME: "custom_table",
        ENTITY_MODELS_MODULE: alt_mod,
        ENTITY_SCHEMAS_MODULE: alt_mod,
    }
    specs = build_cli_arg_specs(settings_cls)
    assert f"-{shortcut}" in specs[field_name].option_strings
    assert_cli_field_roundtrips(
        settings_cls,
        field_name,
        raw_values[field_name],
        raw_values[field_name],
        required=required,
    )


@pytest.mark.parametrize(
    ("settings_cls", "required_field"),
    [
        pytest.param(JsonlExtractSettings, TABLE_NAME, id="extract-table_name"),
        pytest.param(JsonlPydanticIngestSettings, ENTITY_MODELS_MODULE, id="pydantic-entity_models"),
        pytest.param(JsonlJsonschemaIngestSettings, ENTITY_SCHEMAS_MODULE, id="jsonschema-entity_schemas"),
    ],
)
def test_jsonlines_settings_fail_missing_required_field(
    test_defaults: dict[str, Any],
    settings_cls: type[CdmDataLoadersBase],
    required_field: str,
) -> None:
    """Omitting the pipeline-specific required field raises ValidationError."""
    with pytest.raises(ValidationError) as exc_info:
        settings_cls(**test_defaults, dataset_name="test_ds")
    assert {(err["loc"], err["type"]) for err in exc_info.value.errors()} == {((required_field,), "missing")}


@pytest.mark.parametrize("kind", ["pydantic", "jsonschema"], ids=["pydantic", "jsonschema"])
@pytest.mark.parametrize(
    "with_output_schemas",
    [True, False],
    ids=["transforms-and-output-schemas", "transforms-only-unschematized"],
)
def test_validated_jsonl_settings_pass_transforms_and_output_schemas(
    kind: str,
    with_output_schemas: bool,
    monkeypatch: pytest.MonkeyPatch,
    test_defaults: dict[str, Any],
    widget_model: type[BaseModel],
    widget_schema: dict[str, Any],
) -> None:
    """Validated JSONL settings skip default PyArrow conversion for transformed or overridden tables."""
    if kind == "pydantic":
        entities = (
            {
                "widget": widget_model,
                "transformed_raw": UnconstrainedEntityModel,
                "overridden_empty": EmptyEntityModel,
            }
            if with_output_schemas
            else {"widget": UnconstrainedEntityModel}
        )
        mod_name = register_temp_module(
            monkeypatch,
            f"_test_pydantic_{with_output_schemas}",
            ENTITY_MODELS=entities,
        )
        expected_widget_pa = PydanticToPyArrow().convert(widget_model)
        settings_cls: type[JsonlPydanticIngestSettings | JsonlJsonschemaIngestSettings] = JsonlPydanticIngestSettings
        mod_kwarg = ENTITY_MODELS_MODULE
    else:
        schemas = (
            {
                "widget": widget_schema,
                "transformed_raw": NON_OBJECT_ROOT_SCHEMA,
                "overridden_empty": BROKEN_REF_SCHEMA,
            }
            if with_output_schemas
            else {"widget": NON_OBJECT_ROOT_SCHEMA}
        )
        mod_name = register_temp_module(
            monkeypatch,
            f"_test_jsonschema_{with_output_schemas}",
            ENTITY_SCHEMAS=schemas,
        )
        expected_widget_pa = convert_schema_to_pyarrow(ValidatedSchema(jsonschema=widget_schema))
        settings_cls = JsonlJsonschemaIngestSettings
        mod_kwarg = ENTITY_SCHEMAS_MODULE

    if with_output_schemas:
        hooks_mod = register_temp_module(
            monkeypatch,
            f"_test_hooks_{kind}",
            TABLE_TRANSFORMS={"transformed_raw": _identity_widget_transform},
            OUTPUT_SCHEMAS={"overridden_empty": OVERRIDE_PA_SCHEMA},
        )
        output_schemas_module: str | None = hooks_mod
        expected_schema_map: ArrowSchemaRegistry | None = ArrowSchemaRegistry(
            registry={
                "widget": expected_widget_pa,
                "overridden_empty": OVERRIDE_PA_SCHEMA,
            }
        )
    else:
        hooks_mod = register_temp_module(
            monkeypatch,
            f"_test_only_transform_{kind}",
            TABLE_TRANSFORMS={"widget": _identity_widget_transform},
        )
        output_schemas_module = None
        expected_schema_map = None

    settings = settings_cls(
        **test_defaults,
        dataset_name="test_ds",
        transforms_module=hooks_mod,
        output_schemas_module=output_schemas_module,
        **{mod_kwarg: mod_name},
    )
    assert settings.schema_map == expected_schema_map


@pytest.mark.parametrize("kind", ["pydantic", "jsonschema"], ids=["pydantic", "jsonschema"])
def test_validated_jsonl_settings_fail_unknown_transform_table(
    kind: str,
    monkeypatch: pytest.MonkeyPatch,
    test_defaults: dict[str, Any],
    widget_model: type[BaseModel],
    widget_schema: dict[str, Any],
) -> None:
    """Validated JSONL settings reject TABLE_TRANSFORMS keys that are not in the entity registry."""
    mod_name = register_temp_module(
        monkeypatch,
        f"_test_unknown_transform_entities_{kind}",
        ENTITY_MODELS={"widget": widget_model},
        ENTITY_SCHEMAS={"widget": widget_schema},
    )
    transforms_mod = register_temp_module(
        monkeypatch,
        f"_test_unknown_transform_hooks_{kind}",
        TABLE_TRANSFORMS={"unknown": _identity_widget_transform},
    )
    settings_cls, mod_kwarg = (
        (JsonlPydanticIngestSettings, ENTITY_MODELS_MODULE)
        if kind == "pydantic"
        else (JsonlJsonschemaIngestSettings, ENTITY_SCHEMAS_MODULE)
    )
    with pytest.raises(ValidationError, match="Unknown table names in transforms_module: unknown"):
        settings_cls(
            **test_defaults,
            dataset_name="test_ds",
            transforms_module=transforms_mod,
            **{mod_kwarg: mod_name},
        )


@pytest.mark.parametrize("kind", ["pydantic", "jsonschema"], ids=["pydantic", "jsonschema"])
def test_validated_jsonl_settings_fail_unconvertible_schemas(
    kind: str,
    monkeypatch: pytest.MonkeyPatch,
    test_defaults: dict[str, Any],
) -> None:
    """Validated JSONL settings propagate RuntimeError when entity schemas cannot convert to PyArrow."""
    mod_name = register_temp_module(
        monkeypatch,
        f"_test_unconvertible_entities_{kind}",
        ENTITY_MODELS={"bad_one": EmptyEntityModel, "bad_two": UnconstrainedEntityModel},
        ENTITY_SCHEMAS={"bad_one": BROKEN_REF_SCHEMA, "bad_two": NON_OBJECT_ROOT_SCHEMA},
    )
    settings_cls, mod_kwarg = (
        (JsonlPydanticIngestSettings, ENTITY_MODELS_MODULE)
        if kind == "pydantic"
        else (JsonlJsonschemaIngestSettings, ENTITY_SCHEMAS_MODULE)
    )
    with pytest.raises(RuntimeError, match="converting schemas to PyArrow") as exc_info:
        settings_cls(
            **test_defaults,
            dataset_name="test_ds",
            **{mod_kwarg: mod_name},
        )
    assert all(name in str(exc_info.value) for name in ("bad_one:", "bad_two:"))


@pytest.mark.parametrize("buffer_size", [0, -1], ids=["zero", "negative"])
def test_jsonlines_settings_fail_invalid_buffer_size(
    schema_settings_factory: Callable[..., JsonlJsonschemaIngestSettings],
    buffer_size: int,
) -> None:
    """Settings reject nonpositive buffer_size values during construction."""
    with pytest.raises(ValidationError, match="buffer_size"):
        schema_settings_factory(buffer_size=buffer_size)


def test_validated_jsonl_settings_pass_validates_table_transforms_once(
    schema_settings_factory: Callable[..., JsonlJsonschemaIngestSettings],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Settings initialization validates TABLE_TRANSFORMS once per instantiation."""
    spy = MagicMock(wraps=jsonlines_settings_module.validate_table_transforms)
    monkeypatch.setattr(jsonlines_settings_module, "validate_table_transforms", spy)

    schema_settings_factory()

    assert spy.call_count == 1
