"""Tests for XsvIngestSettings field validation, schema loading, and derived properties."""

import importlib.resources
import json
from collections.abc import Callable
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pyarrow as pa
import pytest
from pydantic import ValidationError

import cdm_data_loaders.pipelines.xsv.settings as xsv_settings_module
from cdm_data_loaders.converters.facades.jsonschema_to_pyarrow import (
    JSONSchemaToPyArrowError,
    convert_xsv_schema_to_pyarrow,
)
from cdm_data_loaders.core.fields import DEFAULTS
from cdm_data_loaders.core.registry import ArrowSchemaRegistry
from cdm_data_loaders.pipelines.xsv.settings import (
    PIPELINE_NAME,
    XsvIngestSettings,
)
from cdm_data_loaders.readers.jsonschema_xsv.xsv_validator.schema_utils import ValidatedXsvSchema
from cdm_data_loaders.schemas.jsonschema import (
    CUSTOM_META_SCHEMA_URI,
    DereferencingError,
    get_xsv_meta_schema,
)
from tests.cdm_data_loaders.pipelines.xsv.conftest import DEFAULT_XSV_SCHEMA, VALID_SCHEMA_URI
from tests.helpers import register_temp_module
from tests.xsv_fixtures import COLUMNS

"""defaults and basic construction"""


def test_xsv_settings_pass_defaults(settings_factory: Callable[..., XsvIngestSettings]) -> None:
    """file_glob, buffer_size, and log_interval default to the common CTS defaults."""
    settings = settings_factory()
    assert settings.file_glob == DEFAULTS["file_glob"]
    assert settings.buffer_size == DEFAULTS["buffer_size"]
    assert settings.log_interval == DEFAULTS["log_interval"]
    assert settings.model_config["cli_prog_name"] == PIPELINE_NAME


def test_xsv_settings_pass_loads_and_validates_schema(
    settings_factory: Callable[..., XsvIngestSettings],
) -> None:
    """validated_schema exposes the parsed schema's required columns."""
    settings = settings_factory()
    assert settings.validated_schema.required_cols == COLUMNS


def test_xsv_settings_fail_missing_schema_file(settings_factory: Callable[..., XsvIngestSettings]) -> None:
    """A schema_file that does not exist on disk raises ValidationError."""
    with pytest.raises(ValidationError, match="Path does not point to a file"):
        settings_factory(schema=None, schema_file="does_not_exist.json")


@pytest.mark.parametrize(
    "invalid_schema",
    [
        pytest.param({"required": COLUMNS}, id="missing-dollar-schema"),
        pytest.param({"$schema": VALID_SCHEMA_URI}, id="missing-required"),
        pytest.param({"$schema": VALID_SCHEMA_URI, "required": []}, id="empty-required"),
    ],
)
def test_xsv_settings_fail_invalid_schema(
    settings_factory: Callable[..., XsvIngestSettings], invalid_schema: dict[str, Any]
) -> None:
    """A structurally invalid schema (missing $schema or required) raises RuntimeError."""
    with pytest.raises(RuntimeError, match="Could not load JSON Schema"):
        settings_factory(schema=invalid_schema)


def test_xsv_settings_fail_empty_schema_file_name(settings_factory: Callable[..., XsvIngestSettings]) -> None:
    """An empty schema_file string is rejected by the NonEmptyStr constraint."""
    with pytest.raises(ValidationError):
        settings_factory(schema_file="")


"""derived properties: first_pass_schema and parsing_config"""


@pytest.mark.parametrize(
    ("schema_overrides", "expected_kept_keys"),
    [
        pytest.param(
            {},
            {"$schema", "required", "type", "properties"},
            id="strips_xsv_config_and_omits_absent_metadata_keys",
        ),
        pytest.param(
            {"$id": "https://example.org/xsv.json", "title": "My XSV", "description": "dropped"},
            {"$schema", "$id", "title", "required", "type", "properties"},
            id="preserves_id_and_title_and_drops_description",
        ),
    ],
)
def test_xsv_settings_pass_first_pass_schema_is_loose_and_cached(
    settings_factory: Callable[..., XsvIngestSettings],
    schema_overrides: dict[str, Any],
    expected_kept_keys: set[str],
) -> None:
    """first_pass_schema derives a loose string-or-null schema, preserves metadata keys, and caches the instance."""
    base_schema = {k: v for k, v in DEFAULT_XSV_SCHEMA.items() if k not in ("$id", "title")}
    settings = settings_factory(schema={**base_schema, **schema_overrides})
    first_pass = settings.first_pass_schema
    assert set(first_pass.jsonschema.keys()) == expected_kept_keys
    assert first_pass.required_cols == COLUMNS
    assert first_pass.jsonschema["type"] == "object"
    assert first_pass.jsonschema["properties"] == {col: {"type": ["string", "null"]} for col in COLUMNS}
    assert settings.first_pass_schema is first_pass


@pytest.mark.parametrize(
    ("xsv_config", "expected_parsing_config"),
    [
        pytest.param(None, {}, id="missing_xsv_config_block"),
        pytest.param({"x-quoting-policy": "all"}, {}, id="only_ignored_quoting_policy"),
        pytest.param(
            {"x-delimiter": ",", "x-has-header": True},
            {"delimiter": ",", "missing_header": False},
            id="comma_delimiter_with_header",
        ),
        pytest.param(
            {
                "x-delimiter": "\t",
                "x-comment-char": "#",
                "x-quote": "'",
                "x-escape": "\\",
                "x-null-regex": "^NA$",
                "x-null-cols": ["col_1", "col_2"],
                "x-has-header": False,
                "x-quoting-policy": "all",
            },
            {
                "delimiter": "\t",
                "comment_char": "#",
                "quote": "'",
                "escape": "\\",
                "null_regex": "^NA$",
                "null_regex_cols": ["col_1", "col_2"],
                "missing_header": True,
            },
            id="all_supported_keys_mapped_and_quoting_policy_ignored",
        ),
    ],
)
def test_xsv_settings_pass_parsing_config_resolved_and_cached(
    settings_factory: Callable[..., XsvIngestSettings],
    xsv_config: dict[str, Any] | None,
    expected_parsing_config: dict[str, Any],
) -> None:
    """parsing_config resolves qsv kwargs from the schema's x-xsv-config block and caches the dict."""
    base_schema = {k: v for k, v in DEFAULT_XSV_SCHEMA.items() if k != "x-xsv-config"}
    if xsv_config is not None:
        base_schema["x-xsv-config"] = xsv_config
    settings = settings_factory(schema=base_schema)
    assert settings.parsing_config == expected_parsing_config
    assert settings.parsing_config is settings.parsing_config


@pytest.mark.parametrize(
    "property_name",
    ["validated_schema", "first_pass_schema", "parsing_config", "pyarrow_schema"],
    ids=["validated_schema", "first_pass_schema", "parsing_config", "pyarrow_schema"],
)
def test_xsv_settings_fail_unset_schema_properties_raise(property_name: str) -> None:
    """Accessing validated_schema, first_pass_schema, parsing_config, or pyarrow_schema before validation raises RuntimeError."""
    settings = XsvIngestSettings.model_construct()
    with pytest.raises(RuntimeError, match="Schema has not been validated"):
        getattr(settings, property_name)


"""convert_xsv_schema_to_pyarrow"""


@pytest.mark.parametrize(
    ("raw_schema", "expected_pa_schema"),
    [
        pytest.param(
            {
                "$schema": VALID_SCHEMA_URI,
                "required": ["unlisted_col", "non_null_int", "nullable_str"],
                "properties": {
                    "non_null_int": {"type": "integer"},
                    "nullable_str": {"type": ["string", "null"]},
                    "extra_col": {"type": "boolean"},
                },
            },
            pa.schema(
                [
                    pa.field("unlisted_col", pa.string(), nullable=True),
                    pa.field("non_null_int", pa.int64(), nullable=False),
                    pa.field("nullable_str", pa.string(), nullable=True),
                    pa.field("extra_col", pa.bool_(), nullable=True),
                ]
            ),
            id="orders_required_defaults_unlisted_and_keeps_extra_nullable",
        ),
        pytest.param(
            {
                "$schema": VALID_SCHEMA_URI,
                "required": [
                    "str_not_null",
                    "list_not_null",
                    "list_with_null",
                    "anyof_not_null",
                    "anyof_with_null",
                    "oneof_not_null",
                    "oneof_with_null",
                    "enum_not_null",
                    "enum_with_null",
                ],
                "properties": {
                    "str_not_null": {"type": "string"},
                    "list_not_null": {"type": ["integer"]},
                    "list_with_null": {"type": ["integer", "null"]},
                    "anyof_not_null": {"anyOf": [{"type": "integer"}, {"type": "integer"}]},
                    "anyof_with_null": {"anyOf": [{"type": "integer"}, {"type": "null"}]},
                    "oneof_not_null": {"oneOf": [{"type": "string"}]},
                    "oneof_with_null": {"oneOf": [{"type": "string"}, {"type": "null"}]},
                    "enum_not_null": {"enum": ["a", "b"]},
                    "enum_with_null": {"enum": ["a", "b", None]},
                },
            },
            pa.schema(
                [
                    pa.field("str_not_null", pa.string(), nullable=False),
                    pa.field("list_not_null", pa.int64(), nullable=False),
                    pa.field("list_with_null", pa.int64(), nullable=True),
                    pa.field("anyof_not_null", pa.int64(), nullable=False),
                    pa.field("anyof_with_null", pa.int64(), nullable=True),
                    pa.field("oneof_not_null", pa.string(), nullable=False),
                    pa.field("oneof_with_null", pa.string(), nullable=True),
                    pa.field("enum_not_null", pa.string(), nullable=False),
                    pa.field("enum_with_null", pa.string(), nullable=True),
                ]
            ),
            id="nullability_inference_across_type_anyof_oneof_and_enum",
        ),
        pytest.param(
            {
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "$ref": "#/$defs/RowDef",
                "required": ["id", "label"],
                "$defs": {
                    "RowDef": {
                        "type": "object",
                        "properties": {
                            "id": {"type": "integer"},
                            "label": {"type": ["string", "null"]},
                        },
                    }
                },
            },
            pa.schema(
                [
                    pa.field("id", pa.int64(), nullable=False),
                    pa.field("label", pa.string(), nullable=True),
                ]
            ),
            id="dereferences_root_ref_and_preserves_dollar_schema",
        ),
    ],
)
def test_convert_xsv_schema_to_pyarrow_pass(
    raw_schema: dict[str, Any],
    expected_pa_schema: pa.Schema,
) -> None:
    """convert_xsv_schema_to_pyarrow orders columns by required_cols, infers nullability, and dereferences $ref."""
    validated = ValidatedXsvSchema(jsonschema=raw_schema)
    assert convert_xsv_schema_to_pyarrow(validated) == expected_pa_schema


@pytest.mark.parametrize(
    ("raw_schema", "exc_type"),
    [
        pytest.param(
            {
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "required": ["id"],
                "properties": {"id": {"$ref": "#/$defs/Missing"}},
            },
            DereferencingError,
            id="unresolvable_ref",
        ),
        pytest.param(
            {
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": "array",
                "required": ["id"],
                "properties": {"id": {"type": "integer"}},
            },
            JSONSchemaToPyArrowError,
            id="non_object_root_type",
        ),
    ],
)
def test_convert_xsv_schema_to_pyarrow_fail(
    raw_schema: dict[str, Any],
    exc_type: type[Exception],
) -> None:
    """convert_xsv_schema_to_pyarrow raises DereferencingError or JSONSchemaToPyArrowError on invalid schemas."""
    validated = ValidatedXsvSchema(jsonschema=raw_schema)
    with pytest.raises(exc_type):
        convert_xsv_schema_to_pyarrow(validated)


@pytest.mark.parametrize(
    ("use_transforms", "use_output_schemas"),
    [
        pytest.param(False, False, id="default-from-input-schema"),
        pytest.param(True, False, id="transforms-only-unschematized"),
        pytest.param(False, True, id="output-schemas-only"),
        pytest.param(True, True, id="transforms-with-output-schemas"),
    ],
)
def test_xsv_settings_pass_resolves_schema_map_from_input_and_respects_transforms(
    monkeypatch: pytest.MonkeyPatch,
    settings_factory: Callable[..., XsvIngestSettings],
    use_transforms: bool,
    use_output_schemas: bool,
) -> None:
    """pyarrow_schema and schema_map resolve from input JSON Schema or OUTPUT_SCHEMAS depending on transforms."""
    override_schema = pa.schema([pa.field("number", pa.int64(), nullable=False)])
    hooks_mod = register_temp_module(
        monkeypatch,
        "_test_xsv_settings_hooks",
        TABLE_TRANSFORMS={"my_table": lambda row: {"my_table": [row]}},
        OUTPUT_SCHEMAS={"my_table": override_schema},
    )
    convert_spy = MagicMock(wraps=convert_xsv_schema_to_pyarrow)
    monkeypatch.setattr(xsv_settings_module, "convert_xsv_schema_to_pyarrow", convert_spy)

    settings = settings_factory(
        transforms_module=hooks_mod if use_transforms else None,
        output_schemas_module=hooks_mod if use_output_schemas else None,
    )
    converted_schema = convert_xsv_schema_to_pyarrow(settings.validated_schema)
    if use_output_schemas and not use_transforms:
        expected_map: ArrowSchemaRegistry | None = ArrowSchemaRegistry(registry={"my_table": override_schema})
        assert settings.pyarrow_schema == override_schema
        assert convert_spy.call_count == 0
    elif use_output_schemas and use_transforms:
        expected_map = ArrowSchemaRegistry(registry={"my_table": override_schema})
        assert settings.pyarrow_schema == converted_schema
        assert convert_spy.call_count == 1
    elif use_transforms:
        expected_map = None
        assert settings.pyarrow_schema == converted_schema
        assert convert_spy.call_count == 1
    else:
        assert convert_spy.call_count == 1
        assert settings.pyarrow_schema == converted_schema
        expected_map = ArrowSchemaRegistry(registry={"my_table": converted_schema})

    assert settings.schema_map == expected_map


def test_xsv_settings_fail_unknown_transform_table_name(
    settings_factory: Callable[..., XsvIngestSettings],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """XsvIngestSettings rejects TABLE_TRANSFORMS that reference an unknown table name."""
    transforms_mod = register_temp_module(
        monkeypatch,
        "_test_xsv_unknown_table_transforms",
        TABLE_TRANSFORMS={"unknown_table": lambda r: {"unknown_table": [r]}},
    )
    with pytest.raises(ValidationError, match="Unknown table names"):
        settings_factory(transforms_module=transforms_mod)


def test_xsv_settings_pass_generates_pyarrow_schema_for_jsonl_format(
    monkeypatch: pytest.MonkeyPatch,
    settings_factory: Callable[..., XsvIngestSettings],
) -> None:
    """When loader_file_format is jsonl, XsvIngestSettings resolves pyarrow_schema for reading while schema_map is None."""
    convert_spy = MagicMock(wraps=convert_xsv_schema_to_pyarrow)
    monkeypatch.setattr(xsv_settings_module, "convert_xsv_schema_to_pyarrow", convert_spy)

    settings = settings_factory(loader_file_format="jsonl")

    assert settings.use_pyarrow is False
    assert settings.schema_map is None
    assert convert_spy.call_count == 1
    assert settings.pyarrow_schema == convert_xsv_schema_to_pyarrow(settings.validated_schema)


def test_xsv_settings_pass_lazy_loads_and_validates_custom_metaschema(
    settings_factory: Callable[..., XsvIngestSettings],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """XsvIngestSettings lazily loads and validates the custom XSV metaschema only when needed and validates pipeline schemas against it."""
    plain_draft_schema = {k: v for k, v in DEFAULT_XSV_SCHEMA.items() if k != "x-xsv-config"}
    plain_settings = settings_factory(schema=plain_draft_schema)
    assert plain_settings.validated_schema.has_xsv_metaschema is False
    assert get_xsv_meta_schema.cache_info().currsize == 0

    custom_schema = {**DEFAULT_XSV_SCHEMA, "$schema": CUSTOM_META_SCHEMA_URI}
    custom_settings = settings_factory(schema=custom_schema)
    assert custom_settings.validated_schema.has_xsv_metaschema is True
    assert get_xsv_meta_schema.cache_info().currsize == 1

    invalid_pipeline_schema = {
        **custom_schema,
        "x-xsv-config": {"x-delimiter": "::"},
    }
    with pytest.raises(RuntimeError, match="Could not load JSON Schema"):
        settings_factory(schema=invalid_pipeline_schema)

    get_xsv_meta_schema.cache_clear()
    bad_meta_root = tmp_path / "bad_metaschema_pkg"
    bad_meta_dir = bad_meta_root / "xsv_config" / "0.1.0"
    bad_meta_dir.mkdir(parents=True)
    (bad_meta_dir / "schema.json").write_text(
        json.dumps({"$schema": VALID_SCHEMA_URI, "type": "not-a-valid-type"}),
        encoding="utf-8",
    )
    monkeypatch.setattr(importlib.resources, "files", lambda _pkg: bad_meta_root)
    with pytest.raises(RuntimeError, match="Could not load JSON Schema"):
        settings_factory(schema=custom_schema)
