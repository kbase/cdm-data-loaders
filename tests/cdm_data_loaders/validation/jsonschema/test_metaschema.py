"""Unit tests for ``cdm_data_loaders.validation.jsonschema.metaschema``."""

import importlib.resources
import json
from pathlib import Path
from typing import Any, Final, Never

import httpx
import jsonschema.exceptions
import jsonschema.validators
import pytest
from frozendict import frozendict

import cdm_data_loaders.validation.jsonschema.metaschema as metaschema_module
from cdm_data_loaders.converters.sources.json_schema.permissive import JsonSchemaReader
from cdm_data_loaders.validation.jsonschema.metaschema import (
    CUSTOM_META_SCHEMA_URI,
    DRAFT_2020_12_SCHEMA_URI,
    STRICT_SUPPORTED_SCHEMA_DIALECTS,
    SUPPORTED_JSONSCHEMA_DRAFT_URIS,
    XSV_CONFIG_SCHEMA_REL_PATH,
    check_jsonschema_document,
    ensure_custom_metaschema_registered,
    get_xsv_config_schema,
    get_xsv_meta_schema,
    load_xsv_meta_schema,
    register_xsv_validator,
    resolve_jsonschema_validator,
)
from cdm_data_loaders.validation.jsonschema.models import ValidatedSchema
from tests.helpers.paths_and_constants import preserve_xsv_metaschema_state

SCHEMAS_PACKAGE_DIR: Final[Path] = Path(str(importlib.resources.files("cdm_data_loaders.schemas")))
BUNDLED_JSON_SCHEMA_PATHS: Final[tuple[Path, ...]] = tuple(sorted(SCHEMAS_PACKAGE_DIR.rglob("*.json")))


def test_module_constants_pass_no_hardcoded_metaschema() -> None:
    """Verify hardcoded metaschema dicts are absent and ``CUSTOM_META_SCHEMA_URI`` points to GitHub Pages."""
    assert not hasattr(metaschema_module, "CUSTOM_META_SCHEMA")
    assert not hasattr(metaschema_module, "X_XSV_CONFIG_SCHEMA")
    assert CUSTOM_META_SCHEMA_URI == "https://kbase.github.io/cdm-data-loaders/schemas/xsv_config/0.1.0/schema.json"


def test_load_xsv_meta_schema_pass_loads_bundled_file() -> None:
    """Load and validate the bundled ``schemas/xsv_config/0.1.0/schema.json`` file."""
    bundled_path = importlib.resources.files("cdm_data_loaders.schemas").joinpath(
        *XSV_CONFIG_SCHEMA_REL_PATH.split("/")
    )
    expected = json.loads(bundled_path.read_text(encoding="utf-8"))

    loaded = load_xsv_meta_schema()

    assert isinstance(loaded, frozendict)
    assert dict(loaded) == expected
    assert loaded["$id"] == CUSTOM_META_SCHEMA_URI


@pytest.mark.vcr
def test_load_xsv_meta_schema_pass_remote_fallback(tmp_path: Path) -> None:
    """Fall back to fetching the published URL when the local file does not exist."""
    bundled_path = importlib.resources.files("cdm_data_loaders.schemas").joinpath(
        *XSV_CONFIG_SCHEMA_REL_PATH.split("/")
    )
    expected = json.loads(bundled_path.read_text(encoding="utf-8"))

    loaded = load_xsv_meta_schema(local_schema_path=tmp_path / "missing_schema.json")

    assert isinstance(loaded, frozendict)
    assert dict(loaded) == expected


@pytest.mark.vcr
@pytest.mark.default_cassette("test_load_xsv_meta_schema_pass_remote_fallback.yaml")
@pytest.mark.parametrize(
    "exc_cls",
    [
        pytest.param(ModuleNotFoundError, id="module-not-found-on-files"),
        pytest.param(FileNotFoundError, id="file-not-found-on-files"),
    ],
)
def test_load_xsv_meta_schema_pass_remote_fallback_when_package_files_unavailable(
    monkeypatch: pytest.MonkeyPatch,
    exc_cls: type[Exception],
) -> None:
    """Fall back to the published URL if ``importlib.resources.files`` raises ``FileNotFoundError`` or ``ModuleNotFoundError``."""

    def raise_missing(_pkg: str) -> Never:
        msg = "package resources unavailable"
        raise exc_cls(msg)

    monkeypatch.setattr(importlib.resources, "files", raise_missing)
    loaded = load_xsv_meta_schema()
    assert loaded["$id"] == CUSTOM_META_SCHEMA_URI


@pytest.mark.vcr
@pytest.mark.default_cassette("test_load_xsv_meta_schema_pass_remote_fallback.yaml")
def test_load_xsv_meta_schema_pass_remote_fallback_when_is_file_raises_file_not_found() -> None:
    """Fall back to the published URL if ``Traversable.is_file()`` raises ``FileNotFoundError``."""

    class MissingTraversable:
        def is_file(self) -> bool:
            msg = "missing parent dir"
            raise FileNotFoundError(msg)

    loaded = load_xsv_meta_schema(local_schema_path=MissingTraversable())  # type: ignore[arg-type]
    assert loaded["$id"] == CUSTOM_META_SCHEMA_URI


def test_load_xsv_meta_schema_fail_malformed_local_json(tmp_path: Path) -> None:
    """Raise ``JSONDecodeError`` when an existing local schema file contains invalid JSON."""
    bad_file = tmp_path / "schema.json"
    bad_file.write_text("{not valid json", encoding="utf-8")

    with pytest.raises(json.JSONDecodeError):
        load_xsv_meta_schema(local_schema_path=bad_file)


@pytest.mark.vcr
def test_load_xsv_meta_schema_fail_remote_http_error(tmp_path: Path) -> None:
    """Raise ``HTTPStatusError`` when the fallback remote URL responds with an HTTP error."""
    with pytest.raises(httpx.HTTPStatusError):
        load_xsv_meta_schema(local_schema_path=tmp_path / "missing_schema.json")


@pytest.mark.vcr
def test_load_xsv_meta_schema_fail_remote_invalid_metaschema(tmp_path: Path) -> None:
    """Validate a remotely fetched metaschema against its own metaschema and raise ``SchemaError`` on failure."""
    with pytest.raises(jsonschema.exceptions.SchemaError, match="not-a-valid-type"):
        load_xsv_meta_schema(local_schema_path=tmp_path / "missing_schema.json")


@pytest.mark.parametrize(
    ("metaschema_payload", "error_cls", "match"),
    [
        pytest.param(["not", "an", "object"], TypeError, "Custom metaschema must be a JSON object", id="json-array"),
        pytest.param("string-payload", TypeError, "Custom metaschema must be a JSON object", id="json-string"),
        pytest.param(42, TypeError, "Custom metaschema must be a JSON object", id="json-int"),
        pytest.param(None, TypeError, "Custom metaschema must be a JSON object", id="json-null"),
        pytest.param(
            {"type": "object"},
            ValueError,
            r"JSON Schema is missing the \$schema keyword",
            id="missing-schema-keyword",
        ),
        pytest.param(
            {"$schema": "", "type": "object"},
            ValueError,
            r"JSON Schema is missing the \$schema keyword",
            id="empty-schema-keyword",
        ),
        pytest.param(
            {"$schema": 123, "type": "object"},
            ValueError,
            r"JSON Schema is missing the \$schema keyword",
            id="non-string-schema-keyword",
        ),
        pytest.param(
            {"$schema": CUSTOM_META_SCHEMA_URI, "type": "object"},
            ValueError,
            r"Custom metaschema cannot declare itself as its own \$schema",
            id="self-referential-schema-uri",
        ),
        pytest.param(
            {"$schema": "https://example.org/unknown-draft/schema", "type": "object"},
            ValueError,
            "Unsupported JSON Schema draft",
            id="unsupported-draft-uri",
        ),
        pytest.param(
            {"$schema": "https://json-schema.org/draft/2020-12/schema", "type": "not-a-valid-type"},
            jsonschema.exceptions.SchemaError,
            "not-a-valid-type",
            id="invalid-against-draft-2020-12-metaschema",
        ),
    ],
)
def test_load_xsv_meta_schema_fail_invalid_metaschema_documents(
    tmp_path: Path,
    metaschema_payload: object,
    error_cls: type[Exception],
    match: str,
) -> None:
    """Reject non-object, missing/self/unsupported ``$schema``, and invalid metaschema documents."""
    schema_path = tmp_path / "bad_metaschema.json"
    schema_path.write_text(json.dumps(metaschema_payload), encoding="utf-8")

    with pytest.raises(error_cls, match=match):
        load_xsv_meta_schema(local_schema_path=schema_path)


def test_get_xsv_meta_schema_and_config_schema_pass_cached() -> None:
    """Cache the validated metaschema and extract ``x-xsv-config``."""
    expected_hits = 2
    assert get_xsv_meta_schema.cache_info().currsize == 0

    meta_first = get_xsv_meta_schema()
    meta_second = get_xsv_meta_schema()
    config_schema = get_xsv_config_schema()

    assert meta_first is meta_second
    assert get_xsv_meta_schema.cache_info().hits == expected_hits
    assert get_xsv_meta_schema.cache_info().misses == 1
    assert isinstance(config_schema, frozendict)
    assert dict(config_schema) == meta_first["properties"]["x-xsv-config"]


@pytest.mark.parametrize(
    ("meta_doc", "error_cls", "match"),
    [
        pytest.param(
            {"$schema": "https://json-schema.org/draft/2020-12/schema", "type": "object"},
            ValueError,
            "missing 'x-xsv-config'",
            id="missing-properties-key",
        ),
        pytest.param(
            {
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": "object",
                "properties": {"x-xsv-config": True},
            },
            TypeError,
            "must be a mapping",
            id="boolean-xsv-config-schema",
        ),
    ],
)
def test_get_xsv_config_schema_fail_missing_or_non_mapping_xsv_config_property(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    meta_doc: dict[str, Any],
    error_cls: type[Exception],
    match: str,
) -> None:
    """Raise ``ValueError`` or ``TypeError`` when ``properties['x-xsv-config']`` is absent or non-mapping."""
    incomplete_dir = tmp_path / "xsv_config" / "0.1.0"
    incomplete_dir.mkdir(parents=True)
    (incomplete_dir / "schema.json").write_text(json.dumps(meta_doc), encoding="utf-8")
    monkeypatch.setattr(importlib.resources, "files", lambda _pkg: tmp_path)

    with pytest.raises(error_cls, match=match):
        get_xsv_config_schema()


def test_validated_schema_pass_lazy_loads_custom_metaschema_only_when_referenced() -> None:
    """Leave the custom metaschema unloaded for standard drafts and register it for ``CUSTOM_META_SCHEMA_URI``."""
    standard_schema = {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "properties": {"id": {"type": "string"}},
    }
    validated_standard = ValidatedSchema(jsonschema=standard_schema)
    assert validated_standard.jsonschema == standard_schema
    assert get_xsv_meta_schema.cache_info().currsize == 0
    assert jsonschema.validators.validator_for({"$schema": CUSTOM_META_SCHEMA_URI}, default=None) is None  # type: ignore[arg-type]

    custom_schema = {
        "$schema": CUSTOM_META_SCHEMA_URI,
        "type": "object",
        "required": ["id"],
        "properties": {"id": {"type": "string"}},
        "x-xsv-config": {"x-delimiter": "\t", "x-has-header": True},
    }
    validated_custom = ValidatedSchema(jsonschema=custom_schema)
    assert validated_custom.jsonschema == custom_schema
    assert get_xsv_meta_schema.cache_info().currsize == 1
    assert jsonschema.validators.validator_for({"$schema": CUSTOM_META_SCHEMA_URI}, default=None) is not None  # type: ignore[arg-type]

    invalid_custom_schema = {
        "$schema": CUSTOM_META_SCHEMA_URI,
        "type": "object",
        "x-xsv-config": {"x-delimiter": "::"},
    }
    with pytest.raises(jsonschema.exceptions.SchemaError, match="is too long"):
        ValidatedSchema(jsonschema=invalid_custom_schema)


@pytest.mark.vcr
@pytest.mark.default_cassette("test_load_xsv_meta_schema_pass_remote_fallback.yaml")
def test_validated_schema_pass_lazy_remote_fallback_when_local_schema_absent(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Fetch and validate the custom metaschema from GitHub Pages when absent locally."""
    monkeypatch.setattr(importlib.resources, "files", lambda _pkg: tmp_path)

    pipeline_schema = {
        "$schema": CUSTOM_META_SCHEMA_URI,
        "type": "object",
        "required": ["gene_id"],
        "properties": {"gene_id": {"type": "string"}},
        "x-xsv-config": {"x-delimiter": ","},
    }
    validated = ValidatedSchema(jsonschema=pipeline_schema)
    assert validated.jsonschema == pipeline_schema
    assert get_xsv_meta_schema.cache_info().currsize == 1


def test_json_schema_reader_pass_lazy_registers_xsv_validator() -> None:
    """Register the custom XSV validator on demand when reading a ``CUSTOM_META_SCHEMA_URI`` document."""
    schema = {
        "$schema": CUSTOM_META_SCHEMA_URI,
        "type": "object",
        "properties": {"id": {"type": "string"}},
        "x-xsv-config": {"x-delimiter": ","},
    }
    doc = JsonSchemaReader().read(schema)
    assert doc.dialect == CUSTOM_META_SCHEMA_URI
    assert doc.jsonschema_validator_cls is not None
    assert get_xsv_meta_schema() == doc.jsonschema_validator_cls.META_SCHEMA
    register_xsv_validator()


@pytest.mark.parametrize(
    ("fake_metaschema", "expected_error", "error_match"),
    [
        pytest.param(
            {
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": "object",
                "properties": {"x-xsv-config": True},
            },
            TypeError,
            "must be a mapping",
            id="valid-draft-boolean-xsv-config-schema",
        ),
        pytest.param(
            {
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": "not-a-valid-type",
            },
            jsonschema.exceptions.SchemaError,
            "not-a-valid-type",
            id="invalid-draft-metaschema",
        ),
    ],
)
def test_preserve_xsv_metaschema_state_pass_restores_bundled_schema_while_monkeypatch_active(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    fake_metaschema: dict[str, Any],
    expected_error: type[Exception],
    error_match: str,
) -> None:
    """Restore a prior metaschema cache and validator even before ``monkeypatch`` tears down."""
    fake_dir = tmp_path / "xsv_config" / "0.1.0"
    fake_dir.mkdir(parents=True)
    (fake_dir / "schema.json").write_text(json.dumps(fake_metaschema), encoding="utf-8")
    register_xsv_validator()

    with preserve_xsv_metaschema_state():
        monkeypatch.setattr(importlib.resources, "files", lambda _pkg: tmp_path)
        with pytest.raises(expected_error, match=error_match):
            get_xsv_config_schema()

    meta_schema = get_xsv_meta_schema()
    config_schema = get_xsv_config_schema()
    validator_cls = jsonschema.validators.validator_for({"$schema": CUSTOM_META_SCHEMA_URI})

    assert meta_schema["$id"] == CUSTOM_META_SCHEMA_URI
    assert config_schema["type"] == "object"
    assert meta_schema == validator_cls.META_SCHEMA
    with pytest.raises(jsonschema.exceptions.SchemaError, match="is too long"):
        validator_cls.check_schema(
            {
                "$schema": CUSTOM_META_SCHEMA_URI,
                "required": ["a"],
                "x-xsv-config": {"x-delimiter": ",;"},
            }
        )


def test_preserve_xsv_metaschema_state_pass_leaves_unregistered_when_initially_clean() -> None:
    """Remove cache and validator registrations added inside the block when clean on entry."""
    assert get_xsv_meta_schema.cache_info().currsize == 0
    assert jsonschema.validators.validator_for({"$schema": CUSTOM_META_SCHEMA_URI}, default=None) is None  # type: ignore[arg-type]

    with preserve_xsv_metaschema_state():
        register_xsv_validator()
        assert get_xsv_meta_schema.cache_info().currsize == 1
        assert jsonschema.validators.validator_for({"$schema": CUSTOM_META_SCHEMA_URI}, default=None) is not None  # type: ignore[arg-type]

    assert get_xsv_meta_schema.cache_info().currsize == 0
    assert jsonschema.validators.validator_for({"$schema": CUSTOM_META_SCHEMA_URI}, default=None) is None  # type: ignore[arg-type]


def test_validated_schema_pass_bundled_schemas_discovered() -> None:
    """Discover bundled JSON Schemas under ``cdm_data_loaders.schemas`` including ``xsv_config``."""
    assert BUNDLED_JSON_SCHEMA_PATHS
    assert SCHEMAS_PACKAGE_DIR / XSV_CONFIG_SCHEMA_REL_PATH in BUNDLED_JSON_SCHEMA_PATHS


@pytest.mark.parametrize(
    "schema_path",
    [pytest.param(path, id=path.relative_to(SCHEMAS_PACKAGE_DIR).as_posix()) for path in BUNDLED_JSON_SCHEMA_PATHS],
)
def test_validated_schema_pass_bundled_schemas(schema_path: Path) -> None:
    """Validate each bundled JSON Schema under ``cdm_data_loaders.schemas`` against its declared metaschema."""
    schema = json.loads(schema_path.read_text(encoding="utf-8"))
    validated = ValidatedSchema(jsonschema=schema)
    assert validated.jsonschema == schema


@pytest.mark.parametrize(
    "draft_uri",
    [pytest.param(uri, id=uri) for uri in SUPPORTED_JSONSCHEMA_DRAFT_URIS],
)
def test_resolve_jsonschema_validator_and_check_document_pass_supported_drafts(draft_uri: str) -> None:
    """Resolve validators and check valid documents across all supported JSON Schema draft URIs."""
    assert frozenset({DRAFT_2020_12_SCHEMA_URI, CUSTOM_META_SCHEMA_URI}) == STRICT_SUPPORTED_SCHEMA_DIALECTS
    doc: dict[str, Any] = {"$schema": draft_uri, "type": "object"}
    if draft_uri == CUSTOM_META_SCHEMA_URI:
        doc["required"] = ["id"]
        doc["properties"] = {"id": {"type": "string"}}
    validator_cls = resolve_jsonschema_validator(doc)
    assert validator_cls is not None
    check_jsonschema_document(doc)


def test_ensure_custom_metaschema_registered_and_allow_custom_xsv_flag() -> None:
    """Register the custom XSV validator on demand and reject self-referential custom URIs when disallowed."""
    ensure_custom_metaschema_registered({"$schema": DRAFT_2020_12_SCHEMA_URI})
    assert get_xsv_meta_schema.cache_info().currsize == 0

    with pytest.raises(ValueError, match=r"Custom metaschema cannot declare itself"):
        resolve_jsonschema_validator({"$schema": CUSTOM_META_SCHEMA_URI}, allow_custom_xsv=False)

    ensure_custom_metaschema_registered({"$schema": CUSTOM_META_SCHEMA_URI})
    assert get_xsv_meta_schema.cache_info().currsize == 1


@pytest.mark.parametrize(
    ("bad_schema", "exc_cls", "match"),
    [
        pytest.param({"type": "object"}, ValueError, r"missing the \$schema keyword", id="missing-schema"),
        pytest.param({"$schema": 42}, ValueError, r"missing the \$schema keyword", id="non-string-schema"),
        pytest.param({"$schema": ""}, ValueError, "Unsupported JSON Schema draft", id="empty-schema-uri"),
        pytest.param(
            {"$schema": "https://example.org/unknown"},
            ValueError,
            "Unsupported JSON Schema draft",
            id="unknown-schema-uri",
        ),
        pytest.param(
            {"$schema": DRAFT_2020_12_SCHEMA_URI, "type": "not-a-type"},
            jsonschema.exceptions.SchemaError,
            "not-a-type",
            id="invalid-metaschema-type",
        ),
    ],
)
def test_check_jsonschema_document_fail_invalid_schemas(
    bad_schema: dict[str, Any],
    exc_cls: type[Exception],
    match: str,
) -> None:
    """Reject missing, non-string, unsupported, or metaschema-invalid JSON Schema documents."""
    with pytest.raises(exc_cls, match=match):
        check_jsonschema_document(bad_schema)
