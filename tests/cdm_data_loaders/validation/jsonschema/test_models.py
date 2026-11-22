"""Unit tests for ``cdm_data_loaders.validation.jsonschema.models``."""

import json
import logging
import re
from collections.abc import Callable
from functools import partial
from pathlib import Path
from typing import Any, Final

import jsonschema
import jsonschema.exceptions
import jsonschema.validators
import pytest
from pydantic import ValidationError

from cdm_data_loaders.readers.xsv.models import SEP_TO_EXT
from cdm_data_loaders.validation.jsonschema.metaschema import (
    CUSTOM_META_SCHEMA_URI,
    DRAFT_2020_12_SCHEMA_URI,
    SUPPORTED_JSONSCHEMA_DRAFT_URIS,
    get_xsv_meta_schema,
    register_xsv_validator,
)
from cdm_data_loaders.validation.jsonschema.models import (
    ValidatedSchema,
    ValidatedXsvSchema,
    generate_first_pass_schema,
    generate_header,
    get_schema_parsing_metadata,
    validate_xsv_jsonschema,
)
from tests.helpers.fixtures_xsv import COLUMNS

SCHEMAS_DIR: Final[Path] = Path("tests") / "data" / "qsv" / "schemas"
DELIMITERS: Final[list[str]] = list(SEP_TO_EXT)
VALID_SCHEMA_URI: Final[str] = "https://json-schema.org/draft/2019-09/schema"
SCHEMA_KEY_VALUE: dict[str, str] = {"$schema": VALID_SCHEMA_URI}


@pytest.fixture(scope="session")
def first_pass_schema_path() -> Path:
    """Path to the first-pass JSON Schema fixture."""
    return SCHEMAS_DIR / "first_pass_schema.json"


@pytest.fixture(scope="session")
def post_norm_schema_path() -> Path:
    """Path to the post-normalisation JSON Schema fixture."""
    return SCHEMAS_DIR / "post_norm_schema.json"


@pytest.fixture(scope="session")
def derived_first_pass_schema_path() -> Path:
    """Path to the derived first-pass JSON Schema fixture."""
    return SCHEMAS_DIR / "derived_first_pass_schema.json"


@pytest.fixture(scope="session")
def derived_first_pass_schema(derived_first_pass_schema_path: Path) -> dict[str, Any]:
    """Parsed content of ``derived_first_pass_schema_path``."""
    return json.loads(derived_first_pass_schema_path.read_bytes())


@pytest.fixture
def make_schema_file(tmp_path: Path) -> Callable[..., Path]:
    """Write a JSON-serializable schema payload to a temporary file."""

    def _make(schema: object, file_name: str = "schema.json") -> Path:
        path = tmp_path / file_name
        path.write_text(json.dumps(schema), encoding="utf-8")
        return path

    return _make


INVALID_TOP_LEVEL_SCHEMA_LIST: Final[tuple[Any, ...]] = (
    pytest.param(["a", "b"], id="schema-is-a-list"),
    pytest.param([], id="schema-is-an-empty-list"),
    pytest.param("just a string", id="schema-is-string"),
    pytest.param("", id="schema-is-empty-string"),
    pytest.param(42, id="schema-is-a-number"),
    pytest.param(0, id="schema-is-zero"),
    pytest.param(-1, id="schema-is-negative"),
    pytest.param(None, id="schema-is-null"),
    pytest.param(True, id="schema-is-bool"),
)
MISSING_EMPTY_REQUIRED_LIST = [
    pytest.param({}, id="missing-required-key"),
    pytest.param({"required": []}, id="empty-required-list"),
]
INVALID_REQUIRED_LIST = [
    pytest.param({"required": "not-a-list"}, id="required-is-a-string"),
    pytest.param({"required": None}, id="required-is-null"),
    pytest.param({"required": {"a": 1}}, id="required-is-a-dict"),
]


@pytest.fixture
def valid_schema() -> ValidatedXsvSchema:
    """Minimal ValidatedXsvSchema object."""
    return ValidatedXsvSchema(
        jsonschema={
            "$schema": VALID_SCHEMA_URI,
            "required": COLUMNS,
            "properties": {col: {"type": "string"} for col in COLUMNS},
        },
    )


"""validate_jsonschema"""


def test_validate_jsonschema_pass_valid_schema(make_schema_file: Callable[..., Path]) -> None:
    """A well-formed schema, complete with $schema keyword, is returned unchanged."""
    schema = {
        "$schema": VALID_SCHEMA_URI,
        "type": "object",
        "required": ["a"],
        "properties": {"a": {"type": "string"}},
    }
    schema_path = make_schema_file(schema)

    result = validate_xsv_jsonschema(schema_path)
    assert isinstance(result, ValidatedXsvSchema)
    assert {**result.jsonschema} == schema


def test_validate_jsonschema_fail_missing_file(tmp_path: Path) -> None:
    """A schema_path that doesn't exist on disk raises FileNotFoundError when read."""
    with pytest.raises(FileNotFoundError, match="No such file or directory"):
        validate_xsv_jsonschema(tmp_path / "does-not-exist.json")


def test_validate_jsonschema_fail_invalid_json_content(tmp_path: Path) -> None:
    """A file that isn't valid JSON raises a JSONDecodeError."""
    bad_json_path = tmp_path / "not-json.json"
    bad_json_path.write_text("{this is not: valid json,,,")

    with pytest.raises(json.JSONDecodeError):
        validate_xsv_jsonschema(bad_json_path)


@pytest.mark.parametrize("schema", INVALID_TOP_LEVEL_SCHEMA_LIST)
def test_validate_jsonschema_fail_non_dict_schema_or_invalid_schema_raises_error(
    make_schema_file: Callable[..., Path], schema: list | str | int | None
) -> None:
    """A schema whose top-level JSON value isn't a dict raises TypeError."""
    schema_path = make_schema_file(schema)

    with pytest.raises(ValidationError, match="Input should be a valid dictionary"):
        validate_xsv_jsonschema(schema_path)


@pytest.mark.parametrize("schema", INVALID_REQUIRED_LIST + MISSING_EMPTY_REQUIRED_LIST)
def test_validate_jsonschema_fail_no_schema_keyword_missing_or_empty_required_raises_value_error(
    make_schema_file: Callable[..., Path], schema: dict[str, Any]
) -> None:
    """A schema with no `$schema` keyword raises a ValueError."""
    schema_path = make_schema_file(schema)

    with pytest.raises(ValueError, match=r"JSON Schema is missing the \$schema keyword"):
        validate_xsv_jsonschema(schema_path)


@pytest.mark.parametrize("schema", INVALID_REQUIRED_LIST)
def test_validate_jsonschema_fail_invalid_required_format_error(
    make_schema_file: Callable[..., Path], schema: dict[str, Any]
) -> None:
    """A schema whose required field isn't a list of strings raises SchemaError."""
    schema_path = make_schema_file({**SCHEMA_KEY_VALUE, **schema})

    with pytest.raises(jsonschema.exceptions.SchemaError, match="is not of type 'array'"):
        validate_xsv_jsonschema(schema_path)


@pytest.mark.parametrize("schema", MISSING_EMPTY_REQUIRED_LIST)
def test_validate_jsonschema_pass_missing_empty_required_list_is_fine_fine_fine(
    make_schema_file: Callable[..., Path], schema: dict[str, Any]
) -> None:
    """A schema whose required field is missing or empty throws a validation error."""
    schema_path = make_schema_file({**SCHEMA_KEY_VALUE, **schema})
    with pytest.raises(ValidationError, match="Could not find any required cols in schema"):
        validate_xsv_jsonschema(schema_path)


@pytest.mark.parametrize(
    "schema_uri",
    [
        pytest.param("https://not-a-real-schema.uri", id="unrecognised-uri"),
        pytest.param("not-a-real-schema-uri", id="invalid-uri"),
    ],
)
def test_validate_jsonschema_fail_unsupported_schema_uri(
    make_schema_file: Callable[..., Path],
    schema_uri: str,
) -> None:
    """A `$schema` value that does not correspond to a supported JSON Schema draft raises ValidationError."""
    schema = {"$schema": schema_uri, "required": ["a"]}
    schema_path = make_schema_file(schema)
    with pytest.raises(ValidationError, match=f"Unsupported JSON Schema draft: {re.escape(schema_uri)}"):
        validate_xsv_jsonschema(schema_path)


def test_validate_jsonschema_fail_invalid_schema_content_logs_and_raises_schema_error(
    make_schema_file: Callable[..., Path], caplog: pytest.LogCaptureFixture
) -> None:
    """A schema with other invalid fields raises a SchemaError."""
    schema = {"$schema": VALID_SCHEMA_URI, "type": "not-a-real-type", "required": ["a"]}
    schema_path = make_schema_file(schema)

    with pytest.raises(
        jsonschema.exceptions.SchemaError, match="'not-a-real-type' is not valid under any of the given schemas"
    ):
        validate_xsv_jsonschema(schema_path)

    assert len(caplog.records) == 1
    assert caplog.records[0].getMessage().startswith("Error validating JSON Schema")


@pytest.mark.parametrize(
    "fixture_name", ["first_pass_schema_path", "post_norm_schema_path", "derived_first_pass_schema_path"]
)
def test_validate_jsonschema_fixtures_are_valid_schemas(fixture_name: str, request: pytest.FixtureRequest) -> None:
    """Ensure that all the JSON Schemas used as fixtures are valid."""
    schema = request.getfixturevalue(fixture_name)
    validate_xsv_jsonschema(schema)


"""get_schema_parsing_metadata"""


def test_get_schema_parsing_metadata_pass_transforms_keys() -> None:
    """Keys prefixed with "x-" are stripped of the prefix and hyphens become underscores."""
    vs = ValidatedXsvSchema(
        jsonschema={
            "$schema": VALID_SCHEMA_URI,
            "required": ["a"],
            "properties": {"a": {"type": "string"}},
            "x-xsv-config": {
                "x-has-header": True,
                "x-delimiter": ",",
                "x-comment-char": "#",
                "x-quote": '"',
                "x-escape": "\\",
                "x-quoting-policy": "necessary",
                "x-null-regex": "^(NA|NULL)$",
                "x-null-cols": ["a", "b"],
            },
        },
    )

    result = get_schema_parsing_metadata(vs)

    assert result == {
        "has_header": True,
        "delimiter": ",",
        "comment_char": "#",
        "quote": '"',
        "escape": "\\",
        "quoting_policy": "necessary",
        "null_regex": "^(NA|NULL)$",
        "null_cols": ["a", "b"],
    }


def test_get_schema_parsing_metadata_fail_no_xsv_config_key() -> None:
    """A schema with no x-xsv-config key raises ValueError."""
    vs = ValidatedXsvSchema(
        jsonschema={
            "$schema": VALID_SCHEMA_URI,
            "required": ["a"],
            "properties": {"a": {"type": "string"}},
        },
    )

    with pytest.raises(ValueError, match="No xsv config information found in schema"):
        get_schema_parsing_metadata(vs)


def test_get_schema_parsing_metadata_fail_empty_xsv_config() -> None:
    """A schema with an empty x-xsv-config object raises ValueError."""
    vs = ValidatedXsvSchema(
        jsonschema={
            "$schema": VALID_SCHEMA_URI,
            "required": ["a"],
            "properties": {"a": {"type": "string"}},
            "x-xsv-config": {},
        },
    )

    with pytest.raises(ValueError, match="No xsv config information found in schema"):
        get_schema_parsing_metadata(vs)


"""register_xsv_validator"""


def test_register_xsv_validator_pass_registers_validator_for_custom_uri() -> None:
    """After registration, `validator_for` returns a validator class using the custom meta-schema."""
    register_xsv_validator()

    validator_cls = jsonschema.validators.validator_for({"$schema": CUSTOM_META_SCHEMA_URI})

    assert get_xsv_meta_schema() == validator_cls.META_SCHEMA


def test_register_xsv_validator_pass_idempotent() -> None:
    """Calling register_xsv_validator multiple times does not raise and stays consistent."""
    register_xsv_validator()
    register_xsv_validator()

    validator_cls = jsonschema.validators.validator_for({"$schema": CUSTOM_META_SCHEMA_URI})
    assert get_xsv_meta_schema() == validator_cls.META_SCHEMA


@pytest.mark.parametrize(
    "x_xsv_config",
    [
        pytest.param(
            {
                "x-has-header": True,
                "x-delimiter": ",",
                "x-quote": '"',
                "x-escape": "\\",
                "x-quoting-policy": "necessary",
            },
            id="full-parser-config",
        ),
        pytest.param({"x-null-cols": ["a", "b"]}, id="null-cols-multi"),
        pytest.param({"x-null-cols": ["a"]}, id="null-cols-single"),
    ],
)
def test_custom_xsv_metaschema_pass_accepts_valid_config(
    make_schema_file: Callable[..., Path],
    x_xsv_config: dict[str, Any],
) -> None:
    """A schema with a well-formed x-xsv-config block passes both check_schema and validate_jsonschema."""
    register_xsv_validator()
    validator_cls = jsonschema.validators.validator_for({"$schema": CUSTOM_META_SCHEMA_URI})
    schema = {
        "$schema": CUSTOM_META_SCHEMA_URI,
        "required": ["a", "b"],
        "properties": {"a": {"type": "string"}, "b": {"type": "string"}},
        "x-xsv-config": x_xsv_config,
    }

    validator_cls.check_schema(schema)
    result = validate_xsv_jsonschema(make_schema_file(schema))

    assert isinstance(result, ValidatedXsvSchema)
    assert result.has_xsv_metaschema
    assert result.has_xsv_parser_config
    assert result.jsonschema == schema


@pytest.mark.parametrize(
    "via_file",
    [
        pytest.param(False, id="validator-check-schema"),
        pytest.param(True, id="validate-xsv-jsonschema"),
    ],
)
@pytest.mark.parametrize(
    ("x_xsv_config", "error_match"),
    [
        pytest.param(
            {"x-not-a-real-option": True},
            "Additional properties are not allowed",
            id="additional-property-not-allowed",
        ),
        pytest.param({"x-delimiter": ",;"}, "is too long", id="field-exceeds-max-length"),
        pytest.param({"x-null-cols": "col_a"}, "is not of type 'array'", id="null-cols-wrong-type"),
        pytest.param({"x-null-cols": ["col_a", 42]}, "is not of type 'string'", id="null-cols-non-string-items"),
        pytest.param({"x-null-cols": []}, "should be non-empty", id="null-cols-empty-array"),
        pytest.param({"x-has-header": "yes"}, "is not of type 'boolean'", id="has-header-wrong-type"),
        pytest.param({"x-quoting-policy": "sometimes"}, "is not one of", id="quoting-policy-bad-enum"),
    ],
)
def test_custom_xsv_metaschema_fail_rejects_invalid_config(
    make_schema_file: Callable[..., Path],
    x_xsv_config: dict[str, Any],
    error_match: str,
    via_file: bool,
) -> None:
    """A schema whose x-xsv-config block violates the custom metaschema raises SchemaError."""
    register_xsv_validator()
    schema = {
        "$schema": CUSTOM_META_SCHEMA_URI,
        "required": ["a"],
        "x-xsv-config": x_xsv_config,
    }

    if via_file:
        schema_path = make_schema_file(schema)
        check_fn: Callable[[], object] = partial(validate_xsv_jsonschema, schema_path)
    else:
        validator_cls = jsonschema.validators.validator_for({"$schema": CUSTOM_META_SCHEMA_URI})
        check_fn = partial(validator_cls.check_schema, schema)

    with pytest.raises(jsonschema.exceptions.SchemaError, match=error_match):
        check_fn()


"""generate_header"""


def test_generate_header_pass_uses_defaults(tmp_path: Path, valid_schema: ValidatedXsvSchema) -> None:
    """With no overrides, the header is written to `header.txt` using a tab delimiter."""
    result = generate_header(valid_schema, tmp_path)
    assert result == tmp_path / "header.txt"
    assert result.read_text() == "\t".join(COLUMNS) + "\n"


@pytest.mark.parametrize("delimiter", DELIMITERS)
def test_generate_header_pass_with_various_delimiters(
    tmp_path: Path, delimiter: str, valid_schema: ValidatedXsvSchema
) -> None:
    """Every delimiter supported by SEP_TO_EXT produces a correctly-joined header row."""
    result = generate_header(valid_schema, tmp_path, delimiter=delimiter)
    assert result.read_text() == delimiter.join(COLUMNS) + "\n"


def test_generate_header_pass_with_custom_file_name(tmp_path: Path, valid_schema: ValidatedXsvSchema) -> None:
    """A custom header_file_name is respected in both the returned path and the file written."""
    result = generate_header(valid_schema, tmp_path, header_file_name="custom-header.tsv")
    assert result == tmp_path / "custom-header.tsv"
    assert result.read_text() == "\t".join(COLUMNS) + "\n"


def test_generate_header_pass_overwrites_existing_file(tmp_path: Path, valid_schema: ValidatedXsvSchema) -> None:
    """An existing header file at the target path is overwritten."""
    (tmp_path / "header.txt").write_text("stale content\n")
    result = generate_header(valid_schema, tmp_path)
    assert result.read_text() == "\t".join(COLUMNS) + "\n"


def test_generate_header_fail_missing_target_dir(tmp_path: Path, valid_schema: ValidatedXsvSchema) -> None:
    """A target_dir that doesn't exist fails DirectoryPath validation."""
    missing_dir = tmp_path / "no-such-dir"

    with pytest.raises(ValidationError, match="Path does not point to a directory"):
        generate_header(valid_schema, missing_dir)


def test_generate_header_fail_empty_header_file_name(tmp_path: Path, valid_schema: ValidatedXsvSchema) -> None:
    """An empty header_file_name fails the NonEmptyStr constraint."""
    with pytest.raises(ValidationError, match="String should have at least 1 character"):
        generate_header(valid_schema, tmp_path, header_file_name="")


@pytest.mark.parametrize(
    "delimiter",
    [
        pytest.param("", id="empty-delimiter"),
        pytest.param(",;", id="multi-character-delimiter"),
    ],
)
def test_generate_header_fail_invalid_delimiter_length(
    tmp_path: Path, valid_schema: ValidatedXsvSchema, delimiter: str
) -> None:
    """A delimiter that isn't exactly one character fails the CharStr constraint."""
    err_msg = re.compile("String should have at (mo|lea)st 1 character")
    with pytest.raises(ValidationError, match=err_msg):
        generate_header(valid_schema, tmp_path, delimiter=delimiter)


"""generate_first_pass_schema"""


def test_generate_first_pass_schema_pass_happy_path(
    post_norm_schema_path: Path, derived_first_pass_schema: dict[str, Any]
) -> None:
    """A first pass schema can be generated from an existing schema file."""
    vs = validate_xsv_jsonschema(post_norm_schema_path)
    validated_schema = generate_first_pass_schema(vs)
    assert validated_schema.jsonschema == derived_first_pass_schema


def test_generate_first_pass_schema_pass_bare_minimum(make_schema_file: Callable[[Any, str], Path]) -> None:
    """Test that a schema can be generated from the very barest of bare minimum JSON schema."""
    bare_minimum = {
        "required": ["this", "that"],
        "properties": {"this": {}, "that": {}},
        **SCHEMA_KEY_VALUE,
    }
    type_dict = {"type": ["string", "null"]}
    schema_path = make_schema_file(bare_minimum, "schema.json")
    vs = validate_xsv_jsonschema(schema_path)

    validated_schema = generate_first_pass_schema(vs)
    assert validated_schema.jsonschema == {
        **SCHEMA_KEY_VALUE,
        "required": ["this", "that"],
        "type": "object",
        "properties": {"this": type_dict, "that": type_dict},
    }


def test_generate_first_pass_schema_pass_custom_xsv_metaschema_uri(
    make_schema_file: Callable[[Any, str], Path],
) -> None:
    """A schema using the custom XSV metaschema URI preserves `$schema` in the first-pass schema."""
    schema = {
        "$schema": CUSTOM_META_SCHEMA_URI,
        "required": ["a"],
        "properties": {"a": {"type": "string"}},
        "x-xsv-config": {"x-delimiter": ","},
    }
    schema_path = make_schema_file(schema, "schema.json")
    vs = validate_xsv_jsonschema(schema_path)

    validated_schema = generate_first_pass_schema(vs)
    assert validated_schema.jsonschema == {
        "$schema": CUSTOM_META_SCHEMA_URI,
        "required": ["a"],
        "type": "object",
        "properties": {
            "a": {"type": ["string", "null"]},
        },
    }


def test_generate_first_pass_schema_pass_retains_only_allowed_top_level_keys() -> None:
    """Only $schema, $id, title, and required are copied from the original schema."""
    schema = {
        "$schema": VALID_SCHEMA_URI,
        "$id": "https://example.com/schemas/my-schema.json",
        "title": "My Schema",
        "description": "This should not appear in the first-pass schema",
        "type": "object",
        "additionalProperties": False,
        "required": ["a", "b"],
        "properties": {
            "a": {"type": "string"},
            "b": {"type": "integer"},
        },
    }
    vs = ValidatedXsvSchema(jsonschema=schema)

    validated_schema = generate_first_pass_schema(vs)
    assert validated_schema.jsonschema == {
        "$schema": VALID_SCHEMA_URI,
        "$id": "https://example.com/schemas/my-schema.json",
        "title": "My Schema",
        "type": "object",
        "required": ["a", "b"],
        "properties": {
            "a": {"type": ["string", "null"]},
            "b": {"type": ["string", "null"]},
        },
    }


@pytest.mark.parametrize(
    "draft_uri",
    [pytest.param(uri, id=uri) for uri in SUPPORTED_JSONSCHEMA_DRAFT_URIS],
)
def test_validated_schema_pass_supported_drafts(draft_uri: str) -> None:
    """Accept minimal schemas across every supported JSON Schema draft and compute ``has_xsv_metaschema``."""
    schema: dict[str, Any] = {"$schema": draft_uri}
    expected_is_xsv = draft_uri == CUSTOM_META_SCHEMA_URI

    result = ValidatedSchema(jsonschema=schema)

    assert result.jsonschema == schema
    assert result.has_xsv_metaschema is expected_is_xsv
    assert result.model_dump() == {
        "jsonschema": schema,
        "has_xsv_metaschema": expected_is_xsv,
    }


@pytest.mark.parametrize(
    ("schema", "expected_is_xsv"),
    [
        pytest.param(
            {"$schema": DRAFT_2020_12_SCHEMA_URI, "type": "object"},
            False,
            id="missing-required-and-properties",
        ),
        pytest.param(
            {"$schema": DRAFT_2020_12_SCHEMA_URI, "required": [], "properties": {}},
            False,
            id="empty-required-and-empty-properties",
        ),
        pytest.param(
            {"$schema": DRAFT_2020_12_SCHEMA_URI, "required": ["a"]},
            False,
            id="required-without-properties",
        ),
        pytest.param(
            {"$schema": DRAFT_2020_12_SCHEMA_URI, "properties": {"a": {"type": "string"}}},
            False,
            id="properties-without-required",
        ),
        pytest.param(
            {
                "$schema": DRAFT_2020_12_SCHEMA_URI,
                "required": ["a"],
                "properties": {"b": {"type": "string"}},
            },
            False,
            id="mismatched-required-and-properties",
        ),
        pytest.param(
            {
                "$schema": CUSTOM_META_SCHEMA_URI,
                "type": "object",
                "x-xsv-config": {"x-delimiter": "\t", "x-has-header": False},
            },
            True,
            id="custom-metaschema-with-valid-xsv-config-without-required",
        ),
    ],
)
def test_validated_schema_pass_optional_and_mismatched_xsv_fields(
    schema: dict[str, Any],
    expected_is_xsv: bool,
) -> None:
    """Allow schemas that omit or mismatch ``required`` and ``properties`` when validated as ``ValidatedSchema``."""
    result = ValidatedSchema(jsonschema=schema)

    assert result.jsonschema == schema
    assert result.has_xsv_metaschema is expected_is_xsv
    assert result.model_dump() == {
        "jsonschema": schema,
        "has_xsv_metaschema": expected_is_xsv,
    }


def test_validated_schema_fail_missing_jsonschema_field() -> None:
    """Raise ``ValidationError`` when ``ValidatedSchema`` is instantiated without ``jsonschema``."""
    with pytest.raises(ValidationError, match="Field required"):
        ValidatedSchema()  # type: ignore[call-arg]


@pytest.mark.parametrize("bad_input", INVALID_TOP_LEVEL_SCHEMA_LIST)
def test_validated_schema_fail_non_dict_jsonschema(bad_input: object) -> None:
    """Raise ``ValidationError`` when ``ValidatedSchema`` receives a non-dict ``jsonschema`` value."""
    with pytest.raises(ValidationError, match="Input should be a valid dictionary"):
        ValidatedSchema(jsonschema=bad_input)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("schema", "match"),
    [
        pytest.param({}, r"JSON Schema is missing the \$schema keyword", id="empty-dict"),
        pytest.param({"type": "object"}, r"JSON Schema is missing the \$schema keyword", id="missing-schema-key"),
        pytest.param({"$schema": None}, r"JSON Schema is missing the \$schema keyword", id="none-schema-uri"),
        pytest.param({"$schema": 0}, r"JSON Schema is missing the \$schema keyword", id="zero-schema-uri"),
        pytest.param({"$schema": -1}, r"JSON Schema is missing the \$schema keyword", id="negative-schema-uri"),
        pytest.param({"$schema": True}, r"JSON Schema is missing the \$schema keyword", id="bool-schema-uri"),
        pytest.param({"$schema": []}, r"JSON Schema is missing the \$schema keyword", id="list-schema-uri"),
        pytest.param({"$schema": {}}, r"JSON Schema is missing the \$schema keyword", id="dict-schema-uri"),
        pytest.param({"$schema": ""}, "Unsupported JSON Schema draft: ", id="empty-string-schema-uri"),
        pytest.param({"$schema": "   "}, "Unsupported JSON Schema draft:    ", id="whitespace-schema-uri"),
        pytest.param(
            {"$schema": "https://example.org/unknown-draft/schema"},
            r"Unsupported JSON Schema draft: https://example\.org/unknown-draft/schema",
            id="unknown-schema-uri",
        ),
    ],
)
def test_validated_schema_fail_invalid_schema_uri(schema: dict[str, Any], match: str) -> None:
    """Raise ``ValidationError`` for missing, non-string, empty, or unsupported ``$schema`` URIs."""
    with pytest.raises(ValidationError, match=match):
        ValidatedSchema(jsonschema=schema)


@pytest.mark.parametrize(
    ("schema", "match"),
    [
        pytest.param(
            {"$schema": DRAFT_2020_12_SCHEMA_URI, "type": "not-a-type"},
            "not-a-type",
            id="invalid-type-keyword",
        ),
        pytest.param(
            {"$schema": DRAFT_2020_12_SCHEMA_URI, "required": "col_a"},
            "is not of type 'array'",
            id="non-list-required",
        ),
        pytest.param(
            {"$schema": DRAFT_2020_12_SCHEMA_URI, "required": ["a", "a"]},
            "has non-unique elements",
            id="duplicate-required-entries",
        ),
        pytest.param(
            {"$schema": DRAFT_2020_12_SCHEMA_URI, "required": [1]},
            "is not of type 'string'",
            id="non-string-required-item",
        ),
        pytest.param(
            {"$schema": DRAFT_2020_12_SCHEMA_URI, "properties": ["a"]},
            "is not of type 'object'",
            id="non-dict-properties",
        ),
        pytest.param(
            {"$schema": CUSTOM_META_SCHEMA_URI, "x-xsv-config": {"x-delimiter": "::"}},
            "is too long",
            id="invalid-custom-xsv-config",
        ),
        pytest.param(
            {"$schema": CUSTOM_META_SCHEMA_URI, "x-xsv-config": {}},
            "is not valid under any of the given schemas",
            id="empty-custom-xsv-config",
        ),
    ],
)
def test_validated_schema_fail_metaschema_error_logs_and_raises(
    schema: dict[str, Any],
    match: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Log an error and raise ``SchemaError`` when ``ValidatedSchema`` fails metaschema validation."""
    with pytest.raises(jsonschema.exceptions.SchemaError, match=match):
        ValidatedSchema(jsonschema=schema)

    error_records = [record for record in caplog.records if record.levelno == logging.ERROR]
    assert len(error_records) == 1
    assert error_records[0].getMessage() == "Error validating JSON Schema"


@pytest.mark.parametrize(
    "draft_uri",
    [pytest.param(uri, id=uri) for uri in SUPPORTED_JSONSCHEMA_DRAFT_URIS],
)
def test_validated_xsv_schema_pass_supported_drafts(draft_uri: str) -> None:
    """Accept valid XSV schemas across every supported draft and populate all computed fields."""
    schema: dict[str, Any] = {
        "$schema": draft_uri,
        "required": ["id"],
        "properties": {"id": {"type": "string"}},
    }
    expected_is_xsv = draft_uri == CUSTOM_META_SCHEMA_URI

    result = ValidatedXsvSchema(jsonschema=schema)

    assert isinstance(result, ValidatedSchema)
    assert result.jsonschema == schema
    assert result.has_xsv_metaschema is expected_is_xsv
    assert result.has_xsv_parser_config is False
    assert result.required_cols == ["id"]
    assert result.model_dump() == {
        "jsonschema": schema,
        "has_xsv_metaschema": expected_is_xsv,
        "has_xsv_parser_config": False,
        "required_cols": ["id"],
    }


@pytest.mark.parametrize(
    ("schema", "expected_is_xsv", "expected_has_config", "expected_cols"),
    [
        pytest.param(
            {
                "$schema": DRAFT_2020_12_SCHEMA_URI,
                "required": ["a"],
                "properties": {"a": {}},
            },
            False,
            False,
            ["a"],
            id="single-column-empty-property-subschema",
        ),
        pytest.param(
            {
                "$schema": DRAFT_2020_12_SCHEMA_URI,
                "required": ["col_c", "col_a", "col_b"],
                "properties": {
                    "col_a": {"type": "string"},
                    "col_b": {"type": "integer"},
                    "col_c": {"type": "boolean"},
                },
            },
            False,
            False,
            ["col_c", "col_a", "col_b"],
            id="required-order-preserved-independent-of-properties-order",
        ),
        pytest.param(
            {
                "$schema": DRAFT_2020_12_SCHEMA_URI,
                "required": ["a"],
                "properties": {"a": {"type": "string"}},
                "x-xsv-config": {"x-delimiter": ","},
            },
            False,
            True,
            ["a"],
            id="standard-draft-with-non-empty-xsv-config",
        ),
        pytest.param(
            {
                "$schema": DRAFT_2020_12_SCHEMA_URI,
                "required": ["a"],
                "properties": {"a": {"type": "string"}},
                "x-xsv-config": {},
            },
            False,
            False,
            ["a"],
            id="standard-draft-with-empty-xsv-config",
        ),
        pytest.param(
            {
                "$schema": CUSTOM_META_SCHEMA_URI,
                "required": ["a", "b"],
                "properties": {"a": {"type": "string"}, "b": {"type": "integer"}},
                "x-xsv-config": {"x-delimiter": "\t", "x-has-header": True},
            },
            True,
            True,
            ["a", "b"],
            id="custom-metaschema-with-non-empty-xsv-config",
        ),
        pytest.param(
            {
                "$schema": CUSTOM_META_SCHEMA_URI,
                "required": ["a"],
                "properties": {"a": {"type": "string"}},
            },
            True,
            False,
            ["a"],
            id="custom-metaschema-without-xsv-config",
        ),
    ],
)
def test_validated_xsv_schema_pass_properties_and_config_variants(
    schema: dict[str, Any],
    expected_is_xsv: bool,
    expected_has_config: bool,
    expected_cols: list[str],
) -> None:
    """Compute ``has_xsv_metaschema``, ``has_xsv_parser_config``, and ``required_cols`` accurately."""
    result = ValidatedXsvSchema(jsonschema=schema)

    assert result.jsonschema == schema
    assert result.has_xsv_metaschema is expected_is_xsv
    assert result.has_xsv_parser_config is expected_has_config
    assert result.required_cols == expected_cols
    assert result.model_dump() == {
        "jsonschema": schema,
        "has_xsv_metaschema": expected_is_xsv,
        "has_xsv_parser_config": expected_has_config,
        "required_cols": expected_cols,
    }


def test_validated_xsv_schema_fail_missing_jsonschema_field() -> None:
    """Raise ``ValidationError`` when ``ValidatedXsvSchema`` is instantiated without ``jsonschema``."""
    with pytest.raises(ValidationError, match="Field required"):
        ValidatedXsvSchema()  # type: ignore[call-arg]


@pytest.mark.parametrize("bad_input", INVALID_TOP_LEVEL_SCHEMA_LIST)
def test_validated_xsv_schema_fail_non_dict_jsonschema(bad_input: object) -> None:
    """Raise ``ValidationError`` when ``ValidatedXsvSchema`` receives a non-dict ``jsonschema`` value."""
    with pytest.raises(ValidationError, match="Input should be a valid dictionary"):
        ValidatedXsvSchema(jsonschema=bad_input)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("schema", "match"),
    [
        pytest.param(
            {"required": ["a"], "properties": {"a": {"type": "string"}}},
            r"JSON Schema is missing the \$schema keyword",
            id="missing-schema-uri",
        ),
        pytest.param(
            {"$schema": None, "required": ["a"], "properties": {"a": {"type": "string"}}},
            r"JSON Schema is missing the \$schema keyword",
            id="none-schema-uri",
        ),
        pytest.param(
            {"$schema": 0, "required": ["a"], "properties": {"a": {"type": "string"}}},
            r"JSON Schema is missing the \$schema keyword",
            id="zero-schema-uri",
        ),
        pytest.param(
            {"$schema": "", "required": ["a"], "properties": {"a": {"type": "string"}}},
            "Unsupported JSON Schema draft: ",
            id="empty-string-schema-uri",
        ),
        pytest.param(
            {"$schema": "https://example.org/unsupported", "required": ["a"], "properties": {"a": {"type": "string"}}},
            r"Unsupported JSON Schema draft: https://example\.org/unsupported",
            id="unsupported-schema-uri",
        ),
    ],
)
def test_validated_xsv_schema_fail_invalid_schema_uri(schema: dict[str, Any], match: str) -> None:
    """Raise ``ValidationError`` when ``ValidatedXsvSchema`` has a missing or unsupported ``$schema``."""
    with pytest.raises(ValidationError, match=match):
        ValidatedXsvSchema(jsonschema=schema)


@pytest.mark.parametrize(
    "schema",
    [
        pytest.param({"$schema": DRAFT_2020_12_SCHEMA_URI}, id="missing-required-and-properties"),
        pytest.param(
            {"$schema": DRAFT_2020_12_SCHEMA_URI, "properties": {"a": {"type": "string"}}},
            id="missing-required-with-properties",
        ),
        pytest.param({"$schema": DRAFT_2020_12_SCHEMA_URI, "required": []}, id="empty-required-no-properties"),
        pytest.param(
            {"$schema": DRAFT_2020_12_SCHEMA_URI, "required": [], "properties": {}},
            id="empty-required-and-empty-properties",
        ),
        pytest.param(
            {"$schema": DRAFT_2020_12_SCHEMA_URI, "required": [], "properties": {"a": {"type": "string"}}},
            id="empty-required-with-properties",
        ),
        pytest.param(
            {"$schema": CUSTOM_META_SCHEMA_URI, "properties": {"a": {"type": "string"}}},
            id="custom-metaschema-missing-required",
        ),
    ],
)
def test_validated_xsv_schema_fail_missing_or_empty_required(schema: dict[str, Any]) -> None:
    """Raise ``ValidationError`` when ``required`` is missing or empty."""
    with pytest.raises(ValidationError, match="Could not find any required cols in schema"):
        ValidatedXsvSchema(jsonschema=schema)


@pytest.mark.parametrize(
    "schema",
    [
        pytest.param(
            {"$schema": DRAFT_2020_12_SCHEMA_URI, "required": ["a"]},
            id="missing-properties",
        ),
        pytest.param(
            {"$schema": DRAFT_2020_12_SCHEMA_URI, "required": ["a"], "properties": {}},
            id="empty-properties",
        ),
        pytest.param(
            {"$schema": CUSTOM_META_SCHEMA_URI, "required": ["a"]},
            id="custom-metaschema-missing-properties",
        ),
        pytest.param(
            {"$schema": CUSTOM_META_SCHEMA_URI, "required": ["a"], "properties": {}},
            id="custom-metaschema-empty-properties",
        ),
    ],
)
def test_validated_xsv_schema_fail_missing_or_empty_properties(schema: dict[str, Any]) -> None:
    """Raise ``ValidationError`` when ``properties`` is missing or empty."""
    with pytest.raises(ValidationError, match="Could not find any properties in schema"):
        ValidatedXsvSchema(jsonschema=schema)


@pytest.mark.parametrize(
    "schema",
    [
        pytest.param(
            {
                "$schema": DRAFT_2020_12_SCHEMA_URI,
                "required": ["a", "b"],
                "properties": {"a": {"type": "string"}},
            },
            id="required-has-extra-column",
        ),
        pytest.param(
            {
                "$schema": DRAFT_2020_12_SCHEMA_URI,
                "required": ["a"],
                "properties": {"a": {"type": "string"}, "b": {"type": "string"}},
            },
            id="properties-has-extra-column",
        ),
        pytest.param(
            {
                "$schema": DRAFT_2020_12_SCHEMA_URI,
                "required": ["a"],
                "properties": {"b": {"type": "string"}},
            },
            id="disjoint-required-and-properties",
        ),
        pytest.param(
            {
                "$schema": DRAFT_2020_12_SCHEMA_URI,
                "required": ["SeqId"],
                "properties": {"seqid": {"type": "string"}},
            },
            id="case-mismatch",
        ),
        pytest.param(
            {
                "$schema": DRAFT_2020_12_SCHEMA_URI,
                "required": [""],
                "properties": {"a": {"type": "string"}},
            },
            id="empty-string-required-col-mismatch",
        ),
        pytest.param(
            {
                "$schema": CUSTOM_META_SCHEMA_URI,
                "required": ["sequid"],
                "properties": {"seqid": {"type": "string"}},
            },
            id="custom-metaschema-typo-mismatch",
        ),
    ],
)
def test_validated_xsv_schema_fail_required_properties_mismatch(schema: dict[str, Any]) -> None:
    """Raise ``ValidationError`` when ``required`` and ``properties`` do not contain the exact same column names."""
    with pytest.raises(
        ValidationError,
        match="XSV schemas must have the same set of properties listed under `required` and in `properties`",
    ):
        ValidatedXsvSchema(jsonschema=schema)


@pytest.mark.parametrize(
    ("schema", "match"),
    [
        pytest.param(
            {"$schema": DRAFT_2020_12_SCHEMA_URI, "required": "a", "properties": {"a": {"type": "string"}}},
            "is not of type 'array'",
            id="required-is-string",
        ),
        pytest.param(
            {"$schema": DRAFT_2020_12_SCHEMA_URI, "required": None, "properties": {"a": {"type": "string"}}},
            "is not of type 'array'",
            id="required-is-none",
        ),
        pytest.param(
            {"$schema": DRAFT_2020_12_SCHEMA_URI, "required": 0, "properties": {"a": {"type": "string"}}},
            "is not of type 'array'",
            id="required-is-zero",
        ),
        pytest.param(
            {"$schema": DRAFT_2020_12_SCHEMA_URI, "required": ["a", "a"], "properties": {"a": {"type": "string"}}},
            "has non-unique elements",
            id="duplicate-required-column",
        ),
        pytest.param(
            {"$schema": DRAFT_2020_12_SCHEMA_URI, "required": [123], "properties": {"a": {"type": "string"}}},
            "is not of type 'string'",
            id="non-string-required-column",
        ),
        pytest.param(
            {"$schema": DRAFT_2020_12_SCHEMA_URI, "required": ["a"], "properties": ["a"]},
            "is not of type 'object'",
            id="properties-is-list",
        ),
        pytest.param(
            {"$schema": DRAFT_2020_12_SCHEMA_URI, "required": ["a"], "properties": "a"},
            "is not of type 'object'",
            id="properties-is-string",
        ),
        pytest.param(
            {"$schema": DRAFT_2020_12_SCHEMA_URI, "required": ["a"], "properties": None},
            "is not of type 'object'",
            id="properties-is-none",
        ),
        pytest.param(
            {"$schema": DRAFT_2020_12_SCHEMA_URI, "required": ["a"], "properties": 0},
            "is not of type 'object'",
            id="properties-is-zero",
        ),
        pytest.param(
            {"$schema": DRAFT_2020_12_SCHEMA_URI, "required": ["a"], "properties": {"a": {"type": "not-a-type"}}},
            "not-a-type",
            id="invalid-property-subschema",
        ),
        pytest.param(
            {
                "$schema": CUSTOM_META_SCHEMA_URI,
                "required": ["a"],
                "properties": {"a": {"type": "string"}},
                "x-xsv-config": {"x-delimiter": "::"},
            },
            "is too long",
            id="invalid-xsv-config-under-custom-metaschema",
        ),
        pytest.param(
            {
                "$schema": CUSTOM_META_SCHEMA_URI,
                "required": ["a"],
                "properties": {"a": {"type": "string"}},
                "x-xsv-config": {},
            },
            "is not valid under any of the given schemas",
            id="empty-xsv-config-under-custom-metaschema",
        ),
    ],
)
def test_validated_xsv_schema_fail_metaschema_error_logs_and_raises(
    schema: dict[str, Any],
    match: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Log an error and raise ``SchemaError`` when ``ValidatedXsvSchema`` fails metaschema validation."""
    with pytest.raises(jsonschema.exceptions.SchemaError, match=match):
        ValidatedXsvSchema(jsonschema=schema)

    error_records = [record for record in caplog.records if record.levelno == logging.ERROR]
    assert len(error_records) == 1
    assert error_records[0].getMessage() == "Error validating JSON Schema"
