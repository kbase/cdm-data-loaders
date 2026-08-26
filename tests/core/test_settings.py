"""Tests for the Settings objects used by DLT pipelines."""

from typing import Any

import dlt
import pytest
from frozendict import frozendict
from pydantic import AliasChoices, AliasPath, Field, ValidationError
from pydantic_settings import CliApp
from argparse import ArgumentError
from cdm_data_loaders.core.fields import (
    DEV_MODE,
    INPUT_DIR,
    OUTPUT_DIR,
    USE_DESTINATION,
    USE_OUTPUT_DIR_FOR_PIPELINE_METADATA,
    VALID_DESTINATIONS,
    generate_aliases,
)
from cdm_data_loaders.core.settings import (
    BatchedFileInputSettings,
    CdmDataLoadersBase,
    CtsSettings,
    InputOutputSettings,
)
from tests.core.conftest import (
    DEFAULT_BATCH_FILE_SETTINGS_RECONCILED,
    DEFAULT_CTS_SETTINGS_RECONCILED,
    DESTINATION_TO_OUTPUT,
    TEST_BATCH_FILE_SETTINGS,
    TEST_BATCH_FILE_SETTINGS_RECONCILED,
    TEST_CTS_SETTINGS,
    TEST_CTS_SETTINGS_RECONCILED,
    check_settings,
    make_settings,
    make_settings_autofill_config,
    parametrize_validation_aliases,
)
from tests.helpers import make_cli_arg

SETTINGS_CLASSES = [CtsSettings, BatchedFileInputSettings]

INVALID_DESTINATIONS = ["gcs", "filesystem", "", "LocalFs", "S3"]
INVALID_BOOLEAN_VALUES = ["what", "yep", "nope", "2", -1, "", " ", "wtf", None]

S3 = "is_s3"
OUT = OUTPUT_DIR
RAW = "raw_data_dir"
PIPE = "pipeline_dir"


# argument aliases for the fields, used for CLI parsing
ARG_ALIASES: frozendict[str, list[str]] = frozendict(
    {k: v.validation_alias.choices for k, v in BatchedFileInputSettings.model_fields.items()}
)

# manually specify to avoid recapitulating logic
OUTPUT_PATHS: dict[str, dict[str, Any]] = {
    "": {S3: False, OUT: "", RAW: "raw_data", PIPE: ".dlt_conf"},
    "/": {S3: False, OUT: "/", RAW: "/raw_data", PIPE: "/.dlt_conf"},
    # from destination.local_fs
    "/output_dir": {
        S3: False,
        OUT: "/output_dir",
        RAW: "/output_dir/raw_data",
        PIPE: "/output_dir/.dlt_conf",
    },
    "/output/dir": {
        S3: False,
        OUT: "/output/dir",
        RAW: "/output/dir/raw_data",
        PIPE: "/output/dir/.dlt_conf",
    },
    "s3/some/path/": {
        S3: False,
        OUT: "s3/some/path",
        RAW: "s3/some/path/raw_data",
        PIPE: "s3/some/path/.dlt_conf",
    },
    # normalised form of the above
    "s3/some/path": {
        S3: False,
        OUT: "s3/some/path",
        RAW: "s3/some/path/raw_data",
        PIPE: "s3/some/path/.dlt_conf",
    },
    "s3a://bucket/key": {
        S3: True,
        OUT: "s3a://bucket/key",
        RAW: "s3a://bucket/key/raw_data",
        PIPE: None,
    },
    "s3://test/bucket/": {
        S3: True,
        OUT: "s3://test/bucket",
        RAW: "s3://test/bucket/raw_data",
        PIPE: None,
    },
    # normalised from above
    "s3://test/bucket": {
        S3: True,
        OUT: "s3://test/bucket",
        RAW: "s3://test/bucket/raw_data",
        PIPE: None,
    },
    # from destination.s3
    "s3://some/s3/bucket": {
        S3: True,
        OUT: "s3://some/s3/bucket",
        RAW: "s3://some/s3/bucket/raw_data",
        PIPE: None,
    },
}


# a whole load of values that Pydantic will coerce to a boolean
TRUE_FALSE_VALUES = [
    ("0", False),
    ("1", True),
    ("f", False),
    ("false", False),
    ("False", False),
    ("FALSE", False),
    ("n", False),
    ("no", False),
    ("off", False),
    ("on", True),
    ("t", True),
    ("true", True),
    ("True", True),
    ("TRUE", True),
    ("y", True),
    ("yes", True),
    (0, False),
    (1, True),
    (False, False),
    (True, True),
]


# CdmDataLoadersBase.check_aliases.


# Baseline: no aliases at all
def test_check_aliases_no_additional_fields() -> None:
    """A subclass that adds no fields beyond the base does not raise."""

    class EmptySettings(CdmDataLoadersBase):
        pass

    EmptySettings()


def test_check_aliases_fields_with_no_validation_alias() -> None:
    """Fields with no validation_alias are distinguished by field name only."""

    class PlainSettings(CdmDataLoadersBase):
        field_a: str = "a"
        field_b: str = "b"

    PlainSettings()


"""
Alias type / collision matrix

Each case builds a two-field class with the given validation_alias values
and asserts whether a collision is or is not detected.
"""

ALIAS_TYPE_MATRIX_CASES = [
    pytest.param(None, None, False, None, id="none-none"),
    pytest.param(None, "x", False, None, id="none-vs-distinct-string"),
    pytest.param("x", "y", False, None, id="distinct-strings"),
    pytest.param("shared", "shared", True, "\nshared: field_a, field_b", id="identical-strings"),
    pytest.param("field_a", None, False, None, id="self-referential-alias-no-collision"),
    pytest.param(None, "field_a", True, "\nfield_a: field_a, field_b", id="alias-equals-other-fields-name"),
    pytest.param(AliasChoices("x", "y"), AliasChoices("z", "w"), False, None, id="distinct-aliaschoices"),
    pytest.param(
        AliasChoices("shared", "y"),
        AliasChoices("shared", "z"),
        True,
        "\nshared: field_a, field_b",
        id="shared-within-aliaschoices",
    ),
    pytest.param(
        "shared",
        AliasChoices("shared", "z"),
        True,
        "\nshared: field_a, field_b",
        id="string-vs-aliaschoices-shared",
    ),
    pytest.param(
        AliasPath("nested", 0),
        AliasPath("nested", 0),
        True,
        "\nAliasPath\['nested', 0\]: field_a, field_b",
        id="identical-aliaspath",
    ),
    pytest.param(AliasPath("nested", 0), AliasPath("nested", 1), False, None, id="distinct-aliaspath-index"),
    pytest.param(AliasPath("a", 0), AliasPath("b", 0), False, None, id="distinct-aliaspath-root"),
    pytest.param(
        AliasChoices("w", AliasPath("nested", 0)),
        AliasChoices("q", AliasPath("nested", 0)),
        True,
        r"\nAliasPath\['nested', 0\]: field_a, field_b",
        id="identical-aliaspath-within-aliaschoices",
    ),
    pytest.param(
        AliasPath("nested", 0),
        AliasChoices("q", AliasPath("nested", 0)),
        True,
        "\nAliasPath\['nested', 0\]: field_a, field_b",
        id="bare-aliaspath-vs-aliaspath-in-aliaschoices",
    ),
    pytest.param(
        AliasPath("nested", 0), AliasPath("nested", "0"), False, None, id="aliaspath-int-vs-str-index-not-equal"
    ),
]


@pytest.mark.parametrize(("alias_a", "alias_b", "should_collide", "err_msg"), ALIAS_TYPE_MATRIX_CASES)
def test_check_aliases_alias_type_matrix(alias_a: Any, alias_b: Any, should_collide: bool, err_msg: str | None) -> None:
    """Exercise each combination of alias types across two fields."""

    class TwoFieldSettings(CdmDataLoadersBase):
        field_a: str = Field(default="a", validation_alias=alias_a)
        field_b: str = Field(default="b", validation_alias=alias_b)

    if should_collide:
        with pytest.raises(ValueError, match=err_msg):
            TwoFieldSettings()
    else:
        # Should not raise
        TwoFieldSettings()


def test_check_aliases_pydantic_raises_argparse_error() -> None:
    """Conflicting AliasPaths and validation aliases cause an argparse error."""

    class TwoFieldSettings(CdmDataLoadersBase):
        field_a: str = Field(default="a", validation_alias=AliasPath("nested", 0))
        field_b: str = Field(default="b", validation_alias="nested")

    with pytest.raises(ArgumentError, match="argument --nested: conflicting option string: --nested"):
        TwoFieldSettings()


def test_check_aliases_own_name_within_aliaschoices_no_false_positive() -> None:
    """A field's own name appearing as one of several entries in its AliasChoices is A-OK."""

    class ValidSettings(CdmDataLoadersBase):
        field_a: str = Field(default="a", validation_alias=AliasChoices("field_a", "extra"))
        field_b: str = "b"

    ValidSettings()


# Intra-field duplication (single field, repeated alias values)
def test_check_aliases_duplicate_string_alias_within_same_field() -> None:
    """A field repeating the same string alias twice in its own AliasChoices does not raise."""

    class QuestionableSettings(CdmDataLoadersBase):
        field_a: str = Field(default="a", validation_alias=AliasChoices("dup", "dup"))

    QuestionableSettings()


def test_check_aliases_duplicate_aliaspath_within_same_field() -> None:
    """A field repeating an AliasPath with an identical path twice in its own AliasChoices does not raise."""

    class QuestionableSettings(CdmDataLoadersBase):
        field_a: str = Field(
            default="a",
            validation_alias=AliasChoices(AliasPath("nested", 0), AliasPath("nested", 0)),
        )

    # Should not raise
    QuestionableSettings()


# Multi-field, multi-collision scenarios
def test_check_aliases_three_way_string_collision() -> None:
    """A shared alias across three fields lists all three field names.

    Field names are emitted in alphabetical order.
    """

    class InvalidSettings(CdmDataLoadersBase):
        field_c: str = Field(default="c", validation_alias="shared")
        field_b: str = Field(default="b", validation_alias="shared")
        field_a: str = Field(default="a", validation_alias="shared")

    # field names are emitted in alphabetical order
    with pytest.raises(ValueError, match=r"shared: field_a, field_b, field_c"):
        InvalidSettings()


def test_check_aliases_multiple_independent_collisions_reported() -> None:
    """A field with two aliases that collide with aliases for other fields shows up twice in the error message.

    Errors are sorted alphabetically by alias.
    """

    class InvalidSettings(CdmDataLoadersBase):
        field_a: str = Field(default="a", validation_alias=AliasChoices("alias_z", "alias_x"))
        field_b: str = Field(default="b", validation_alias="alias_z")
        field_c: str = Field(default="c", validation_alias="alias_x")

    with pytest.raises(ValueError, match="alias_x: field_a, field_c\nalias_z: field_a, field_b"):
        InvalidSettings()


def test_check_aliases_aliaspath_error_message_format() -> None:
    """The error message renders a colliding AliasPath key using its path list."""

    class InvalidSettings(CdmDataLoadersBase):
        field_a: str = Field(default="a", validation_alias=AliasPath("nested", 0))
        field_b: str = Field(default="b", validation_alias=AliasPath("nested", 0))

    with pytest.raises(
        ValueError, match=r"The following aliases are used more than once:\nAliasPath\['nested', 0\]: field_a, field_b"
    ):
        InvalidSettings()


# Inheritance
def test_check_aliases_inherited_field_collision() -> None:
    """A subclass field whose alias matches a field name inherited from a parent class is caught."""

    class BaseFieldSettings(CdmDataLoadersBase):
        field_a: str = "a"

    class ChildFieldSettings(BaseFieldSettings):
        field_b: str = Field(default="b", validation_alias="field_a")

    with pytest.raises(ValueError, match=r"field_a: field_a, field_b"):
        ChildFieldSettings()


def test_check_aliases_inherited_field_no_collision() -> None:
    """A subclass field with a distinct alias does not collide with an inherited field."""

    class BaseFieldSettings(CdmDataLoadersBase):
        field_a: str = "a"

    class ChildFieldSettings(BaseFieldSettings):
        field_b: str = Field(default="b", validation_alias="unrelated")

    ChildFieldSettings()


# alias=... keyword and serialization_alias
def test_check_aliases_alias_keyword_fallback_collision() -> None:
    """Fields using `alias=...` are checked.

    Pydantic assigns `validation_alias` from `alias` when `validation_alias` is not set explicitly.
    """

    class InvalidSettings(CdmDataLoadersBase):
        field_a: str = Field(default="a", alias="shared")
        field_b: str = Field(default="b", alias="shared")

    with pytest.raises(ValueError, match=r"shared: field_a, field_b"):
        InvalidSettings()


def test_check_aliases_serialization_alias_ignored() -> None:
    """Colliding serialization_alias values are not flagged, since only validation_alias affects input parsing."""

    class ValidSettings(CdmDataLoadersBase):
        field_a: str = Field(default="a", serialization_alias="shared")
        field_b: str = Field(default="b", serialization_alias="shared")

    ValidSettings()


# Direct classmethod invocation (bypassing full model construction)
def test_check_aliases_direct_call_returns_data_unchanged() -> None:
    """check_aliases returns the input data unmodified when there is no collision."""
    data = {"input_dir": "/tmp"}
    result = CdmDataLoadersBase.check_aliases(data)  # pyright: ignore[reportCallIssue]
    assert result is data


def test_check_aliases_direct_call_error_message_exact() -> None:
    """Calling check_aliases directly raises a plain ValueError with the constructed message."""

    class InvalidSettings(CdmDataLoadersBase):
        field_a: str = Field(default="a", validation_alias="shared")
        field_b: str = Field(default="b", validation_alias="shared")

    with pytest.raises(ValueError, match="The following aliases are used more than once:\nshared: field_a, field_b"):
        InvalidSettings.check_aliases({})  # pyright: ignore[reportCallIssue]


# Integration with generate_aliases
def test_check_aliases_generate_aliases_short_flag_collision() -> None:
    """Two fields using generate_aliases with the same short_alias collide."""

    class InvalidSettings(CdmDataLoadersBase):
        foo_field: str = Field(default="a", validation_alias=generate_aliases("foo_field", "x"))
        bar_field: str = Field(default="b", validation_alias=generate_aliases("bar_field", "x"))

    with pytest.raises(ValueError, match=r"x: bar_field, foo_field"):
        InvalidSettings()


def test_check_aliases_generate_aliases_distinct_short_flags_no_collision() -> None:
    """Two fields using generate_aliases with distinct short_alias values do not collide."""

    class ValidSettings(CdmDataLoadersBase):
        foo_field: str = Field(default="a", validation_alias=generate_aliases("foo_field", "f"))
        bar_field: str = Field(default="b", validation_alias=generate_aliases("bar_field", "b"))

    # Should not raise
    ValidSettings()


# Integration: production settings classes
@pytest.mark.parametrize("settings_cls", [InputOutputSettings, CtsSettings, BatchedFileInputSettings])
def test_check_aliases_production_classes_no_collisions(settings_cls: type[CdmDataLoadersBase]) -> None:
    """The real settings classes' generated aliases produce no collisions."""
    # Should not raise
    settings_cls.check_aliases({})  # pyright: ignore[reportCallIssue]


# InputOutputSettings
def test_input_output_settings_validate_dir_path() -> None:
    """Test that InputOutputSettings correctly strips trailing slashes."""

    class TestIO(InputOutputSettings):
        log_config_file: str = "log.json"  # pyright: ignore[reportIncompatibleVariableOverride]
        input_dir: str = "/input/"
        output_dir: str = "/output//"

    s = TestIO()
    assert s.input_dir == "/input"
    assert s.output_dir == "/output"
    assert s.log_config_file == "log.json"

    t = InputOutputSettings(input_dir="/input/", output_dir="/output//", log_config_file="log.json")
    for attr in ["input_dir", "output_dir", "log_config_file"]:
        assert getattr(s, attr) == getattr(t, attr)


def test_input_output_settings_preserve_root() -> None:
    """Test that InputOutputSettings preserves the root directory slash."""

    class TestIO(InputOutputSettings):
        log_config_file: str = "log.json"  # pyright: ignore[reportIncompatibleVariableOverride]
        input_dir: str = "/"
        output_dir: str = "/"

    s = TestIO()
    assert s.input_dir == "/"
    assert s.output_dir == "/"
    assert s.log_config_file == "log.json"


def pytest_generate_tests(metafunc: pytest.Metafunc) -> None:
    """Dynamically generate tests for every alias of each user-settable field in a settings object."""
    marker = metafunc.definition.get_closest_marker("settings_cls")
    if marker is None:
        return
    parametrize_validation_aliases(metafunc, marker.args[0])


@pytest.mark.settings_cls.with_args(CtsSettings)
@pytest.mark.usefixtures("patch_dlt_config")
def test_cts_settings_aliases(request: pytest.FixtureRequest, validation_alias: str, field_name: str) -> None:
    """Test the CtsSettings aliases for a given field name."""
    settings_cls = request.node.get_closest_marker("settings_cls").args[0]
    settings: CtsSettings = make_settings_autofill_config(
        settings_cls, {validation_alias: TEST_CTS_SETTINGS[field_name]}
    )
    assert getattr(settings, field_name) == TEST_CTS_SETTINGS_RECONCILED[field_name]


@pytest.mark.settings_cls.with_args(BatchedFileInputSettings)
@pytest.mark.usefixtures("patch_dlt_config")
def test_batch_file_settings_aliases(request: pytest.FixtureRequest, validation_alias: str, field_name: str) -> None:
    """Test the BatchedFileInputSettings aliases for a given field name."""
    settings_cls = request.node.get_closest_marker("settings_cls").args[0]
    settings = make_settings_autofill_config(settings_cls, {validation_alias: TEST_BATCH_FILE_SETTINGS[field_name]})
    assert getattr(settings, field_name) == TEST_BATCH_FILE_SETTINGS_RECONCILED[field_name]


@pytest.mark.settings_cls.with_args(CtsSettings)
@pytest.mark.usefixtures("patch_dlt_config")
def test_cts_settings_cliapp_aliases(request: pytest.FixtureRequest, validation_alias: str, field_name: str) -> None:
    """Test the CtsSettings aliases for a given model field name, initialised using CliApp.run."""
    settings_cls = request.node.get_closest_marker("settings_cls").args[0]
    settings = CliApp.run(
        model_cls=settings_cls,
        cli_args=[
            f"{'--' if len(validation_alias) > 1 else '-'}{validation_alias}",
            str(TEST_CTS_SETTINGS[field_name]),
        ],
    )
    assert getattr(settings, field_name) == TEST_CTS_SETTINGS_RECONCILED[field_name]


@pytest.mark.settings_cls.with_args(BatchedFileInputSettings)
@pytest.mark.usefixtures("patch_dlt_config")
def test_batch_file_settings_cliapp_aliases(
    request: pytest.FixtureRequest, validation_alias: str, field_name: str
) -> None:
    """Test the BatchedFileInputSettings aliases for a given model field name, initialised using CliApp.run."""
    settings_cls = request.node.get_closest_marker("settings_cls").args[0]

    settings = CliApp.run(
        model_cls=settings_cls,
        cli_args=[
            f"{'--' if len(validation_alias) > 1 else '-'}{validation_alias}",
            str(TEST_BATCH_FILE_SETTINGS[field_name]),
        ],
    )
    assert getattr(settings, field_name) == TEST_BATCH_FILE_SETTINGS_RECONCILED[field_name]


# Generic settings tests
@pytest.mark.parametrize(
    ("settings_cls", "args", "expected"),
    [
        # default values
        (CtsSettings, {}, DEFAULT_CTS_SETTINGS_RECONCILED),
        (BatchedFileInputSettings, {}, DEFAULT_BATCH_FILE_SETTINGS_RECONCILED),
        # all args specified
        (CtsSettings, TEST_CTS_SETTINGS, TEST_CTS_SETTINGS_RECONCILED),
        (BatchedFileInputSettings, TEST_BATCH_FILE_SETTINGS, TEST_BATCH_FILE_SETTINGS_RECONCILED),
    ],
)
@pytest.mark.usefixtures("patch_dlt_config")
def test_settings_all_settings_specified(
    settings_cls: type[CtsSettings], args: dict[str, Any], expected: dict[str, Any]
) -> None:
    """Ensure the CTS settings are set up correctly."""
    s = make_settings_autofill_config(settings_cls, args)
    check_settings(s, expected)


@pytest.mark.parametrize("settings_cls", SETTINGS_CLASSES)
@pytest.mark.usefixtures("patch_dlt_config")
def test_cli_app_run_default_settings(settings_cls: type[CtsSettings]) -> None:
    """Ensure the CTS settings are set up correctly, CLI version."""
    s = CliApp.run(settings_cls)
    expected = (
        DEFAULT_CTS_SETTINGS_RECONCILED if settings_cls == CtsSettings else DEFAULT_BATCH_FILE_SETTINGS_RECONCILED
    )
    check_settings(s, expected)


@pytest.mark.parametrize("settings_cls", SETTINGS_CLASSES)
def test_settings_no_dlt_config_error(settings_cls: type[CtsSettings]) -> None:
    """Ensure an error is raised if there is no dlt_config."""
    with pytest.raises(ValidationError, match=r"dlt_config must be defined"):
        make_settings(settings_cls, dlt_config=None)


@pytest.mark.parametrize("invalid_destination_config", [{}, {"destination": {}}])
@pytest.mark.parametrize("settings_cls", SETTINGS_CLASSES)
def test_settings_no_destinations_set(
    settings_cls: type[CtsSettings], invalid_destination_config: dict[str, dict[Any, Any]]
) -> None:
    """Ensure that destinations are specified in the dlt config."""
    with pytest.raises(ValueError, match="No valid destinations found in dlt configuration"):
        make_settings(settings_cls, dlt_config=invalid_destination_config)


# same thing but via CliApp.run
@pytest.mark.parametrize("settings_cls", SETTINGS_CLASSES)
@pytest.mark.parametrize(
    ("dlt_config", "error", "err_msg"),
    [
        (None, ValidationError, "dlt_config must be defined"),
        ({}, ValueError, "No valid destinations found in dlt configuration"),
        ({"destination": {}}, ValueError, "No valid destinations found in dlt configuration"),
    ],
)
@pytest.mark.usefixtures("patch_dlt_config")
def test_cli_app_run_dlt_config_errors(
    settings_cls: type[CtsSettings],
    error: type[Exception],
    err_msg: str,
) -> None:
    """Test all the variants of the Settings fields."""
    with pytest.raises(error, match=err_msg):
        CliApp.run(settings_cls)


# destination tests
@pytest.mark.parametrize("settings_cls", SETTINGS_CLASSES)
@pytest.mark.parametrize(USE_DESTINATION, VALID_DESTINATIONS)
@pytest.mark.usefixtures("patch_dlt_config")
def test_settings_valid_destinations_accepted(use_destination: str, settings_cls: type[CtsSettings]) -> None:
    """Test valid destinations against the settings class."""
    s = make_settings_autofill_config(settings_cls, {USE_DESTINATION: use_destination})
    assert s.use_destination == use_destination


@pytest.mark.parametrize("settings_cls", SETTINGS_CLASSES)
@pytest.mark.parametrize(USE_DESTINATION, INVALID_DESTINATIONS)
@pytest.mark.usefixtures("patch_dlt_config")
def test_settings_invalid_destination_raises(use_destination: str, settings_cls: type[CtsSettings]) -> None:
    """Ensure that an unrecognised use_destination raises a ValidationError."""
    with pytest.raises(ValidationError, match=r"use_destination must be one of \['local_fs', 's3'\]"):
        make_settings_autofill_config(settings_cls, {USE_DESTINATION: use_destination})


@pytest.mark.parametrize("settings_cls", SETTINGS_CLASSES)
def test_settings_destination_has_no_bucket_url(settings_cls: type[CtsSettings]) -> None:
    """Ensure that destinations have a bucket_url."""
    with pytest.raises(ValueError, match="No bucket_url specified for destination local_fs"):
        make_settings(
            settings_cls,
            dlt_config={"destination": {"local_fs": None}},
            kwargs={USE_DESTINATION: "local_fs"},
        )


# destination tests, CLI versions
@pytest.mark.parametrize("settings_cls", SETTINGS_CLASSES)
@pytest.mark.parametrize(USE_DESTINATION, VALID_DESTINATIONS)
@pytest.mark.parametrize("destination_arg", ARG_ALIASES[USE_DESTINATION])
@pytest.mark.usefixtures("patch_dlt_config")
def test_cli_app_run_valid_destinations_accepted(
    use_destination: str, settings_cls: type[CtsSettings], destination_arg: str
) -> None:
    """Test valid destinations using the command line."""
    s = CliApp.run(settings_cls, cli_args=[make_cli_arg(destination_arg), use_destination])
    assert s.use_destination == use_destination


@pytest.mark.parametrize("settings_cls", SETTINGS_CLASSES)
@pytest.mark.parametrize(USE_DESTINATION, INVALID_DESTINATIONS)
@pytest.mark.parametrize("destination_arg", ARG_ALIASES[USE_DESTINATION])
@pytest.mark.usefixtures("patch_dlt_config")
def test_cli_app_run_invalid_destinations_raises(
    use_destination: str,
    settings_cls: type[CtsSettings],
    destination_arg: str,
) -> None:
    """Test invalid destinations using the command line."""
    with pytest.raises(ValidationError, match="use_destination must be one of"):
        CliApp.run(settings_cls, cli_args=[make_cli_arg(destination_arg), use_destination])


@pytest.mark.parametrize("settings_cls", SETTINGS_CLASSES)
@pytest.mark.parametrize("destination_arg", ARG_ALIASES[USE_DESTINATION])
def test_cli_app_run_destination_has_no_bucket_url(
    settings_cls: type[CtsSettings], destination_arg: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Ensure that destinations have a bucket_url."""
    dlt_config = {"destination": {"local_fs": None}}
    monkeypatch.setattr(dlt, "config", dlt_config)
    with pytest.raises(ValueError, match="No bucket_url specified for destination local_fs"):
        CliApp.run(settings_cls, cli_args=[make_cli_arg(destination_arg), "local_fs"])


# boolean fields
@pytest.mark.parametrize("settings_cls", SETTINGS_CLASSES)
@pytest.mark.parametrize(("input_arg", "value"), TRUE_FALSE_VALUES)
@pytest.mark.parametrize("input_arg_name", [USE_OUTPUT_DIR_FOR_PIPELINE_METADATA, DEV_MODE])
@pytest.mark.usefixtures("patch_dlt_config")
def test_settings_boolean_variants_accepted(
    input_arg: str, value: bool, input_arg_name: str, settings_cls: type[CtsSettings]
) -> None:
    """Ensure that each valid boolean value is accepted without error."""
    s = make_settings_autofill_config(settings_cls, {input_arg_name: input_arg})  # type: ignore[reportArgumentType]
    assert getattr(s, input_arg_name) == value


@pytest.mark.parametrize("settings_cls", SETTINGS_CLASSES)
@pytest.mark.parametrize("value", INVALID_BOOLEAN_VALUES)
@pytest.mark.parametrize("input_arg_name", [USE_OUTPUT_DIR_FOR_PIPELINE_METADATA, DEV_MODE])
@pytest.mark.usefixtures("patch_dlt_config")
def test_settings_invalid_boolean_variants_raises(
    value: bool, input_arg_name: str, settings_cls: type[CtsSettings]
) -> None:
    """Ensure that each invalid boolean value is throws an error."""
    with pytest.raises(ValidationError, match="Input should be a valid boolean"):
        make_settings_autofill_config(settings_cls, {input_arg_name: value})  # type: ignore[reportArgumentType]


@pytest.mark.parametrize("settings_cls", SETTINGS_CLASSES)
@pytest.mark.parametrize(("input_arg", "value"), TRUE_FALSE_VALUES)
@pytest.mark.parametrize("input_arg_name", [USE_OUTPUT_DIR_FOR_PIPELINE_METADATA, DEV_MODE])
@pytest.mark.usefixtures("patch_dlt_config")
def test_cli_app_run_boolean_variants_accepted(
    input_arg: str, value: bool, input_arg_name: str, settings_cls: type[CtsSettings]
) -> None:
    """Ensure that each invalid boolean value is throws an error."""
    s = CliApp.run(settings_cls, cli_args=[make_cli_arg(input_arg_name), str(input_arg)])
    if input_arg_name in ARG_ALIASES[USE_OUTPUT_DIR_FOR_PIPELINE_METADATA]:
        assert s.use_output_dir_for_pipeline_metadata == value
    elif input_arg_name in ARG_ALIASES[DEV_MODE]:
        assert s.dev_mode == value


@pytest.mark.parametrize("settings_cls", SETTINGS_CLASSES)
@pytest.mark.parametrize("value", INVALID_BOOLEAN_VALUES)
@pytest.mark.parametrize(
    "input_arg_name",
    [*ARG_ALIASES[USE_OUTPUT_DIR_FOR_PIPELINE_METADATA], *ARG_ALIASES[DEV_MODE]],
)
@pytest.mark.usefixtures("patch_dlt_config")
def test_cli_app_run_invalid_boolean_values_raises(
    value: bool, input_arg_name: str, settings_cls: type[CtsSettings]
) -> None:
    """Ensure that each invalid boolean value is throws an error."""
    with pytest.raises(ValidationError, match="Input should be a valid boolean"):
        CliApp.run(settings_cls, cli_args=[make_cli_arg(input_arg_name), str(value)])


# input and output path coercion
@pytest.mark.parametrize("settings_cls", SETTINGS_CLASSES)
@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("/some/path/", "/some/path"),
        ("/some/path//", "/some/path"),
        ("/some/path", "/some/path"),
        ("/", "/"),
        ("", ""),
    ],
)
@pytest.mark.parametrize("field_name", [INPUT_DIR, OUTPUT_DIR])
@pytest.mark.usefixtures("patch_dlt_config")
def test_settings_trailing_slash_stripped(
    settings_cls: type[CtsSettings],
    raw: str,
    expected: str,
    field_name: str,
) -> None:
    """Ensure that validate_dir_path removes trailing slashes but leaves directory slashes intact."""
    s = make_settings_autofill_config(settings_cls, {field_name: raw})
    # output_dir gets filled in with the default if it is falsy
    if field_name == OUTPUT_DIR and raw == "":
        expected = "/output_dir"
    assert getattr(s, field_name) == expected


# CLI App: ignore extra properties
@pytest.mark.parametrize("settings_cls", SETTINGS_CLASSES)
@pytest.mark.usefixtures("patch_dlt_config")
def test_cli_app_run_invalid_params_ignored(settings_cls: type[CtsSettings]) -> None:
    """Test that invalid parameter values are ignored."""
    s = CliApp.run(
        settings_cls,
        cli_args=[
            "--some_random_arg",
            "some value",
            "-q",
            "answer",
        ],
    )
    output = s.model_dump()

    assert "some value" not in output.values()
    assert "answer" not in output.values()


# values set during reconcile_with_dlt_config
@pytest.mark.parametrize("settings_cls", SETTINGS_CLASSES)
@pytest.mark.parametrize(USE_DESTINATION, VALID_DESTINATIONS)
@pytest.mark.usefixtures("patch_dlt_config")
def test_settings_reconcile_with_dlt_config_output_resolved_from_dlt_config_bucket_url(
    settings_cls: type[CtsSettings],
    use_destination: str,
    dlt_config: dict[str, Any],
) -> None:
    """When output_dir is empty, it is populated from dlt config's bucket_url."""
    s = make_settings_autofill_config(settings_cls, {OUTPUT_DIR: "", USE_DESTINATION: use_destination})
    assert s.output_dir == dlt_config[f"destination.{use_destination}.bucket_url"]


# properties derived from self.output_dir: pipeline_dir and raw_data_dir
@pytest.mark.parametrize("settings_cls", SETTINGS_CLASSES)
@pytest.mark.parametrize(
    OUTPUT_DIR,
    list(OUTPUT_PATHS.keys()),
)
@pytest.mark.parametrize(USE_OUTPUT_DIR_FOR_PIPELINE_METADATA, [True, False])
@pytest.mark.parametrize(USE_DESTINATION, VALID_DESTINATIONS)
@pytest.mark.usefixtures("patch_dlt_config")
def test_settings_generate_pipeline_raw_data_dirs(
    settings_cls: type[CtsSettings],
    output_dir: str,
    use_output_dir_for_pipeline_metadata: bool,
    use_destination: str,
) -> None:
    """Ensure that the correct paths are generated for pipeline and raw data directories.

    Ensure that the destination set in `use_destination` concurs with any output_dir path set.

    Ensure that pipeline directories cannot be set if the output_dir is set to s3.
    """
    make_settings_args = {
        OUTPUT_DIR: output_dir,
        USE_DESTINATION: use_destination,
        USE_OUTPUT_DIR_FOR_PIPELINE_METADATA: use_output_dir_for_pipeline_metadata,
    }

    expected = {
        **DEFAULT_CTS_SETTINGS_RECONCILED,
        USE_DESTINATION: use_destination,
        USE_OUTPUT_DIR_FOR_PIPELINE_METADATA: use_output_dir_for_pipeline_metadata,
        OUTPUT_DIR: DESTINATION_TO_OUTPUT[use_destination] if output_dir == "" else OUTPUT_PATHS[output_dir][OUT],
    }
    if settings_cls == BatchedFileInputSettings:
        expected = {**DEFAULT_BATCH_FILE_SETTINGS_RECONCILED, **expected}

    if (OUTPUT_PATHS[expected[OUTPUT_DIR]][S3] and use_destination == "local_fs") or (
        OUTPUT_PATHS[expected[OUTPUT_DIR]][S3] is False and use_destination == "s3"
    ):
        with pytest.raises(ValueError, match="Mismatch between output location and use_destination"):
            make_settings_autofill_config(settings_cls, make_settings_args)
        return

    if use_output_dir_for_pipeline_metadata and OUTPUT_PATHS[expected[OUTPUT_DIR]][S3] is True:
        # can't have pipeline dir on s3
        with pytest.raises(ValueError, match="It is not currently possible to have the pipeline directory on s3"):
            make_settings_autofill_config(settings_cls, make_settings_args)
        return

    s = make_settings_autofill_config(settings_cls, make_settings_args)

    # get the pipeline and raw data dirs from OUTPUT_PATHS
    expected["raw_data_dir"] = OUTPUT_PATHS[expected[OUTPUT_DIR]][RAW]
    # No pipeline_dir if use_output_dir_for_pipeline_metadata is not set
    expected["pipeline_dir"] = (
        OUTPUT_PATHS[expected[OUTPUT_DIR]][PIPE] if use_output_dir_for_pipeline_metadata else None
    )
    check_settings(s, expected)
