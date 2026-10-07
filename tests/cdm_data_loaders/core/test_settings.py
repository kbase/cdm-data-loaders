"""Tests for cdm_data_loaders.core.settings."""

from collections.abc import Mapping
from typing import Final, cast

import pytest
from frozendict import frozendict
from pydantic import AliasChoices, AliasPath, Field, ValidationError
from pydantic_settings import BaseSettings, CliApp, SettingsConfigDict, SettingsError

from cdm_data_loaders.core.destination import PIPELINE_METADATA_NOT_LOCAL
from cdm_data_loaders.core.fields import (
    BUFFER_SIZE,
    DLT_DEV_MODE,
    INPUT_DIR,
    LOG_CONFIG_FILE,
    LOG_INTERVAL,
    OUTPUT_DIR,
    USE_DESTINATION,
    USE_OUTPUT_DIR_FOR_PIPELINE_METADATA,
)
from cdm_data_loaders.core.settings import (
    DEFAULT_SETTINGS_CONFIG_DICT,
    CdmDataLoadersBase,
    CtsSettings,
    InputOutputSettings,
    LoggerSettings,
    _alias_names,
    default_settings_with_shortcuts,
)
from tests.cdm_data_loaders.core.conftest import SETTINGS_SOURCES, SettingsFactory

SETTINGS_LOGGER: Final[str] = "cdm_data_loaders.core.settings"
OUTPUT_IS_LOCAL: Final[str] = "output_is_local"
RAW_DATA_DIR: Final[str] = "raw_data_dir"
PIPELINE_DIR: Final[str] = "pipeline_dir"
NON_EMPTY: Final[str] = "String should have at least 1 character"
INVALID_BOOL: Final[str] = "Input should be a valid boolean, unable to interpret input"
NOT_POSITIVE: Final[str] = "Input should be greater than 0"

CTS_DEFAULT_DUMP: Final = frozendict(
    {
        BUFFER_SIZE: 100,
        DLT_DEV_MODE: False,
        INPUT_DIR: "/input_dir",
        LOG_CONFIG_FILE: None,
        LOG_INTERVAL: 1000,
        OUTPUT_DIR: None,
        USE_DESTINATION: "local_fs",
        USE_OUTPUT_DIR_FOR_PIPELINE_METADATA: False,
        OUTPUT_IS_LOCAL: None,
        RAW_DATA_DIR: None,
        PIPELINE_DIR: None,
    }
)


def define_settings(
    fields: Mapping[str, object],
    cli_shortcuts: Mapping[str, str | list[str]] | None = None,
    *,
    cli_kebab_case: bool = True,
) -> type[CdmDataLoadersBase]:
    """Define a str-field CdmDataLoadersBase subclass named 'Probe'; class creation runs check_aliases."""
    config = SettingsConfigDict(cli_kebab_case=cli_kebab_case)
    if cli_shortcuts is not None:
        config["cli_shortcuts"] = dict(cli_shortcuts)
    namespace = {"__annotations__": dict.fromkeys(fields, str), **fields, "model_config": config}
    return cast("type[CdmDataLoadersBase]", type("Probe", (CdmDataLoadersBase,), namespace))


def io_dump(
    input_dir: str = "/input_dir", output_dir: str | None = None, log_config_file: str | None = None
) -> dict[str, str | None]:
    """Expected model_dump() of an InputOutputSettings object."""
    return {INPUT_DIR: input_dir, OUTPUT_DIR: output_dir, LOG_CONFIG_FILE: log_config_file}


# _alias_names


@pytest.mark.parametrize(
    ("alias", "expected"),
    [
        pytest.param(None, set(), id="none"),
        pytest.param("alpha", {"alpha"}, id="str"),
        pytest.param(AliasPath("outer", "inner", 0), {"outer"}, id="alias_path_str_first"),
        pytest.param(AliasPath(0), set(), id="alias_path_int_first"),  # pyright: ignore[reportArgumentType]
        pytest.param(AliasChoices("a", "b"), {"a", "b"}, id="alias_choices_str"),
        pytest.param(
            AliasChoices("a", AliasPath("b", 0), AliasPath(1, "c")),  # pyright: ignore[reportArgumentType]
            {"a", "b"},
            id="alias_choices_mixed",
        ),
    ],
)
def test_alias_names_pass(alias: str | AliasChoices | AliasPath | None, expected: set[str]) -> None:
    """Each alias form yields its top-level string names only."""
    assert _alias_names(alias) == expected


# default_settings_with_shortcuts


@pytest.mark.parametrize(
    ("kwargs", "expected_extra"),
    [
        pytest.param({}, {}, id="no_args"),
        pytest.param({"cli_prog_name": "uniref"}, {"cli_prog_name": "uniref"}, id="prog_name"),
        pytest.param(
            {"cli_shortcuts": {"input_dir": "i", "output_dir": ["o", "out"]}},
            {"cli_shortcuts": {"input-dir": "i", "output-dir": ["o", "out"]}},
            id="shortcuts_kebab_cased",
        ),
    ],
)
def test_default_settings_with_shortcuts_pass(kwargs: dict[str, object], expected_extra: dict[str, object]) -> None:
    """The default config is extended with cli_prog_name and kebab-cased cli_shortcuts."""
    assert default_settings_with_shortcuts(**kwargs) == {**DEFAULT_SETTINGS_CONFIG_DICT, **expected_extra}  # pyright: ignore[reportArgumentType]


# CdmDataLoadersBase.check_aliases


@pytest.mark.parametrize(
    ("fields", "cli_shortcuts", "cli_kebab_case"),
    [
        pytest.param({}, None, True, id="no_fields"),
        pytest.param({"alpha": "a", "beta": "b"}, {"alpha": "a"}, True, id="str_shortcut"),
        pytest.param({"alpha": "a"}, {"alpha": ["a", "x"]}, True, id="list_shortcuts"),
        pytest.param(
            {"alpha": Field("a", validation_alias=AliasChoices("first", AliasPath("second", 0))), "beta": "b"},
            {"second": "s"},
            True,
            id="shortcut_targets_alias_path_name",
        ),
        pytest.param(
            {"alpha_beta": Field("x", validation_alias="alpha-beta")},
            {"alpha_beta": "a"},
            False,
            id="no_kebab-own_alias_differs_only_by_dash",
        ),
    ],
)
def test_check_aliases_pass_valid_names(
    fields: dict[str, object], cli_shortcuts: dict[str, str | list[str]] | None, cli_kebab_case: bool
) -> None:
    """Non-colliding names, aliases and shortcuts are accepted when the class is defined."""
    settings_cls = define_settings(fields, cli_shortcuts, cli_kebab_case=cli_kebab_case)
    assert settings_cls.check_aliases() is None


@pytest.mark.parametrize(
    ("fields", "cli_shortcuts", "expected"),
    [
        pytest.param(
            {"help": "x"}, None, "Probe: CLI name 'help' for 'help' is reserved by argparse", id="reserved_field_name"
        ),
        pytest.param(
            {"alpha": "a"},
            {"alpha": "h"},
            "Probe: CLI name 'h' for 'alpha' is reserved by argparse",
            id="reserved_shortcut",
        ),
        pytest.param(
            {"alpha": "a", "beta": "b"},
            {"alpha": "beta"},
            "Probe: CLI name 'beta' is claimed by both 'alpha' and 'beta'",
            id="shortcut_is_other_field_name",
        ),
        pytest.param(
            {"alpha": "a", "beta": "b"},
            {"alpha": "x", "beta": "x"},
            "Probe: CLI name 'x' is claimed by both 'alpha' and 'beta'",
            id="same_shortcut_two_fields",
        ),
        pytest.param(
            {"log_config_file": "a", "other": "b"},
            {"other": "log_config_file"},
            "Probe: CLI name 'log_config_file' is claimed by both 'log_config_file' and 'other'",
            id="underscore_dash_collision",
        ),
        pytest.param(
            {"alpha": Field("a", alias="beta"), "beta": "b"},
            None,
            "Probe: CLI name 'beta' is claimed by both 'alpha' and 'beta'",
            id="alias_is_other_field_name",
        ),
        pytest.param(
            {"output_dir": "x"},
            {"output-dir": "output-dir"},
            "Probe: CLI name 'output-dir' is claimed twice by 'output_dir'",
            id="shortcut_is_own_name",
        ),
        pytest.param(
            {"alpha": "a"},
            {"alpha": ["a", "a"]},
            "Probe: CLI name 'a' is claimed twice by 'alpha'",
            id="repeated_shortcut",
        ),
        pytest.param(
            {"input_dir": "x"},
            {"input_dir": "i"},
            "Probe: cli_shortcuts target 'input_dir' does not match any CLI argument (did you mean 'input-dir'?)",
            id="target_not_kebab_case",
        ),
        pytest.param(
            {"alpha": "a"},
            {"nope": "n"},
            "Probe: cli_shortcuts target 'nope' does not match any CLI argument",
            id="unknown_target",
        ),
        pytest.param(
            {"alpha": "a"},
            {"alpha": "-a"},
            "Probe: invalid shortcut '-a' for 'alpha'; omit the leading dashes",
            id="leading_dash",
        ),
        pytest.param(
            {"alpha": "a"}, {"alpha": ""}, "Probe: invalid shortcut '' for 'alpha'; omit the leading dashes", id="empty"
        ),
        pytest.param(
            {"alpha": "a"},
            {"alpha": ["-a", "h"], "nope": "n"},
            "Probe: invalid shortcut '-a' for 'alpha'; omit the leading dashes\nProbe: CLI name 'h' for 'alpha' is reserved by argparse\nProbe: cli_shortcuts target 'nope' does not match any CLI argument",
            id="all_errors_reported",
        ),
    ],
)
def test_check_aliases_fail_invalid_names(
    fields: dict[str, object], cli_shortcuts: dict[str, str | list[str]] | None, expected: str
) -> None:
    """Reserved, colliding, mistargeted or malformed names raise a SettingsError when the class is defined."""
    with pytest.raises(SettingsError) as exc_info:
        define_settings(fields, cli_shortcuts)
    assert str(exc_info.value) == expected


# production settings classes


@pytest.mark.parametrize(
    ("settings_cls", "cli_args", "field_name", "expected"),
    [
        pytest.param(InputOutputSettings, ["-i", "/in/"], INPUT_DIR, "/in", id="InputOutputSettings--i"),
        pytest.param(InputOutputSettings, ["-o", "/out/"], OUTPUT_DIR, "/out", id="InputOutputSettings--o"),
        pytest.param(CtsSettings, ["-i", "/in/"], INPUT_DIR, "/in", id="CtsSettings--i"),
        pytest.param(CtsSettings, ["-o", "/out/"], OUTPUT_DIR, "/out", id="CtsSettings--o"),
        pytest.param(CtsSettings, ["-d", "s3"], USE_DESTINATION, "s3", id="CtsSettings--d"),
        pytest.param(CtsSettings, ["-p", "true"], USE_OUTPUT_DIR_FOR_PIPELINE_METADATA, True, id="CtsSettings--p"),
    ],
)
def test_settings_classes_pass_cli_shortcuts(
    settings_cls: type[BaseSettings], cli_args: list[str], field_name: str, expected: object
) -> None:
    """Each configured shortcut is registered on the CLI and populates its target field."""
    assert getattr(CliApp.run(settings_cls, cli_args=cli_args), field_name) == expected


@pytest.mark.parametrize("source", SETTINGS_SOURCES)
@pytest.mark.parametrize(
    ("values", "expected"),
    [
        pytest.param({}, io_dump(), id="not_given"),
        pytest.param(
            {INPUT_DIR: "/in/", OUTPUT_DIR: "/out//", LOG_CONFIG_FILE: "log.json"},
            io_dump("/in", "/out", "log.json"),
            id="trailing_slashes",
        ),
        pytest.param({INPUT_DIR: "/", OUTPUT_DIR: "///"}, io_dump("/", "/"), id="root"),
        pytest.param({INPUT_DIR: " /in/ ", OUTPUT_DIR: " /out/ "}, io_dump("/in", "/out"), id="surrounding_whitespace"),
        pytest.param(
            {INPUT_DIR: "s3://bucket/in/", OUTPUT_DIR: "s3a://bucket/out//"},
            io_dump("s3://bucket/in", "s3a://bucket/out"),
            id="url_trailing_slashes",
        ),
        pytest.param(
            {INPUT_DIR: "file:///", OUTPUT_DIR: "s3://"}, io_dump("file:///", "s3://"), id="bare_protocol_roots"
        ),
        pytest.param({OUTPUT_DIR: ""}, io_dump(), id="empty_output_dir_is_unset"),
        pytest.param({OUTPUT_DIR: "  \t "}, io_dump(), id="whitespace_output_dir_is_unset"),
    ],
)
def test_input_output_settings_pass_dir_values(
    make_settings: SettingsFactory, source: str, values: dict[str, str], expected: dict[str, str | None]
) -> None:
    """Directory values are stripped and normalised; a blank output_dir becomes None."""
    assert make_settings(InputOutputSettings, source, values).model_dump() == expected


@pytest.mark.parametrize("source", SETTINGS_SOURCES)
@pytest.mark.parametrize(
    ("values", "expected"),
    [
        pytest.param({}, CTS_DEFAULT_DUMP, id="defaults"),
        pytest.param(
            {
                BUFFER_SIZE: "25",
                DLT_DEV_MODE: "true",
                INPUT_DIR: "/in/",
                LOG_CONFIG_FILE: "log.json",
                LOG_INTERVAL: "5",
                OUTPUT_DIR: "/out/",
                USE_DESTINATION: "custom_dest",
                USE_OUTPUT_DIR_FOR_PIPELINE_METADATA: "true",
            },
            {
                BUFFER_SIZE: 25,
                DLT_DEV_MODE: True,
                INPUT_DIR: "/in",
                LOG_CONFIG_FILE: "log.json",
                LOG_INTERVAL: 5,
                OUTPUT_DIR: "/out",
                USE_DESTINATION: "custom_dest",
                USE_OUTPUT_DIR_FOR_PIPELINE_METADATA: True,
                OUTPUT_IS_LOCAL: True,
                RAW_DATA_DIR: "/out/raw_data",
                PIPELINE_DIR: "/out/.dlt_conf",
            },
            id="all_values",
        ),
    ],
)
def test_cts_settings_pass_values(
    make_settings: SettingsFactory, source: str, values: dict[str, str], expected: Mapping[str, object]
) -> None:
    """CtsSettings applies defaults, coerces string input, and derives computed fields, from every source."""
    assert make_settings(CtsSettings, source, values).model_dump() == expected


def test_cts_settings_pass_unknown_cli_args_ignored() -> None:
    """Unrecognised CLI arguments are ignored rather than raising."""
    settings = CliApp.run(CtsSettings, cli_args=["--not-a-field", "x", "-q", "y", "--output-dir", "/out"])
    assert settings.model_dump() == {
        **CTS_DEFAULT_DUMP,
        OUTPUT_DIR: "/out",
        OUTPUT_IS_LOCAL: True,
        RAW_DATA_DIR: "/out/raw_data",
    }


@pytest.mark.parametrize(
    ("output_dir", "use_output_dir_for_pipeline_metadata", "expected"),
    [
        pytest.param(None, False, (None, None, None, None), id="unresolved"),
        pytest.param(None, True, (None, None, None, None), id="unresolved-metadata"),
        pytest.param("/out", False, ("/out", True, "/out/raw_data", None), id="local"),
        pytest.param("/out", True, ("/out", True, "/out/raw_data", "/out/.dlt_conf"), id="local-metadata"),
        pytest.param("/", True, ("/", True, "/raw_data", "/.dlt_conf"), id="root-metadata"),
        pytest.param(
            "relative/out/",
            True,
            ("relative/out", True, "relative/out/raw_data", "relative/out/.dlt_conf"),
            id="relative",
        ),
        pytest.param("file:///", True, ("file:///", True, "file:///raw_data", "file:///.dlt_conf"), id="file_url_root"),
        pytest.param("s3://bucket/key/", False, ("s3://bucket/key", False, "s3://bucket/key/raw_data", None), id="s3"),
        pytest.param(
            "s3a://bucket/key", False, ("s3a://bucket/key", False, "s3a://bucket/key/raw_data", None), id="s3a"
        ),
    ],
)
def test_cts_settings_pass_computed_fields(
    output_dir: str | None,
    use_output_dir_for_pipeline_metadata: bool,
    expected: tuple[str | None, bool | None, str | None, str | None],
) -> None:
    """output_is_local, raw_data_dir and pipeline_dir are derived from the normalised output_dir."""
    settings = CtsSettings(
        output_dir=output_dir, use_output_dir_for_pipeline_metadata=use_output_dir_for_pipeline_metadata
    )  # pyright: ignore[reportCallIssue]
    assert (settings.output_dir, settings.output_is_local, settings.raw_data_dir, settings.pipeline_dir) == expected


@pytest.mark.parametrize("source", SETTINGS_SOURCES)
@pytest.mark.parametrize(
    ("output_dir", "reported_url"),
    [
        pytest.param("s3://bucket/key", "s3://bucket/key", id="s3"),
        pytest.param("s3a://bucket/key/", "s3a://bucket/key", id="s3a_normalised"),
        pytest.param("gs://bucket", "gs://bucket", id="gs"),
    ],
)
def test_cts_settings_fail_remote_pipeline_metadata(
    make_settings: SettingsFactory, source: str, output_dir: str, reported_url: str
) -> None:
    """Pipeline metadata cannot be written to an explicit remote output_dir."""
    with pytest.raises(ValidationError) as exc_info:
        make_settings(CtsSettings, source, {OUTPUT_DIR: output_dir, USE_OUTPUT_DIR_FOR_PIPELINE_METADATA: "true"})
    assert [(err["loc"], err["msg"]) for err in exc_info.value.errors()] == [
        ((), f"Value error, {PIPELINE_METADATA_NOT_LOCAL.format(url=reported_url)}")
    ]


@pytest.mark.parametrize("source", SETTINGS_SOURCES)
@pytest.mark.parametrize(
    ("settings_cls", "values", "expected_errors"),
    [
        pytest.param(LoggerSettings, {LOG_CONFIG_FILE: ""}, [((LOG_CONFIG_FILE,), NON_EMPTY)], id="empty_log_config"),
        pytest.param(InputOutputSettings, {INPUT_DIR: ""}, [((INPUT_DIR,), NON_EMPTY)], id="empty_input_dir"),
        pytest.param(InputOutputSettings, {INPUT_DIR: "   "}, [((INPUT_DIR,), NON_EMPTY)], id="blank_input_dir"),
        pytest.param(CtsSettings, {BUFFER_SIZE: "0"}, [((BUFFER_SIZE,), NOT_POSITIVE)], id="zero_buffer_size"),
        pytest.param(CtsSettings, {LOG_INTERVAL: "0"}, [((LOG_INTERVAL,), NOT_POSITIVE)], id="zero_log_interval"),
        pytest.param(
            CtsSettings,
            {LOG_INTERVAL: "1.5"},
            [((LOG_INTERVAL,), "Input should be a valid integer, unable to parse string as an integer")],
            id="non_int_log_interval",
        ),
        pytest.param(CtsSettings, {DLT_DEV_MODE: "maybe"}, [((DLT_DEV_MODE,), INVALID_BOOL)], id="invalid_dev_mode"),
        pytest.param(
            CtsSettings,
            {USE_OUTPUT_DIR_FOR_PIPELINE_METADATA: "2"},
            [((USE_OUTPUT_DIR_FOR_PIPELINE_METADATA,), INVALID_BOOL)],
            id="invalid_metadata_flag",
        ),
        pytest.param(CtsSettings, {USE_DESTINATION: ""}, [((USE_DESTINATION,), NON_EMPTY)], id="empty_destination"),
        pytest.param(CtsSettings, {USE_DESTINATION: "  "}, [((USE_DESTINATION,), NON_EMPTY)], id="blank_destination"),
    ],
)
def test_settings_classes_fail_invalid_values(
    make_settings: SettingsFactory,
    source: str,
    settings_cls: type[BaseSettings],
    values: dict[str, str],
    expected_errors: list[tuple[tuple[str, ...], str]],
) -> None:
    """Invalid field values are rejected with a precise validation error from every source."""
    with pytest.raises(ValidationError) as exc_info:
        make_settings(settings_cls, source, values)
    assert [(err["loc"], err["msg"]) for err in exc_info.value.errors()] == expected_errors
