"""Tests for the Settings objects used by DLT pipelines."""

from pathlib import Path
from typing import Any

import pytest
from pydantic import ValidationError
from pydantic_settings import CliApp

from cdm_data_loaders.pipelines.cts_defaults import (
    ARG_ALIASES,
    DEFAULT_START_AT,
    VALID_DESTINATIONS,
    BatchedFileInputSettings,
    CtsSettings,
)
from tests.pipelines.conftest import (
    DEFAULT_BATCH_FILE_SETTINGS_RECONCILED,
    DEFAULT_CTS_SETTINGS_RECONCILED,
    TEST_BATCH_FILE_SETTINGS,
    TEST_BATCH_FILE_SETTINGS_RECONCILED,
    TEST_CTS_SETTINGS,
    TEST_CTS_SETTINGS_RECONCILED,
    check_settings,
    make_settings,
)

SETTINGS_CLASSES = [CtsSettings, BatchedFileInputSettings]

INVALID_DESTINATIONS = ["gcs", "filesystem", "", "LocalFs", "S3"]
INVALID_BOOLEAN_VALUES = ["what", "yep", "nope", "2", -1, "", " ", "wtf", None]

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
def test_settings_all_settings_specified(
    dlt_config: dict[str, Any], settings_cls: type[CtsSettings], args: dict[str, Any], expected: dict[str, Any]
) -> None:
    """Ensure the CTS settings are set up correctly."""
    s = make_settings(settings_cls, dlt_config=dlt_config, **args)
    check_settings(s, dict(expected))


@pytest.mark.parametrize("settings_cls", SETTINGS_CLASSES)
def test_cli_app_run_default_settings(settings_cls: type[CtsSettings], dlt_config: dict[str, Any]) -> None:
    """Ensure the CTS settings are set up correctly, CLI version."""
    s = CliApp.run(settings_cls, dlt_config=dlt_config)
    expected = (
        DEFAULT_CTS_SETTINGS_RECONCILED if settings_cls == CtsSettings else DEFAULT_BATCH_FILE_SETTINGS_RECONCILED
    )
    check_settings(s, dict(expected))


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
def test_cli_app_run_dlt_config_errors(
    settings_cls: type[CtsSettings], dlt_config: None | dict[str, Any], error: type[Exception], err_msg: str
) -> None:
    """Test all the variants of the Settings fields."""
    with pytest.raises(error, match=err_msg):
        CliApp.run(settings_cls, dlt_config=dlt_config)


# destination tests
@pytest.mark.parametrize("settings_cls", SETTINGS_CLASSES)
@pytest.mark.parametrize("use_destination", VALID_DESTINATIONS)
def test_settings_valid_destinations_accepted(
    use_destination: str, settings_cls: type[CtsSettings], dlt_config: dict[str, Any]
) -> None:
    """Test valid destinations against the settings class."""
    s = make_settings(settings_cls, dlt_config=dlt_config, use_destination=use_destination)
    assert s.use_destination == use_destination


@pytest.mark.parametrize("settings_cls", SETTINGS_CLASSES)
@pytest.mark.parametrize("use_destination", INVALID_DESTINATIONS)
def test_settings_invalid_destination_raises(
    use_destination: str, settings_cls: type[CtsSettings], dlt_config: dict[str, Any]
) -> None:
    """Ensure that an unrecognised use_destination raises a ValidationError."""
    with pytest.raises(ValidationError, match=r"use_destination must be one of \['local_fs', 's3'\]"):
        make_settings(settings_cls, dlt_config=dlt_config, use_destination=use_destination)


@pytest.mark.parametrize("settings_cls", SETTINGS_CLASSES)
def test_settings_destination_has_no_bucket_url(settings_cls: type[CtsSettings]) -> None:
    """Ensure that destinations have a bucket_url."""
    with pytest.raises(ValueError, match="No bucket_url specified for destination local_fs"):
        make_settings(settings_cls, dlt_config={"destination": {"local_fs": None}}, use_destination="local_fs")


# destination tests, CLI versions
@pytest.mark.parametrize("settings_cls", SETTINGS_CLASSES)
@pytest.mark.parametrize("use_destination", VALID_DESTINATIONS)
@pytest.mark.parametrize("destination_arg", ARG_ALIASES["use_destination"])
def test_cli_app_run_valid_destinations_accepted(
    use_destination: str, settings_cls: type[CtsSettings], destination_arg: str, dlt_config: dict[str, Any]
) -> None:
    """Test valid destinations using the command line."""
    s = CliApp.run(settings_cls, dlt_config=dlt_config, cli_args=[destination_arg, use_destination])
    assert s.use_destination == use_destination


@pytest.mark.parametrize("settings_cls", SETTINGS_CLASSES)
@pytest.mark.parametrize("use_destination", INVALID_DESTINATIONS)
@pytest.mark.parametrize("destination_arg", ARG_ALIASES["use_destination"])
def test_cli_app_run_invalid_destinations_raises(
    use_destination: str, settings_cls: type[CtsSettings], destination_arg: str, dlt_config: dict[str, Any]
) -> None:
    """Test invalid destinations using the command line."""
    with pytest.raises(ValidationError, match="use_destination must be one of"):
        CliApp.run(settings_cls, dlt_config=dlt_config, cli_args=[destination_arg, use_destination])


@pytest.mark.parametrize("settings_cls", SETTINGS_CLASSES)
@pytest.mark.parametrize("destination_arg", ARG_ALIASES["use_destination"])
def test_cli_app_run_destination_has_no_bucket_url(settings_cls: type[CtsSettings], destination_arg: str) -> None:
    """Ensure that destinations have a bucket_url."""
    with pytest.raises(ValueError, match="No bucket_url specified for destination local_fs"):
        CliApp.run(settings_cls, dlt_config={"destination": {"local_fs": None}}, cli_args=[destination_arg, "local_fs"])


# boolean fields
@pytest.mark.parametrize("settings_cls", SETTINGS_CLASSES)
@pytest.mark.parametrize(("input_arg", "value"), TRUE_FALSE_VALUES)
@pytest.mark.parametrize("input_arg_name", ["use_output_dir_for_pipeline_metadata", "dev_mode"])
def test_settings_boolean_variants_accepted(
    input_arg: str, value: bool, input_arg_name: str, settings_cls: type[CtsSettings], dlt_config: dict[str, Any]
) -> None:
    """Ensure that each valid boolean value is accepted without error."""
    s = make_settings(settings_cls, dlt_config=dlt_config, **{input_arg_name: input_arg})  # type: ignore[reportArgumentType]
    assert getattr(s, input_arg_name) == value


@pytest.mark.parametrize("settings_cls", SETTINGS_CLASSES)
@pytest.mark.parametrize("value", INVALID_BOOLEAN_VALUES)
@pytest.mark.parametrize("input_arg_name", ["use_output_dir_for_pipeline_metadata", "dev_mode"])
def test_settings_invalid_boolean_variants_raises(
    value: bool, input_arg_name: str, settings_cls: type[CtsSettings], dlt_config: dict[str, Any]
) -> None:
    """Ensure that each invalid boolean value is throws an error."""
    with pytest.raises(ValidationError, match="Input should be a valid boolean"):
        make_settings(settings_cls, dlt_config=dlt_config, **{input_arg_name: value})  # type: ignore[reportArgumentType]


@pytest.mark.parametrize("settings_cls", SETTINGS_CLASSES)
@pytest.mark.parametrize(("input_arg", "value"), TRUE_FALSE_VALUES)
@pytest.mark.parametrize("input_arg_name", ["use_output_dir_for_pipeline_metadata", "dev_mode"])
def test_cli_app_run_boolean_variants_accepted(
    input_arg: str, value: bool, input_arg_name: str, settings_cls: type[CtsSettings], dlt_config: dict[str, Any]
) -> None:
    """Ensure that each invalid boolean value is throws an error."""
    s = CliApp.run(settings_cls, dlt_config=dlt_config, cli_args=[input_arg_name, str(input_arg)])
    if input_arg_name in ARG_ALIASES["use_output_dir_for_pipeline_metadata"]:
        assert s.use_output_dir_for_pipeline_metadata == value
    elif input_arg_name in ARG_ALIASES["dev_mode"]:
        assert s.dev_mode == value


@pytest.mark.parametrize("settings_cls", SETTINGS_CLASSES)
@pytest.mark.parametrize("value", INVALID_BOOLEAN_VALUES)
@pytest.mark.parametrize(
    "input_arg_name", [*ARG_ALIASES["use_output_dir_for_pipeline_metadata"], *ARG_ALIASES["dev_mode"]]
)
def test_cli_app_run_invalid_boolean_values_raises(
    value: bool, input_arg_name: str, settings_cls: type[CtsSettings], dlt_config: dict[str, Any]
) -> None:
    """Ensure that each invalid boolean value is throws an error."""
    with pytest.raises(ValidationError, match="Input should be a valid boolean"):
        CliApp.run(settings_cls, dlt_config=dlt_config, cli_args=[input_arg_name, str(value)])


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
@pytest.mark.parametrize("field_name", ["input_dir", "output"])
def test_settings_trailing_slash_stripped(
    settings_cls: type[CtsSettings],
    raw: str,
    expected: str,
    field_name: str,
    dlt_config: dict[str, Any],
) -> None:
    """Ensure that validate_dir_path removes trailing slashes but leaves directory slashes intact."""
    s = make_settings(settings_cls, dlt_config=dlt_config, **{field_name: raw})
    # output gets filled in with the default if it is falsy
    if field_name == "output" and raw == "":
        expected = "/output_dir"
    assert getattr(s, field_name) == expected


# All arguments set, using CliApp.run
@pytest.mark.parametrize("settings_cls", SETTINGS_CLASSES)
@pytest.mark.parametrize("dev_mode", ARG_ALIASES["dev_mode"])
@pytest.mark.parametrize("input_dir", ARG_ALIASES["input_dir"])
@pytest.mark.parametrize("output", ARG_ALIASES["output"])
@pytest.mark.parametrize("start_at", ARG_ALIASES["start_at"])
@pytest.mark.parametrize("use_destination", ARG_ALIASES["use_destination"])
@pytest.mark.parametrize(
    "use_output_dir_for_pipeline_metadata",
    ARG_ALIASES["use_output_dir_for_pipeline_metadata"],
)
def test_cli_app_run_alt_settings(  # noqa: PLR0913
    settings_cls: type[CtsSettings],
    dev_mode: str,
    input_dir: str,
    output: str,
    start_at: str,
    use_destination: str,
    use_output_dir_for_pipeline_metadata: str,
    dlt_config: dict[str, Any],
) -> None:
    """Test all the variants of the Settings fields."""
    # TEST_BATCH_FILE_SETTINGS is identical to TEST_CTS_SETTINGS, but also includes start_at
    cli_args = [
        dev_mode,
        TEST_BATCH_FILE_SETTINGS["dev_mode"],
        input_dir,
        TEST_BATCH_FILE_SETTINGS["input_dir"],
        output,
        TEST_BATCH_FILE_SETTINGS["output"],
        use_destination,
        TEST_BATCH_FILE_SETTINGS["use_destination"],
        use_output_dir_for_pipeline_metadata,
        TEST_BATCH_FILE_SETTINGS["use_output_dir_for_pipeline_metadata"],
    ]
    # add in start_at for the BatchedFileInputSettings
    if settings_cls == BatchedFileInputSettings:
        cli_args.extend([start_at, TEST_BATCH_FILE_SETTINGS["start_at"]])

    expected = TEST_CTS_SETTINGS_RECONCILED if settings_cls == CtsSettings else TEST_BATCH_FILE_SETTINGS_RECONCILED

    s = CliApp.run(
        settings_cls,
        dlt_config=dlt_config,
        cli_args=cli_args,
    )
    check_settings(s, expected)


# CLI App: ignore extra properties
@pytest.mark.parametrize("settings_cls", SETTINGS_CLASSES)
def test_cli_app_run_invalid_params_ignored(settings_cls: type[CtsSettings], dlt_config: dict[str, Any]) -> None:
    """Test that invalid parameter values are ignored."""
    s = CliApp.run(
        settings_cls,
        dlt_config=dlt_config,
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
@pytest.mark.parametrize("use_destination", VALID_DESTINATIONS)
def test_settings_reconcile_with_dlt_config_output_resolved_from_dlt_config_bucket_url(
    settings_cls: type[CtsSettings],
    use_destination: str,
    dlt_config: dict[str, Any],
) -> None:
    """When output is empty, it is populated from dlt config's bucket_url."""
    s = make_settings(
        settings_cls,
        dlt_config=dlt_config,
        output="",
        use_destination=use_destination,
    )
    assert s.output == dlt_config[f"destination.{use_destination}.bucket_url"]


# properties derived from self.output
@pytest.mark.parametrize("settings_cls", SETTINGS_CLASSES)
@pytest.mark.parametrize("output", ["", "/output/dir", "some/convoluted/path/to/dir/"])
@pytest.mark.parametrize("use_output_dir_for_pipeline_metadata", [True, False])
@pytest.mark.parametrize("use_destination", VALID_DESTINATIONS)
def test_settings_generate_pipeline_raw_data_dirs(
    settings_cls: type[CtsSettings],
    output: str,
    use_output_dir_for_pipeline_metadata: bool,
    dlt_config: dict[str, Any],
    use_destination: str,
) -> None:
    """Ensure that the correct paths are generated for pipeline and raw data directories."""
    s = make_settings(
        settings_cls,
        dlt_config=dlt_config,
        output=output,
        use_destination=use_destination,
        use_output_dir_for_pipeline_metadata=use_output_dir_for_pipeline_metadata,
    )

    expected = {
        **DEFAULT_CTS_SETTINGS_RECONCILED,
        "use_destination": use_destination,
        "use_output_dir_for_pipeline_metadata": use_output_dir_for_pipeline_metadata,
        "output": output.rstrip("/") or dlt_config[f"destination.{use_destination}.bucket_url"],
    }

    if settings_cls == BatchedFileInputSettings:
        expected["start_at"] = DEFAULT_START_AT

    # list containing the projected raw_data_dir and pipeline_dir
    expected_properties = {
        "": [str(Path(expected["output"]) / "raw_data"), str(Path(expected["output"]) / ".dlt_conf")],
        "/output/dir": [str(Path("/output/dir") / "raw_data"), str(Path("/output/dir") / ".dlt_conf")],
        "some/convoluted/path/to/dir/": [
            str(Path("some/convoluted/path/to/dir") / "raw_data"),
            str(Path("some/convoluted/path/to/dir") / ".dlt_conf"),
        ],
    }

    expected["raw_data_dir"] = expected_properties[output][0]
    expected["pipeline_dir"] = expected_properties[output][1] if use_output_dir_for_pipeline_metadata else None

    check_settings(s, expected)
