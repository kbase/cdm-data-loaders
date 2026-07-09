"""Shared fixtures for pipelines tests."""

from pathlib import Path
from typing import Any, Final

import dlt
import dlt.common.configuration.accessors
from frozendict import frozendict
from pydantic_settings import BaseSettings

from cdm_data_loaders.core.fields import (
    DEV_MODE,
    INPUT_DIR,
    LOG_CONFIG_FILE,
    OUTPUT,
    START_AT,
    USE_DESTINATION,
    USE_OUTPUT_DIR_FOR_PIPELINE_METADATA,
)
from cdm_data_loaders.core.settings import (
    DEFAULT_BATCH_FILE_SETTINGS,
    DEFAULT_CTS_SETTINGS,
    CtsSettings,
)
from tests.conftest import _generate_dlt_config, TEST_DLT_CONFIG

CASSETTES_DIR = "tests/cassettes"

START_AT_VALUE: Final[int] = 50
START_AT_STRING: Final[str] = "50"
TEST_LOG_CONFIG_FILE: Final[str] = "log_conf.json"

DESTINATION_TO_OUTPUT = frozendict(
    {
        "local_fs": TEST_DLT_CONFIG["destination.local_fs.bucket_url"],
        "s3": TEST_DLT_CONFIG["destination.s3.bucket_url"],
    }
)

DESTINATION_OUTPUT: Final[str] = DESTINATION_TO_OUTPUT[DEFAULT_CTS_SETTINGS["use_destination"]]

DEFAULT_CTS_SETTINGS_RECONCILED = frozendict(
    {
        **DEFAULT_CTS_SETTINGS,
        OUTPUT: DESTINATION_OUTPUT,
        "raw_data_dir": f"{DESTINATION_OUTPUT}/raw_data",
        "pipeline_dir": None,
    }
)

DEFAULT_BATCH_FILE_SETTINGS_RECONCILED = frozendict({**DEFAULT_BATCH_FILE_SETTINGS, **DEFAULT_CTS_SETTINGS_RECONCILED})

TEST_CTS_SETTINGS = frozendict(
    {
        DEV_MODE: "false",
        INPUT_DIR: "/dir/path",
        LOG_CONFIG_FILE: "some/path",
        OUTPUT: "/some/dir",
        USE_DESTINATION: "local_fs",
        USE_OUTPUT_DIR_FOR_PIPELINE_METADATA: "true",
    }
)

TEST_CTS_SETTINGS_RECONCILED = frozendict(
    {
        **TEST_CTS_SETTINGS,
        DEV_MODE: False,
        USE_OUTPUT_DIR_FOR_PIPELINE_METADATA: True,
        "pipeline_dir": "/some/dir/.dlt_conf",
        "raw_data_dir": "/some/dir/raw_data",
    }
)

TEST_BATCH_FILE_SETTINGS = frozendict(
    **TEST_CTS_SETTINGS,
    start_at=START_AT_STRING,
)

TEST_BATCH_FILE_SETTINGS_RECONCILED = frozendict(
    {
        **TEST_CTS_SETTINGS_RECONCILED,
        START_AT: START_AT_VALUE,
        "pipeline_dir": "/some/dir/.dlt_conf",
        "raw_data_dir": "/some/dir/raw_data",
    }
)


def generate_cli_arguments(
    alias_dict: dict[str, list[str]] | frozendict[str, list[str]],
    *args: dict[str, list[str]] | frozendict[str, list[str]],
) -> frozendict[str, list[str]]:
    """Generate the corresponding command line arguments from a list of aliases.

    :param alias_dict: dictionary of arguments and list of valid aliases
    :type alias_dict: dict[str, list[str]] | frozendict[str, list[str]]
    :param args: more alias_dicts
    :type args: dict[str, list[str]] | frozendict[str, list[str]]
    :return: dictionary of args and list of valid cmd line args
    :rtype: frozendict[str, list[str]]
    """
    all_aliases = {**alias_dict}
    for a in args:
        all_aliases = {**all_aliases, **a}

    return frozendict(
        {k: [f"-{item}" if len(item) == 1 else f"--{item}" for item in v] for k, v in all_aliases.items()}
    )


def make_settings(
    settings_cls: type[CtsSettings],
    dlt_config: dict[str, Any] | None = None,
    **kwargs: str | int | Path | dict[str, Any] | dlt.common.configuration.accessors._ConfigAccessor,
) -> BaseSettings:  # CtsSettings | BatchedFileInputSettings | NcbiRestApiSettings | AtbSettings:
    """Generate a validated Settings object."""
    return settings_cls(**{"dlt_config": dlt_config, **kwargs})


def make_settings_autofill_config(
    settings_cls: type[CtsSettings],
    **kwargs: str | int | Path | dict[str, Any] | bool | dlt.common.configuration.accessors._ConfigAccessor | None,
) -> (
    BaseSettings
):  # CtsSettings | BatchedFileInputSettings | NcbiRestApiSettings | AtbSettings | UniProtSettings | UnirefSettings:
    """Generate a validated Settings object, supplying the dlt_config if necessary."""
    if not kwargs:
        kwargs = {}
    if "dlt_config" not in kwargs:
        kwargs["dlt_config"] = _generate_dlt_config()
    return settings_cls.model_validate(kwargs)


def check_settings(
    settings_object: CtsSettings,
    expected: dict[str, Any] | frozendict,
) -> None:
    """Check that the settings object has the expected values."""
    assert settings_object.dlt_config is not None
    assert settings_object.model_dump(exclude={"dlt_config"}) == expected

    # make sure we have both raw_data_dir and pipeline_dir
    assert "raw_data_dir" in expected
    assert "pipeline_dir" in expected
    for attr, value in expected.items():
        assert getattr(settings_object, attr) == value
