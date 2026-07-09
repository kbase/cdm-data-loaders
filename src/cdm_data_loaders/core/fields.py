"""Common defaults for running pipelines on the KBase CTS."""

from typing import Annotated, Any, Final

import dlt.common.configuration.accessors
from frozendict import frozendict
from pydantic import AliasChoices, Field
from pydantic_settings import CliSuppress

from cdm_data_loaders.utils.batcher import MIN_START_AT

# TODO: frozendict can be moved to the stdlib implementation when py 3.15 is released.


INPUT_MOUNT: Final[str] = "/input_dir"
OUTPUT_MOUNT: Final[str] = "/output_dir"

VALID_DESTINATIONS: list[str] = ["local_fs", "s3"]

# Common fields
DEV_MODE = "dev_mode"
INPUT_DIR = "input_dir"
LOG_CONFIG_FILE = "log_config_file"
OUTPUT = "output"
START_AT = "start_at"
USE_DESTINATION = "use_destination"
USE_OUTPUT_DIR_FOR_PIPELINE_METADATA = "use_output_dir_for_pipeline_metadata"

# Default values for the common fields
DEFAULTS = frozendict(
    {
        DEV_MODE: False,
        INPUT_DIR: INPUT_MOUNT,
        LOG_CONFIG_FILE: None,
        # N.b. this gets replaced by destination.local_fs.bucket_url
        OUTPUT: "",
        START_AT: MIN_START_AT,
        USE_DESTINATION: "local_fs",
        USE_OUTPUT_DIR_FOR_PIPELINE_METADATA: False,
    }
)

DEFAULT_PIPELINE_BATCH_SIZE: Final[int] = 50

# short aliases for the fields, used for CLI parsing
SHORT_ALIASES = frozendict(
    {
        DEV_MODE: [],
        USE_DESTINATION: ["-d"],
        USE_OUTPUT_DIR_FOR_PIPELINE_METADATA: ["-p"],
    }
)


def generate_aliases(field_name: str, short_aliases: bool = True) -> list[str]:  # noqa: FBT001, FBT002
    """Generate a list of aliases for a given field name.

    :param field_name: The name of the field for which to generate aliases.
    :type field_name: str
    :param short_aliases: Whether to include short aliases.
    :type short_aliases: bool
    :return: A list of aliases for the field.
    :rtype: list[str]
    """
    field_names = [f"--{field_name}"]
    if "_" in field_name:
        field_names.append(f"--{field_name.replace('_', '-')}")

    if short_aliases:
        return [*SHORT_ALIASES.get(field_name, [f"-{field_name[0]}"]), *field_names]
    return field_names


ARG_ALIASES = frozendict(
    {
        k: generate_aliases(k)
        for k in [
            DEV_MODE,
            INPUT_DIR,
            LOG_CONFIG_FILE,
            OUTPUT,
            START_AT,
            USE_DESTINATION,
            USE_OUTPUT_DIR_FOR_PIPELINE_METADATA,
        ]
    }
)

DevMode = Annotated[
    bool,
    Field(
        default=DEFAULTS[DEV_MODE],
        description="Whether to run the pipeline in dev mode, which saves raw API responses to disk and disables compression for easier debugging.",
        validation_alias=AliasChoices(*[alias.strip("-") for alias in ARG_ALIASES[DEV_MODE]]),
    ),
]
# this should really just be _Accessor but leaving the dict version in for ease of testing
DltConfig = Annotated[
    CliSuppress[dlt.common.configuration.accessors._Accessor | dict[str, Any]],  # noqa: SLF001
    Field(
        description="DLT configuration for the pipeline.",
    ),
]
InputDir = Annotated[
    str,
    Field(
        default=DEFAULTS[INPUT_DIR],
        description="Location of directory containing file(s) to import",
        # explicitly allow both kebab case and snake case
        validation_alias=AliasChoices(*[alias.strip("-") for alias in ARG_ALIASES[INPUT_DIR]]),
    ),
]
LogConfigFile = Annotated[
    str | None,
    Field(
        default=DEFAULTS[LOG_CONFIG_FILE],
        description="Location of configuration file for the logger",
        validation_alias=AliasChoices(*[alias.strip("-") for alias in ARG_ALIASES[LOG_CONFIG_FILE]]),
    ),
]
Output = Annotated[
    str,
    Field(
        default=DEFAULTS[OUTPUT],
        description="Location to save imported data to, if different from the default supplied by the destination config",
        validation_alias=AliasChoices(*[alias.strip("-") for alias in ARG_ALIASES[OUTPUT]]),
    ),
]
StartAt = Annotated[
    int,
    Field(
        default=DEFAULTS[START_AT],
        description="File to start import at",
        validation_alias=AliasChoices(*[alias.strip("-") for alias in ARG_ALIASES[START_AT]]),
    ),
]
UseDestination = Annotated[
    str,
    Field(
        default=DEFAULTS[USE_DESTINATION],
        description=f"DLT destination configuration to use for data output. Data to be saved to s3 should use the destination 's3'; to save data locally, use the destination 'local_fs'. The output directory can be specified using the 'output' field. Choices: {VALID_DESTINATIONS}",
        validation_alias=AliasChoices(*[alias.strip("-") for alias in ARG_ALIASES[USE_DESTINATION]]),
    ),
]
UseOutputDirForPipelineMetadata = Annotated[
    bool,
    Field(
        default=DEFAULTS[USE_OUTPUT_DIR_FOR_PIPELINE_METADATA],
        description="If true, use the output directory for pipeline metadata. Note: pipeline metadata cannot be stored in an S3 bucket, so this option should only be used when the destination is 'local_fs'.",
        validation_alias=AliasChoices(
            *[alias.strip("-") for alias in ARG_ALIASES[USE_OUTPUT_DIR_FOR_PIPELINE_METADATA]]
        ),
    ),
]
