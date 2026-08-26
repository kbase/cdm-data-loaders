"""Common defaults for running pipelines on the KBase CTS."""

from typing import Annotated, Any, Final

import dlt
import dlt.common.configuration.accessors
from frozendict import frozendict
from pydantic import AliasChoices, Field, PositiveInt

INPUT_MOUNT: Final[str] = "/input_dir"
OUTPUT_MOUNT: Final[str] = "/output_dir"

VALID_DESTINATIONS: list[str] = ["local_fs", "s3"]

# Common fields
BUFFER_SIZE = "buffer_size"
DEV_MODE = "dev_mode"
DLT_CONFIG = "dlt_config"
INPUT_DIR = "input_dir"
LOG_CONFIG_FILE = "log_config_file"
LOG_INTERVAL = "log_interval"
OUTPUT_DIR = "output_dir"
START_AT = "start_at"
USE_DESTINATION = "use_destination"
USE_OUTPUT_DIR_FOR_PIPELINE_METADATA = "use_output_dir_for_pipeline_metadata"


MIN_START_AT: Final[int] = 1

# Default values for the common fields
DEFAULTS = frozendict(
    {
        BUFFER_SIZE: 100,
        DEV_MODE: False,
        INPUT_DIR: INPUT_MOUNT,
        LOG_CONFIG_FILE: None,
        LOG_INTERVAL: 1000,
        # N.b. this gets replaced by destination.local_fs.bucket_url in CtsSettings and derivatives
        OUTPUT_DIR: "",
        START_AT: MIN_START_AT,
        USE_DESTINATION: "local_fs",
        USE_OUTPUT_DIR_FOR_PIPELINE_METADATA: False,
    }
)

DEFAULT_PIPELINE_BATCH_SIZE: Final[int] = 50


def generate_aliases(field_name: str, short_alias: str | None = None) -> AliasChoices:
    """Generate a list of aliases for a given field name.

    :param field_name: The name of the field for which to generate aliases.
    :type field_name: str
    :param short_aliases: Whether to include short aliases.
    :type short_aliases: bool
    :return: A non-redundant list of aliases for the field in an AliasChoices object.
    :rtype: AliasChoices
    """
    if not field_name:
        err_msg = "No field_name supplied"
        raise ValueError(err_msg)

    field_names = [field_name]
    if "_" in field_name:
        field_names.append(field_name.replace("_", "-"))

    if short_alias and short_alias not in field_names:
        field_names = [short_alias, *field_names]

    return AliasChoices(*field_names)


BufferSize = Annotated[
    PositiveInt,
    Field(
        default=DEFAULTS[BUFFER_SIZE],
        description="Number of rows to buffer per table before yielding a batch to the destination. Must be a positive integer.",
        validation_alias=generate_aliases(BUFFER_SIZE),
    ),
]
DevMode = Annotated[
    bool,
    Field(
        default=DEFAULTS[DEV_MODE],
        description="Whether to run the pipeline in dev mode, which saves raw API responses to disk and disables compression for easier debugging.",
        validation_alias=generate_aliases(DEV_MODE),
    ),
]
# this should really just be _Accessor but leaving the dict version in for ease of testing
DltConfig = Annotated[
    dlt.common.configuration.accessors._Accessor | dict[str, Any],  # noqa: SLF001
    Field(description="DLT configuration for the pipeline.", default_factory=lambda: dlt.config),
]
InputDir = Annotated[
    str,
    Field(
        default=DEFAULTS[INPUT_DIR],
        description="Location of directory containing file(s) to import",
        validation_alias=generate_aliases(INPUT_DIR, "i"),
    ),
]
LogConfigFile = Annotated[
    str | None,
    Field(
        default=DEFAULTS[LOG_CONFIG_FILE],
        description="Location of configuration file for the logger",
        validation_alias=generate_aliases(LOG_CONFIG_FILE),
    ),
]
LogInterval = Annotated[
    PositiveInt,
    Field(
        default=DEFAULTS[LOG_INTERVAL],
        description="How often (in number of processed entries) to emit a progress log message. Must be a positive integer.",
        validation_alias=generate_aliases(LOG_INTERVAL),
    ),
]
OutputDir = Annotated[
    str,
    Field(
        default=DEFAULTS[OUTPUT_DIR],
        description="Location to save imported data to, if different from the default supplied by the destination config",
        validation_alias=generate_aliases(OUTPUT_DIR, "o"),
    ),
]
StartAt = Annotated[
    int,
    Field(
        default=DEFAULTS[START_AT],
        description="File to start import at",
        validation_alias=generate_aliases(START_AT, "s"),
    ),
]
UseDestination = Annotated[
    str,
    Field(
        default=DEFAULTS[USE_DESTINATION],
        description=f"DLT destination configuration to use for data output. Data to be saved to s3 should use the destination 's3'; to save data locally, use the destination 'local_fs'. The output directory can be specified using the 'output_dir' field. Choices: {VALID_DESTINATIONS}",
        validation_alias=generate_aliases(USE_DESTINATION, "d"),
    ),
]
UseOutputDirForPipelineMetadata = Annotated[
    bool,
    Field(
        default=DEFAULTS[USE_OUTPUT_DIR_FOR_PIPELINE_METADATA],
        description="If true, use the output directory for pipeline metadata. Note: pipeline metadata cannot be stored in an S3 bucket, so this option should only be used when the destination is 'local_fs'.",
        validation_alias=generate_aliases(USE_OUTPUT_DIR_FOR_PIPELINE_METADATA, "p"),
    ),
]
