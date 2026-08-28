"""Common defaults for running pipelines on the KBase CTS."""

from typing import Annotated, Any, Final

import dlt
import dlt.common.configuration.accessors
from frozendict import frozendict
from pydantic import Field, PositiveInt

INPUT_MOUNT: Final[str] = "/input_dir"
OUTPUT_MOUNT: Final[str] = "/output_dir"

VALID_DESTINATIONS: Final[list[str]] = ["local_fs", "s3"]

# Common fields
BATCH_SIZE: Final[str] = "batch_size"
BUFFER_SIZE: Final[str] = "buffer_size"
DEV_MODE: Final[str] = "dev_mode"
DLT_CONFIG: Final[str] = "dlt_config"
INPUT_DIR: Final[str] = "input_dir"
LOG_CONFIG_FILE: Final[str] = "log_config_file"
LOG_INTERVAL: Final[str] = "log_interval"
OUTPUT_DIR: Final[str] = "output_dir"
START_AT: Final[str] = "start_at"
USE_DESTINATION: Final[str] = "use_destination"
USE_OUTPUT_DIR_FOR_PIPELINE_METADATA: Final[str] = "use_output_dir_for_pipeline_metadata"


MIN_START_AT: Final[int] = 1

# Default values for the common fields
DEFAULTS = frozendict(
    {
        BATCH_SIZE: 1000,
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


BatchSize = Annotated[
    PositiveInt,
    Field(
        default=DEFAULTS[BATCH_SIZE],
        description="Number of items per batch",
    ),
]

BufferSize = Annotated[
    PositiveInt,
    Field(
        default=DEFAULTS[BUFFER_SIZE],
        description="Number of rows to buffer per table before yielding a batch to the destination. Must be a positive integer.",
    ),
]
DevMode = Annotated[
    bool,
    Field(
        default=DEFAULTS[DEV_MODE],
        description="Whether to run the pipeline in dev mode, which saves raw API responses to disk and disables compression for easier debugging.",
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
    ),
]
LogConfigFile = Annotated[
    str | None,
    Field(
        default=DEFAULTS[LOG_CONFIG_FILE],
        description="Location of configuration file for the logger",
    ),
]
LogInterval = Annotated[
    PositiveInt,
    Field(
        default=DEFAULTS[LOG_INTERVAL],
        description="How often (in number of processed entries) to emit a progress log message. Must be a positive integer.",
    ),
]
OutputDir = Annotated[
    str,
    Field(
        default=DEFAULTS[OUTPUT_DIR],
        description="Location to save imported data to, if different from the default supplied by the destination config",
    ),
]
StartAt = Annotated[
    PositiveInt,
    Field(
        default=DEFAULTS[START_AT],
        description="File to start import at",
    ),
]
UseDestination = Annotated[
    str,
    Field(
        default=DEFAULTS[USE_DESTINATION],
        description=f"DLT destination configuration to use for data output. Data to be saved to s3 should use the destination 's3'; to save data locally, use the destination 'local_fs'. The output directory can be specified using the 'output_dir' field. Choices: {VALID_DESTINATIONS}",
    ),
]
UseOutputDirForPipelineMetadata = Annotated[
    bool,
    Field(
        default=DEFAULTS[USE_OUTPUT_DIR_FOR_PIPELINE_METADATA],
        description="If true, use the output directory for pipeline metadata. Note: pipeline metadata cannot be stored in an S3 bucket, so this option should only be used when the destination is 'local_fs'.",
    ),
]
