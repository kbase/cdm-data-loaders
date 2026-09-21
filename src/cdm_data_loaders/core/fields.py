"""Common defaults for running pipelines on the KBase CTS."""

from enum import StrEnum
from typing import Annotated, Any, Final

import dlt
import dlt.common.configuration.accessors
from frozendict import frozendict
from pydantic import Field, PositiveInt, StringConstraints
from pydantic_settings import CLI_SUPPRESS

INPUT_MOUNT: Final[str] = "/input_dir"
OUTPUT_MOUNT: Final[str] = "/output_dir"

DEFAULT_JSONL_FILE_GLOB: Final[str] = "*.jsonl*"
DEFAULT_XML_FILE_GLOB: Final[str] = "*.xml*"
GZIP_SUFFIX: Final[str] = ".gz"

# output file formats
JSONL: Final[str] = "jsonl"
PARQUET: Final[str] = "parquet"

# destinations
LOCAL_FS: Final[str] = "local_fs"
S3: Final[str] = "s3"


VALID_DESTINATIONS: Final[list[str]] = [LOCAL_FS, S3]

# Common fields
BATCH_SIZE: Final[str] = "batch_size"
BUFFER_SIZE: Final[str] = "buffer_size"
DATASET_NAME: Final[str] = "dataset_name"
DEV_MODE: Final[str] = "dev_mode"
DLT_CONFIG: Final[str] = "dlt_config"
FILE_GLOB: Final[str] = "file_glob"
INPUT_DIR: Final[str] = "input_dir"
LOADER_FILE_FORMAT: Final[str] = "loader_file_format"
LOG_CONFIG_FILE: Final[str] = "log_config_file"
LOG_INTERVAL: Final[str] = "log_interval"
MAX_TABLE_NESTING: Final[str] = "max_table_nesting"
OUTPUT_DIR: Final[str] = "output_dir"
PRESERVE_TABLE_NESTING: Final[str] = "preserve_table_nesting"
START_AT: Final[str] = "start_at"
TABLE_NAME: Final[str] = "table_name"
USE_DESTINATION: Final[str] = "use_destination"
USE_OUTPUT_DIR_FOR_PIPELINE_METADATA: Final[str] = "use_output_dir_for_pipeline_metadata"


MIN_START_AT: Final[int] = 1

# Default values for the common fields
DEFAULTS = frozendict(
    {
        BATCH_SIZE: 1000,
        BUFFER_SIZE: 100,
        DEV_MODE: False,
        FILE_GLOB: "*",
        INPUT_DIR: INPUT_MOUNT,
        LOADER_FILE_FORMAT: PARQUET,
        LOG_CONFIG_FILE: None,
        LOG_INTERVAL: 1000,
        MAX_TABLE_NESTING: 0,
        # N.b. this gets replaced by destination.local_fs.bucket_url in CtsSettings and derivatives
        OUTPUT_DIR: "",
        PRESERVE_TABLE_NESTING: False,
        START_AT: MIN_START_AT,
        USE_DESTINATION: LOCAL_FS,
        USE_OUTPUT_DIR_FOR_PIPELINE_METADATA: False,
    }
)

DEFAULT_PIPELINE_BATCH_SIZE: Final[int] = 50

NonEmptyStr = Annotated[str, StringConstraints(min_length=1)]


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
DatasetName = Annotated[NonEmptyStr, Field(description="The name of the dataset being produced")]
DevMode = Annotated[
    bool,
    Field(
        default=DEFAULTS[DEV_MODE],
        description="Whether to run the pipeline in dev mode, which saves raw API responses to disk and disables compression for easier debugging.",
    ),
]
# this should really just be _Accessor but leaving the dict version in for ease of testing
# suppressed from CLI help/argparse output as it is not a value a user should ever set directly.
DltConfig = Annotated[
    dlt.common.configuration.accessors._Accessor | dict[str, Any] | None,  # noqa: SLF001
    Field(
        description="DLT configuration for the pipeline.",
        default_factory=lambda: dlt.config,
        # exclude from model_dump()
        exclude=True,
        repr=False,
    ),
    CLI_SUPPRESS,
]
FileGlob = Annotated[str, Field(default=DEFAULTS[FILE_GLOB], description="File glob")]
InputDir = Annotated[
    NonEmptyStr,
    Field(
        default=DEFAULTS[INPUT_DIR],
        description="Location of directory containing file(s) to import",
    ),
]


class LoaderFileFormatEnum(StrEnum):
    """Valid values for loader file format."""

    JSONL = JSONL
    PARQUET = PARQUET


LoaderFileFormat = Annotated[
    LoaderFileFormatEnum,
    Field(
        default=DEFAULTS[LOADER_FILE_FORMAT],
        description=f"Format to save the output to the destination as. Choices: {[member.value for member in LoaderFileFormatEnum.__members__.values()]}",
    ),
]
LogConfigFile = Annotated[
    NonEmptyStr | None,
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
MaxTableNesting = Annotated[
    int,
    Field(
        gt=-1,
        description="Maximum level of nesting of output datasets. For infinite nesting, set to 0.",
        default=DEFAULTS[MAX_TABLE_NESTING],
    ),
]
OutputDir = Annotated[
    str,
    Field(
        default=DEFAULTS[OUTPUT_DIR],
        description="Location to save imported data to, if different from the default supplied by the destination config",
    ),
]
PreserveTableNesting = Annotated[
    bool,
    Field(
        default=DEFAULTS[PRESERVE_TABLE_NESTING],
        description="Whether or not nested data should be flattened out into separate tables",
    ),
]
StartAt = Annotated[
    PositiveInt,
    Field(
        default=DEFAULTS[START_AT],
        description="File to start import at",
    ),
]
TableName = Annotated[str, Field(description="The name of the table to export parsed data to")]
UseDestination = Annotated[
    NonEmptyStr,
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
