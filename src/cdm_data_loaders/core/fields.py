"""Common fields and defaults for data loading and wrangling pipelines."""

import re
from enum import StrEnum
from typing import Annotated, Final

from frozendict import frozendict
from pydantic import AfterValidator, Field, FilePath, PositiveInt, StringConstraints

INPUT_MOUNT: Final[str] = "/input_dir"
OUTPUT_MOUNT: Final[str] = "/output_dir"

DEFAULT_JSONL_FILE_GLOB: Final[str] = "*.jsonl*"
XML_GZ_FILE_GLOB: Final[str] = "*.xml.gz"
DEFAULT_XML_FILE_GLOB: Final[str] = "*.xml*"
REJECTED_TABLE_NAME: Final[str] = "_rejected"
GZIP_SUFFIX: Final[str] = ".gz"

CTS_GLOBSTAR: Final[str] = "__GLOBSTAR__"
CTS_STAR: Final[str] = "__STAR__"
CTS_QMARK: Final[str] = "__QMARK__"
CTS_LBRACK: Final[str] = "__LBRACK__"
CTS_RBRACK: Final[str] = "__RBRACK__"
CTS_BANG: Final[str] = "__BANG__"

CTS_GLOB_WILDCARDS: Final[frozendict[str, str]] = frozendict(
    {
        CTS_GLOBSTAR: "**",
        CTS_STAR: "*",
        CTS_QMARK: "?",
        CTS_LBRACK: "[",
        CTS_RBRACK: "]",
        CTS_BANG: "!",
    }
)
CTS_GLOB_WILDCARD_PATTERN: Final[re.Pattern[str]] = re.compile(
    "|".join(re.escape(wildcard) for wildcard in CTS_GLOB_WILDCARDS)
)
CTS_GLOB_WILDCARDS_DESCRIPTION: Final[str] = (
    "Accepts CTS wildcard tokens: "
    "`__STAR__` (`*`), `__GLOBSTAR__` (`**`), `__QMARK__` (`?`), "
    "`__LBRACK__` (`[`), `__RBRACK__` (`]`), `__BANG__` (`!`)."
)

# output file formats
ICEBERG: Final[str] = "iceberg"
JSONL: Final[str] = "jsonl"
PARQUET: Final[str] = "parquet"


class LoaderFileFormatEnum(StrEnum):
    """Valid values for loader file format."""

    ICEBERG = ICEBERG
    JSONL = JSONL
    PARQUET = PARQUET


PYARROW_FILE_FORMATS: Final[frozenset[LoaderFileFormatEnum]] = frozenset(
    {
        LoaderFileFormatEnum.ICEBERG,
        LoaderFileFormatEnum.PARQUET,
    }
)


# destination config block names
CTS: Final[str] = "cts"

# field names
BUFFER_SIZE: Final[str] = "buffer_size"
DATASET_NAME: Final[str] = "dataset_name"
DLT_DEV_MODE: Final[str] = "dlt_dev_mode"
FILE_GLOB: Final[str] = "file_glob"
INPUT_DIR: Final[str] = "input_dir"
LOADER_FILE_FORMAT: Final[str] = "loader_file_format"
LOG_CONFIG_FILE: Final[str] = "log_config_file"
LOG_INTERVAL: Final[str] = "log_interval"
OUTPUT_DIR: Final[str] = "output_dir"
OUTPUT_SCHEMAS_MODULE: Final[str] = "output_schemas_module"
PREPROCESSORS_MODULE: Final[str] = "preprocessors_module"
TABLE_NAME: Final[str] = "table_name"
TRANSFORMS_MODULE: Final[str] = "transforms_module"
USE_DESTINATION: Final[str] = "use_destination"
USE_OUTPUT_DIR_FOR_PIPELINE_METADATA: Final[str] = "use_output_dir_for_pipeline_metadata"

PIPELINE_DIR_NAME: Final[str] = "pipeline_metadata"
"""Directory where dlt will put run metadata, schemas, etc."""
REPORT_FILE_NAME: Final[str] = "report.txt"
"""Name of the report file."""


# Default values for the common fields
DEFAULTS = frozendict(
    {
        BUFFER_SIZE: 100,
        DLT_DEV_MODE: False,
        FILE_GLOB: "*",
        LOADER_FILE_FORMAT: LoaderFileFormatEnum.PARQUET,
        LOG_CONFIG_FILE: None,
        LOG_INTERVAL: 1000,
        OUTPUT_SCHEMAS_MODULE: None,
        PREPROCESSORS_MODULE: None,
        TRANSFORMS_MODULE: None,
        USE_DESTINATION: CTS,
        USE_OUTPUT_DIR_FOR_PIPELINE_METADATA: False,
    }
)


NonEmptyStr = Annotated[str, StringConstraints(min_length=1)]
CharStr = Annotated[str, StringConstraints(min_length=1, max_length=1)]


BufferSize = Annotated[
    PositiveInt,
    Field(
        default=DEFAULTS[BUFFER_SIZE],
        description="Number of rows to buffer per table before yielding a batch to the destination. Must be a positive integer.",
    ),
]
DatasetName = Annotated[NonEmptyStr, Field(description="The name of the dataset being produced")]
DltDevMode = Annotated[
    bool,
    Field(
        default=DEFAULTS[DLT_DEV_MODE],
        description="Enables DLT's dev mode. In dev mode, dlt resets the pipeline state on each run and creates a new dataset with a unique timestamped name for each run.",
    ),
]


def _replace_cts_glob_wildcard(match: re.Match[str]) -> str:
    """Return the glob metacharacter for a matched CTS wildcard token.

    :param match: regex match for a CTS wildcard token
    :type match: re.Match[str]
    :return: glob metacharacter for the matched token
    :rtype: str
    """
    return CTS_GLOB_WILDCARDS[match.group(0)]


def expand_cts_glob_wildcards(value: str) -> str:
    """Replace CTS wildcard tokens in ``value`` with glob metacharacters.

    :param value: raw or CTS-tokenized glob pattern
    :type value: str
    :return: glob pattern with CTS wildcard tokens expanded
    :rtype: str
    """
    return CTS_GLOB_WILDCARD_PATTERN.sub(_replace_cts_glob_wildcard, value)


def file_glob_description(summary: str) -> str:
    """Build a CLI field description for ``file_glob`` including CTS wildcard tokens.

    :param summary: short description of matched input files
    :type summary: str
    :return: field description with CTS wildcard token reference
    :rtype: str
    """
    base = summary.rstrip(".")
    return f"{base}. {CTS_GLOB_WILDCARDS_DESCRIPTION}"


FileGlob = Annotated[
    NonEmptyStr,
    AfterValidator(expand_cts_glob_wildcards),
    Field(
        default=DEFAULTS[FILE_GLOB],
        description=file_glob_description("File glob for input files"),
    ),
]
LoaderFileFormat = Annotated[
    LoaderFileFormatEnum,
    Field(
        default=DEFAULTS[LOADER_FILE_FORMAT],
        description=f"Format of the output files. Choices: {[member.value for member in LoaderFileFormatEnum]}",
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
        description="How often (in number of processed entries) to emit a progress log message.",
    ),
]
OutputSchemasModuleSpec = Annotated[
    NonEmptyStr | None,
    Field(
        default=DEFAULTS[OUTPUT_SCHEMAS_MODULE],
        description=(
            "Optional Python module path exporting OUTPUT_SCHEMAS "
            "(dict[str, pyarrow.Schema]) for PyArrow schema overrides."
        ),
    ),
]
PreprocessorsModuleSpec = Annotated[
    NonEmptyStr | None,
    Field(
        default=DEFAULTS[PREPROCESSORS_MODULE],
        description=(
            "Optional Python module path exporting FILE_PREPROCESSORS "
            "(Sequence[FilePreprocessFn] or dict[str, Sequence[FilePreprocessFn] | FilePreprocessFn]) "
            "for file-level validation and filtering before opening and parsing."
        ),
    ),
]


def _reject_reserved_table_name(value: str) -> str:
    """Reject the reserved _rejected table name."""
    if value == REJECTED_TABLE_NAME:
        msg = f"Table name {REJECTED_TABLE_NAME!r} is reserved for pipeline error records."
        raise ValueError(msg)
    return value


TableName = Annotated[
    NonEmptyStr,
    AfterValidator(_reject_reserved_table_name),
    Field(description="The name of the table to export parsed data to"),
]
TransformsModuleSpec = Annotated[
    NonEmptyStr | None,
    Field(
        default=DEFAULTS[TRANSFORMS_MODULE],
        description=(
            "Optional Python module path exporting TABLE_TRANSFORMS "
            "(dict[str, RecordTransformFn]) for record filtering, transformation, "
            "and multi-table splitting."
        ),
    ),
]
UseDestination = Annotated[
    NonEmptyStr,
    Field(
        # no default - value must be supplied
        description="Name of the dlt destination to load data into. The dlt config must set destination.<name>.destination_type; the destination's location and credentials also come from the dlt config.",
    ),
]
UseOutputDirForPipelineMetadata = Annotated[
    bool,
    Field(
        default=DEFAULTS[USE_OUTPUT_DIR_FOR_PIPELINE_METADATA],
        description="Store pipeline metadata in `<output_dir>/<PIPELINE_DIR_NAME>`. output_dir must be set and on the local filesystem.",
    ),
]
XmlTag = Annotated[
    NonEmptyStr,
    Field(
        description="XML tag to capture the contents of",
    ),
]
XsdFile = Annotated[
    FilePath,
    Field(
        description="Path to the XSD file describing the XML documents to be imported.",
    ),
]
