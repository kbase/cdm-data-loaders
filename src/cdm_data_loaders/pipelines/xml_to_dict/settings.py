"""Settings class for the XML to dictionary pipeline for the KBase CTS."""

from typing import Annotated, Final

from pydantic import Field, PrivateAttr
from pydantic_settings import SettingsConfigDict

from cdm_data_loaders.core.fields import (
    DEFAULT_XML_FILE_GLOB,
    DEFAULTS,
    FILE_GLOB,
    MAX_TABLE_NESTING,
    BufferSize,
    DatasetName,
    FileGlob,
    LoaderFileFormat,
    LoaderFileFormatEnum,
    LogInterval,
    MaxTableNesting,
    NonEmptyStr,
    TableName,
)
from cdm_data_loaders.core.settings import CLI_SHORTCUTS, DEFAULT_SETTINGS_CONFIG_DICT, CtsSettings

PIPELINE_NAME: Final[str] = "xml_to_dict_ingest"
DEFAULT_LOADER_FILE_FORMAT: Final[str] = LoaderFileFormatEnum.JSONL
VALID_LOADER_FILE_FORMATS: Final[list[str]] = [member.value for member in LoaderFileFormatEnum.__members__.values()]


class XmlToDictSettings(CtsSettings):
    """Settings for the XML ingestion pipeline."""

    model_config = SettingsConfigDict(
        **DEFAULT_SETTINGS_CONFIG_DICT,
        cli_prog_name=PIPELINE_NAME,
        cli_shortcuts={
            **CLI_SHORTCUTS,
            FILE_GLOB.replace("_", "-"): "g",
        },
    )

    _max_table_nesting: MaxTableNesting = PrivateAttr(default=DEFAULTS[MAX_TABLE_NESTING])

    buffer_size: BufferSize
    dataset_name: DatasetName
    file_glob: Annotated[
        FileGlob,
        Field(
            default=DEFAULT_XML_FILE_GLOB,
            description="Glob pattern for XML files inside each entity's input subdirectory.",
        ),
    ]
    loader_file_format: LoaderFileFormat = Field(default=DEFAULT_LOADER_FILE_FORMAT)
    log_interval: LogInterval
    table_name: TableName
    xml_tag: Annotated[
        NonEmptyStr,
        Field(
            description="XML tag to capture the contents of",
        ),
    ]

    @property
    def max_table_nesting(self) -> int:
        """Max table nesting value."""
        return self._max_table_nesting
