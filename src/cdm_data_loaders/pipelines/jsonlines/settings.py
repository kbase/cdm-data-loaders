"""Setting for the JSONL validate-and-load pipeline for the KBase CTS."""

from typing import Annotated, Final

from pydantic import Field, PrivateAttr, field_validator
from pydantic_settings import SettingsConfigDict

from cdm_data_loaders.core.fields import (
    DEFAULT_JSONL_FILE_GLOB,
    DEFAULTS,
    FILE_GLOB,
    LOADER_FILE_FORMAT,
    BufferSize,
    DatasetName,
    FileGlob,
    LoaderFileFormat,
    PreserveTableNesting,
    NonEmptyStr,
)
from cdm_data_loaders.core.settings import CLI_SHORTCUTS, DEFAULT_SETTINGS_CONFIG_DICT, CtsSettings

PIPELINE_NAME: Final[str] = "jsonlines_ingest"
ENTITY_MODELS_MODULE: Final[str] = "entity_models_module"
SCHEMA_FILES_MODULE: Final[str] = "schema_files_module"
TABLE_NAMES: Final[str] = "table_names"

JSONL_CLI_SHORTCUTS = {
    **CLI_SHORTCUTS,
    TABLE_NAMES.replace("_", "-"): "t",
    FILE_GLOB.replace("_", "-"): "g",
}


class JsonlIngestSettings(CtsSettings):
    """Settings for the JSONL ingest pipeline."""

    model_config = SettingsConfigDict(
        **DEFAULT_SETTINGS_CONFIG_DICT,
        cli_prog_name=PIPELINE_NAME,
        cli_shortcuts=JSONL_CLI_SHORTCUTS,
    )

    preserve_table_nesting: PreserveTableNesting

    buffer_size: BufferSize
    dataset_name: DatasetName
    file_glob: Annotated[
        FileGlob,
        Field(
            default=DEFAULT_JSONL_FILE_GLOB,
            description="Glob pattern for JSONL files inside each entity's input subdirectory.",
        ),
    ]

    loader_file_format: LoaderFileFormat = Field(default=DEFAULTS[LOADER_FILE_FORMAT])


class ValidatedJsonlIngestSettings(JsonlIngestSettings):
    """JSONL ingestion pipeline with validation."""

    table_names: list[NonEmptyStr] | None = Field(
        default=None,
        description="Table names to process. Defaults to every table in entity_models_module.",
    )

    @field_validator(TABLE_NAMES, mode="before")
    @classmethod
    def split_table_names(cls, v: str | list[str] | None) -> list[str] | None:
        """Split a comma-separated string into a list. Pass lists and None through unchanged."""
        if v is None or isinstance(v, list):
            return v
        return [name.strip() for name in v.split(",") if name.strip()] or None


class JsonlPydanticIngestSettings(ValidatedJsonlIngestSettings):
    """Pipeline settings for JSONL pipeline that uses Pydantic for validation."""

    model_config = SettingsConfigDict(
        **DEFAULT_SETTINGS_CONFIG_DICT,
        cli_prog_name=PIPELINE_NAME,
        cli_shortcuts={
            **JSONL_CLI_SHORTCUTS,
            ENTITY_MODELS_MODULE.replace("_", "-"): "m",
        },
    )

    entity_models_module: Annotated[
        str,
        Field(
            description=(
                "Dotted import path to a module that defines `ENTITY_MODELS: dict[str, type[BaseModel]]`. "
                "Each key is a table name and a subdirectory of `input_dir`. "
                "Each value is the Pydantic model for that table."
            ),
        ),
    ]


class JsonlJsonschemaIngestSettings(ValidatedJsonlIngestSettings):
    """Pipeline settings for JSONL pipeline that uses JSONSchema for validation."""

    model_config = SettingsConfigDict(
        **DEFAULT_SETTINGS_CONFIG_DICT,
        cli_prog_name=PIPELINE_NAME,
        cli_shortcuts={
            **JSONL_CLI_SHORTCUTS,
            SCHEMA_FILES_MODULE.replace("_", "-"): "m",
        },
    )

    schema_files_module: Annotated[
        str,
        Field(
            description=(
                "Dotted import path to a module that defines `SCHEMA_FILES: dict[str, str]`. "
                "Each key is a table name and a subdirectory of `input_dir`. "
                "Each value is the path to the JSONSchema for that table."
            ),
        ),
    ]
