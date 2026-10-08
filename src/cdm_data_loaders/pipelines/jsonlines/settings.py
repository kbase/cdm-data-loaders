"""Setting for the JSONL validate-and-load pipeline for the KBase CTS."""

from typing import Annotated, Final

from pydantic import Field, field_validator
from pydantic_settings import SettingsConfigDict

from cdm_data_loaders.core.fields import (
    DEFAULT_JSONL_FILE_GLOB,
    DEFAULTS,
    FILE_GLOB,
    LOADER_FILE_FORMAT,
    TABLE_NAME,
    BufferSize,
    DatasetName,
    FileGlob,
    LoaderFileFormat,
    NonEmptyStr,
    TableName,
)
from cdm_data_loaders.core.settings import CLI_SHORTCUTS, DEFAULT_SETTINGS_CONFIG_DICT, CtsSettings

EXTRACT_PIPELINE_NAME: Final[str] = "jsonlines_ingest"
PYDANTIC_PIPELINE_NAME: Final[str] = "jsonlines_ingest_pydantic_validator"
JSONSCHEMA_PIPELINE_NAME: Final[str] = "jsonlines_ingest_jsonschema_validator"

ENTITY_MODELS_MODULE: Final[str] = "entity_models_module"
SCHEMA_FILES_MODULE: Final[str] = "schema_files_module"
TABLE_NAMES: Final[str] = "table_names"


class JsonlIngestSettings(CtsSettings):
    """Settings for the JSONL ingest pipeline."""

    buffer_size: BufferSize
    dataset_name: DatasetName
    file_glob: Annotated[
        FileGlob,
        Field(
            default=DEFAULT_JSONL_FILE_GLOB,
            description="Glob pattern for JSONL files",
        ),
    ]
    loader_file_format: LoaderFileFormat = Field(default=DEFAULTS[LOADER_FILE_FORMAT])


class JsonlExtractSettings(JsonlIngestSettings):
    """Settings for the JSONL extraction-only pipeline."""

    model_config = SettingsConfigDict(
        **DEFAULT_SETTINGS_CONFIG_DICT,
        cli_prog_name=EXTRACT_PIPELINE_NAME,
        cli_shortcuts={
            **CLI_SHORTCUTS,
            TABLE_NAME.replace("_", "-"): "t",
            FILE_GLOB.replace("_", "-"): "g",
        },
    )

    table_name: TableName


class ValidatedJsonlIngestSettings(JsonlIngestSettings):
    """JSONL ingestion pipeline with validation."""

    table_names: list[NonEmptyStr] | None = Field(
        default=None,
        description="Table names to process. Defaults to every table in the configured registry.",
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
        cli_prog_name=PYDANTIC_PIPELINE_NAME,
        cli_shortcuts={
            **CLI_SHORTCUTS,
            ENTITY_MODELS_MODULE.replace("_", "-"): "m",
            TABLE_NAMES.replace("_", "-"): "t",
            FILE_GLOB.replace("_", "-"): "g",
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
    """Pipeline settings for JSONL ingestion with JSON Schema validation."""

    model_config = SettingsConfigDict(
        **DEFAULT_SETTINGS_CONFIG_DICT,
        cli_prog_name=JSONSCHEMA_PIPELINE_NAME,
        cli_shortcuts={
            **CLI_SHORTCUTS,
            SCHEMA_FILES_MODULE.replace("_", "-"): "m",
            FILE_GLOB.replace("_", "-"): "g",
        },
    )

    schema_files_module: Annotated[
        str,
        Field(
            description=(
                "Dotted import path to a module that defines `ENTITY_SCHEMAS: dict[str, dict]`. "
                "Each key is a table name and a subdirectory of `input_dir`. "
                "Each value is a JSON Schema dictionary declaring its draft with `$schema`."
            ),
        ),
    ]
