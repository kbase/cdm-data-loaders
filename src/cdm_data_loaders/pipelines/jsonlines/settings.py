"""Setting for the JSONL validate-and-load pipeline for the KBase CTS."""

from typing import Annotated, Final

from pydantic import Field
from pydantic_settings import SettingsConfigDict

from cdm_data_loaders.core.fields import (
    DEFAULT_JSONL_FILE_GLOB,
    FILE_GLOB,
    TABLE_NAME,
    FileGlob,
    TableName,
)
from cdm_data_loaders.core.settings import (
    FileFormatIngestSettings,
    default_settings_with_shortcuts,
    select_cli_shortcuts,
)

EXTRACT_PIPELINE_NAME: Final[str] = "jsonlines_extract"
PYDANTIC_PIPELINE_NAME: Final[str] = "jsonlines_pydantic"
JSONSCHEMA_PIPELINE_NAME: Final[str] = "jsonlines_jsonschema"

ENTITY_MODELS_MODULE: Final[str] = "entity_models_module"
SCHEMA_FILES_MODULE: Final[str] = "schema_files_module"

LOCAL_CLI_SHORTCUTS: Final[dict[str, str]] = {
    ENTITY_MODELS_MODULE: "m",
    FILE_GLOB: "g",
    SCHEMA_FILES_MODULE: "m",
    TABLE_NAME: "t",
}


class JsonlIngestSettings(FileFormatIngestSettings):
    """Settings for the JSONL ingest pipeline."""

    file_glob: Annotated[
        FileGlob,
        Field(
            default=DEFAULT_JSONL_FILE_GLOB,
            description="Glob pattern for JSONL files",
        ),
    ]


class JsonlExtractSettings(JsonlIngestSettings):
    """Settings for the JSONL extraction-only pipeline."""

    model_config: SettingsConfigDict = default_settings_with_shortcuts(
        cli_prog_name=EXTRACT_PIPELINE_NAME,
        cli_shortcuts=select_cli_shortcuts(LOCAL_CLI_SHORTCUTS, FILE_GLOB, TABLE_NAME),
    )

    table_name: TableName


class JsonlPydanticIngestSettings(JsonlIngestSettings):
    """Pipeline settings for JSONL pipeline that uses Pydantic for validation."""

    model_config: SettingsConfigDict = default_settings_with_shortcuts(
        cli_prog_name=PYDANTIC_PIPELINE_NAME,
        cli_shortcuts=select_cli_shortcuts(LOCAL_CLI_SHORTCUTS, ENTITY_MODELS_MODULE, FILE_GLOB),
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


class JsonlJsonschemaIngestSettings(JsonlIngestSettings):
    """Pipeline settings for JSONL ingestion with JSON Schema validation."""

    model_config: SettingsConfigDict = default_settings_with_shortcuts(
        cli_prog_name=JSONSCHEMA_PIPELINE_NAME,
        cli_shortcuts=select_cli_shortcuts(LOCAL_CLI_SHORTCUTS, FILE_GLOB, SCHEMA_FILES_MODULE),
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
