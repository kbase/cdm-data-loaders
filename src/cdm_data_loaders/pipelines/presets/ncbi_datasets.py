"""Preset JSONL JSON Schema ingestion pipeline for NCBI Datasets reports."""

from typing import ClassVar, Final

from pydantic_settings import SettingsConfigDict

from cdm_data_loaders.core.cli import CliRunner
from cdm_data_loaders.core.fields import FILE_GLOB
from cdm_data_loaders.core.settings import default_settings_with_shortcuts, select_cli_shortcuts
from cdm_data_loaders.pipelines.jsonlines.jsonschema import run_jsonlines_pipeline
from cdm_data_loaders.pipelines.jsonlines.settings import (
    LOCAL_CLI_SHORTCUTS,
    JsonlJsonschemaIngestSettings,
)

NCBI_DATASETS_PIPELINE_NAME: Final[str] = "ncbi_datasets"
NCBI_DATASETS_SCHEMA_MODULE: Final[str] = "cdm_data_loaders.datasets.ncbi_datasets.hooks"


class NcbiDatasetsSettings(JsonlJsonschemaIngestSettings):
    """Pipeline settings for NCBI Datasets JSONL ingestion (``annotation_report`` and ``dataset_report``)."""

    model_config: SettingsConfigDict = default_settings_with_shortcuts(
        cli_prog_name=NCBI_DATASETS_PIPELINE_NAME,
        cli_shortcuts=select_cli_shortcuts(LOCAL_CLI_SHORTCUTS, FILE_GLOB),  # pyright: ignore[reportArgumentType]
    )

    dataset_name: ClassVar[str] = NCBI_DATASETS_PIPELINE_NAME
    entity_schemas: ClassVar[str] = NCBI_DATASETS_SCHEMA_MODULE
    transforms_module: ClassVar[str | None] = NCBI_DATASETS_SCHEMA_MODULE
    output_schemas_module: ClassVar[str | None] = NCBI_DATASETS_SCHEMA_MODULE


cli = CliRunner(NcbiDatasetsSettings, run_jsonlines_pipeline)


if __name__ == "__main__":
    cli()
