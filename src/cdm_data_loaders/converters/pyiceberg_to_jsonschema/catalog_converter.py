"""CLI interface for creating a dump of table schemas from an Iceberg catalog."""

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Annotated, Final

from pydantic import Field
from pydantic_settings import SettingsConfigDict
from pyiceberg.catalog import load_catalog

from cdm_data_loaders.converters.pyiceberg_to_jsonschema.converter import table_to_json_schema
from cdm_data_loaders.core.fields import (
    OUTPUT_DIR,
    NonEmptyStr,
    OutputDir,
)
from cdm_data_loaders.core.settings import CLI_SHORTCUTS, DEFAULT_SETTINGS_CONFIG_DICT, LoggerSettings

PIPELINE_NAME: Final[str] = "iceberg_catalog_converter"


class IcebergToJsonSchemaSettings(LoggerSettings):
    """Settings for generating a set of JSONSchemas from an Iceberg data catalog."""

    model_config = SettingsConfigDict(
        **DEFAULT_SETTINGS_CONFIG_DICT,
        cli_prog_name=PIPELINE_NAME,
        cli_shortcuts={
            **CLI_SHORTCUTS,
            OUTPUT_DIR.replace("_", "-"): "o",
            "catalog": "c",
        },
    )

    catalog: Annotated[NonEmptyStr, Field(description="Name of the catalog to retrieve schemas from")]
    output_dir: OutputDir


def dump_catalog_schemas(settings: IcebergToJsonSchemaSettings) -> None:
    """Iterate through a catalog and dump out the table schemas as JSONSchema."""
    catalog = load_catalog(settings.catalog)
    out_path = Path(settings.output_dir)
    out_path.mkdir(parents=True, exist_ok=True)
    generated_at = datetime.now(UTC).isoformat()

    for namespace in catalog.list_namespaces():
        for identifier in catalog.list_tables(namespace):
            table = catalog.load_table(identifier)
            schema_doc = table_to_json_schema(table, identifier)
            schema_doc["x-iceberg"]["generated_at"] = generated_at

            file_name = ".".join(identifier) + ".schema.json"
            (out_path / file_name).write_text(json.dumps(schema_doc, indent=2))


if __name__ == "__main__":
    settings = IcebergToJsonSchemaSettings()  # pyright: ignore[reportCallIssue]
    dump_catalog_schemas(settings)
