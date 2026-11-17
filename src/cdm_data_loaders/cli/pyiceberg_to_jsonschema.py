"""CLI interface for creating a JSON Schema dump of table schemas from an Iceberg catalog."""

import json
import operator
from logging import Logger, getLogger
from pathlib import Path
from typing import Any, Final

from pydantic import Field
from pydantic_settings import SettingsConfigDict

from cdm_data_loaders.cli.pyiceberg_dump import (
    CATALOG_CLI_SHORTCUTS,
    CatalogDumpDriver,
    IcebergCatalogDumpSettings,
    dump_catalog,
)
from cdm_data_loaders.converters.core.io import write_schema_text
from cdm_data_loaders.converters.emitters.json_schema.common import JSON_SCHEMA_DIALECT
from cdm_data_loaders.converters.facades.pyiceberg_to_jsonschema import table_to_json_schema
from cdm_data_loaders.core.cli import CliRunner
from cdm_data_loaders.core.settings import default_settings_with_shortcuts

logger: Logger = getLogger(__name__)

PIPELINE_NAME: Final[str] = "pyiceberg_to_jsonschema"


class IcebergToJsonSchemaSettings(IcebergCatalogDumpSettings):
    """Settings for generating a set of JSON Schemas from an Iceberg data catalog."""

    model_config: SettingsConfigDict = default_settings_with_shortcuts(
        cli_prog_name=PIPELINE_NAME, cli_shortcuts=CATALOG_CLI_SHORTCUTS
    )

    group_by_namespace: bool = Field(default=False, description="Export one JSON Schema per namespace")


def _make_namespace_schema(namespace_name: str) -> dict[str, Any]:
    """Build an empty namespace-scoped JSON Schema dictionary.

    :param namespace_name: Dot-joined Iceberg namespace name.
    :type namespace_name: str
    :return: Empty namespace JSON Schema document.
    :rtype: dict[str, Any]
    """
    return {
        "$schema": JSON_SCHEMA_DIALECT,
        "$id": f"urn:iceberg:{namespace_name}",
        "title": namespace_name,
    }


def _write_schema(schema_doc: dict[str, Any], out_file: Path, generated_at: str) -> None:
    """Write a timestamped schema, warning before replacing an existing file.

    :param schema_doc: JSON Schema dictionary to serialize.
    :type schema_doc: dict[str, Any]
    :param out_file: Destination file path.
    :type out_file: Path
    :param generated_at: ISO-8601 timestamp string.
    :type generated_at: str
    """
    schema_doc.setdefault("x-iceberg", {})["generated_at"] = generated_at
    write_schema_text(json.dumps(schema_doc, indent=2), out_file, logger)


JSON_SCHEMA_DUMP_DRIVER: Final[CatalogDumpDriver[dict[str, Any]]] = CatalogDumpDriver(
    file_suffix=".schema.json",
    make_namespace_schema=_make_namespace_schema,
    convert_table=table_to_json_schema,
    grouped_entries_getter=operator.methodcaller("get", "$defs"),
    write_schema=_write_schema,
)


def dump_catalog_schemas(settings: IcebergToJsonSchemaSettings) -> None:
    """Iterate through an Iceberg catalog and write table schemas as JSON Schema documents.

    Tables that fail to load or convert are logged and skipped without aborting
    remaining tables. ``generated_at`` carries a single run timestamp across all
    emitted schemas. With ``group_by_namespace``, each non-empty namespace writes
    one file whose ``$defs`` holds its table schemas.

    :param settings: Catalog name and output directory.
    :type settings: IcebergToJsonSchemaSettings
    """
    dump_catalog(settings, JSON_SCHEMA_DUMP_DRIVER, logger)


cli = CliRunner(IcebergToJsonSchemaSettings, dump_catalog_schemas)


if __name__ == "__main__":
    cli()
