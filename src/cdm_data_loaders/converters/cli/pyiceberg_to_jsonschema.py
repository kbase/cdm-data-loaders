"""CLI interface for creating a JSON Schema dump of table schemas from an Iceberg catalog."""

import json
from logging import Logger, getLogger
from pathlib import Path
from typing import Any, Final

from pydantic import Field
from pydantic_settings import SettingsConfigDict
from pyiceberg.table import Table

from cdm_data_loaders.converters.cli.iceberg_base import (
    CATALOG_CLI_SHORTCUTS,
    CatalogDumpDriver,
    IcebergCatalogDumpSettings,
    dump_catalog,
    write_schema_text,
)
from cdm_data_loaders.converters.emitters.json_schema import JSON_SCHEMA_DIALECT
from cdm_data_loaders.converters.facades.pyiceberg_to_jsonschema import table_to_json_schema
from cdm_data_loaders.core.settings import default_settings_with_shortcuts
from cdm_data_loaders.pipelines.core import PipelineCli

logger: Logger = getLogger(__name__)

PIPELINE_NAME: Final[str] = "iceberg_catalog_converter"


class IcebergToJsonSchemaSettings(IcebergCatalogDumpSettings):
    """Settings for generating a set of JSONSchemas from an Iceberg data catalog."""

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


def _convert_table(
    table: Table,
    identifier: tuple[str, ...],
    namespace_schema: dict[str, Any] | None,
) -> dict[str, Any]:
    """Convert a single Iceberg table to JSON Schema.

    :param table: Loaded PyIceberg table.
    :type table: Table
    :param identifier: Table identifier tuple.
    :type identifier: tuple[str, ...]
    :param namespace_schema: Optional shared namespace JSON Schema document.
    :type namespace_schema: dict[str, Any] | None
    :return: Converted JSON Schema dictionary.
    :rtype: dict[str, Any]
    """
    return table_to_json_schema(table, identifier, schema=namespace_schema)


def _has_grouped_entries(namespace_schema: dict[str, Any]) -> bool:
    """Return True if ``namespace_schema`` contains at least one table definition in ``$defs``.

    :param namespace_schema: Namespace JSON Schema dictionary.
    :type namespace_schema: dict[str, Any]
    :return: ``True`` if ``$defs`` is non-empty.
    :rtype: bool
    """
    return bool(namespace_schema.get("$defs"))


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
    convert_table=_convert_table,
    has_grouped_entries=_has_grouped_entries,
    write_schema=_write_schema,
)


def dump_catalog_schemas(settings: IcebergToJsonSchemaSettings) -> None:
    """Iterate through a catalog and dump out the table schemas as JSONSchema.

    A table that fails to load or convert is logged and skipped; it does not
    abort the dump of the remaining tables. ``generated_at`` is stamped
    identically on every table in a single run (it reflects the run, not each
    table's individual read time). An existing output file for a table is
    overwritten, with a warning logged first. With ``group_by_namespace``,
    each nonempty namespace produces one file whose ``$defs`` holds its table schemas.

    :param settings: Catalog name and output directory.
    :type settings: IcebergToJsonSchemaSettings
    """
    dump_catalog(settings, JSON_SCHEMA_DUMP_DRIVER, logger)


cli = PipelineCli(IcebergToJsonSchemaSettings, dump_catalog_schemas)


if __name__ == "__main__":
    cli()
