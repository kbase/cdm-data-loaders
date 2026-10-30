"""CLI interface for creating a dump of table schemas from an Iceberg catalog."""

import json
from logging import Logger, getLogger
from pathlib import Path
from typing import Any, Final

from pydantic import Field
from pydantic_settings import SettingsConfigDict

from cdm_data_loaders.converters._iceberg_catalog_dump import (
    CATALOG_CLI_SHORTCUTS,
    IcebergCatalogDumpSettings,
    dump_catalog,
    write_schema_text,
)
from cdm_data_loaders.converters.emitters.json_schema import JSON_SCHEMA_DIALECT
from cdm_data_loaders.converters.pyiceberg_to_jsonschema import table_to_json_schema
from cdm_data_loaders.core.settings import default_settings_with_shortcuts

logger: Logger = getLogger(__name__)

PIPELINE_NAME: Final[str] = "iceberg_catalog_converter"


class IcebergToJsonSchemaSettings(IcebergCatalogDumpSettings):
    """Settings for generating a set of JSONSchemas from an Iceberg data catalog."""

    model_config: SettingsConfigDict = default_settings_with_shortcuts(
        cli_prog_name=PIPELINE_NAME, cli_shortcuts=CATALOG_CLI_SHORTCUTS
    )

    group_by_namespace: bool = Field(default=False, description="Export one JSON Schema per namespace")


def _write_schema(schema_doc: dict[str, Any], out_file: Path, generated_at: str) -> None:
    """Write a timestamped schema, warning before replacing an existing file."""
    schema_doc.setdefault("x-iceberg", {})["generated_at"] = generated_at
    write_schema_text(json.dumps(schema_doc, indent=2), out_file, logger)


def dump_catalog_schemas(settings: IcebergToJsonSchemaSettings) -> None:
    """Iterate through a catalog and dump out the table schemas as JSONSchema.

    A table that fails to load or convert is logged and skipped; it does not
    abort the dump of the remaining tables. `generated_at` is stamped
    identically on every table in a single run (it reflects the run, not each
    table's individual read time). An existing output file for a table is
    overwritten, with a warning logged first. With ``group_by_namespace``,
    each nonempty namespace produces one file whose ``$defs`` holds its table schemas.

    :param settings: Catalog name and output directory.
    :returns: None.
    """
    dump_catalog(
        settings,
        file_suffix=".schema.json",
        make_namespace_schema=lambda namespace_name: {
            "$schema": JSON_SCHEMA_DIALECT,
            "$id": f"urn:iceberg:{namespace_name}",
            "title": namespace_name,
        },
        convert_table=lambda table, identifier, namespace_schema: table_to_json_schema(
            table, identifier, schema=namespace_schema
        ),
        has_grouped_entries=lambda namespace_schema: bool(namespace_schema.get("$defs")),
        write_schema=_write_schema,
        log=logger,
    )


if __name__ == "__main__":
    settings = IcebergToJsonSchemaSettings()  # pyright: ignore[reportCallIssue]
    dump_catalog_schemas(settings)
