"""CLI interface for creating a dump of table schemas from an Iceberg catalog."""

import json
from datetime import UTC, datetime
from logging import Logger, getLogger
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

logger: Logger = getLogger(__name__)

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
    """Iterate through a catalog and dump out the table schemas as JSONSchema.

    A table that fails to load or convert is logged and skipped; it does not
    abort the dump of the remaining tables. `generated_at` is stamped
    identically on every table in a single run (it reflects the run, not each
    table's individual read time). An existing output file for a table is
    overwritten, with a warning logged first.
    """
    catalog = load_catalog(settings.catalog)
    out_path = Path(settings.output_dir)
    out_path.mkdir(parents=True, exist_ok=True)
    generated_at = datetime.now(UTC).isoformat()

    namespaces = catalog.list_namespaces()
    logger.info("Dumping schemas for %d namespace(s) from catalog %r", len(namespaces), settings.catalog)
    failures = 0
    for namespace in namespaces:
        for identifier in catalog.list_tables(namespace):
            try:
                table = catalog.load_table(identifier)
                schema_doc = table_to_json_schema(table, identifier)
            except Exception:
                failures += 1
                logger.exception("Failed to convert table %s; skipping", identifier)
                continue
            schema_doc["x-iceberg"]["generated_at"] = generated_at

            file_name = ".".join(identifier) + ".schema.json"
            out_file = out_path / file_name
            if out_file.exists():
                logger.warning("Overwriting existing schema file %s", out_file)
            out_file.write_text(json.dumps(schema_doc, indent=2))
            logger.info("Wrote schema for table %s to %s", identifier, out_file)

    if failures:
        logger.warning("Completed with %d table(s) skipped due to conversion failures", failures)


if __name__ == "__main__":
    settings = IcebergToJsonSchemaSettings()  # pyright: ignore[reportCallIssue]
    dump_catalog_schemas(settings)
