"""CLI interface for creating a dump of table schemas from an Iceberg catalog."""

from logging import Logger, getLogger
from pathlib import Path
from typing import Final, cast

from linkml_runtime.dumpers import yaml_dumper
from linkml_runtime.linkml_model.meta import Annotation, Prefix, SchemaDefinition
from pydantic import Field
from pydantic_settings import SettingsConfigDict

from cdm_data_loaders.converters._iceberg_catalog_dump import (
    CATALOG_CLI_SHORTCUTS,
    IcebergCatalogDumpSettings,
    dump_catalog,
    write_schema_text,
)
from cdm_data_loaders.converters.emitters.linkml import LINKML_PREFIX
from cdm_data_loaders.converters.pyiceberg_to_linkml import table_to_linkml
from cdm_data_loaders.core.settings import default_settings_with_shortcuts

logger: Logger = getLogger(__name__)

PIPELINE_NAME: Final[str] = "iceberg_catalog_linkml_converter"


class IcebergToLinkMLSettings(IcebergCatalogDumpSettings):
    """Settings for generating a set of LinkML schemas from an Iceberg data catalog."""

    model_config: SettingsConfigDict = default_settings_with_shortcuts(
        cli_prog_name=PIPELINE_NAME, cli_shortcuts=CATALOG_CLI_SHORTCUTS
    )

    group_by_namespace: bool = Field(default=False, description="Export one LinkML schema per namespace")


def _write_schema(schema: SchemaDefinition, out_file: Path, generated_at: str) -> None:
    """Write a timestamped schema, warning before replacing an existing file."""
    cast("dict[str, Annotation]", schema.annotations)["generated_at"] = Annotation(
        tag="generated_at", value=generated_at
    )
    write_schema_text(yaml_dumper.dumps(schema), out_file, logger)


def _make_namespace_schema(namespace_name: str) -> SchemaDefinition:
    """Build an empty namespace-scoped LinkML SchemaDefinition."""
    return SchemaDefinition(
        id=f"urn:iceberg:{namespace_name}",
        name=namespace_name,
        prefixes=[
            Prefix(prefix_prefix="iceberg", prefix_reference=f"urn:iceberg:{namespace_name}#"),
            Prefix(prefix_prefix="linkml", prefix_reference=LINKML_PREFIX),
        ],
        default_prefix="iceberg",
        imports=["linkml:types"],
    )


def dump_catalog_schemas(settings: IcebergToLinkMLSettings) -> None:
    """Iterate through a catalog and dump out the table schemas as LinkML.

    A table that fails to load or convert is logged and skipped; it does not
    abort the dump of the remaining tables. `generated_at` is stamped
    identically on every table in a single run (it reflects the run, not each
    table's individual read time). An existing output file for a table is
    overwritten, with a warning logged first. With ``group_by_namespace``,
    each nonempty namespace produces one file containing its table classes.

    :param settings: Catalog name and output directory.
    :returns: None.
    """
    dump_catalog(
        settings,
        file_suffix=".schema.yaml",
        make_namespace_schema=_make_namespace_schema,
        convert_table=lambda table, identifier, namespace_schema: table_to_linkml(
            table, identifier, schema=namespace_schema
        ),
        has_grouped_entries=lambda namespace_schema: bool(namespace_schema.classes),
        write_schema=_write_schema,
        log=logger,
    )


if __name__ == "__main__":
    settings = IcebergToLinkMLSettings()  # pyright: ignore[reportCallIssue]
    dump_catalog_schemas(settings)
