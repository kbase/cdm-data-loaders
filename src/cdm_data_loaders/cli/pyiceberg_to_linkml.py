"""CLI interface for creating a LinkML dump of table schemas from an Iceberg catalog."""

import operator
from logging import Logger, getLogger
from pathlib import Path
from typing import Final, cast

from linkml_runtime.dumpers import yaml_dumper
from linkml_runtime.linkml_model.meta import Annotation, Prefix, SchemaDefinition
from pydantic import Field
from pydantic_settings import SettingsConfigDict

from cdm_data_loaders.cli.pyiceberg_dump import (
    CATALOG_CLI_SHORTCUTS,
    CatalogDumpDriver,
    IcebergCatalogDumpSettings,
    dump_catalog,
)
from cdm_data_loaders.converters.core.io import write_schema_text
from cdm_data_loaders.converters.emitters.linkml import LINKML_PREFIX
from cdm_data_loaders.converters.facades.pyiceberg_to_linkml import table_to_linkml
from cdm_data_loaders.core.cli import CliRunner
from cdm_data_loaders.core.settings import default_settings_with_shortcuts

logger: Logger = getLogger(__name__)

PIPELINE_NAME: Final[str] = "pyiceberg_to_linkml"


class IcebergToLinkMLSettings(IcebergCatalogDumpSettings):
    """Settings for generating a set of LinkML schemas from an Iceberg data catalog."""

    model_config: SettingsConfigDict = default_settings_with_shortcuts(
        cli_prog_name=PIPELINE_NAME, cli_shortcuts=CATALOG_CLI_SHORTCUTS
    )

    group_by_namespace: bool = Field(default=False, description="Export one LinkML schema per namespace")


def _write_schema(schema: SchemaDefinition, out_file: Path, generated_at: str) -> None:
    """Write a timestamped schema, warning before replacing an existing file.

    :param schema: LinkML ``SchemaDefinition`` to serialize.
    :type schema: SchemaDefinition
    :param out_file: Destination file path.
    :type out_file: Path
    :param generated_at: ISO-8601 timestamp string.
    :type generated_at: str
    """
    cast("dict[str, Annotation]", schema.annotations)["generated_at"] = Annotation(
        tag="generated_at", value=generated_at
    )
    write_schema_text(yaml_dumper.dumps(schema), out_file, logger)


def _make_namespace_schema(namespace_name: str) -> SchemaDefinition:
    """Build an empty namespace-scoped LinkML ``SchemaDefinition``.

    :param namespace_name: Dot-joined Iceberg namespace name.
    :type namespace_name: str
    :return: Empty namespace ``SchemaDefinition``.
    :rtype: SchemaDefinition
    """
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


LINKML_DUMP_DRIVER: Final[CatalogDumpDriver[SchemaDefinition]] = CatalogDumpDriver(
    file_suffix=".schema.yaml",
    make_namespace_schema=_make_namespace_schema,
    convert_table=table_to_linkml,
    grouped_entries_getter=operator.attrgetter("classes"),
    write_schema=_write_schema,
)


def dump_catalog_schemas(settings: IcebergToLinkMLSettings) -> None:
    """Iterate through an Iceberg catalog and write table schemas as LinkML documents.

    Tables that fail to load or convert are logged and skipped without aborting
    remaining tables. ``generated_at`` carries a single run timestamp across all
    emitted schemas. With ``group_by_namespace``, each non-empty namespace writes
    one file containing its table classes.

    :param settings: Catalog name and output directory.
    :type settings: IcebergToLinkMLSettings
    """
    dump_catalog(settings, LINKML_DUMP_DRIVER, logger)


cli = CliRunner(IcebergToLinkMLSettings, dump_catalog_schemas)


if __name__ == "__main__":
    cli()
