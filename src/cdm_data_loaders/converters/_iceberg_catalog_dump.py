"""Shared CLI settings and catalog traversal for Iceberg catalog schema dumpers."""

from collections.abc import Callable
from datetime import UTC, datetime
from logging import Logger
from pathlib import Path
from typing import Annotated, Final

from pydantic import Field
from pydantic_settings import SettingsConfigDict
from pyiceberg.catalog import load_catalog
from pyiceberg.table import Table

from cdm_data_loaders.core.fields import OUTPUT_DIR, NonEmptyStr
from cdm_data_loaders.core.path_fields import LocalOutputDir
from cdm_data_loaders.core.settings import LoggerSettings, default_settings_with_shortcuts

CATALOG_CLI_SHORTCUTS: Final[dict[str, str]] = {
    OUTPUT_DIR: "o",
    "catalog": "c",
    "group_by_namespace": "g",
}


class IcebergCatalogDumpSettings(LoggerSettings):
    """Shared settings for dumping table schemas from an Iceberg data catalog."""

    model_config: SettingsConfigDict = default_settings_with_shortcuts(cli_shortcuts=CATALOG_CLI_SHORTCUTS)

    catalog: Annotated[NonEmptyStr, Field(description="Name of the catalog to retrieve schemas from")]
    output_dir: LocalOutputDir
    group_by_namespace: bool = Field(default=False, description="Export one schema per namespace")


def write_schema_text(text: str, out_file: Path, log: Logger) -> None:
    """Write serialized schema text to disk, warning before replacing an existing file."""
    if out_file.exists():
        log.warning("Overwriting existing schema file %s", out_file)
    out_file.write_text(text, encoding="utf-8")
    log.info("Wrote schema to %s", out_file)


def dump_catalog[SchemaT](  # noqa: PLR0913
    settings: IcebergCatalogDumpSettings,
    *,
    file_suffix: str,
    make_namespace_schema: Callable[[str], SchemaT],
    convert_table: Callable[[Table, tuple[str, ...], SchemaT | None], SchemaT],
    has_grouped_entries: Callable[[SchemaT], bool],
    write_schema: Callable[[SchemaT, Path, str], None],
    log: Logger,
) -> None:
    """Iterate through an Iceberg catalog and write converted table or namespace schemas."""
    generated_at = datetime.now(UTC).isoformat()
    failures = 0

    with load_catalog(settings.catalog) as catalog:
        namespaces = catalog.list_namespaces()
        log.info("Dumping schemas for %d namespace(s) from catalog %r", len(namespaces), settings.catalog)
        for namespace in namespaces:
            namespace_name = ".".join(namespace)
            namespace_schema: SchemaT | None = (
                make_namespace_schema(namespace_name) if settings.group_by_namespace else None
            )
            for identifier in sorted(catalog.list_tables(namespace)):
                try:
                    table = catalog.load_table(identifier)
                    schema_doc = convert_table(table, identifier, namespace_schema)
                except Exception:
                    failures += 1
                    log.exception("Failed to convert table %s; skipping", identifier)
                    continue

                if namespace_schema is None:
                    write_schema(
                        schema_doc,
                        settings.output_dir / (".".join(identifier) + file_suffix),
                        generated_at,
                    )

            if namespace_schema is not None and has_grouped_entries(namespace_schema):
                write_schema(namespace_schema, settings.output_dir / (namespace_name + file_suffix), generated_at)

    if failures:
        log.warning("Completed with %d table(s) skipped due to conversion failures", failures)
