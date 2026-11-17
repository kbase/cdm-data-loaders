r"""CLI interface for applying PyArrow table and field definitions to an Iceberg table in Polaris.

Usage::

    uv run apply_polaris_schema_definitions \
        --catalog polaris \
        --table my_namespace.id_mapping \
        --schema-source cdm_data_loaders.datasets.uniprot.idmapping:UNIPROT_IDMAPPING_OUTPUT_SCHEMAS \
        --schema-key id_mapping
"""

import json
import sys
from collections.abc import Mapping, Sequence
from logging import Logger, getLogger
from pathlib import Path
from typing import Annotated, Any, Final

import pyarrow as pa
from pydantic import Field
from pydantic_settings import SettingsConfigDict

from cdm_data_loaders.converters.facades.jsonschema_to_pyarrow import JSONSchemaToPyArrow
from cdm_data_loaders.core.cli import CliRunner
from cdm_data_loaders.core.fields import NonEmptyStr
from cdm_data_loaders.core.registry import ArrowSchemaRegistry, load_python_module, split_module_attr_spec
from cdm_data_loaders.core.settings import LoggerSettings, default_settings_with_shortcuts
from cdm_data_loaders.utils.iceberg.catalog import (
    DEFAULT_CATALOG_NAME,
    PolarisCatalogConfig,
    connect_polaris_catalog,
)
from cdm_data_loaders.utils.iceberg.schema_sync import (
    DefinitionSyncOptions,
    SchemaDefinitionsReport,
    apply_schema_definitions,
    extract_metadata_definition,
)

logger: Logger = getLogger(__name__)

PIPELINE_NAME: Final[str] = "apply_polaris_schema_definitions"
CLI_SHORTCUTS: Final[dict[str, str]] = {
    "catalog": "c",
    "table": "t",
    "schema_source": "s",
    "schema_key": "k",
}


class PolarisSchemaDefinitionsSettings(LoggerSettings):
    """CLI settings for applying PyArrow table and field definitions to a Polaris table."""

    model_config: SettingsConfigDict = default_settings_with_shortcuts(
        cli_prog_name=PIPELINE_NAME,
        cli_shortcuts=CLI_SHORTCUTS,
    )

    catalog: Annotated[
        NonEmptyStr,
        Field(
            default=DEFAULT_CATALOG_NAME,
            description="Catalog name in ~/.pyiceberg.yaml or Polaris catalog identifier",
        ),
    ]
    table: Annotated[
        NonEmptyStr,
        Field(description="Dot-separated table identifier in Polaris (for example, 'my_namespace.my_table')"),
    ]
    schema_source: Annotated[
        NonEmptyStr,
        Field(
            description=(
                "PyArrow schema source: 'module.path[:ATTR]', 'path/to/schema.py[:ATTR]', or 'path/to/schema.json'"
            )
        ),
    ]
    schema_key: Annotated[
        NonEmptyStr | None,
        Field(
            default=None,
            description="Key in a schema mapping or ArrowSchemaRegistry when schema_source exports multiple schemas",
        ),
    ]
    table_description: Annotated[
        NonEmptyStr | None,
        Field(default=None, description="Table comment override replacing schema-level metadata"),
    ]
    uri: Annotated[
        NonEmptyStr | None,
        Field(
            default=None,
            description="Polaris REST catalog URI override (for example, 'http://polaris:8181/api/catalog')",
        ),
    ]
    warehouse: Annotated[
        NonEmptyStr | None,
        Field(default=None, description="Polaris warehouse name override"),
    ]
    credential: Annotated[
        NonEmptyStr | None,
        Field(default=None, description="OAuth2 'client_id:client_secret' credential override"),
    ]
    token: Annotated[
        NonEmptyStr | None,
        Field(default=None, description="Bearer token override"),
    ]
    scope: Annotated[
        NonEmptyStr | None,
        Field(default=None, description="OAuth2 scope override (for example, 'PRINCIPAL_ROLE:ALL')"),
    ]
    metadata_keys: Annotated[
        tuple[NonEmptyStr, ...],
        Field(
            default=("doc", "comment", "description"),
            description="Ordered Arrow metadata keys inspected for table and field definitions",
        ),
    ]
    normalize_root_names: Annotated[
        bool,
        Field(
            default=True,
            description="Fall back to dlt snake_case root column names when exact Arrow names are absent",
        ),
    ]
    strict: Annotated[
        bool,
        Field(
            default=False,
            description="Raise an error if any PyArrow field has no match in the target Polaris table",
        ),
    ]
    dry_run: Annotated[
        bool,
        Field(
            default=False,
            description="Compute table and field definition updates without committing to Polaris",
        ),
    ]


def _select_schema_from_object(
    obj: object,
    source_label: str,
    *,
    schema_key: str | None = None,
) -> pa.Schema:
    """Extract a ``pa.Schema`` from a schema instance, mapping, or ``ArrowSchemaRegistry``.

    :param obj: candidate object exported by a schema module
    :type obj: object
    :param source_label: human-readable label for error messages
    :type source_label: str
    :param schema_key: key within a schema mapping or ``ArrowSchemaRegistry``
    :type schema_key: str | None
    :return: resolved PyArrow schema
    :rtype: pa.Schema
    :raises TypeError: if ``obj`` is not a ``pa.Schema`` or mapping of schemas
    :raises ValueError: if ``schema_key`` is missing, unexpected, or not found
    """
    if isinstance(obj, pa.Schema):
        if schema_key is not None:
            msg = f"schema_key={schema_key!r} cannot be used because {source_label} is already a single pyarrow.Schema"
            raise ValueError(msg)
        return obj

    mapping: Mapping[str, Any] | None = None
    if isinstance(obj, ArrowSchemaRegistry):
        mapping = obj.registry
    elif isinstance(obj, Mapping):
        mapping = obj

    if mapping is None:
        msg = (
            f"{source_label} must be a pyarrow.Schema, ArrowSchemaRegistry, "
            f"or Mapping[str, pyarrow.Schema], got {type(obj).__name__}"
        )
        raise TypeError(msg)

    if schema_key is None:
        available = ", ".join(sorted(str(k) for k in mapping))
        msg = f"{source_label} contains multiple schemas ({available}); pass schema_key / --schema-key to select one"
        raise ValueError(msg)

    if schema_key not in mapping:
        available = ", ".join(sorted(str(k) for k in mapping))
        msg = f"Schema key {schema_key!r} not found in {source_label}; available keys: {available}"
        raise ValueError(msg)

    selected = mapping[schema_key]
    if not isinstance(selected, pa.Schema):
        msg = f"{source_label}[{schema_key!r}] must be a pyarrow.Schema, got {type(selected).__name__}"
        raise TypeError(msg)
    return selected


def _load_json_schema_as_pyarrow(json_path: Path, *, schema_key: str | None = None) -> pa.Schema:
    """Convert a JSON Schema file on disk into a ``pa.Schema``, preserving top-level ``description``.

    :param json_path: path to a Draft 2020-12 JSON Schema file
    :type json_path: Path
    :param schema_key: disallowed for single-document JSON Schema files
    :type schema_key: str | None
    :return: converted PyArrow schema with ``b"comment"`` table metadata when ``description`` is set
    :rtype: pa.Schema
    :raises ValueError: if ``schema_key`` is provided
    :raises TypeError: if the JSON root is not an object
    """
    if schema_key is not None:
        msg = f"schema_key={schema_key!r} is not supported when loading JSON Schema file {json_path}"
        raise ValueError(msg)
    raw = json.loads(json_path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        msg = f"JSON Schema file {json_path} must contain a JSON object at root"
        raise TypeError(msg)
    converted = JSONSchemaToPyArrow().convert(raw)
    root_desc = raw.get("description")
    if isinstance(root_desc, str) and root_desc.strip() and extract_metadata_definition(converted.metadata) is None:
        merged_meta: dict[bytes | str, bytes | str] = dict(converted.metadata or {})
        merged_meta[b"comment"] = root_desc
        converted = converted.with_metadata(merged_meta)
    return converted


def load_pyarrow_schema(schema_source: str, *, schema_key: str | None = None) -> pa.Schema:
    """Load a ``pa.Schema`` from a Python module/file spec or a JSON Schema file.

    :param schema_source: module path, Python file path (with ``:ATTR`` suffix if needed), or ``.json`` file path
    :type schema_source: str
    :param schema_key: key within a schema mapping or ``ArrowSchemaRegistry``
    :type schema_key: str | None
    :return: loaded PyArrow schema
    :rtype: pa.Schema
    :raises ValueError: if no matching schema attribute can be resolved
    """
    target, attr_name = split_module_attr_spec(schema_source)
    target_path = Path(target)
    if attr_name is None and target_path.suffix.lower() == ".json":
        return _load_json_schema_as_pyarrow(target_path, schema_key=schema_key)

    module = load_python_module(target)
    if attr_name is not None:
        if not hasattr(module, attr_name):
            msg = f"Module {target!r} has no attribute {attr_name!r}"
            raise ValueError(msg)
        return _select_schema_from_object(getattr(module, attr_name), f"{target}:{attr_name}", schema_key=schema_key)

    if schema_key is not None:
        for candidate in ("OUTPUT_SCHEMAS", "SCHEMAS"):
            if hasattr(module, candidate):
                return _select_schema_from_object(
                    getattr(module, candidate),
                    f"{target}:{candidate}",
                    schema_key=schema_key,
                )
        msg = f"Module {target!r} defines neither 'OUTPUT_SCHEMAS' nor 'SCHEMAS' for schema_key={schema_key!r}"
        raise ValueError(msg)

    if hasattr(module, "SCHEMA"):
        return _select_schema_from_object(module.SCHEMA, f"{target}:SCHEMA")

    msg = f"Module {target!r} has no default 'SCHEMA' attribute; specify '{target}:<ATTR>' or pass --schema-key"
    raise ValueError(msg)


def run_from_settings(settings: PolarisSchemaDefinitionsSettings) -> SchemaDefinitionsReport:
    """Connect to the configured Polaris catalog, load the PyArrow schema, and apply definitions.

    :param settings: parsed CLI or programmatic settings
    :type settings: PolarisSchemaDefinitionsSettings
    :return: report summarizing applied and skipped definitions
    :rtype: SchemaDefinitionsReport
    """
    schema = load_pyarrow_schema(settings.schema_source, schema_key=settings.schema_key)
    catalog_cfg = PolarisCatalogConfig(
        catalog=settings.catalog,
        uri=settings.uri,
        warehouse=settings.warehouse,
        credential=settings.credential,
        token=settings.token,
        scope=settings.scope,
    )
    options = DefinitionSyncOptions(
        table_description=settings.table_description,
        metadata_keys=settings.metadata_keys,
        normalize_root_names=settings.normalize_root_names,
        strict=settings.strict,
        dry_run=settings.dry_run,
    )
    with connect_polaris_catalog(catalog_cfg) as catalog:
        report = apply_schema_definitions(catalog, settings.table, schema, options=options)

    action = "Planned" if report.dry_run else "Applied"
    logger.info(
        "%s definitions on %s: table_comment=%r, updated_fields=%d, unchanged_fields=%d",
        action,
        ".".join(report.table_identifier),
        report.table_comment_applied,
        len(report.updated_fields),
        len(report.unchanged_fields),
    )
    return report


def apply_polaris_definitions(settings: PolarisSchemaDefinitionsSettings) -> SchemaDefinitionsReport:
    """Apply PyArrow definitions to Polaris and emit the JSON report to standard output.

    :param settings: parsed CLI settings
    :type settings: PolarisSchemaDefinitionsSettings
    :return: report summarizing applied and skipped definitions
    :rtype: SchemaDefinitionsReport
    """
    report = run_from_settings(settings)
    sys.stdout.write(f"{report.model_dump_json(indent=2)}\n")
    return report


cli = CliRunner(PolarisSchemaDefinitionsSettings, apply_polaris_definitions)


def main(argv: Sequence[str] | None = None) -> int:
    """Parse CLI arguments and apply PyArrow schema definitions to a Polaris table.

    :param argv: CLI argument sequence (defaults to ``sys.argv[1:]`` when ``None``)
    :type argv: Sequence[str] | None
    :return: exit code ``0`` on completion
    :rtype: int
    """
    cli_args = list(argv) if argv is not None else None
    cli.run(cli_args=cli_args)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
