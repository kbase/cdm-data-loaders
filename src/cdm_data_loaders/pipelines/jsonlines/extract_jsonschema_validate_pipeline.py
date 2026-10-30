"""Load JSONL records validated against an external JSON Schema registry.

The module selected by ``schema_files_module`` must define ``ENTITY_SCHEMAS``:
a mapping of table names to JSON Schema dictionaries, each declaring ``$schema``.
Each validated JSON Schema is dereferenced and converted to a ``pyarrow.Schema``
via ``JSONSchemaToPyArrow``. Files are read from one input subdirectory per
table. Valid records are coerced to the target ``pyarrow.Schema`` and buffered
into ``pyarrow.Table`` pages via ``ArrowListBuffer``; parsing and validation
failures go to ``<table>_rejected`` with source_file, line_no, raw_record, and
error_detail. Format checks are enabled.
"""

import json
from logging import Logger, getLogger
from typing import Any

import pyarrow as pa
from dlt.common.pipeline import LoadInfo
from dlt.extract.resource import DltResource
from jsonschema import FormatChecker
from jsonschema.exceptions import SchemaError
from jsonschema.validators import validator_for

from cdm_data_loaders.converters.jsonschema_to_pyarrow import (
    JSONSchemaToPyArrow,
    JSONSchemaToPyArrowError,
    coerce_record_for_pyarrow,
)
from cdm_data_loaders.pipelines.core import PipelineCli
from cdm_data_loaders.pipelines.jsonlines.base import (
    build_entity_resource as build_shared_entity_resource,
)
from cdm_data_loaders.pipelines.jsonlines.base import (
    load_registry_dict,
    run_registry_ingest,
)
from cdm_data_loaders.pipelines.jsonlines.settings import JSONSCHEMA_PIPELINE_NAME, JsonlJsonschemaIngestSettings
from cdm_data_loaders.readers.jsonlines import ParseErrorResolver
from cdm_data_loaders.utils.jsonschema.dereferencer import DereferencingError, dereference_schema

logger: Logger = getLogger(__name__)


def load_entity_schemas(module_path: str) -> dict[str, dict[str, Any]]:
    """Import and validate a non-empty ENTITY_SCHEMAS registry.

    Raises ValueError for malformed registries, missing draft declarations, or
    unsupported drafts, TypeError for invalid schema types, and RuntimeError
    listing invalid schemas by table.

    :param module_path: dotted import path of the module defining ENTITY_SCHEMAS
    :type module_path: str
    :return: mapping of table name to JSONSchema model
    :rtype: dict[str, dict[str, Any]]
    :raises ValueError: if ENTITY_SCHEMAS is missing, empty, or not a dict
    """
    registry = load_registry_dict(module_path, "ENTITY_SCHEMAS")
    errors: dict[str, str] = {}
    for table_name, schema in registry.items():
        if not isinstance(table_name, str) or not table_name.strip():
            err_msg = "ENTITY_SCHEMAS keys must be non-empty table names"
            raise ValueError(err_msg)
        if not isinstance(schema, dict):
            err_msg = f"JSON Schema for {table_name} must be a dict"
            raise TypeError(err_msg)
        if "$schema" not in schema:
            err_msg = f"JSON Schema for {table_name} is missing the $schema keyword"
            raise ValueError(err_msg)
        if not isinstance(schema["$schema"], str):
            err_msg = f"JSON Schema for {table_name} must declare a $schema string"
            raise TypeError(err_msg)
        validator = validator_for(schema)
        declared = schema["$schema"]
        metaschema = getattr(validator, "META_SCHEMA", None)
        declared_supported = metaschema is None or "$schema" not in metaschema or metaschema["$schema"] == declared
        if not declared_supported:
            err_msg = f"Unsupported JSON Schema draft for {table_name}: {declared}"
            raise ValueError(err_msg)
        try:
            validator.check_schema(schema)
        except SchemaError as error:
            logger.exception("Error validating JSON Schema")
            errors[table_name] = str(error)

    if errors:
        details = "".join(f"{table_name}: {error}\n" for table_name, error in errors.items())
        err_msg = f"The following errors were found when validating the schemas supplied in {module_path}:\n{details}"
        raise RuntimeError(err_msg)
    return registry


def convert_schema_to_pyarrow(schema: dict[str, Any]) -> pa.Schema:
    """Dereference a validated JSON Schema and convert it to a PyArrow schema.

    :param schema: validated JSON Schema dictionary declaring ``$schema``
    :type schema: dict[str, Any]
    :return: target PyArrow schema for the entity table
    :rtype: pa.Schema
    :raises DereferencingError: if ``$ref`` or ``allOf`` cannot be resolved
    :raises JSONSchemaToPyArrowError: if the dereferenced schema cannot be converted to a PyArrow schema
    """
    dereferenced = dereference_schema(schema)
    if "$schema" not in dereferenced and "$schema" in schema:
        dereferenced = {"$schema": schema["$schema"], **dereferenced}
    return JSONSchemaToPyArrow().convert(dereferenced)


def build_pyarrow_schemas(table_to_schema: dict[str, dict[str, Any]]) -> dict[str, pa.Schema]:
    """Convert all entity JSON Schemas to PyArrow schemas up front.

    Collects any dereferencing or conversion errors across all tables and raises
    a single ``RuntimeError`` listing every failing table.

    :param table_to_schema: mapping of table name to validated JSON Schema dictionary
    :type table_to_schema: dict[str, dict[str, Any]]
    :return: mapping of table name to PyArrow schema
    :rtype: dict[str, pa.Schema]
    :raises RuntimeError: if one or more schemas cannot be converted to PyArrow schemas
    """
    pyarrow_schemas: dict[str, pa.Schema] = {}
    errors: dict[str, str] = {}
    for table_name, schema in table_to_schema.items():
        try:
            pyarrow_schemas[table_name] = convert_schema_to_pyarrow(schema)
        except (DereferencingError, JSONSchemaToPyArrowError) as error:
            logger.exception("Error converting JSON Schema to PyArrow for %s", table_name)
            errors[table_name] = str(error)

    if errors:
        details = "".join(f"{table_name}: {error}\n" for table_name, error in errors.items())
        err_msg = f"The following errors were found when converting schemas to PyArrow:\n{details}"
        raise RuntimeError(err_msg)
    return pyarrow_schemas


def _make_resolve_error(schema: dict[str, Any]) -> ParseErrorResolver:
    """Build the per-record JSON Schema validation closure.

    Schema failures report every broken rule as a JSON list of messages.
    """
    validator = validator_for(schema)(schema, format_checker=FormatChecker())

    def _validate(record: dict[str, Any]) -> str | None:
        errors = sorted(validator.iter_errors(record), key=str)
        if errors:
            return json.dumps([error.message for error in errors])
        return None

    return _validate


def build_entity_resource(
    table_name: str,
    schema: dict[str, Any],
    settings: JsonlJsonschemaIngestSettings,
    *,
    pyarrow_schema: pa.Schema | None = None,
) -> DltResource:
    """Read plain or gzip JSONL files for one entity and route validated pages.

    A missing entity directory produces no rows. Validated records are coerced
    to the target PyArrow schema and buffered into ``pyarrow.Table`` pages via
    ``ArrowListBuffer``. Nested values are preserved rather than normalized into
    child tables.

    :param table_name: table name
    :type  table_name: str
    :param schema: JSONSchema for the table
    :type  schema: dict[str, Any]
    :param settings: pipeline configuration
    :type  settings: JsonlJsonschemaIngestSettings
    :param pyarrow_schema: precomputed PyArrow schema, or None to convert ``schema``
    :type  pyarrow_schema: pa.Schema | None
    :return: resource yielding validated and rejected records for this entity
    :rtype: DltResource
    """
    resolved_pa_schema = pyarrow_schema if pyarrow_schema is not None else convert_schema_to_pyarrow(schema)
    return build_shared_entity_resource(
        table_name,
        settings,
        resolve_error=_make_resolve_error(schema),
        transform_valid=lambda record: coerce_record_for_pyarrow(record, resolved_pa_schema),
        pyarrow_schema=resolved_pa_schema,
    )


def run_jsonlines_pipeline(settings: JsonlJsonschemaIngestSettings) -> LoadInfo | None:
    """Validate the registry, build PyArrow schemas, and load requested tables.

    :param settings: pipeline configuration
    :type settings:  JsonlJsonschemaIngestSettings
    :raises ValueError: if settings.table_names includes a name not in ENTITY_SCHEMAS
    :raises RuntimeError: if any schema fails JSON Schema validation or PyArrow conversion
    :return: load information for the pipeline
    :rtype:  LoadInfo | None
    """
    table_to_schema = load_entity_schemas(settings.schema_files_module)
    pyarrow_schemas = build_pyarrow_schemas(table_to_schema)
    return run_registry_ingest(
        settings,
        table_to_schema,
        build_resource=lambda table_name, schema, cfg: build_entity_resource(
            table_name,
            schema,
            cfg,
            pyarrow_schema=pyarrow_schemas[table_name],
        ),
        pipeline_name=JSONSCHEMA_PIPELINE_NAME,
    )


cli = PipelineCli(JsonlJsonschemaIngestSettings, run_jsonlines_pipeline)


if __name__ == "__main__":
    cli()
