"""Load JSONL records validated against an external JSON Schema registry.

The module selected by ``entity_schemas`` must define ``ENTITY_SCHEMAS``:
a mapping of table names to JSON Schema dictionaries, each declaring ``$schema``.
Each validated JSON Schema is dereferenced and converted to a ``pyarrow.Schema``
via ``JSONSchemaToPyArrow``. Files are read from one input subdirectory per
table. Valid records are coerced to the target ``pyarrow.Schema`` and buffered
into ``pyarrow.Table`` pages via ``DictBuffer``; parsing and validation
failures go to ``_rejected`` with table_name, source_file, line_no, raw_record,
and error_detail. Format checks are enabled.
"""

import json
from typing import Any

from dlt.common.pipeline import LoadInfo
from dlt.extract.resource import DltResource
from jsonschema import FormatChecker
from jsonschema.validators import validator_for

from cdm_data_loaders.pipelines.core import PipelineCli
from cdm_data_loaders.pipelines.jsonlines.base import (
    build_entity_resource as build_shared_entity_resource,
)
from cdm_data_loaders.pipelines.jsonlines.base import run_registry_ingest
from cdm_data_loaders.pipelines.jsonlines.settings import (
    JSONSCHEMA_PIPELINE_NAME,
    JsonlJsonschemaIngestSettings,
)
from cdm_data_loaders.readers.jsonlines import ParseErrorResolver
from cdm_data_loaders.readers.jsonschema_xsv.xsv_validator.schema_utils import ValidatedSchema


def _make_resolve_error(schema: ValidatedSchema) -> ParseErrorResolver:
    """Build the per-record JSON Schema validation closure.

    Schema failures report every broken rule as a JSON list of messages.
    """
    raw_schema = schema.jsonschema
    validator = validator_for(raw_schema)(raw_schema, format_checker=FormatChecker())

    def _validate(record: dict[str, Any]) -> str | None:
        errors = sorted(validator.iter_errors(record), key=str)
        if errors:
            return json.dumps([error.message for error in errors])
        return None

    return _validate


def build_entity_resource(
    table_name: str,
    schema: ValidatedSchema,
    settings: JsonlJsonschemaIngestSettings,
) -> DltResource:
    """Read plain or gzip JSONL files for one entity and route validated pages.

    A missing entity directory produces no rows. Validated records are coerced
    to the target PyArrow schema(s) and buffered into ``pyarrow.Table`` pages via
    ``DictBuffer``. Nested values are preserved rather than normalized into
    child tables.

    :param table_name: table name
    :type  table_name: str
    :param schema: validated JSON Schema for the table
    :type  schema: ValidatedSchema
    :param settings: pipeline configuration
    :type  settings: JsonlJsonschemaIngestSettings
    :return: resource yielding validated and rejected records for this entity
    :rtype: DltResource
    """
    return build_shared_entity_resource(
        table_name,
        settings,
        resolve_error=_make_resolve_error(schema),
    )


def run_jsonlines_pipeline(settings: JsonlJsonschemaIngestSettings) -> LoadInfo | None:
    """Load requested tables using the validated registry and precomputed PyArrow schemas.

    :param settings: pipeline configuration
    :type settings:  JsonlJsonschemaIngestSettings
    :return: load information for the pipeline
    :rtype:  LoadInfo | None
    """
    return run_registry_ingest(
        settings,
        registry=settings.schema_registry.registry,
        build_resource=build_entity_resource,
        pipeline_name=JSONSCHEMA_PIPELINE_NAME,
    )


cli = PipelineCli(JsonlJsonschemaIngestSettings, run_jsonlines_pipeline)


if __name__ == "__main__":
    cli()
