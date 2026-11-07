"""JSONL validate-and-load pipeline for the KBase CTS.

Reads JSONL files. Validates each record against a Pydantic model.
Writes valid and invalid records to separate tables.

The entity models come from an external module set by
`entity_models`. That module must define:

    ENTITY_MODELS: dict[str, type[BaseModel]]

Each key is a table name. Each value is the Pydantic model for that
table. Example:

    from datetime import date
    from pydantic import BaseModel

    class Sample(BaseModel):
        id: str
        collection_date: date

    ENTITY_MODELS: dict[str, type[BaseModel]] = {"sample": Sample}

`input_dir` must contain one subdirectory per table name in
`ENTITY_MODELS`. Each subdirectory holds that table's JSONL files.
Files may be plain text or gzip-compressed (`.jsonl` or `.jsonl.gz`).

Valid records go to a table named after the entity. Records that fail
JSON parsing or model validation go to the `_rejected` table. Each
rejected record includes the target table name, source file, line number,
raw text, and error detail.
Write disposition is `append` or `replace` only. No merge is performed.
"""

import json
import threading
from collections.abc import Callable
from typing import Any

from dlt.common.pipeline import LoadInfo
from dlt.extract import DltResource
from pydantic import BaseModel, ValidationError

from cdm_data_loaders.pipelines.core import PipelineCli
from cdm_data_loaders.pipelines.jsonlines.base import (
    build_entity_resource as build_shared_entity_resource,
)
from cdm_data_loaders.pipelines.jsonlines.base import run_registry_ingest
from cdm_data_loaders.pipelines.jsonlines.settings import (
    PYDANTIC_PIPELINE_NAME,
    JsonlPydanticIngestSettings,
)
from cdm_data_loaders.readers.jsonlines import ParseErrorResolver


def _make_validator_callbacks(
    model: type[BaseModel],
) -> tuple[ParseErrorResolver, Callable[[dict[str, Any]], dict[str, Any]]]:
    """Build paired validation and dump callbacks that validate each record at most once."""
    state = threading.local()

    def _validate(record: dict[str, Any]) -> str | None:
        try:
            state.validated = (record, model.model_validate(record).model_dump(mode="python"))
        except ValidationError as e:
            state.validated = None
            return json.dumps(e.errors(), default=str)
        return None

    def _dump_validated(record: dict[str, Any]) -> dict[str, Any]:
        cached = getattr(state, "validated", None)
        state.validated = None
        if cached is not None and cached[0] is record:
            return cached[1]
        return model.model_validate(record).model_dump(mode="python")

    return _validate, _dump_validated


def build_entity_resource(
    table_name: str, model: type[BaseModel], settings: JsonlPydanticIngestSettings
) -> DltResource:
    """Build the resource that reads, validates, and routes one entity's records.

    Reads files matching `settings.file_glob` from
    `settings.input_dir/table_name`. If that directory does not exist,
    uses an empty file list instead. A missing entity then produces zero
    rows, not an error.

    :param table_name: entity name; also the input_dir subdirectory name
    :type  table_name: str
    :param model: Pydantic model used to validate this entity's records
    :type  model: type[BaseModel]
    :param settings: pipeline settings
    :type  settings: JsonlPydanticIngestSettings
    :return: resource yielding validated and rejected records for this entity
    :rtype: DltResource
    """
    resolve_error, dump_validated = _make_validator_callbacks(model)

    return build_shared_entity_resource(
        table_name,
        settings,
        resolve_error=resolve_error,
        transform_valid=dump_validated,
    )


def run_jsonlines_with_validation_pipeline(settings: JsonlPydanticIngestSettings) -> LoadInfo | None:
    """Run the JSONL validate-and-load pipeline for every requested table.

    :param settings: pipeline configuration
    :type  settings: JsonlPydanticIngestSettings
    :return: load information for the pipeline
    :rtype: LoadInfo | None
    """
    return run_registry_ingest(
        settings,
        registry=settings.model_registry.registry,
        build_resource=build_entity_resource,
        pipeline_name=PYDANTIC_PIPELINE_NAME,
    )


cli = PipelineCli(JsonlPydanticIngestSettings, run_jsonlines_with_validation_pipeline)


if __name__ == "__main__":
    cli()
