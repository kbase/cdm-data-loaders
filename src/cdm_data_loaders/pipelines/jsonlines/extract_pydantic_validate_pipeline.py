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

import functools
import json
import threading
from collections.abc import Callable
from typing import Any

from dlt.common.pipeline import LoadInfo
from dlt.extract import DltResource
from pydantic import BaseModel, ValidationError

from cdm_data_loaders.coercion.pydantic_to_arrow import coerce_pydantic_record_for_pyarrow
from cdm_data_loaders.core.cli import CliRunner
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
    *,
    dump_to_dict: bool = False,
    by_alias: bool = False,
) -> tuple[ParseErrorResolver, Callable[[dict[str, Any]], BaseModel | dict[str, Any]]]:
    """Build paired validation and coercion-input callbacks that validate each record at most once.

    :param model: Pydantic ``BaseModel`` subclass used to validate incoming JSONL records
    :type model: type[BaseModel]
    :param dump_to_dict: when ``True``, return ``model_dump(mode="python", by_alias=by_alias)`` instead of the ``BaseModel`` instance
    :type dump_to_dict: bool
    :param by_alias: when ``True`` and ``dump_to_dict`` is ``True``, serialize dumped dictionary keys using field aliases
    :type by_alias: bool
    :return: tuple ``(resolve_error, extract_validated)``
    :rtype: tuple[ParseErrorResolver, Callable[[dict[str, Any]], BaseModel | dict[str, Any]]]
    """
    state = threading.local()

    def _convert_instance(instance: BaseModel) -> BaseModel | dict[str, Any]:
        if dump_to_dict:
            return instance.model_dump(mode="python", by_alias=by_alias)
        return instance

    def _validate(record: dict[str, Any]) -> str | None:
        try:
            state.validated = (record, _convert_instance(model.model_validate(record)))
        except ValidationError as e:
            state.validated = None
            return json.dumps(e.errors(), default=str)
        return None

    def _extract_validated(record: dict[str, Any]) -> BaseModel | dict[str, Any]:
        cached = getattr(state, "validated", None)
        state.validated = None
        if cached is not None and cached[0] is record:
            return cached[1]
        return _convert_instance(model.model_validate(record))

    return _validate, _extract_validated


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
    has_pyarrow_schema = (
        settings.use_pyarrow
        and settings.get_table_transform(table_name) is None
        and settings.schema_map is not None
        and table_name in settings.schema_map.registry
    )
    resolve_error, extract_validated = _make_validator_callbacks(
        model,
        dump_to_dict=not has_pyarrow_schema,
        by_alias=settings.by_alias,
    )
    record_coercer = (
        functools.partial(coerce_pydantic_record_for_pyarrow, by_alias=settings.by_alias)
        if settings.by_alias
        else coerce_pydantic_record_for_pyarrow
    )

    return build_shared_entity_resource(
        table_name,
        settings,
        resolve_error=resolve_error,
        transform_valid=extract_validated,
        record_coercer=record_coercer,
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


cli = CliRunner(JsonlPydanticIngestSettings, run_jsonlines_with_validation_pipeline)


if __name__ == "__main__":
    cli()
