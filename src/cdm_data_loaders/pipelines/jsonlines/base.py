"""Shared machinery for the JSONL registry-ingest pipelines.

Each pipeline reads one JSONL input subdirectory per table, routes valid records
to a table named after the entity, and routes failures to ``_rejected``
with provenance columns (``table_name``, ``source_file``, ``line_no``, ``raw_record``,
``error_detail``). This module holds the pieces all registry pipelines share;
each pipeline module supplies only its validator and registry loader.
"""

from collections.abc import Callable, Generator, Iterator
from typing import Any

import dlt
from dlt.common.pipeline import LoadInfo
from dlt.common.schema.typing import TSchemaContractDict
from dlt.common.storages.fsspec_filesystem import FileItemDict
from dlt.common.typing import TDataItems
from dlt.extract.resource import DltResource
from pydantic import BaseModel

from cdm_data_loaders.core.registry import (
    RegistryKey,
)
from cdm_data_loaders.pipelines.core import (
    DEFAULT_SCHEMA_CONTRACT,
    build_file_ingest_kwargs,
    build_filesystem_resource,
    run_pipeline,
)
from cdm_data_loaders.readers.jsonlines import (
    ParseErrorResolver,
    make_page_router,
    stream_jsonl_lines,
)
from cdm_data_loaders.schemas.jsonschema import ValidatedSchema

ResourceFactory = Callable[[str, Any, Any], DltResource]


def _stream_entity_lines(items: Iterator[FileItemDict], settings: Any) -> Generator[TDataItems, Any, Any]:  # noqa: ANN401
    """Stream pages of JSONL line records using ``settings.buffer_size``."""
    yield from stream_jsonl_lines(items, settings.buffer_size)


def build_entity_resource(
    table_name: str,
    settings: Any,  # noqa: ANN401
    resolve_error: ParseErrorResolver,
    *,
    transform_valid: Callable[[Any], Any] | None = None,
) -> DltResource:
    """Read plain or gzip JSONL files for one entity and route validated pages.

    A missing entity directory produces no rows. Nested values are preserved
    rather than normalized into child tables.

    :param table_name: table name; also the input_dir subdirectory name
    :type  table_name: str
    :param settings: pipeline configuration with input_dir, file_glob, buffer_size
    :type  settings: Any
    :param resolve_error: function returning None for valid records or error detail
    :type  resolve_error: ParseErrorResolver
    :param transform_valid: validation function applied to each record before yielding
    :type  transform_valid: Callable[[Any], Any] | None
    :return: resource yielding validated and rejected records for this entity
    :rtype: DltResource
    """
    raw = build_filesystem_resource(
        settings,
        _stream_entity_lines,
        name=f"{table_name}_raw",
        file_glob=f"{table_name}/{settings.file_glob}",
        table_name=table_name,
        max_table_nesting=0,
    )
    validated = dlt.transformer(
        make_page_router(
            table_name,
            settings.buffer_size,
            resolve_error,
            transform_valid=transform_valid,
            table_transform=settings.get_table_transform(table_name),
            schema_map=settings.schema_map,
            use_pyarrow=settings.use_pyarrow,
        ),
        data_from=raw,
        name=f"{table_name}_validated",
        max_table_nesting=0,
        parallelized=True,
    )
    validated.apply_hints(schema_contract=TSchemaContractDict(**DEFAULT_SCHEMA_CONTRACT))  # pyright: ignore[reportCallIssue]
    return validated


def run_registry_ingest(
    settings: Any,  # noqa: ANN401
    registry: dict[RegistryKey, type[BaseModel] | ValidatedSchema],
    build_resource: ResourceFactory,
    pipeline_name: str,
    pipeline_run_kwargs: dict[str, Any] | None = None,
) -> LoadInfo | None:
    """Load tables using the shared pipeline runner.

    :param settings: pipeline configuration
    :type  settings: Any
    :param registry: registry of table name to its schema/model entry
    :type  registry: dict[RegistryKey, type[BaseModel] | ValidatedSchema]
    :param build_resource: function building one entity's resource
    :type  build_resource: ResourceFactory
    :param pipeline_name: registered pipeline name
    :type  pipeline_name: str
    :param pipeline_run_kwargs: extra kwargs for pipeline.run
    :type  pipeline_run_kwargs: dict[str, Any] | None
    :return: load information for the pipeline
    :rtype:  LoadInfo | None
    """
    resources = [build_resource(table_name, registry[table_name], settings) for table_name in sorted(registry)]
    ingest_kwargs = build_file_ingest_kwargs(settings, pipeline_name, fallback_dataset_name=True)
    if pipeline_run_kwargs:
        ingest_kwargs["pipeline_run_kwargs"].update(pipeline_run_kwargs)
    return run_pipeline(
        settings=settings,
        resource=resources,
        **ingest_kwargs,
    )
