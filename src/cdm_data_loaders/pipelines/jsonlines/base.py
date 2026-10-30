"""Shared machinery for the JSONL registry-ingest pipelines.

Each pipeline reads one JSONL input subdirectory per table, routes valid records
to a table named after the entity, and routes failures to ``<table>_rejected``
(or ``_invalid``) with provenance columns. This module holds the pieces all
three pipelines share; each pipeline module supplies only its validator and
registry loader.
"""

from collections.abc import Callable
from importlib import import_module
from logging import Logger, getLogger
from typing import TYPE_CHECKING, Any

import dlt
import pyarrow as pa
from dlt.common.pipeline import LoadInfo
from dlt.common.schema.typing import TSchemaContractDict
from dlt.sources.filesystem import filesystem

from cdm_data_loaders.pipelines.core import (
    DEFAULT_REJECTED_SUFFIX,
    DEFAULT_SCHEMA_CONTRACT,
    build_file_ingest_kwargs,
    run_pipeline,
)
from cdm_data_loaders.readers.jsonlines import (
    ParseErrorResolver,
    make_page_router,
    stream_jsonl_lines,
)

if TYPE_CHECKING:
    from dlt.extract.resource import DltResource

logger: Logger = getLogger(__name__)

ResourceFactory = Callable[[str, dict[str, Any], Any], "DltResource"]


def load_registry_dict(module_path: str, attribute_name: str, type_hint: str = "") -> dict[str, Any]:
    """Import ``module_path`` and return its non-empty dictionary attribute ``attribute_name``.

    :param module_path: dotted import path of the registry module
    :type module_path: str
    :param attribute_name: name of the dictionary attribute on the module
    :type attribute_name: str
    :param type_hint: optional suffix included in the ValueError message when missing or empty
    :type type_hint: str
    :return: the registry dictionary
    :rtype: dict[str, Any]
    :raises ValueError: if the attribute is missing, empty, or not a dict
    """
    module = import_module(module_path)
    registry = getattr(module, attribute_name, None)
    if not isinstance(registry, dict) or not registry:
        err_msg = f"{module_path} does not define a non-empty {attribute_name}{type_hint}"
        raise ValueError(err_msg)
    return registry


def build_entity_resource(  # noqa: PLR0913
    table_name: str,
    settings: Any,  # noqa: ANN401
    resolve_error: ParseErrorResolver,
    *,
    rejected_suffix: str = DEFAULT_REJECTED_SUFFIX,
    include_record: bool = False,
    transform_valid: Callable[[Any], Any] | None = None,
    pyarrow_schema: pa.Schema | None = None,
) -> "DltResource":
    """Read plain or gzip JSONL files for one entity and route validated pages.

    A missing entity directory produces no rows. Nested values are preserved
    rather than normalized into child tables.

    :param table_name: table name; also the input_dir subdirectory name
    :type  table_name: str
    :param settings: pipeline configuration with input_dir, file_glob, buffer_size
    :type  settings: Any
    :param resolve_error: function returning None for valid records or error detail
    :type  resolve_error: ParseErrorResolver
    :param rejected_suffix: suffix for the rejected table name
    :type  rejected_suffix: str
    :param include_record: whether rejected rows carry the parsed record value
    :type  include_record: bool
    :param transform_valid: function applied to each valid record before yielding
    :type  transform_valid: Callable[[Any], Any] | None
    :param pyarrow_schema: optional PyArrow schema for buffering valid records as Arrow tables
    :type  pyarrow_schema: pa.Schema | None
    :return: resource yielding validated and rejected records for this entity
    :rtype: DltResource
    """
    entity_dir = settings.input_dir / table_name
    if entity_dir.is_dir():
        files = filesystem(bucket_url=str(entity_dir), file_glob=settings.file_glob)
    else:
        logger.warning("Entity directory does not exist, skipping: %s", entity_dir)
        files = dlt.resource([], name=f"{table_name}_files")

    raw = dlt.transformer(
        stream_jsonl_lines,
        data_from=files,
        name=f"{table_name}_raw",
        max_table_nesting=0,
        parallelized=True,
    )
    raw.bind(settings.buffer_size)
    validated = dlt.transformer(
        make_page_router(
            table_name,
            settings.buffer_size,
            resolve_error,
            rejected_suffix=rejected_suffix,
            include_record=include_record,
            transform_valid=transform_valid,
            pyarrow_schema=pyarrow_schema,
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
    table_to_schema: dict[str, dict[str, Any]],
    build_resource: ResourceFactory,
    pipeline_name: str,
    pipeline_run_kwargs: dict[str, Any] | None = None,
) -> LoadInfo | None:
    """Load the requested tables using the shared pipeline runner.

    Defaults to every table in the registry when `settings.table_names` is None.
    Raises ValueError for unknown table names.

    :param settings: pipeline configuration
    :type  settings: Any
    :param table_to_schema: mapping of table name to its schema entry
    :type  table_to_schema: dict[str, dict[str, Any]]
    :param build_resource: function building one entity's resource
    :type  build_resource: ResourceFactory
    :param pipeline_name: registered pipeline name
    :type  pipeline_name: str
    :param pipeline_run_kwargs: extra kwargs for pipeline.run
    :type  pipeline_run_kwargs: dict[str, Any] | None
    :raises ValueError: if settings.table_names includes a name not in the registry
    :return: load information for the pipeline
    :rtype:  LoadInfo | None
    """
    table_names = settings.table_names or sorted(table_to_schema)
    unknown = sorted(set(table_names) - set(table_to_schema))
    if unknown:
        err_msg = f"Unknown table name(s) requested: {unknown}; available: {sorted(table_to_schema)}"
        raise ValueError(err_msg)
    resources = [build_resource(table_name, table_to_schema[table_name], settings) for table_name in table_names]
    ingest_kwargs = build_file_ingest_kwargs(settings, pipeline_name, fallback_dataset_name=True)
    if pipeline_run_kwargs:
        ingest_kwargs["pipeline_run_kwargs"].update(pipeline_run_kwargs)
    return run_pipeline(
        settings=settings,
        resource=resources,
        **ingest_kwargs,
    )
