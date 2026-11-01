"""Shared machinery for the JSONL registry-ingest pipelines.

Each pipeline reads one JSONL input subdirectory per table, routes valid records
to a table named after the entity, and routes failures to ``_rejected``
with provenance columns (``table_name``, ``source_file``, ``line_no``, ``raw_record``,
``error_detail``). This module holds the pieces all registry pipelines share;
each pipeline module supplies only its validator and registry loader.
"""

from collections.abc import Callable
from importlib import import_module
from logging import Logger, getLogger
from typing import TYPE_CHECKING, Any

import dlt
import pyarrow as pa
from dlt.common.pipeline import LoadInfo
from dlt.common.schema.typing import TSchemaContractDict
from dlt.common.storages.fsspec_filesystem import fsspec_filesystem
from dlt.sources.filesystem import filesystem
from fsspec import AbstractFileSystem

from cdm_data_loaders.core.transforms import TableTransform
from cdm_data_loaders.pipelines.core import (
    DEFAULT_SCHEMA_CONTRACT,
    build_file_ingest_kwargs,
    run_pipeline,
)
from cdm_data_loaders.readers.jsonlines import (
    ParseErrorResolver,
    make_page_router,
    stream_jsonl_lines,
)
from cdm_data_loaders.utils.file_items import join_url

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


def entity_files(
    bucket_url: str,
    table_name: str,
    file_glob: str,
    credentials: AbstractFileSystem | None = None,
) -> "DltResource":
    """List the files for one entity, which live in the ``table_name`` subdirectory of ``bucket_url``.

    Works for any filesystem the dlt filesystem source supports. A missing subdirectory produces an
    empty resource and a warning. On object stores such as S3, which have no real directories, an
    empty prefix counts as missing.

    :param bucket_url: bucket URL or local directory holding one subdirectory per entity
    :type  bucket_url: str
    :param table_name: entity / table name; also the subdirectory name
    :type  table_name: str
    :param file_glob: glob for the entity's files, relative to its subdirectory
    :type  file_glob: str
    :param credentials: authenticated fsspec filesystem for ``bucket_url``; None uses fsspec defaults
    :type  credentials: AbstractFileSystem | None
    :return: dlt filesystem resource listing the entity's files, or an empty resource
    :rtype: DltResource
    """
    entity_url = join_url(bucket_url, table_name)
    fs = credentials if credentials is not None else fsspec_filesystem(entity_url)[0]
    if not fs.isdir(entity_url):
        logger.warning("Entity directory does not exist, skipping: %s", entity_url)
        return dlt.resource([], name=f"{table_name}_files")

    if credentials is None:
        return filesystem(bucket_url=entity_url, file_glob=file_glob)
    return filesystem(bucket_url=entity_url, credentials=credentials, file_glob=file_glob)


def build_entity_resource(  # noqa: PLR0913
    table_name: str,
    settings: Any,  # noqa: ANN401
    resolve_error: ParseErrorResolver,
    *,
    transform_valid: Callable[[Any], Any] | None = None,
    pyarrow_schema: pa.Schema | None = None,
    table_transform: TableTransform | None = None,
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
    :param transform_valid: function applied to each valid record before yielding
    :type  transform_valid: Callable[[Any], Any] | None
    :param pyarrow_schema: optional PyArrow schema for buffering records as Arrow tables
    :type  pyarrow_schema: pa.Schema | None
    :param table_transform: optional `TableTransform` for filtering, transforming, or splitting records
    :type  table_transform: TableTransform | None
    :return: resource yielding validated and rejected records for this entity
    :rtype: DltResource
    """
    files = entity_files(str(settings.input_dir), table_name, settings.file_glob)

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
            transform_valid=transform_valid,
            pyarrow_schema=pyarrow_schema,
            table_transform=table_transform,
        ),
        data_from=raw,
        name=f"{table_name}_validated",
        max_table_nesting=0,
        parallelized=True,
    )
    validated.apply_hints(schema_contract=TSchemaContractDict(**DEFAULT_SCHEMA_CONTRACT))  # pyright: ignore[reportCallIssue]
    return validated


def validate_table_transforms(
    settings: Any,  # noqa: ANN401
    table_to_schema: dict[str, Any],
) -> None:
    """Ensure every key in `settings.table_transforms` matches a registered entity table."""
    table_transforms = getattr(settings, "table_transforms", None) or {}
    unknown_transforms = sorted(set(table_transforms) - set(table_to_schema))
    if unknown_transforms:
        err_msg = f"Unknown table names in transforms_module: {', '.join(unknown_transforms)}"
        raise ValueError(err_msg)


def run_registry_ingest(
    settings: Any,  # noqa: ANN401
    table_to_schema: dict[str, dict[str, Any]],
    build_resource: ResourceFactory,
    pipeline_name: str,
    pipeline_run_kwargs: dict[str, Any] | None = None,
) -> LoadInfo | None:
    """Load tables using the shared pipeline runner.

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
    :return: load information for the pipeline
    :rtype:  LoadInfo | None
    """
    validate_table_transforms(settings, table_to_schema)
    resources = [
        build_resource(table_name, table_to_schema[table_name], settings) for table_name in sorted(table_to_schema)
    ]
    ingest_kwargs = build_file_ingest_kwargs(settings, pipeline_name, fallback_dataset_name=True)
    if pipeline_run_kwargs:
        ingest_kwargs["pipeline_run_kwargs"].update(pipeline_run_kwargs)
    return run_pipeline(
        settings=settings,
        resource=resources,
        **ingest_kwargs,
    )
