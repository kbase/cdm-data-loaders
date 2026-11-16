"""Pipeline filesystem input set up and processing."""

from collections.abc import Callable, Generator, Iterable
from dataclasses import dataclass
from logging import Logger, getLogger
from typing import TYPE_CHECKING, Any

import dlt
import pyarrow as pa
from dlt.common.schema.typing import TSchemaContractDict
from dlt.common.storages.fsspec_filesystem import FileItemDict
from dlt.common.typing import TDataItems
from dlt.extract import DltResource
from dlt.sources.filesystem import filesystem
from dlt.sources.filesystem.helpers import fsspec_from_resource

from cdm_data_loaders.core.fields import ICEBERG, PARQUET, REJECTED_TABLE_NAME, LoaderFileFormatEnum
from cdm_data_loaders.core.settings import (
    FileFormatIngestSettings,
    InputLocalOutputSettings,
)
from cdm_data_loaders.utils.buffer import REJECTED_PYARROW_SCHEMA, make_rejected_row
from cdm_data_loaders.utils.file_items import source_id

if TYPE_CHECKING:
    from cdm_data_loaders.core.registry import FilePreprocessFn
    from cdm_data_loaders.core.transforms import OutputTableHints

logger: Logger = getLogger(__name__)


@dataclass(slots=True)
class FilesystemTracker:
    """Mutable collector for ingested file paths and file pre-processing errors."""

    ingested_files: list[str]
    preprocess_errors: list[str]

    def record_file(self, item: Any) -> Any:  # noqa: ANN401
        """Append ``item["relative_path"]`` to ``ingested_files`` if present and return ``item``.

        :param item: file item or record yielded by a filesystem resource
        :type item: Any
        :return: unmodified ``item``
        :rtype: Any
        """
        if isinstance(item, dict) and "relative_path" in item:
            self.ingested_files.append(item["relative_path"])
        return item


def find_filesystem_resources(resource: DltResource | list[DltResource]) -> list[DltResource]:
    """Return unique root ``filesystem`` resources reachable from ``resource``.

    :param resource: single dlt resource or list of dlt resources to inspect
    :type resource: DltResource | list[DltResource]
    :return: deduplicated list of root ``filesystem`` resources in traversal order
    :rtype: list[DltResource]
    """
    found: list[DltResource] = []
    seen_pipes: set[str] = set()
    resources = resource if isinstance(resource, list) else [resource]

    for res in resources:
        curr: DltResource | None = res
        while isinstance(curr, DltResource):
            pipe = curr._pipe  # noqa: SLF001
            if curr.name == "filesystem" and pipe.instance_id not in seen_pipes:
                seen_pipes.add(pipe.instance_id)
                found.append(curr)
            curr = curr._parent  # noqa: SLF001
    return found


def attach_file_collector(resource: DltResource | list[DltResource]) -> FilesystemTracker:
    """Attach or bind a tracker to root ``filesystem`` resources to record ingested files and pre-processing errors.

    :param resource: single dlt resource or list of dlt resources to inspect
    :type resource: DltResource | list[DltResource]
    :return: tracker collecting ingested file paths and pre-processing error messages
    :rtype: FilesystemTracker
    """
    collector = FilesystemTracker(ingested_files=[], preprocess_errors=[])
    for fs_resource in find_filesystem_resources(resource):
        pipe = fs_resource._pipe  # noqa: SLF001
        tracker: FilesystemTracker | None = getattr(pipe, "_fs_tracker", None)
        if tracker is None:
            tracker = FilesystemTracker(ingested_files=[], preprocess_errors=[])
            pipe._fs_tracker = tracker  # type: ignore[attr-defined]  # noqa: SLF001
            fs_resource.add_map(tracker.record_file)
        tracker.ingested_files = collector.ingested_files
        tracker.preprocess_errors = collector.preprocess_errors
    return collector


def _collect_relative_paths(fs_resources: Iterable[DltResource]) -> list[str]:
    """Return sorted unique ``relative_path`` entries yielded by ``fs_resources``.

    :param fs_resources: dlt ``filesystem`` resources to iterate
    :type fs_resources: Iterable[DltResource]
    :return: sorted unique POSIX relative file paths
    :rtype: list[str]
    """
    paths: set[str] = set()
    for fs_resource in fs_resources:
        for yielded in fs_resource:
            batch = [yielded] if isinstance(yielded, dict) else yielded
            for item in batch:
                if isinstance(item, dict) and "relative_path" in item:
                    paths.add(str(item["relative_path"]))
    return sorted(paths)


def list_input_dir_files(fs_resource: DltResource, input_dir: str) -> list[str]:
    """List all files under ``input_dir`` via dlt ``filesystem`` as sorted POSIX relative paths.

    :param fs_resource: root ``filesystem`` resource providing fsspec client configuration
    :type fs_resource: DltResource
    :param input_dir: local directory path or remote fsspec bucket URL
    :type input_dir: str
    :return: sorted POSIX file paths relative to ``input_dir``
    :rtype: list[str]
    """
    all_files = filesystem(
        bucket_url=input_dir,
        credentials=fsspec_from_resource(fs_resource),
        file_glob="**",
    )
    return _collect_relative_paths([all_files])


def collect_file_listing(
    settings: FileFormatIngestSettings,
    resource: DltResource | list[DltResource],
) -> tuple[list[str], list[str]]:
    """Collect ``(input_dir_files, matched_files)`` from root ``filesystem`` resources without reading files.

    :param settings: file-format ingest settings with ``input_dir``
    :type settings: FileFormatIngestSettings
    :param resource: single dlt resource or list of dlt resources to inspect
    :type resource: DltResource | list[DltResource]
    :raises ValueError: if ``resource`` does not contain any root ``filesystem`` resource
    :return: tuple of sorted ``input_dir_files`` and sorted ``matched_files``
    :rtype: tuple[list[str], list[str]]
    """
    fs_resources = find_filesystem_resources(resource)
    if not fs_resources:
        msg = "loader_file_format='file_listing' requires at least one dlt filesystem resource."
        raise ValueError(msg)

    input_dir_files = list_input_dir_files(fs_resources[0], str(settings.input_dir))
    matched_files = _collect_relative_paths(fs_resources)
    return input_dir_files, matched_files


def build_filesystem_resource(  # noqa: PLR0913
    settings: InputLocalOutputSettings,
    reader_fn: Callable[..., Any] | DltResource,
    *reader_args: Any,  # noqa: ANN401
    name: str,
    file_glob: str | None = None,
    table_name: str | None = None,
    schema_contract: TSchemaContractDict | None = None,
    **transformer_kwargs: Any,  # noqa: ANN401
) -> DltResource:
    """Build a dlt resource that pipes ``filesystem(settings.input_dir, file_glob)`` into a parallelized transformer.

    :param settings: pipeline settings with ``input_dir`` (and optional ``file_glob``)
    :type settings: InputLocalOutputSettings
    :param reader_fn: transformer function or resource accepting ``(items, settings, *reader_args)``
    :type reader_fn: Callable[..., Any] | DltResource
    :param reader_args: positional arguments bound to the transformer after ``settings``
    :type reader_args: Any
    :param name: name of the resulting dlt transformer resource
    :type name: str
    :param file_glob: glob pattern for files; defaults to ``settings.file_glob`` if present
    :type file_glob: str | None
    :param table_name: target table name for per-table file pre-processors and ``_rejected`` provenance
    :type table_name: str | None
    :param schema_contract: optional dlt schema contract hint applied to the resource
    :type schema_contract: TSchemaContractDict | None
    :param transformer_kwargs: additional keyword arguments passed to ``dlt.transformer``
    :type transformer_kwargs: Any
    :return: configured ``DltResource``
    :rtype: DltResource
    """
    fs_args: dict[str, Any] = {"bucket_url": str(settings.input_dir)}
    if file_glob is not None:
        fs_args["file_glob"] = file_glob
    elif glob := getattr(settings, "file_glob", None):
        fs_args["file_glob"] = glob

    files = filesystem(**fs_args)
    tracker = FilesystemTracker(ingested_files=[], preprocess_errors=[])
    files._pipe._fs_tracker = tracker  # type: ignore[attr-defined]  # noqa: SLF001

    resolved_table_name = (
        table_name
        if table_name is not None
        else (settings.table_name if isinstance(getattr(settings, "table_name", None), str) else None)  # pyright: ignore[reportAttributeAccessIssue]
    )
    preprocessors: tuple[FilePreprocessFn, ...] = (
        settings.get_file_preprocessors(resolved_table_name) if hasattr(settings, "get_file_preprocessors") else ()  # pyright: ignore[reportAttributeAccessIssue]
    )
    raw_fn: Callable[..., Any] = (
        getattr(reader_fn._pipe.gen, "__GEN__", reader_fn._pipe.gen)  # type: ignore[arg-type]  # noqa: SLF001
        if isinstance(reader_fn, DltResource)
        else reader_fn
    )

    def _preprocess_and_read(
        items: Iterable[FileItemDict],
        *args: Any,  # noqa: ANN401
        **kwargs: Any,  # noqa: ANN401
    ) -> Generator[TDataItems, Any, Any]:
        raw_items = [items] if isinstance(items, dict) else list(items)
        valid_items: list[FileItemDict] = []
        for file_item in raw_items:
            curr_item: FileItemDict | None = file_item
            failed = False
            for preprocess_fn in preprocessors:
                if curr_item is None:
                    break
                try:
                    curr_item = preprocess_fn(curr_item)
                except Exception as exc:  # noqa: BLE001
                    src_id = source_id(curr_item)
                    err_msg = f"File pre-processing failed for {src_id}: {exc}"
                    logger.error(err_msg)  # noqa: TRY400
                    tracker.preprocess_errors.append(err_msg)
                    rejected_row = make_rejected_row(
                        resolved_table_name,
                        str(exc),
                        source_file=src_id,
                    )
                    rejected_page: TDataItems = (
                        pa.Table.from_pylist([rejected_row], schema=REJECTED_PYARROW_SCHEMA)
                        if getattr(settings, "use_pyarrow", True)
                        else [rejected_row]
                    )
                    yield dlt.mark.with_table_name(
                        rejected_page,
                        REJECTED_TABLE_NAME,
                    )
                    failed = True
                    break
            if not failed and curr_item is not None:
                tracker.record_file(curr_item)
                valid_items.append(curr_item)

        if valid_items or not raw_items:
            result = raw_fn(valid_items, *args, **kwargs)
            if result is not None:
                yield from result

    parallelized = bool(transformer_kwargs.pop("parallelized", True))
    reader = dlt.transformer(_preprocess_and_read, name=name, parallelized=parallelized, **transformer_kwargs)
    resource: DltResource = files | reader(settings, *reader_args)
    if schema_contract is not None:
        resource.apply_hints(schema_contract=TSchemaContractDict(**schema_contract))  # pyright: ignore[reportCallIssue]
    if isinstance(settings, FileFormatIngestSettings) and settings.loader_file_format == LoaderFileFormatEnum.ICEBERG:
        resource.apply_hints(table_format="iceberg", file_format="parquet")
    output_table_hints: tuple[OutputTableHints, ...] = (
        settings.get_output_table_hints() if hasattr(settings, "get_output_table_hints") else ()
    )
    for table_hint in output_table_hints:
        table_hint.apply_to_resource(resource)
    return resource


def make_per_file_reader(
    file_fn: Callable[..., Iterable[Any]],
) -> Callable[..., Generator[TDataItems, Any, Any]]:
    """Wrap a single-file reader ``file_fn(file_item, *args)`` into a dlt transformer function over ``items``.

    :param file_fn: per-file reader callable accepting ``(file_item, *args)``
    :type file_fn: Callable[..., Iterable[Any]]
    :return: generator function over ``(items, *args)`` suitable for ``dlt.transformer``
    :rtype: Callable[..., Generator[TDataItems, Any, Any]]
    """

    def _read_items(items: Iterable[FileItemDict], *args: Any) -> Generator[TDataItems, Any, Any]:  # noqa: ANN401
        for file_item in items:
            yield from file_fn(file_item, *args)

    return _read_items


def build_per_file_filesystem_resource(
    settings: InputLocalOutputSettings,
    file_fn: Callable[..., Iterable[Any]],
    *reader_args: Any,  # noqa: ANN401
    name: str,
    file_glob: str | None = None,
    schema_contract: TSchemaContractDict | None = None,
    **transformer_kwargs: Any,  # noqa: ANN401
) -> DltResource:
    """Build a dlt resource that runs a single-file reader ``file_fn(file_item, settings, *reader_args)`` over ``filesystem`` items.

    :param settings: pipeline settings with ``input_dir`` (and optional ``file_glob``)
    :type settings: InputLocalOutputSettings
    :param file_fn: single-file reader callable accepting ``(file_item, settings, *reader_args)``
    :type file_fn: Callable[..., Iterable[Any]]
    :param reader_args: positional arguments bound to ``file_fn`` after ``settings``
    :type reader_args: Any
    :param name: name of the resulting dlt transformer resource
    :type name: str
    :param file_glob: glob pattern for files; defaults to ``settings.file_glob`` if present
    :type file_glob: str | None
    :param schema_contract: optional dlt schema contract hint applied to the resource
    :type schema_contract: TSchemaContractDict | None
    :param transformer_kwargs: additional keyword arguments passed to ``build_filesystem_resource``
    :type transformer_kwargs: Any
    :return: configured ``DltResource``
    :rtype: DltResource
    """
    return build_filesystem_resource(
        settings,
        make_per_file_reader(file_fn),
        *reader_args,
        name=name,
        file_glob=file_glob,
        schema_contract=schema_contract,
        **transformer_kwargs,
    )


def build_file_ingest_kwargs(
    settings: FileFormatIngestSettings,
    pipeline_name: str,
    *,
    fallback_dataset_name: bool = False,
) -> dict[str, dict[str, Any]]:
    """Build the standard ``pipeline_kwargs`` and ``pipeline_run_kwargs`` for a file-ingest pipeline.

    :param settings: file-format ingest settings with ``dataset_name`` and ``loader_file_format``
    :type settings: FileFormatIngestSettings
    :param pipeline_name: name of the dlt pipeline
    :type pipeline_name: str
    :param fallback_dataset_name: whether to fall back to ``pipeline_name`` when ``settings.dataset_name`` is None
    :type fallback_dataset_name: bool
    :return: mapping with ``pipeline_kwargs`` and ``pipeline_run_kwargs`` suitable for ``run_pipeline``
    :rtype: dict[str, dict[str, Any]]
    """
    dataset_name = (settings.dataset_name or pipeline_name) if fallback_dataset_name else settings.dataset_name
    run_kwargs: dict[str, Any] = (
        {"loader_file_format": str(PARQUET), "table_format": str(ICEBERG)}
        if settings.loader_file_format == LoaderFileFormatEnum.ICEBERG
        else {"loader_file_format": str(settings.loader_file_format)}
    )
    return {
        "pipeline_kwargs": {
            "pipeline_name": pipeline_name,
            "dataset_name": dataset_name,
        },
        "pipeline_run_kwargs": run_kwargs,
    }
