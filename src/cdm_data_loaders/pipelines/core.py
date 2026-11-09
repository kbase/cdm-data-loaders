"""Common reusable pipeline elements."""

import weakref
from collections.abc import Callable, Generator, Iterable
from dataclasses import dataclass
from datetime import datetime
from logging import Logger, getLogger
from pathlib import Path
from types import TracebackType
from typing import TYPE_CHECKING, Any, Final

import dlt
import pyarrow as pa
import yaml
from dlt.common.pipeline import LoadInfo
from dlt.common.runtime.slack import send_slack_message
from dlt.common.schema.typing import TSchemaContractDict
from dlt.common.storages.fsspec_filesystem import FileItemDict
from dlt.common.typing import TDataItems
from dlt.destinations.impl.filesystem.filesystem import FilesystemClient
from dlt.extract import DltResource
from dlt.pipeline.exceptions import PipelineStepFailed
from dlt.sources.filesystem import filesystem
from pydantic import ValidationError
from pydantic_settings import CliApp, SettingsError
from pyiceberg.catalog import sql as pyiceberg_sql
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.catalog.sql import SqlCatalog

from cdm_data_loaders.core.fields import ICEBERG, JSONL, PARQUET, REJECTED_TABLE_NAME, LoaderFileFormatEnum
from cdm_data_loaders.core.settings import (
    CtsSettings,
    FileFormatIngestSettings,
    InputLocalOutputSettings,
    LoggerSettings,
)
from cdm_data_loaders.utils.buffer import REJECTED_PYARROW_SCHEMA, make_rejected_row
from cdm_data_loaders.utils.cdm_logger import init_logger
from cdm_data_loaders.utils.file_items import source_id

if TYPE_CHECKING:
    from cdm_data_loaders.core.registry import FilePreprocessFn

WEBHOOK_NOT_CONFIGURED: Final[str] = "Slack webhook not configured"
NO_MESSAGE: Final[str] = "No message supplied"
LOAD_INFO_TABLE_NAME: Final[str] = "_load_info"
DEFAULT_SCHEMA_CONTRACT: Final[TSchemaContractDict] = {
    "tables": "evolve",
    "columns": "evolve",
    "data_type": "discard_row",
}

yaml.SafeDumper.add_multi_representer(datetime, yaml.SafeDumper.represent_datetime)

logger: Logger = getLogger(__name__)

_iceberg_catalog_cleanup_installed: bool = False


def _wrap_catalog_finalizer(
    catalog_cls: type[SqlCatalog | RestCatalog],
    get_cleanup_fn: Callable[[Any], Callable[[], Any] | None],
) -> None:
    """Attach a weakref finalizer on catalog init and invoke it on close."""
    orig_init = catalog_cls.__init__
    orig_close = catalog_cls.close

    def _init(self: Any, name: str, **properties: str) -> None:  # noqa: ANN401
        try:
            orig_init(self, name, **properties)
        finally:
            if (cleanup := get_cleanup_fn(self)) is not None:
                self._finalizer = weakref.finalize(self, cleanup)

    def _close(self: Any) -> None:  # noqa: ANN401
        if (finalizer := getattr(self, "_finalizer", None)) is not None:
            finalizer()
        orig_close(self)

    catalog_cls.__init__ = _init  # type: ignore[method-assign]
    catalog_cls.close = _close  # type: ignore[method-assign]


def _install_iceberg_catalog_cleanup() -> None:
    """Register weakref finalizers on PyIceberg catalogs and close cached catalogs on FilesystemClient exit.

    SqlCatalog holds a SQLAlchemy Engine <-> SingletonThreadPool cycle and RestCatalog holds a
    requests.Session <-> AuthManager cycle; neither defines a finalizer, and dlt's FilesystemClient.__exit__
    is a no-op. Furthermore, dlt runs load jobs on worker threads while garbage collection or callers may
    dispose catalogs on the main thread, which fails with sqlite3.ProgrammingError unless check_same_thread=False
    is set on SQLite connections.
    """
    global _iceberg_catalog_cleanup_installed  # noqa: PLW0603
    if _iceberg_catalog_cleanup_installed:
        return
    _iceberg_catalog_cleanup_installed = True

    orig_create_engine = pyiceberg_sql.create_engine

    def _create_engine(url: Any, *args: Any, **kwargs: Any) -> Any:  # noqa: ANN401
        if str(url).startswith("sqlite"):
            connect_args = dict(kwargs.get("connect_args") or {})
            connect_args.setdefault("check_same_thread", False)
            kwargs["connect_args"] = connect_args
        return orig_create_engine(url, *args, **kwargs)

    pyiceberg_sql.create_engine = _create_engine

    _wrap_catalog_finalizer(
        SqlCatalog,
        lambda cat: cat.engine.dispose if hasattr(cat, "engine") else None,
    )
    _wrap_catalog_finalizer(
        RestCatalog,
        lambda cat: session.close if (session := getattr(cat, "_session", None)) is not None else None,
    )

    orig_fs_exit = FilesystemClient.__exit__

    def _fs_client_exit(
        self: FilesystemClient,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        catalog = self._catalog
        self._catalog = None
        self._catalog_namespaces.clear()
        try:
            if catalog is not None and hasattr(catalog, "close"):
                catalog.close()
        finally:
            orig_fs_exit(self, exc_type, exc_val, exc_tb)  # type: ignore[arg-type]

    FilesystemClient.__exit__ = _fs_client_exit  # type: ignore[method-assign]


_install_iceberg_catalog_cleanup()


@dataclass(frozen=True, slots=True)
class PipelineCli[S: LoggerSettings]:
    """Console-script entry point for a pipeline; ``run`` returns the LoadInfo."""

    settings_cls: type[S]
    pipeline_fn: Callable[[S], LoadInfo | None]

    def __call__(self) -> None:
        """Parse sys.argv and run the pipeline. Returns None so the process exits with 0."""
        self.run()

    def run(self, cli_args: list[str] | None = None, **settings_kwargs: Any) -> LoadInfo | None:  # noqa: ANN401
        """Parse ``cli_args`` (default ``sys.argv[1:]``), run the pipeline and return its LoadInfo."""
        return run_cli(self.settings_cls, self.pipeline_fn, settings_kwargs, cli_args)


@dataclass(frozen=True, slots=True)
class PipelineReport:
    """Summary information for a pipeline run report."""

    pipeline_name: str
    dataset_name: str
    started_at: datetime
    finished_at: datetime
    pipeline_load_info: LoadInfo | None = None
    pipeline_err_msg: str | None = None
    info_load_err_msg: str | None = None
    ingested_files: list[str] | None = None
    preprocess_errors: list[str] | None = None


@dataclass(slots=True)
class _FilesystemTracker:
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


def send_slack_message_carefully(
    slack_hook: str,
    message: str,
    is_markdown: bool = False,  # noqa: FBT001, FBT002
) -> None:
    """Carefully send a slack message by wrapping it in a try/except.

    :param slack_hook: slack webhook URL
    :type slack_hook: str
    :param message: message to be slacked
    :type message: str
    :param is_markdown: whether or not the message is markdown, defaults to False
    :type is_markdown: bool, optional
    """
    if not slack_hook:
        logger.warning("Cannot send slack message: %s", WEBHOOK_NOT_CONFIGURED)
        return
    if not message or not message.strip():
        logger.warning("Cannot send slack message: %s", NO_MESSAGE)
        return

    try:
        send_slack_message(slack_hook, message.strip(), is_markdown)
    except Exception:
        logger.exception("Failed to send slack message")


def run_cli(
    settings_cls: type[LoggerSettings],
    pipeline_fn: Callable[[Any], LoadInfo | None],
    settings_kwargs: dict[str, Any] | None = None,
    cli_args: list[str] | None = None,
) -> LoadInfo | None:
    """Run a DLT pipeline from a generic CLI entry point.

    :param settings_cls: the Settings class to instantiate
    :type  settings_cls: type[LoggerSettings]
    :param pipeline_fn: the run_pipeline function to call with the config
    :type  pipeline_fn: Callable[[Any], LoadInfo | None]
    :param settings_kwargs: any extra non-cli/env var settings to be added
    :type  settings_kwargs: dict[str, Any] | None, optional
    :param cli_args: command line args
    :type  cli_args: list[str] | None, optional
    :return: pipeline load information
    :rtype: LoadInfo | None
    """
    try:
        settings = CliApp.run(settings_cls, cli_args=cli_args, **(settings_kwargs or {}))
        init_logger(settings)
    except (SettingsError, ValidationError, ValueError):
        logger.exception("Error initialising config")
        raise
    except Exception:
        logger.exception("Unexpected error setting up config")
        raise
    return pipeline_fn(settings)


def write_pipeline_report(
    report_path: Path,
    report: PipelineReport,
) -> None:
    """Write a summary report of a pipeline run to ``report_path``.

    :param report_path: path to the report file to write
    :type  report_path: Path
    :param report: pipeline report information to write
    :type  report: PipelineReport
    """
    report_data = {
        "pipeline": report.pipeline_name,
        "dataset": report.dataset_name,
        "started": report.started_at,
        "finished": report.finished_at,
    }

    if report.pipeline_load_info is None or report.pipeline_load_info.loads_ids is None:
        report_data["status"] = "error"
        if report.pipeline_err_msg:
            report_data["pipeline_err_msg"] = report.pipeline_err_msg
    else:
        failed_messages = [
            failed_job.failed_message
            for load_package in report.pipeline_load_info.load_packages
            for failed_job in load_package.jobs["failed_jobs"]
            if failed_job.failed_message
        ]
        if report.preprocess_errors:
            failed_messages.extend(report.preprocess_errors)
        status = "completed with errors" if failed_messages else "completed"
        report_data["status"] = status
        report_data["load_ids"] = report.pipeline_load_info.loads_ids
        if report.ingested_files:
            report_data["ingested_files"] = report.ingested_files
        if failed_messages:
            report_data["load_errors"] = failed_messages
        if report.info_load_err_msg is None:
            report_data["pipeline_load_info_errs"] = None
        else:
            report_data["pipeline_load_info_errs"] = report.info_load_err_msg

    try:
        with report_path.open("w", encoding="utf-8") as fh:
            yaml.safe_dump(report_data, fh, default_flow_style=False, sort_keys=False)
    except Exception as err:
        report_err_msg = f"Could not write pipeline report file: {err!s}"
        logger.exception(report_err_msg)


def save_pipeline_metadata(  # noqa: PLR0913, PLR0917
    settings: CtsSettings,
    pipeline: dlt.Pipeline,
    load_info: LoadInfo | None,
    err_msg: str | None,
    ingested_files: list[str] | None = None,
    preprocess_errors: list[str] | None = None,
) -> None:
    """Save the pipeline metadata to a unique directory in settings.output_dir.

    Writes a brief report of the pipeline state and saves it to report_<unique pipeline load id>.txt.

    :param settings: settings for this pipeline
    :type  settings: CtsSettings
    :param pipeline: dlt pipeline instance
    :type  pipeline: dlt.Pipeline
    :param load_info: pipeline load info, or None if the pipeline did not complete
    :type  load_info: LoadInfo | None
    :param err_msg: error message indicating why pipeline failed, or None if the pipeline succeeded
    :type  err_msg: str | None
    :param ingested_files: relative paths of files ingested from the filesystem resource, if any
    :type  ingested_files: list[str] | None, optional
    :param preprocess_errors: error messages from files that failed pre-processing, if any
    :type  preprocess_errors: list[str] | None, optional
    """
    started_at = pipeline.last_trace.started_at
    finished_at = pipeline.last_trace.finished_at

    info_load_err_msg: str | None = None
    if load_info is not None:
        try:
            # save the load info as JSONL in the same destination as the ingested data
            load_info_resource: DltResource = dlt.resource(
                [load_info.asdict()],
                name=LOAD_INFO_TABLE_NAME,
                max_table_nesting=0,
                columns={
                    k: {"data_type": "text"}
                    for k in [
                        "environment",
                        "staging_type",
                        "staging_name",
                        "staging_displayable_credentials",
                        "dataset_name",
                    ]
                },
            )
            load_info_save_info: LoadInfo = pipeline.run(load_info_resource, loader_file_format=str(JSONL))
            if load_info_save_info.has_failed_jobs:
                info_load_err_msg = f"Loading pipeline load info had failed jobs: {load_info_save_info}"
                logger.error(info_load_err_msg)
        except Exception as e:
            info_load_err_msg = f"Loading pipeline load info failed: {e!s}"
            logger.exception(info_load_err_msg)

    # write a summary and save it to the output directory
    report = PipelineReport(
        pipeline_name=pipeline.pipeline_name,
        dataset_name=pipeline.dataset_name,
        started_at=started_at,
        finished_at=finished_at,
        pipeline_load_info=load_info,
        pipeline_err_msg=err_msg,
        info_load_err_msg=info_load_err_msg,
        ingested_files=ingested_files,
        preprocess_errors=preprocess_errors,
    )
    write_pipeline_report(settings.report_path, report)


def _attach_file_collector(resource: DltResource | list[DltResource]) -> _FilesystemTracker:
    """Attach or bind a tracker to root ``filesystem`` resources to record ingested files and pre-processing errors."""
    collector = _FilesystemTracker(ingested_files=[], preprocess_errors=[])
    seen_pipes: set[str] = set()
    resources = resource if isinstance(resource, list) else [resource]

    for res in resources:
        curr: DltResource | None = res
        while isinstance(curr, DltResource):
            pipe = curr._pipe  # noqa: SLF001
            if curr.name == "filesystem" and pipe.instance_id not in seen_pipes:
                seen_pipes.add(pipe.instance_id)
                tracker: _FilesystemTracker | None = getattr(pipe, "_fs_tracker", None)
                if tracker is None:
                    tracker = _FilesystemTracker(ingested_files=[], preprocess_errors=[])
                    pipe._fs_tracker = tracker  # type: ignore[attr-defined]  # noqa: SLF001
                    curr.add_map(tracker.record_file)
                tracker.ingested_files = collector.ingested_files
                tracker.preprocess_errors = collector.preprocess_errors
            curr = curr._parent  # noqa: SLF001
    return collector


def run_pipeline(
    settings: CtsSettings,
    resource: DltResource | list[DltResource],
    destination_kwargs: dict[str, Any] | None = None,
    pipeline_kwargs: dict[str, Any] | None = None,
    pipeline_run_kwargs: dict[str, Any] | None = None,
) -> LoadInfo | None:
    """Execute a dlt pipeline.

    :param settings: pipeline settings
    :type settings: CtsSettings
    :param resource: dlt resource to run
    :type resource: DltResource | list[DltResource]
    :param destination_kwargs: keyword arguments for the dlt destination
    :type destination_kwargs: dict[str, Any] | None
    :param pipeline_kwargs: keyword arguments for the dlt pipeline
    :type pipeline_kwargs: dict[str, Any] | None
    :param pipeline_run_kwargs: keyword arguments for the dlt pipeline run
    :type pipeline_run_kwargs: dict[str, Any] | None
    :raises ValueError: if settings.use_destination is not configured in the dlt config
    :return: a load info object, if successful; otherwise None
    :rtype: LoadInfo | None
    """
    pipeline_kwargs = dict(pipeline_kwargs or {})

    # set the output directory for all the pipeline gubbins
    if settings.use_output_dir_for_pipeline_metadata:
        pipeline_kwargs["pipelines_dir"] = settings.pipeline_dir

    # update dev mode
    if settings.dlt_dev_mode:
        pipeline_kwargs["dev_mode"] = settings.dlt_dev_mode

    pipeline = dlt.pipeline(**pipeline_kwargs)

    try:
        pipeline.destination = dlt.destination(settings.use_destination, **(destination_kwargs or {}))
    except Exception as e:
        err_msg = f"Invalid destination configuration for {settings.use_destination}"
        logger.exception(err_msg)
        raise ValueError(err_msg) from e

    slack_hook: str | None = pipeline.runtime_config.slack_incoming_hook

    if not slack_hook:
        logger.info("No Slack alerts will be sent: %s", WEBHOOK_NOT_CONFIGURED)

    collector = _attach_file_collector(resource)
    load_info: LoadInfo | None = None
    err_msg: str | None = None
    try:
        load_info = pipeline.run(resource, **(pipeline_run_kwargs or {}))
    except PipelineStepFailed as e:
        # errors raise PipelineStepFailed
        err_msg = f"Pipeline failed: {e!s}"
        logger.exception(err_msg)

    has_errors = bool(load_info and load_info.has_failed_jobs) or bool(collector.preprocess_errors)
    if load_info:
        logger.info(load_info)
        if has_errors:
            logger.warning("Pipeline contains failed jobs")
        else:
            logger.info("All pipeline jobs completed successfully")

    save_pipeline_metadata(
        settings,
        pipeline,
        load_info,
        err_msg,
        ingested_files=collector.ingested_files or None,
        preprocess_errors=collector.preprocess_errors or None,
    )

    if slack_hook:
        msg = err_msg or f"Pipeline completed {'with errors' if has_errors else 'successfully!'}"
        send_slack_message_carefully(slack_hook, msg)

    return load_info


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
    """
    fs_args: dict[str, Any] = {"bucket_url": str(settings.input_dir)}
    if file_glob is not None:
        fs_args["file_glob"] = file_glob
    elif glob := getattr(settings, "file_glob", None):
        fs_args["file_glob"] = glob

    files = filesystem(**fs_args)
    tracker = _FilesystemTracker(ingested_files=[], preprocess_errors=[])
    files._pipe._fs_tracker = tracker  # type: ignore[attr-defined]  # noqa: SLF001

    resolved_table_name = (
        table_name
        if table_name is not None
        else (settings.table_name if isinstance(getattr(settings, "table_name", None), str) else None)
    )
    preprocessors: tuple[FilePreprocessFn, ...] = (
        settings.get_file_preprocessors(resolved_table_name) if hasattr(settings, "get_file_preprocessors") else ()
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
    return resource


def make_per_file_reader(
    file_fn: Callable[..., Iterable[Any]],
) -> Callable[..., Generator[TDataItems, Any, Any]]:
    """Wrap a single-file reader ``file_fn(file_item, *args)`` into a dlt transformer function over ``items``."""

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
    """Build a dlt resource that runs a single-file reader ``file_fn(file_item, settings, *reader_args)`` over ``filesystem`` items."""
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
