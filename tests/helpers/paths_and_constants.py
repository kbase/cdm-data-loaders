"""Shared repository paths, S3/dlt constants, marker helpers, and state-preservation context managers."""

import gzip
import importlib.resources
import logging
import sys
from collections.abc import Generator, Iterable
from contextlib import contextmanager
from dataclasses import dataclass
from functools import partial
from pathlib import Path
from typing import Any, Final, Self
from unittest.mock import patch

from _pytest.logging import LogCaptureHandler
from _pytest.mark.expression import Expression
from frozendict import frozendict

import cdm_data_loaders.utils.logging as cdm_logging
from cdm_data_loaders.core.reporting import LOAD_INFO_TABLE_NAME
from cdm_data_loaders.utils.gz import GZIP_SUFFIX
from cdm_data_loaders.validation.jsonschema.metaschema import CUSTOM_META_SCHEMA_URI, get_xsv_meta_schema

LOCAL_FS: Final[str] = "local_fs"
S3: Final[str] = "s3"
VALID_DESTINATIONS: Final[tuple[str, ...]] = (LOCAL_FS, S3)
DEFAULT_DLT_TABLES: Final[frozenset[str]] = frozenset(
    {"_dlt_version", "_dlt_loads", "_dlt_pipeline_state", LOAD_INFO_TABLE_NAME}
)

TEST_BUCKET: Final[str] = "test_bucket"
ALT_BUCKET: Final[str] = "alt_bucket"
BUCKETS: Final[tuple[str, ...]] = (TEST_BUCKET, ALT_BUCKET)
CONFIG_BUCKET: Final[frozendict[str, str]] = frozendict({LOCAL_FS: "/output_dir", S3: "s3://some/s3/bucket"})

BASE_DIR: Final[Path] = Path("tests").parent
REPO_ROOT: Final[Path] = Path(__file__).resolve().parents[2]
TEST_DATA_DIR: Final[Path] = Path("tests") / "data"
CASSETTES_DIR: Final[Path] = Path("tests") / "cassettes"
SCHEMA_CONVERTER_SPECS_DIR: Final[Path] = BASE_DIR / "specs" / "schema_converters"
XSD_SPEC_DIR: Final[Path] = SCHEMA_CONVERTER_SPECS_DIR / "xsd_to_pyarrow"
PIPELINE_SPECS_DIR: Final[Path] = BASE_DIR / "specs" / "pipelines"
SPECS_ROOT: Final[Path] = (REPO_ROOT / "specs").resolve()

TRANSIENT_MODULE_NAMES: Final[frozenset[str]] = frozenset({"schema", "expected_schema"})
TRANSIENT_MODULE_PREFIXES: Final[tuple[str, ...]] = ("_fixture_", "_test_")
NATIVE_SPECLESS_MODULES: Final[frozenset[str]] = frozenset({"_openssl", "_openssl.lib", "specs"})
NATIVE_SPECLESS_PREFIXES: Final[tuple[str, ...]] = ("_openssl", "cryptography.", "specs.", "_cython_", "_duckdb.")

REFERENCE_XML_NS: Final[str] = "http://uniprot.org/uniref"
REFERENCE_XML_TAG: Final[str] = f"{{{REFERENCE_XML_NS}}}entry"
REFERENCE_XML_FIXTURE_DIR: Final[Path] = TEST_DATA_DIR / "uniprot" / "uniref"
N_REFERENCE_XML_ENTRIES: Final[int] = 100


def generate_dlt_config() -> dict[str, Any]:
    """Return a fresh dlt configuration dictionary seeded with ``local_fs`` and ``s3`` destinations.

    :return: Default test ``dlt.config`` mapping.
    :rtype: dict[str, Any]
    """
    return {
        "destination": {
            LOCAL_FS: {"bucket_url": CONFIG_BUCKET[LOCAL_FS], "destination_type": "filesystem"},
            S3: {"bucket_url": CONFIG_BUCKET[S3], "destination_type": "filesystem"},
        },
        "destination.local_fs.bucket_url": CONFIG_BUCKET[LOCAL_FS],
        "destination.local_fs.destination_type": "filesystem",
        "destination.s3.bucket_url": CONFIG_BUCKET[S3],
        "destination.s3.destination_type": "filesystem",
        "normalize.data_writer.disable_compression": False,
    }


def _matches_marker(target: str, name: str, **_kwargs: str | int | bool | None) -> bool:
    """Return whether a marker expression identifier matches ``target``.

    :param target: Marker name to match.
    :type target: str
    :param name: Candidate identifier passed by ``Expression.evaluate``.
    :type name: str
    :param _kwargs: Optional marker keyword arguments passed by ``Expression.evaluate``.
    :type _kwargs: str | int | bool | None
    :return: ``True`` when ``name == target``.
    :rtype: bool
    """
    return name == target


def is_marker_explicitly_selected(markexpr: str, marker: str) -> bool:
    """Determine whether a ``-m`` marker expression selects ``marker``.

    Returns ``False`` when ``markexpr`` is empty, omits ``marker`` (such as
    ``not docs``), negates ``marker`` (such as ``not state_audit``), or fails
    to parse.

    :param markexpr: Raw ``-m`` marker expression string from ``config.option.markexpr``.
    :type markexpr: str
    :param marker: Marker identifier to check.
    :type marker: str
    :return: ``True`` if ``marker`` appears in ``markexpr`` and evaluates to selected.
    :rtype: bool
    """
    stripped = markexpr.strip()
    if not stripped:
        return False
    try:
        compiled = Expression.compile(stripped)
    except SyntaxError:
        return False
    return marker in compiled.idents() and compiled.evaluate(partial(_matches_marker, marker))


@dataclass(frozen=True, slots=True)
class LoggerState:
    """Snapshot of mutable configuration on a single ``logging.Logger``."""

    level: int = logging.NOTSET
    propagate: bool = True
    disabled: bool = False
    filters: tuple[Any, ...] = ()
    handlers: tuple[logging.Handler, ...] = ()
    formatters: tuple[logging.Formatter | None, ...] = ()

    @classmethod
    def capture(cls, lg: logging.Logger, exclude: logging.Handler | None = None) -> Self:
        """Record the state of ``lg``, omitting ``exclude`` and pytest capture handlers.

        :param lg: Logger instance to inspect.
        :type lg: logging.Logger
        :param exclude: Optional handler managed outside the snapshot.
        :type exclude: logging.Handler | None
        :return: Captured ``LoggerState``.
        :rtype: Self
        """
        handlers = tuple(h for h in lg.handlers if h is not exclude and not isinstance(h, LogCaptureHandler))
        return cls(
            level=lg.level,
            propagate=lg.propagate,
            disabled=lg.disabled,
            filters=tuple(lg.filters),
            handlers=handlers,
            formatters=tuple(h.formatter for h in handlers),
        )

    def restore(self, lg: logging.Logger, *extra_handlers: logging.Handler) -> None:
        """Restore ``lg`` to the recorded state while keeping live pytest capture handlers.

        :param lg: Logger instance to restore.
        :type lg: logging.Logger
        :param extra_handlers: Externally managed handlers to preserve on ``lg``.
        :type extra_handlers: logging.Handler
        """
        live_capture_handlers = [
            h
            for h in lg.handlers
            if isinstance(h, LogCaptureHandler) and h not in self.handlers and h not in extra_handlers
        ]
        lg.setLevel(self.level)
        lg.propagate = self.propagate
        lg.disabled = self.disabled
        lg.filters[:] = self.filters
        lg.handlers[:] = [*self.handlers, *extra_handlers, *live_capture_handlers]
        for handler, formatter in zip(self.handlers, self.formatters, strict=True):
            handler.setFormatter(formatter)


@contextmanager
def preserve_logging_state(exclude_handler: logging.Handler | None = None) -> Generator[None]:
    """Snapshot all logger states, ``cdm_logging.applied_key``, and ``sys.excepthook``, restoring them on exit.

    :param exclude_handler: Optional handler managed per test phase.
    :type exclude_handler: logging.Handler | None
    :return: Context manager yielding ``None``.
    :rtype: Generator[None]
    """
    root = logging.getLogger()
    manager = logging.Logger.manager
    saved_manager_disable = manager.disable
    saved_excepthook = sys.excepthook
    root_state = LoggerState.capture(root, exclude=exclude_handler)
    saved = {
        name: LoggerState.capture(lg, exclude=exclude_handler)
        for name, lg in manager.loggerDict.items()
        if isinstance(lg, logging.Logger)
    }
    saved_applied_key = cdm_logging.applied_key
    cdm_logging.applied_key = None

    try:
        yield
    finally:
        loggers = {name: lg for name, lg in manager.loggerDict.items() if isinstance(lg, logging.Logger)}
        extra_handlers = (exclude_handler,) if exclude_handler is not None else ()
        known = {*root_state.handlers, *extra_handlers, *(h for state in saved.values() for h in state.handlers)}
        for lg in [root, *loggers.values()]:
            for handler in lg.handlers:
                if handler not in known and not isinstance(handler, LogCaptureHandler):
                    handler.close()
        root_state.restore(root, *extra_handlers)
        for name, lg in loggers.items():
            saved.get(name, LoggerState()).restore(lg)
        logging.disable(saved_manager_disable)
        cdm_logging.applied_key = saved_applied_key
        sys.excepthook = saved_excepthook


@contextmanager
def preserve_xsv_metaschema_state() -> Generator[None]:
    """Reset the XSV metaschema cache around a block and restore validator state on exit.

    :return: Context manager yielding ``None``.
    :rtype: Generator[None]
    """
    original_resources_files = importlib.resources.files
    was_cached = get_xsv_meta_schema.cache_info().currsize > 0
    get_xsv_meta_schema.cache_clear()
    try:
        with (
            patch.dict("jsonschema.validators._VALIDATORS") as validators_reg,
            patch.dict("jsonschema.validators._META_SCHEMAS") as meta_schemas_reg,
        ):
            validators_reg.pop(CUSTOM_META_SCHEMA_URI, None)
            meta_schemas_reg.pop(CUSTOM_META_SCHEMA_URI, None)
            yield
    finally:
        importlib.resources.files = original_resources_files
        get_xsv_meta_schema.cache_clear()
        if was_cached:
            get_xsv_meta_schema()


def copy_test_files(source_dir: Path, file_names: Iterable[str], dest_dir: Path) -> Path:
    """Copy test files into ``dest_dir``, preserving relative paths and gzip-compressing ``.gz`` targets.

    :param source_dir: Directory holding the source test files.
    :type source_dir: Path
    :param file_names: Relative file paths to copy.
    :type file_names: Iterable[str]
    :param dest_dir: Target directory (created if missing).
    :type dest_dir: Path
    :return: Populated ``dest_dir``.
    :rtype: Path
    """
    dest_dir.mkdir(parents=True, exist_ok=True)
    for name in file_names:
        target = dest_dir / name
        target.parent.mkdir(parents=True, exist_ok=True)
        if name.endswith(GZIP_SUFFIX):
            content = (source_dir / name.removesuffix(GZIP_SUFFIX)).read_bytes()
            with gzip.open(target, "wb") as handle:
                handle.write(content)
        else:
            target.write_bytes((source_dir / name).read_bytes())
    return dest_dir
