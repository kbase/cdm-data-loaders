"""Unified test execution framework for running CDM data ingestion pipelines in core, cli, or docker mode."""

import os
import shutil
import subprocess
import sys
import sysconfig
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from pathlib import Path
from types import ModuleType
from typing import Any, Final, Self
from unittest.mock import patch
from uuid import uuid4

import dlt
import pytest
import yaml
from dlt.common.pipeline import LoadInfo
from dlt.destinations import filesystem

from cdm_data_loaders.core import cli as core_cli
from cdm_data_loaders.core.fields import PIPELINE_DIR_NAME
from cdm_data_loaders.core.pipeline_catalog import (
    PipelineIdentifier,
    PipelineSpec,
    discover_pipeline_targets,
    resolve_pipeline,
)
from cdm_data_loaders.core.registry import split_module_attr_spec
from cdm_data_loaders.core.reporting import LOAD_INFO_TABLE_NAME
from cdm_data_loaders.core.settings import CtsSettings, LoggerSettings
from tests.helpers.cli_assertions import settings_to_argv
from tests.helpers.paths_and_constants import BASE_DIR, CONFIG_BUCKET, LOCAL_FS, preserve_logging_state
from tests.helpers.table_readers import read_jsonl_file

DEFAULT_DOCKER_IMAGE_TAG: Final[str] = "ghcr.io/kbase/cdm-data-loaders:test_container"
CONTAINER_INPUT_DIR: Final[str] = "/input_dir"
CONTAINER_OUTPUT_DIR: Final[str] = "/output_dir"
CONTAINER_AUX_DIR: Final[str] = "/aux_dir"
CONTAINER_PYMODULES_DIR: Final[str] = "/pymodules"
CONTAINER_MOUNTED_PACKAGES: Final[frozenset[str]] = frozenset({"specs", "tests"})
LIBRARY_PATH_KEYS: Final[tuple[str, ...]] = ("purelib", "platlib", "stdlib", "platstdlib")

# flags whose values are local file paths that must be staged into the container in Docker mode
FILE_PATH_FLAGS: Final[frozenset[str]] = frozenset(
    {
        "--xsd-file",
        "--schema-file",
        "-s",
        "--log-config-file",
    }
)

# flags whose values are Python module paths or .py file specs that may need staging in Docker mode
MODULE_PATH_FLAGS: Final[frozenset[str]] = frozenset(
    {
        "--entity-models",
        "--entity-schemas",
        "-m",
        "--preprocessors-module",
        "--transforms-module",
        "--output-schemas-module",
    }
)

INPUT_DIR_FLAGS: Final[frozenset[str]] = frozenset({"--input-dir", "-i"})
OUTPUT_DIR_FLAGS: Final[frozenset[str]] = frozenset({"--output-dir", "-o"})
DESTINATION_FLAGS: Final[frozenset[str]] = frozenset({"--use-destination", "-d"})

_built_docker_images: set[str] = set()


class ExecutionMode(StrEnum):
    """Supported pipeline test execution modes."""

    CORE = "core"
    CLI = "cli"
    DOCKER = "docker"


EXECUTION_MODES: Final[tuple[Any, ...]] = (
    pytest.param(ExecutionMode.CORE, id="core"),
    pytest.param(ExecutionMode.CLI, id="cli"),
    pytest.param(ExecutionMode.DOCKER, id="docker", marks=pytest.mark.docker),
)


class _PreservedStrDatetime(datetime):
    """Datetime subclass that preserves the exact string representation written to the report file."""

    _raw_str: str

    def __new__(cls, raw_str: str) -> Self:
        """Create a datetime from an ISO-formatted string while preserving its original string spelling."""
        dt = datetime.fromisoformat(raw_str)
        instance = super().__new__(
            cls,
            dt.year,
            dt.month,
            dt.day,
            dt.hour,
            dt.minute,
            dt.second,
            dt.microsecond,
            tzinfo=dt.tzinfo,
        )
        instance._raw_str = raw_str
        return instance

    def __str__(self) -> str:
        """Return the verbatim timestamp string from the pipeline report."""
        return self._raw_str


@dataclass
class ReconstructedLoadInfo:
    """LoadInfo-compatible object reconstructed from pipeline output files on disk."""

    pipeline: dlt.Pipeline
    dataset_name: str
    loads_ids: list[str]
    started_at: datetime
    has_failed_jobs: bool = False
    finished_at: datetime | None = None
    first_run: bool = False
    destination_name: str = LOCAL_FS
    destination_type: str = "filesystem"
    load_packages: list[Any] = field(default_factory=list)
    metrics: dict[str, Any] = field(default_factory=dict)
    _raw_dict: dict[str, Any] = field(default_factory=dict, repr=False)

    def asdict(self) -> dict[str, Any]:
        """Return a dictionary representation matching dlt's LoadInfo.asdict()."""
        if self._raw_dict:
            return dict(self._raw_dict)
        return {
            "dataset_name": self.dataset_name,
            "destination_name": self.destination_name,
            "destination_type": self.destination_type,
            "finished_at": self.finished_at,
            "first_run": self.first_run,
            "has_failed_jobs": self.has_failed_jobs,
            "loads_ids": list(self.loads_ids),
            "pipeline": {"pipeline_name": self.pipeline.pipeline_name},
            "started_at": self.started_at,
        }


def _read_saved_load_info_row(dataset_dir: Path) -> dict[str, Any]:
    """Read the serialized load_info row from ``<dataset_dir>/_load_info`` if present."""
    load_info_dir = dataset_dir / LOAD_INFO_TABLE_NAME
    if not load_info_dir.is_dir():
        return {}
    for file_path in sorted(load_info_dir.glob("*.jsonl*")):
        if not file_path.is_file():
            continue
        rows = read_jsonl_file(file_path)
        if rows:
            return rows[0]
    return {}


def reconstruct_load_info(
    output_dir: Path,
    bucket_dir: Path | None = None,
    *,
    use_destination: str | None = None,
) -> LoadInfo | None:
    """Reconstruct a LoadInfo-compatible object and attached dlt.Pipeline from disk artifacts.

    Reads the ``report.txt`` file written by ``save_pipeline_metadata`` in
    ``output_dir / PIPELINE_DIR_NAME`` or ``output_dir`` and attaches a ``dlt.Pipeline``
    pointing at the filesystem destination containing ``<dataset_name>`` (or the named
    ``use_destination`` from ``dlt.config`` when supplied).

    :param output_dir: directory containing ``report.txt`` or ``pipeline_metadata/report.txt``
    :type output_dir: Path
    :param bucket_dir: optional separate filesystem destination bucket directory
    :type bucket_dir: Path | None
    :param use_destination: optional configured dlt destination block name
    :type use_destination: str | None
    :return: reconstructed LoadInfo, or None if the report indicates an error or no report exists
    :rtype: LoadInfo | None
    """
    output_dir = Path(output_dir)
    candidate_reports = (output_dir / PIPELINE_DIR_NAME / "report.txt", output_dir / "report.txt")
    report_files = sorted(
        (path for path in candidate_reports if path.is_file()),
        key=lambda path: path.stat().st_mtime,
    )
    if not report_files:
        return None

    report_file = report_files[-1]
    with report_file.open("r", encoding="utf-8") as fh:
        report_data = yaml.safe_load(fh)
    if not isinstance(report_data, dict):
        return None

    required_keys = {"pipeline", "dataset", "started", "finished", "status"}
    if not required_keys.issubset(report_data):
        return None

    pipeline_name = str(report_data["pipeline"])
    dataset_name = str(report_data["dataset"])
    started_at = _PreservedStrDatetime(str(report_data["started"]))
    report_finished_at = _PreservedStrDatetime(str(report_data["finished"]))

    status_str = str(report_data["status"])
    if status_str == "error":
        return None
    has_failed_jobs = status_str != "completed"

    raw_load_ids = report_data.get("load_ids") or []
    loads_ids = [str(item) for item in raw_load_ids]

    # Determine where the dataset directory actually lives on disk
    if (output_dir / dataset_name).is_dir():
        actual_bucket_dir = output_dir
    elif bucket_dir is not None and (Path(bucket_dir) / dataset_name).is_dir():
        actual_bucket_dir = Path(bucket_dir)
    else:
        actual_bucket_dir = Path(bucket_dir) if bucket_dir is not None else output_dir

    saved_row = _read_saved_load_info_row(actual_bucket_dir / dataset_name)
    finished_at_raw = saved_row.get("finished_at")
    finished_at = _PreservedStrDatetime(str(finished_at_raw)) if finished_at_raw else report_finished_at

    # Use a fresh working directory outside output_dir so dlt syncs the schema from the destination
    # without colliding with stale schemas in ~/.dlt/pipelines or adding files to output_dir.
    reconstructed_id = loads_ids[0] if loads_ids else uuid4().hex
    reconstructed_pipelines_dir = output_dir.parent / f".reconstructed_dlt_{reconstructed_id}"
    destination_arg: Any = (
        use_destination if use_destination is not None else filesystem(bucket_url=str(actual_bucket_dir))
    )
    pipeline = dlt.pipeline(
        pipeline_name=pipeline_name,
        destination=destination_arg,
        dataset_name=dataset_name,
        pipelines_dir=str(reconstructed_pipelines_dir),
    )

    return ReconstructedLoadInfo(  # pyright: ignore[reportReturnType]
        pipeline=pipeline,
        dataset_name=dataset_name,
        loads_ids=loads_ids,
        started_at=started_at,
        has_failed_jobs=has_failed_jobs,
        finished_at=finished_at,
        first_run=bool(saved_row.get("first_run", False)),
        destination_name=str(saved_row.get("destination_name", use_destination or LOCAL_FS)),
        destination_type=str(saved_row.get("destination_type", "filesystem")),
        _raw_dict=saved_row,
    )


def _docker_image_exists(image_tag: str) -> bool:
    """Return True if ``image_tag`` already exists in the local Docker image store."""
    result = subprocess.run(  # noqa: S603
        ["docker", "image", "inspect", image_tag],  # noqa: S607
        capture_output=True,
        text=True,
        check=False,
    )
    return result.returncode == 0


def ensure_docker_image(image_tag: str | None = None, *, force_rebuild: bool = False) -> str:
    """Build the Docker image from the repository Dockerfile if not already present or built in this session.

    :param image_tag: optional image tag; defaults to ``ghcr.io/kbase/cdm-data-loaders:test_container``
    :type image_tag: str | None
    :param force_rebuild: force running ``docker build`` even if already built in this session
    :type force_rebuild: bool
    :raises RuntimeError: if ``docker build`` fails
    :return: Docker image tag
    :rtype: str
    """
    tag = image_tag or os.environ.get("CDM_TEST_DOCKER_IMAGE") or DEFAULT_DOCKER_IMAGE_TAG
    if not force_rebuild and (tag in _built_docker_images or _docker_image_exists(tag)):
        _built_docker_images.add(tag)
        return tag

    cmd = ["docker", "build", "-t", tag, str(BASE_DIR.resolve())]
    result = subprocess.run(cmd, capture_output=True, text=True, check=False)  # noqa: S603
    if result.returncode != 0:
        err_msg = (
            f"Docker image build failed (exit {result.returncode}):\nSTDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"
        )
        raise RuntimeError(err_msg)

    _built_docker_images.add(tag)
    return tag


def _extract_flag_value(cli_args: list[str], flags: frozenset[str]) -> str | None:
    """Extract the value immediately following any of ``flags`` in ``cli_args``."""
    for idx, arg in enumerate(cli_args):
        if arg in flags and idx + 1 < len(cli_args):
            return cli_args[idx + 1]
        for flag in flags:
            if arg.startswith(f"{flag}="):
                return arg.split("=", maxsplit=1)[1]
    return None


def _normalize_cli_args(cli_args: list[str]) -> list[str]:
    """Strip 0-byte ``.conf`` placeholder log files created by test fixtures that bypass ``init_logger``."""
    normalized: list[str] = []
    idx = 0
    while idx < len(cli_args):
        arg = cli_args[idx]
        if arg == "--log-config-file" and idx + 1 < len(cli_args):
            log_path = Path(cli_args[idx + 1])
            if log_path.suffix.lower() == ".conf" and (not log_path.exists() or log_path.stat().st_size == 0):
                idx += 2
                continue
        normalized.append(arg)
        idx += 1
    return normalized


def _library_roots() -> frozenset[Path]:
    """Return resolved and unresolved standard-library and site-packages roots.

    :return: Set of host library root directories.
    :rtype: frozenset[Path]
    """
    sys_paths = sysconfig.get_paths()
    roots: set[Path] = set()
    for key in LIBRARY_PATH_KEYS:
        raw = sys_paths.get(key)
        if raw:
            path = Path(raw)
            roots.add(path.absolute())
            roots.add(path.resolve())
    return frozenset(roots)


def _is_container_importable_module(module_name: str, module_file: Path) -> bool:
    """Return whether ``module_name`` resolves to ``module_file`` inside the Docker container without staging.

    :param module_name: Valid dotted Python module name in ``sys.modules``.
    :type module_name: str
    :param module_file: Host file path backing ``module_name``.
    :type module_file: Path
    :return: ``True`` if ``module_name`` is importable from stdlib, ``.venv``, ``/app/src``, or ``/app``.
    :rtype: bool
    """
    parts = module_name.split(".")
    tail = parts[-1]
    matches_tail = module_file.name.split(".", maxsplit=1)[0] == tail or (
        module_file.name == "__init__.py" and module_file.parent.name == tail
    )
    if not matches_tail:
        return False

    candidates = (module_file.absolute(), module_file.resolve())
    if any(cand.is_relative_to(root) for cand in candidates for root in _library_roots()):
        return True

    rel_mod = Path(*parts)
    resolved_file = candidates[1]
    search_roots: list[Path] = [(BASE_DIR / "src").resolve()]
    if parts[0] in CONTAINER_MOUNTED_PACKAGES:
        search_roots.append(BASE_DIR.resolve())

    return any(
        (root / rel_mod).with_suffix(".py").resolve() == resolved_file
        or (root / rel_mod / "__init__.py").resolve() == resolved_file
        for root in search_roots
    )


def _write_pymodule_file(module_name: str, pymodules_dir: Path) -> Path:
    """Prepare parent package directories under ``pymodules_dir`` and return the target ``.py`` path.

    :param module_name: Valid dotted Python module name.
    :type module_name: str
    :param pymodules_dir: Staging root mounted at ``/pymodules`` in Docker.
    :type pymodules_dir: Path
    :return: Target ``.py`` file path under ``pymodules_dir``.
    :rtype: Path
    """
    parts = module_name.split(".")
    pymodules_dir.mkdir(parents=True, exist_ok=True)
    current_pkg = pymodules_dir
    for pkg_part in parts[:-1]:
        current_pkg = current_pkg / pkg_part
        current_pkg.mkdir(parents=True, exist_ok=True)
        init_file = current_pkg / "__init__.py"
        if not init_file.exists():
            init_file.write_text("", encoding="utf-8")
    return pymodules_dir.joinpath(*parts).with_suffix(".py")


def _stage_file_module(
    module: ModuleType,
    module_name: str,
    mod_file: Path,
    pymodules_dir: Path,
    seen: set[str],
) -> bool:
    """Copy a non-container-importable file-backed module and its dynamic dependencies into ``pymodules_dir``.

    :param module: Loaded Python module object.
    :type module: ModuleType
    :param module_name: Dotted module name registered in ``sys.modules``.
    :type module_name: str
    :param mod_file: Host ``.py`` file backing ``module``.
    :type mod_file: Path
    :param pymodules_dir: Host directory mounted at ``/pymodules`` inside the container.
    :type pymodules_dir: Path
    :param seen: Set of already-visited module names.
    :type seen: set[str]
    :return: ``True`` if ``mod_file`` was copied to ``pymodules_dir``, else ``False``.
    :rtype: bool
    """
    if _is_container_importable_module(module_name, mod_file) or not mod_file.is_file():
        return False
    target_file = _write_pymodule_file(module_name, pymodules_dir)
    shutil.copy2(mod_file, target_file)
    for attr_val in vars(module).values():
        if isinstance(attr_val, ModuleType):
            materialize_dynamic_module(attr_val.__name__, pymodules_dir, seen)
        else:
            dep_mod_name = getattr(attr_val, "__module__", None)
            if isinstance(dep_mod_name, str):
                materialize_dynamic_module(dep_mod_name, pymodules_dir, seen)
    return True


def materialize_dynamic_module(
    module_name: str,
    pymodules_dir: Path,
    visited: set[str] | None = None,
) -> bool:
    """Materialize an in-memory or non-container-importable ``sys.modules`` entry into ``pymodules_dir``.

    :param module_name: Dotted module name registered in ``sys.modules``.
    :type module_name: str
    :param pymodules_dir: Host directory mounted at ``/pymodules`` inside the container.
    :type pymodules_dir: Path
    :param visited: Optional set of already-inspected module names to prevent cycles.
    :type visited: set[str] | None
    :return: ``True`` if a module file was written to ``pymodules_dir``, else ``False``.
    :rtype: bool
    """
    seen = visited if visited is not None else set()
    if module_name in seen:
        return False
    seen.add(module_name)

    module = sys.modules.get(module_name)
    if module is None or not all(part.isidentifier() for part in module_name.split(".")):
        return False

    raw_file = getattr(module, "__file__", None)
    if raw_file is not None:
        return _stage_file_module(module, module_name, Path(raw_file), pymodules_dir, seen)

    if hasattr(module, "ENTITY_SCHEMAS"):
        target_file = _write_pymodule_file(module_name, pymodules_dir)
        schemas = module.ENTITY_SCHEMAS
        target_file.write_text(f"ENTITY_SCHEMAS = {schemas!r}\n", encoding="utf-8")
        return True

    if hasattr(module, "ENTITY_MODELS"):
        target_file = _write_pymodule_file(module_name, pymodules_dir)
        models: dict[str, Any] = module.ENTITY_MODELS
        imports: list[str] = []
        entries: list[str] = []
        for idx, (table_name, model_cls) in enumerate(models.items()):
            materialize_dynamic_module(model_cls.__module__, pymodules_dir, seen)
            alias = f"_Model_{idx}"
            imports.append(f"from {model_cls.__module__} import {model_cls.__name__} as {alias}")
            entries.append(f"    {table_name!r}: {alias},")
        code = "\n".join([*imports, "", "ENTITY_MODELS = {", *entries, "}", ""])
        target_file.write_text(code, encoding="utf-8")
        return True

    return False


def _stage_aux_file(host_file: Path, aux_dir: Path, aux_counter: int) -> str:
    """Copy ``host_file`` into ``aux_dir`` with an indexed filename and return its container path.

    :param host_file: Host file path to stage.
    :type host_file: Path
    :param aux_dir: Host auxiliary directory mounted at ``/aux_dir``.
    :type aux_dir: Path
    :param aux_counter: Monotonic file index avoiding basename collisions.
    :type aux_counter: int
    :return: Container path string under ``/aux_dir``.
    :rtype: str
    """
    resolved = host_file.resolve()
    aux_dir.mkdir(parents=True, exist_ok=True)
    staged_name = f"{aux_counter}_{resolved.name}"
    shutil.copy2(resolved, aux_dir / staged_name)
    return f"{CONTAINER_AUX_DIR}/{staged_name}"


def _write_docker_dlt_config(config_path: Path, use_destination: str) -> None:
    """Write ``config.toml`` for the container with ``/input_dir`` and ``/output_dir`` buckets."""
    config_path.parent.mkdir(parents=True, exist_ok=True)
    destinations = {"cts", LOCAL_FS, use_destination}
    blocks: list[str] = []
    for dest in sorted(destinations):
        blocks.append(f'[destination.{dest}]\nbucket_url = "{CONTAINER_OUTPUT_DIR}"\ndestination_type = "filesystem"\n')
        blocks.append(f'[sources.{dest}]\nbucket_url = "{CONTAINER_INPUT_DIR}"\n')
    for section in ("extract", "normalize", "load"):
        workers = dlt.config.get(f"{section}.workers")
        if workers is not None:
            blocks.append(f"[{section}]\nworkers = {int(workers)}\n")
    config_path.write_text("\n".join(blocks), encoding="utf-8")


def _make_container_accessible(root: Path, *, writable: bool = True) -> None:
    """Ensure bind-mounted host paths are accessible to the container's nonroot user on Linux."""
    dir_mode = 0o777 if writable else 0o755
    file_mode = 0o666 if writable else 0o644
    if root.is_dir():
        root.chmod(dir_mode)
        for child in root.rglob("*"):
            if not child.is_symlink():
                child.chmod(dir_mode if child.is_dir() else file_mode)
    elif root.is_file() and not root.is_symlink():
        root.chmod(file_mode)


class PipelineTestRunner:
    """Execute any CDM ingestion pipeline in ``core``, ``cli``, or ``docker`` mode."""

    def __init__(
        self,
        mode: ExecutionMode | str = ExecutionMode.CORE,
        docker_image: str | None = None,
        *,
        dlt_config_dir: Path | None = None,
        preserve_dlt_destination: bool = False,
    ) -> None:
        """Initialize the runner with the target execution mode, optional Docker image tag, and dlt config options."""
        self.mode = ExecutionMode(mode)
        self.docker_image = docker_image
        self.dlt_config_dir = Path(dlt_config_dir) if dlt_config_dir is not None else None
        self.preserve_dlt_destination = preserve_dlt_destination

    def __call__(
        self,
        pipeline: PipelineIdentifier,
        settings: CtsSettings | LoggerSettings | None = None,
        *,
        cli_args: list[str] | None = None,
        **overrides: Any,  # noqa: ANN401
    ) -> LoadInfo | None:
        """Alias for :meth:`run` so the runner can be passed as a ``run_fn`` callback."""
        return self.run(pipeline, settings, cli_args=cli_args, **overrides)

    def run(
        self,
        pipeline: PipelineIdentifier,
        settings: CtsSettings | LoggerSettings | None = None,
        *,
        cli_args: list[str] | None = None,
        **overrides: Any,  # noqa: ANN401
    ) -> LoadInfo | None:
        """Execute ``pipeline`` in the configured mode and return its ``LoadInfo``.

        :param pipeline: pipeline name, module, CliRunner, PipelineSpec, or pipeline run function
        :type pipeline: PipelineIdentifier
        :param settings: optional pre-built settings instance
        :type settings: CtsSettings | LoggerSettings | None
        :param cli_args: optional explicit CLI argument list (excluding the program name)
        :type cli_args: list[str] | None
        :param overrides: optional field overrides applied if building or updating settings
        :type overrides: Any
        :raises ValueError: if neither settings nor cli_args are provided and settings cannot be built
        :return: pipeline LoadInfo (or ReconstructedLoadInfo in cli/docker modes)
        :rtype: LoadInfo | None
        """
        spec = resolve_pipeline(pipeline)
        effective_settings = settings
        if effective_settings is not None and overrides:
            effective_settings = effective_settings.model_copy(update=overrides)
        elif effective_settings is None and cli_args is None:
            if spec.settings_cls is None:
                err_msg = f"Pipeline {spec.cli_command!r} has no settings_cls; supply cli_args explicitly."
                raise ValueError(err_msg)
            effective_settings = spec.settings_cls(**overrides)

        raw_args = list(cli_args) if cli_args is not None else settings_to_argv(effective_settings)
        # Strip leading program name if the caller passed a full sys.argv list
        if raw_args and not raw_args[0].startswith("-"):
            raw_args = raw_args[1:]
        args = _normalize_cli_args(raw_args)

        output_dir_str = _extract_flag_value(args, OUTPUT_DIR_FLAGS) or (
            str(getattr(effective_settings, "output_dir", "")) if effective_settings else None
        )
        if not output_dir_str:
            err_msg = "Could not determine output_dir from settings or cli_args"
            raise ValueError(err_msg)
        output_dir = Path(output_dir_str)
        output_dir.mkdir(parents=True, exist_ok=True)

        use_destination = (
            _extract_flag_value(args, DESTINATION_FLAGS)
            or (str(getattr(effective_settings, "use_destination", LOCAL_FS)) if effective_settings else None)
            or LOCAL_FS
        )

        if self.mode == ExecutionMode.CORE:
            return self._run_core(spec, args, output_dir, use_destination)
        if self.mode == ExecutionMode.CLI:
            return self._run_cli(spec, args, output_dir, use_destination)
        return self._run_docker(spec, args, output_dir, use_destination)

    def _resolve_bucket_dir(self, output_dir: Path, use_destination: str) -> Path:
        """Determine the active host bucket directory for ``use_destination``."""
        configured = dlt.config.get(f"destination.{use_destination}.bucket_url")
        default_buckets = {CONFIG_BUCKET.get(LOCAL_FS), "/configured/out/", None}
        if configured and configured not in default_buckets and Path(str(configured)).exists():
            return Path(str(configured))
        return output_dir

    def _run_core(
        self,
        spec: PipelineSpec,
        args: list[str],
        output_dir: Path,
        use_destination: str,
    ) -> LoadInfo | None:
        """Execute the pipeline in-process via ``cdm_data_loaders.core.cli.run_cli``."""
        if spec.settings_cls is None or spec.pipeline_fn is None:
            err_msg = f"Pipeline {spec.cli_command!r} does not expose settings_cls and pipeline_fn for run_cli"
            raise ValueError(err_msg)

        if self.preserve_dlt_destination:
            with preserve_logging_state():
                return core_cli.run_cli(spec.settings_cls, spec.pipeline_fn, cli_args=args)

        bucket_dir = self._resolve_bucket_dir(output_dir, use_destination)
        with (
            preserve_logging_state(),
            dlt.config.values(
                {
                    f"destination.{use_destination}.destination_type": "filesystem",
                    f"destination.{use_destination}.bucket_url": str(bucket_dir),
                }
            ),
        ):
            return core_cli.run_cli(spec.settings_cls, spec.pipeline_fn, cli_args=args)

    def _run_cli(
        self,
        spec: PipelineSpec,
        args: list[str],
        output_dir: Path,
        use_destination: str,
    ) -> LoadInfo | None:
        """Execute the pipeline via ``cdm_data_loaders.pipelines.<pipeline>.cli()`` and reconstruct LoadInfo."""
        if spec.cli_callable is None:
            err_msg = f"Pipeline {spec.cli_command!r} does not expose a cli() entry point"
            raise ValueError(err_msg)

        bucket_dir = self._resolve_bucket_dir(output_dir, use_destination)
        if self.preserve_dlt_destination:
            with (
                preserve_logging_state(),
                patch.object(sys, "argv", [spec.cli_command, *args]),
            ):
                spec.cli_callable()
            return reconstruct_load_info(output_dir, bucket_dir=bucket_dir, use_destination=use_destination)

        with (
            preserve_logging_state(),
            patch.object(sys, "argv", [spec.cli_command, *args]),
            dlt.config.values(
                {
                    f"destination.{use_destination}.destination_type": "filesystem",
                    f"destination.{use_destination}.bucket_url": str(bucket_dir),
                }
            ),
        ):
            spec.cli_callable()

        return reconstruct_load_info(output_dir, bucket_dir=bucket_dir)

    def _stage_docker_dlt_config(self, dlt_conf_dir: Path, use_destination: str) -> None:
        """Populate ``dlt_conf_dir`` from ``self.dlt_config_dir`` (and ``$HOME/.pyiceberg.yaml`` if present) or write default config."""
        dlt_conf_dir.mkdir(parents=True, exist_ok=True)
        if self.dlt_config_dir is not None and self.dlt_config_dir.is_dir():
            for conf_name in ("config.toml", "secrets.toml"):
                src_conf = self.dlt_config_dir / conf_name
                if src_conf.is_file():
                    shutil.copy2(src_conf, dlt_conf_dir / conf_name)
        else:
            _write_docker_dlt_config(dlt_conf_dir / "config.toml", use_destination)

        home_pyiceberg = Path.home() / ".pyiceberg.yaml"
        if home_pyiceberg.is_file():
            shutil.copy2(home_pyiceberg, dlt_conf_dir / ".pyiceberg.yaml")

    def _run_docker(
        self,
        spec: PipelineSpec,
        args: list[str],
        output_dir: Path,
        use_destination: str,
    ) -> LoadInfo | None:
        """Execute the pipeline inside the Docker container and reconstruct LoadInfo from mounted output."""
        image_tag = ensure_docker_image(self.docker_image)
        input_dir_str = _extract_flag_value(args, INPUT_DIR_FLAGS)
        if not input_dir_str:
            err_msg = "Docker execution mode requires --input-dir / -i in settings or cli_args"
            raise ValueError(err_msg)
        host_output_dir = output_dir.resolve()

        stage_dir = host_output_dir.parent / f".docker_stage_{uuid4().hex[:8]}"
        staged_input_dir = stage_dir / "input_dir"
        aux_dir = stage_dir / "aux"
        pymodules_dir = stage_dir / "pymodules"
        staged_input_dir.mkdir(parents=True, exist_ok=True)

        if "://" not in input_dir_str and Path(input_dir_str).resolve().is_dir():
            shutil.copytree(Path(input_dir_str).resolve(), staged_input_dir, dirs_exist_ok=True)

        self._stage_docker_dlt_config(staged_input_dir / "dlt_conf", use_destination)

        container_args = self._translate_args_for_docker(args, aux_dir, pymodules_dir)

        _make_container_accessible(staged_input_dir, writable=True)
        _make_container_accessible(host_output_dir, writable=True)
        if aux_dir.is_dir():
            _make_container_accessible(aux_dir, writable=False)
        if pymodules_dir.is_dir():
            _make_container_accessible(pymodules_dir, writable=False)

        docker_cmd: list[str] = [
            "docker",
            "run",
            "--rm",
            "-v",
            f"{staged_input_dir}:{CONTAINER_INPUT_DIR}",
            "-v",
            f"{host_output_dir}:{CONTAINER_OUTPUT_DIR}",
            "-v",
            f"{(BASE_DIR / 'tests').resolve()}:/app/tests:ro",
            "-v",
            f"{(BASE_DIR / 'specs').resolve()}:/app/specs:ro",
        ]
        if aux_dir.is_dir():
            docker_cmd.extend(["-v", f"{aux_dir.resolve()}:{CONTAINER_AUX_DIR}:ro"])
        if pymodules_dir.is_dir():
            docker_cmd.extend(
                [
                    "-v",
                    f"{pymodules_dir.resolve()}:{CONTAINER_PYMODULES_DIR}:ro",
                    "-e",
                    f"PYTHONPATH={CONTAINER_PYMODULES_DIR}:/app/src:/app",
                ]
            )
        else:
            docker_cmd.extend(["-e", "PYTHONPATH=/app/src:/app"])

        registered_modules = {module_path for _, module_path in discover_pipeline_targets().values()}
        docker_target = (
            spec.cli_command if spec.module_path in registered_modules else f"{spec.module_path.replace('.', '/')}.py"
        )
        docker_cmd.extend([image_tag, "pipeline", docker_target, *container_args])

        result = subprocess.run(docker_cmd, capture_output=True, text=True, check=False)  # noqa: S603
        stderr_and_stdout = f"{result.stdout}\n{result.stderr}".strip()
        if (result.returncode != 0 or "exited with status" in stderr_and_stdout) and (
            "Unknown table name" in stderr_and_stdout or "ValueError" in stderr_and_stdout
        ):
            raise ValueError(stderr_and_stdout)
        if result.returncode != 0:
            err_msg = (
                f"Docker pipeline '{spec.cli_command}' failed with exit code {result.returncode}:\n"
                f"Command: {' '.join(docker_cmd)}\n"
                f"STDOUT:\n{result.stdout}\n"
                f"STDERR:\n{result.stderr}"
            )
            raise RuntimeError(err_msg)

        return reconstruct_load_info(
            host_output_dir,
            bucket_dir=host_output_dir,
            use_destination=use_destination if self.preserve_dlt_destination else None,
        )

    def _translate_args_for_docker(
        self,
        args: list[str],
        aux_dir: Path,
        pymodules_dir: Path,
    ) -> list[str]:
        """Rewrite host file paths and materialize dynamic modules for container execution.

        :param args: Normalized CLI argument tokens for the target pipeline.
        :type args: list[str]
        :param aux_dir: Host auxiliary directory mounted at ``/aux_dir``.
        :type aux_dir: Path
        :param pymodules_dir: Host directory mounted at ``/pymodules``.
        :type pymodules_dir: Path
        :return: Translated CLI argument list referencing container paths.
        :rtype: list[str]
        """
        translated: list[str] = []
        idx = 0
        aux_counter = 0
        while idx < len(args):
            token = args[idx]
            if token in INPUT_DIR_FLAGS and idx + 1 < len(args):
                input_val = args[idx + 1]
                translated.extend([token, input_val if "://" in input_val else CONTAINER_INPUT_DIR])
                idx += 2
                continue
            if token in OUTPUT_DIR_FLAGS and idx + 1 < len(args):
                translated.extend([token, CONTAINER_OUTPUT_DIR])
                idx += 2
                continue
            if token in FILE_PATH_FLAGS and idx + 1 < len(args):
                staged_path = _stage_aux_file(Path(args[idx + 1]), aux_dir, aux_counter)
                aux_counter += 1
                translated.extend([token, staged_path])
                idx += 2
                continue
            if token in MODULE_PATH_FLAGS and idx + 1 < len(args):
                raw_spec = args[idx + 1]
                target, explicit_attr = split_module_attr_spec(raw_spec)
                candidate_path = Path(target)
                if candidate_path.suffix == ".py" or candidate_path.is_file():
                    staged_path = _stage_aux_file(candidate_path, aux_dir, aux_counter)
                    aux_counter += 1
                    staged_spec = f"{staged_path}:{explicit_attr}" if explicit_attr else staged_path
                    translated.extend([token, staged_spec])
                else:
                    materialize_dynamic_module(target, pymodules_dir)
                    translated.extend([token, raw_spec])
                idx += 2
                continue
            translated.append(token)
            idx += 1
        return translated
