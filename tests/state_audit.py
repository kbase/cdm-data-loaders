"""Passive pytest plugin and helpers for auditing unreverted global state mutations."""

import asyncio
import importlib
import importlib.resources
import json
import logging
import os
import sys
import tempfile
import warnings
from collections.abc import Generator, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from types import ModuleType
from typing import Any, Final

import boto3
import jsonschema.validators
import pytest
from dlt.common.configuration.container import Container
from dlt.common.pipeline import PipelineContext

import cdm_data_loaders.utils.file_transfer.s3.client as s3_client
from cdm_data_loaders.utils import cdm_logger

REPO_ROOT: Final[Path] = Path(__file__).resolve().parent.parent
SPECS_DIR: Final[Path] = (REPO_ROOT / "specs").resolve()
TRANSIENT_MODULE_NAMES: Final[frozenset[str]] = frozenset({"schema", "expected_schema"})
TRANSIENT_MODULE_PREFIXES: Final[tuple[str, ...]] = ("_fixture_", "_test_")
NATIVE_SPECLESS_MODULES: Final[frozenset[str]] = frozenset({"_openssl", "_openssl.lib"})
NATIVE_SPECLESS_PREFIXES: Final[tuple[str, ...]] = ("cryptography.", "specs.", "_cython_", "_duckdb.")
THIRD_PARTY_WARMUP_MODULES: Final[tuple[str, ...]] = (
    "bioregistry",
    "duckdb",
    "frictionless",
    "linkml.linter.linter",
    "openpyxl",
    "pyiceberg.catalog",
    "xmlschema",
)
TRACKED_MODULE_PREFIXES: Final[tuple[str, ...]] = ("cdm_data_loaders", "tests", "scripts", "docs_macros")
PURE_MEMO_CACHE_SUFFIXES: Final[tuple[str, ...]] = (
    ".metadata_keys_for",
    ".urlsplit",
    "._cached_xsd_context",
    ".ceph_reachable",
)
DEDICATED_GLOBAL_VARS: Final[frozenset[str]] = frozenset(
    {
        "cdm_data_loaders.utils.cdm_logger.applied_key",
        "cdm_data_loaders.utils.file_transfer.s3.client._s3_client",
    }
)
SYSTEM_TEMP_ROOT: Final[Path] = Path(tempfile.gettempdir()).resolve()
DEFAULT_LOGGER_STATES: Final[frozenset[tuple[object, ...]]] = frozenset(
    {
        (logging.NOTSET, True, False, (), ()),
        (logging.NOTSET, True, False, (("NullHandler", 0),), ()),
    }
)


@dataclass(frozen=True, slots=True)
class StateSnapshot:
    """Point-in-time snapshot of process-wide and module-level mutable state."""

    cwd: str
    environ: dict[str, str]
    sys_argv: list[str]
    sys_path: list[str]
    sys_modules: dict[str, ModuleType | None]
    warnings_filters: list[tuple[Any, ...]]
    importlib_resources_files_id: int
    boto3_default_session_set: bool
    s3_client_cached: bool
    dlt_pipeline_active: bool
    cdm_logger_applied_key: object
    logging_disable_level: int
    loggers: dict[str, tuple[Any, ...]]
    jsonschema_validators: frozenset[str]
    jsonschema_meta_schemas: frozenset[str]
    module_vars: dict[str, tuple[Any, ...]]
    caches: dict[str, int]
    scoped_fixtures: dict[str, tuple[Any, ...]]


def fingerprint_value(value: object) -> tuple[Any, ...]:
    """Compute a structural comparison fingerprint for a Python value.

    :param value: Value to fingerprint.
    :type value: object
    :return: Hashable/equatable tuple representing ``value``.
    :rtype: tuple[Any, ...]
    """
    if value is None or isinstance(value, bool | int | float | str | bytes):
        return (type(value).__name__, value)
    if isinstance(value, Path):
        return ("Path", str(value))
    if isinstance(value, dict | list | tuple):
        return (type(value).__name__, repr(value))
    if isinstance(value, set | frozenset):
        return (type(value).__name__, tuple(sorted(repr(item) for item in value)))
    return ("obj", type(value).__qualname__, id(value))


def is_normal_imported_module(name: str, mod: ModuleType | None) -> bool:
    """Determine whether a ``sys.modules`` entry represents a standard package import rather than a test artifact.

    :param name: Module key in ``sys.modules``.
    :type name: str
    :param mod: Bound module object or ``None``.
    :type mod: ModuleType | None
    :return: ``True`` if the module is a normal stdlib, third-party, or project import.
    :rtype: bool
    """
    if mod is None or name in NATIVE_SPECLESS_MODULES or name.startswith(NATIVE_SPECLESS_PREFIXES):
        return True
    if (
        name in TRANSIENT_MODULE_NAMES
        or name.startswith(TRANSIENT_MODULE_PREFIXES)
        or getattr(mod, "__spec__", None) is None
    ):
        return False
    mod_file = getattr(mod, "__file__", None)
    if mod_file is None:
        return True
    resolved = Path(mod_file).resolve()
    return (
        not resolved.is_relative_to(SPECS_DIR)
        and not resolved.is_relative_to(SYSTEM_TEMP_ROOT)
        and "/pytest-of-" not in resolved.as_posix()
    )


def snapshot_loggers() -> dict[str, tuple[Any, ...]]:
    """Capture the root logger and all non-default named loggers.

    :return: Mapping from logger name (``"<root>"`` for root) to state tuple.
    :rtype: dict[str, tuple[Any, ...]]
    """
    snap: dict[str, tuple[Any, ...]] = {}
    root = logging.getLogger()
    snap["<root>"] = (
        root.level,
        root.propagate,
        root.disabled,
        tuple((type(handler).__qualname__, handler.level) for handler in root.handlers),
        tuple(type(flt).__qualname__ for flt in root.filters),
    )
    for name, logger in logging.Logger.manager.loggerDict.items():
        if not isinstance(logger, logging.Logger):
            continue
        state = (
            logger.level,
            logger.propagate,
            logger.disabled,
            tuple((type(handler).__qualname__, handler.level) for handler in logger.handlers),
            tuple(type(flt).__qualname__ for flt in logger.filters),
        )
        if state not in DEFAULT_LOGGER_STATES:
            snap[name] = state
    return snap


def snapshot_module_vars_and_caches(
    modules: Mapping[str, ModuleType | None] | None = None,
) -> tuple[dict[str, tuple[Any, ...]], dict[str, int]]:
    """Capture module-level value fingerprints and ``functools`` cache sizes across repository modules.

    :param modules: Optional module mapping to inspect; defaults to ``sys.modules``.
    :type modules: Mapping[str, ModuleType | None] | None
    :return: Pair of ``(module_var_fingerprints, cache_sizes)``.
    :rtype: tuple[dict[str, tuple[Any, ...]], dict[str, int]]
    """
    target_modules = sys.modules if modules is None else modules
    vars_snap: dict[str, tuple[Any, ...]] = {}
    caches_snap: dict[str, int] = {}
    for mod_name, mod in list(target_modules.items()):
        if mod is None or not mod_name.startswith(TRACKED_MODULE_PREFIXES):
            continue
        for attr, val in list(vars(mod).items()):
            if attr.startswith("__") or isinstance(val, ModuleType | type):
                continue
            qual = f"{mod_name}.{attr}"
            cache_info_fn = getattr(val, "cache_info", None)
            if callable(cache_info_fn):
                info = cache_info_fn()
                currsize = getattr(info, "currsize", None)
                if isinstance(currsize, int):
                    def_mod = getattr(val, "__module__", None) or mod_name
                    def_qual = getattr(val, "__qualname__", None) or attr
                    caches_snap[f"{def_mod}.{def_qual}"] = currsize
                continue
            if callable(val):
                continue
            vars_snap[qual] = fingerprint_value(val)
    return vars_snap, caches_snap


def snapshot_scoped_fixtures(item: pytest.Item | None) -> dict[str, tuple[Any, ...]]:
    """Capture fingerprints of already-cached module/class/session fixture containers on ``item``.

    :param item: Active pytest test item, or ``None`` during collection.
    :type item: pytest.Item | None
    :return: Mapping of scoped fixture identifier to container fingerprint.
    :rtype: dict[str, tuple[Any, ...]]
    """
    if item is None:
        return {}
    fixture_info = vars(item).get("_fixtureinfo")
    if fixture_info is None:
        return {}
    snap: dict[str, tuple[Any, ...]] = {}
    for argname, fixture_defs in fixture_info.name2fixturedefs.items():
        for fixture_def in fixture_defs:
            if fixture_def.scope == "function" or fixture_def.cached_result is None:
                continue
            val = fixture_def.cached_result[0]
            if isinstance(val, dict | list | set):
                base_id = getattr(fixture_def, "baseid", "") or "<root>"
                snap[f"{base_id}::{argname}[{fixture_def.scope}]"] = fingerprint_value(val)
    return snap


def take_state_snapshot(item: pytest.Item | None = None) -> StateSnapshot:
    """Record a complete global-state snapshot before or after a collection/test step.

    :param item: Optional pytest item when snapshotting around a test execution.
    :type item: pytest.Item | None
    :return: Captured ``StateSnapshot``.
    :rtype: StateSnapshot
    """
    mod_vars, caches = snapshot_module_vars_and_caches()
    validators_map = vars(jsonschema.validators)["_VALIDATORS"]
    meta_schemas_map = vars(jsonschema.validators)["_META_SCHEMAS"]
    return StateSnapshot(
        cwd=str(Path.cwd()),
        environ=dict(os.environ),
        sys_argv=list(sys.argv),
        sys_path=list(sys.path),
        sys_modules=dict(sys.modules),
        warnings_filters=list(warnings.filters),
        importlib_resources_files_id=id(importlib.resources.files),
        boto3_default_session_set=boto3.DEFAULT_SESSION is not None,
        s3_client_cached=vars(s3_client).get("_s3_client") is not None,
        dlt_pipeline_active=Container()[PipelineContext].is_active(),
        cdm_logger_applied_key=cdm_logger.applied_key,
        logging_disable_level=logging.Logger.manager.disable,
        loggers=snapshot_loggers(),
        jsonschema_validators=frozenset(validators_map.keys()),
        jsonschema_meta_schemas=frozenset(meta_schemas_map.keys()),
        module_vars=mod_vars,
        caches=caches,
        scoped_fixtures=snapshot_scoped_fixtures(item),
    )


def _diff_environ(before_env: dict[str, str], after_env: dict[str, str]) -> dict[str, Any]:
    """Compute added, removed, and modified environment variables between two snapshots.

    :param before_env: Environment dictionary before execution.
    :type before_env: dict[str, str]
    :param after_env: Environment dictionary after execution.
    :type after_env: dict[str, str]
    :return: Non-empty diff dictionary if mutated, otherwise ``{}``.
    :rtype: dict[str, Any]
    """
    if before_env == after_env:
        return {}
    added = {k: after_env[k] for k in sorted(after_env.keys() - before_env.keys())}
    removed = {k: before_env[k] for k in sorted(before_env.keys() - after_env.keys())}
    changed = {
        k: {"before": before_env[k], "after": after_env[k]}
        for k in sorted(before_env.keys() & after_env.keys())
        if before_env[k] != after_env[k]
    }
    return {"added": added, "removed": removed, "changed": changed}


def _is_collected_target_module(mod: ModuleType | None, collected_module_path: Path | None) -> bool:
    """Check whether ``mod`` is the test module being collected or one of its package/conftest ancestors.

    :param mod: Candidate module added to ``sys.modules``.
    :type mod: ModuleType | None
    :param collected_module_path: Resolved path of the module being collected, or ``None`` outside collection.
    :type collected_module_path: Path | None
    :return: ``True`` if ``mod`` corresponds to ``collected_module_path`` or an ancestor ``__init__.py``/``conftest.py``.
    :rtype: bool
    """
    if mod is None or collected_module_path is None:
        return False
    mod_file = getattr(mod, "__file__", None)
    if mod_file is None:
        return False
    resolved_mod = Path(mod_file).resolve()
    if resolved_mod == collected_module_path:
        return True
    return resolved_mod.name in {"__init__.py", "conftest.py"} and collected_module_path.is_relative_to(
        resolved_mod.parent
    )


def _is_collection_sys_path_prepend(
    before_path: list[str],
    after_path: list[str],
    collected_module_path: Path | None,
) -> bool:
    """Check whether a ``sys.path`` change is pytest's standard package-root prepend during module collection.

    :param before_path: ``sys.path`` before importing the collected module.
    :type before_path: list[str]
    :param after_path: ``sys.path`` after importing the collected module.
    :type after_path: list[str]
    :param collected_module_path: Resolved path of the module being collected, or ``None`` outside collection.
    :type collected_module_path: Path | None
    :return: ``True`` if ``after_path`` only prepends an ancestor directory of ``collected_module_path``.
    :rtype: bool
    """
    if collected_module_path is None or len(after_path) != len(before_path) + 1:
        return False
    if after_path[1:] != before_path:
        return False
    return collected_module_path.is_relative_to(Path(after_path[0]).resolve())


def _diff_sys_modules(
    before_mods: dict[str, ModuleType | None],
    after_mods: dict[str, ModuleType | None],
    collected_module_path: Path | None = None,
) -> dict[str, list[str]]:
    """Compute leaked synthetic additions, replacements, and removals in ``sys.modules``.

    :param before_mods: ``sys.modules`` copy before execution.
    :type before_mods: dict[str, ModuleType | None]
    :param after_mods: ``sys.modules`` copy after execution.
    :type after_mods: dict[str, ModuleType | None]
    :param collected_module_path: Optional resolved module path when diffing around collection.
    :type collected_module_path: Path | None
    :return: Mapping of ``sys.modules`` mutation categories to sorted module names.
    :rtype: dict[str, list[str]]
    """
    removed = sorted(k for k in before_mods.keys() - after_mods.keys() if not k.startswith("_pytest"))
    replaced = sorted(k for k in before_mods.keys() & after_mods.keys() if before_mods[k] is not after_mods[k])
    added = sorted(
        k
        for k in after_mods.keys() - before_mods.keys()
        if not is_normal_imported_module(k, after_mods[k])
        and not _is_collected_target_module(after_mods[k], collected_module_path)
    )
    return {
        label: names
        for label, names in (
            ("added_synthetic_or_fixture", added),
            ("replaced", replaced),
            ("removed", removed),
        )
        if names
    }


def _diff_loggers(
    before_loggers: dict[str, tuple[Any, ...]],
    after_loggers: dict[str, tuple[Any, ...]],
) -> dict[str, dict[str, Any]]:
    """Compute logger configuration differences between two snapshots.

    :param before_loggers: Logger states before execution.
    :type before_loggers: dict[str, tuple[Any, ...]]
    :param after_loggers: Logger states after execution.
    :type after_loggers: dict[str, tuple[Any, ...]]
    :return: Per-logger ``{"before": ..., "after": ...}`` mapping for changed loggers.
    :rtype: dict[str, dict[str, Any]]
    """
    if before_loggers == after_loggers:
        return {}
    return {
        name: {"before": before_loggers.get(name), "after": after_loggers.get(name)}
        for name in sorted(before_loggers.keys() | after_loggers.keys())
        if before_loggers.get(name) != after_loggers.get(name)
    }


def _diff_jsonschema_registry(before: StateSnapshot, after: StateSnapshot) -> dict[str, list[str]]:
    """Compute added or removed validator/metaschema URIs in ``jsonschema.validators``.

    :param before: Snapshot before execution.
    :type before: StateSnapshot
    :param after: Snapshot after execution.
    :type after: StateSnapshot
    :return: Registry diff dictionary if mutated, otherwise ``{}``.
    :rtype: dict[str, list[str]]
    """
    added_validators = sorted(after.jsonschema_validators - before.jsonschema_validators)
    removed_validators = sorted(before.jsonschema_validators - after.jsonschema_validators)
    added_meta = sorted(after.jsonschema_meta_schemas - before.jsonschema_meta_schemas)
    removed_meta = sorted(before.jsonschema_meta_schemas - after.jsonschema_meta_schemas)
    if not (added_validators or removed_validators or added_meta or removed_meta):
        return {}
    return {
        "added_validators": added_validators,
        "removed_validators": removed_validators,
        "added_meta_schemas": added_meta,
        "removed_meta_schemas": removed_meta,
    }


def _diff_module_vars_and_caches(before: StateSnapshot, after: StateSnapshot) -> dict[str, Any]:
    """Compute mutated module-level globals and non-memoization cache size changes.

    :param before: Snapshot before execution.
    :type before: StateSnapshot
    :param after: Snapshot after execution.
    :type after: StateSnapshot
    :return: Dictionary containing ``module_vars`` and/or ``caches`` keys when mutated.
    :rtype: dict[str, Any]
    """
    result: dict[str, Any] = {}
    var_diffs: list[str] = []
    for qual, before_fp in before.module_vars.items():
        if qual in DEDICATED_GLOBAL_VARS:
            continue
        if qual not in after.module_vars:
            var_diffs.append(f"{qual} (deleted)")
        elif after.module_vars[qual] != before_fp:
            var_diffs.append(qual)
    if var_diffs:
        result["module_vars"] = sorted(var_diffs)

    cache_diffs: dict[str, dict[str, int]] = {}
    for qual, after_size in after.caches.items():
        if qual.endswith(PURE_MEMO_CACHE_SUFFIXES):
            continue
        before_size = before.caches.get(qual, 0)
        if after_size != before_size:
            cache_diffs[qual] = {"before": before_size, "after": after_size}
    if cache_diffs:
        result["caches"] = cache_diffs
    return result


def diff_state_snapshots(
    before: StateSnapshot,
    after: StateSnapshot,
    collected_module_path: Path | None = None,
) -> dict[str, Any]:
    """Compare two ``StateSnapshot`` instances and return all unreverted global state mutations.

    :param before: Snapshot captured prior to module collection or test execution.
    :type before: StateSnapshot
    :param after: Snapshot captured after module collection or test teardown.
    :type after: StateSnapshot
    :param collected_module_path: Optional resolved module path when auditing module collection.
    :type collected_module_path: Path | None
    :return: Dictionary of detected mutations keyed by state category (empty if clean).
    :rtype: dict[str, Any]
    """
    diffs: dict[str, Any] = {}
    scalar_checks: tuple[tuple[str, object, object], ...] = (
        ("cwd", before.cwd, after.cwd),
        ("sys_argv", before.sys_argv, after.sys_argv),
        ("boto3_default_session", before.boto3_default_session_set, after.boto3_default_session_set),
        ("s3_client_singleton", before.s3_client_cached, after.s3_client_cached),
        ("dlt_pipeline_active", before.dlt_pipeline_active, after.dlt_pipeline_active),
        ("logging_disable_level", before.logging_disable_level, after.logging_disable_level),
    )
    for key, b_val, a_val in scalar_checks:
        if b_val != a_val:
            diffs[key] = {"before": b_val, "after": a_val}
    if before.sys_path != after.sys_path and not _is_collection_sys_path_prepend(
        before.sys_path, after.sys_path, collected_module_path
    ):
        diffs["sys_path"] = {"before": before.sys_path, "after": after.sys_path}

    if before.warnings_filters != after.warnings_filters:
        diffs["warnings_filters"] = {
            "before_len": len(before.warnings_filters),
            "after_len": len(after.warnings_filters),
        }
    if before.importlib_resources_files_id != after.importlib_resources_files_id:
        diffs["importlib_resources_files"] = "patched and not restored"
    if before.cdm_logger_applied_key != after.cdm_logger_applied_key:
        diffs["cdm_logger_applied_key"] = {
            "before": repr(before.cdm_logger_applied_key),
            "after": repr(after.cdm_logger_applied_key),
        }

    diffs.update(
        {
            key: subdiff
            for key, subdiff in (
                ("environ", _diff_environ(before.environ, after.environ)),
                ("sys_modules", _diff_sys_modules(before.sys_modules, after.sys_modules, collected_module_path)),
                ("loggers", _diff_loggers(before.loggers, after.loggers)),
                ("jsonschema_registry", _diff_jsonschema_registry(before, after)),
            )
            if subdiff
        }
    )

    diffs.update(_diff_module_vars_and_caches(before, after))

    mutated_fixtures = sorted(
        k
        for k in before.scoped_fixtures
        if k in after.scoped_fixtures and before.scoped_fixtures[k] != after.scoped_fixtures[k]
    )
    if mutated_fixtures:
        diffs["scoped_fixtures_mutated_in_place"] = mutated_fixtures

    return diffs


def format_leak_report(
    collection_leaks: Sequence[tuple[str, dict[str, Any]]],
    test_leaks: Sequence[tuple[str, dict[str, Any]]],
) -> str:
    """Format collection-time and test-runtime global state leaks into a human-readable report.

    :param collection_leaks: Sequence of ``(module_rel_path, diffs)`` pairs.
    :type collection_leaks: Sequence[tuple[str, dict[str, Any]]]
    :param test_leaks: Sequence of ``(test_nodeid, diffs)`` pairs.
    :type test_leaks: Sequence[tuple[str, dict[str, Any]]]
    :return: Formatted multi-line report string, or ``""`` when both sequences are empty.
    :rtype: str
    """
    if not collection_leaks and not test_leaks:
        return ""
    header = (
        f"[state-audit] Detected unreverted global state mutations "
        f"(collection={len(collection_leaks)}, tests={len(test_leaks)}):"
    )
    lines = [header]
    for module_path, diffs in collection_leaks:
        lines.append(f"  Collection leak in {module_path}:")
        lines.append(json.dumps(diffs, indent=4, sort_keys=True))
    for nodeid, diffs in test_leaks:
        lines.append(f"  Test leak in {nodeid}:")
        lines.append(json.dumps(diffs, indent=4, sort_keys=True))
    return "\n".join(lines)


class StateAuditPlugin:
    """Passive pytest plugin recording unreverted global state mutations per module and per test."""

    def __init__(self) -> None:
        """Initialize empty leak accumulators and audited test counter."""
        self.collection_leaks: list[tuple[str, dict[str, Any]]] = []
        self.test_leaks: list[tuple[str, dict[str, Any]]] = []
        self.total_tests: int = 0

    @property
    def has_leaks(self) -> bool:
        """Return whether any collection-time or test-runtime leak was recorded.

        :return: ``True`` if at least one leak was detected.
        :rtype: bool
        """
        return bool(self.collection_leaks or self.test_leaks)

    def pytest_sessionstart(self) -> None:
        """Pre-import third-party libraries that perform one-time global registrations on first import."""
        for mod_name in THIRD_PARTY_WARMUP_MODULES:
            importlib.import_module(mod_name)

    @pytest.hookimpl(hookwrapper=True)
    def pytest_make_collect_report(self, collector: pytest.Collector) -> Generator[None]:
        """Audit global state mutations around importing ``conftest.py`` and test modules during collection.

        :param collector: Pytest collector node being evaluated.
        :type collector: pytest.Collector
        :return: Hookwrapper generator yielding ``None``.
        :rtype: Generator[None]
        """
        if not isinstance(collector, pytest.Module | pytest.Directory):
            yield
            return
        before = take_state_snapshot()
        yield
        after = take_state_snapshot()
        resolved = collector.path.resolve()
        target_path = (resolved / "conftest.py") if isinstance(collector, pytest.Directory) else resolved
        diffs = diff_state_snapshots(before, after, collected_module_path=target_path)
        if diffs:
            label = str(resolved.relative_to(REPO_ROOT)) if resolved.is_relative_to(REPO_ROOT) else str(resolved)
            self.collection_leaks.append((label, diffs))

    @pytest.hookimpl(hookwrapper=True)
    def pytest_runtest_protocol(self, item: pytest.Item) -> Generator[None]:
        """Audit global state mutations across setup, call, and teardown of ``item``.

        :param item: Pytest test item being executed.
        :type item: pytest.Item
        :return: Hookwrapper generator yielding ``None``.
        :rtype: Generator[None]
        """
        if "state_audit" in item.keywords:
            yield
            return
        self.total_tests += 1
        before = take_state_snapshot(item)
        yield
        after = take_state_snapshot(item)
        diffs = diff_state_snapshots(before, after)
        if diffs:
            self.test_leaks.append((item.nodeid, diffs))

    def pytest_sessionfinish(self, session: pytest.Session, exitstatus: int) -> None:
        """Fail the pytest session if any unreverted global state mutation was detected.

        :param session: Finishing pytest session.
        :type session: pytest.Session
        :param exitstatus: Current session exit status code.
        :type exitstatus: int
        """
        if self.has_leaks and exitstatus == 0:
            session.exitstatus = int(pytest.ExitCode.TESTS_FAILED)

    def pytest_terminal_summary(self, terminalreporter: pytest.TerminalReporter) -> None:
        """Write a summary of global state audit results to the pytest terminal reporter.

        :param terminalreporter: Pytest terminal reporter plugin instance.
        :type terminalreporter: pytest.TerminalReporter
        """
        terminalreporter.section("global state mutation audit")
        if not self.has_leaks:
            terminalreporter.write_line(
                f"[state-audit] Passed: 0 unreverted global state mutations across {self.total_tests} test(s)."
            )
            return
        terminalreporter.write_line(format_leak_report(self.collection_leaks, self.test_leaks))


@dataclass(frozen=True, slots=True)
class SubprocessAuditResult:
    """Captured exit code and decoded output streams from an isolated pytest subprocess."""

    returncode: int
    stdout: str
    stderr: str


async def _exec_pytest_subprocess(args: Sequence[str], cwd: Path) -> SubprocessAuditResult:
    """Spawn a clean ``python -m pytest`` child process asynchronously without a shell.

    :param args: Command-line arguments forwarded to ``python -m pytest``.
    :type args: Sequence[str]
    :param cwd: Working directory for the child process.
    :type cwd: Path
    :return: Captured ``SubprocessAuditResult``.
    :rtype: SubprocessAuditResult
    """
    child_env = {k: v for k, v in os.environ.items() if k != "PYTEST_CURRENT_TEST"}
    proc = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "pytest",
        *args,
        cwd=cwd,
        env=child_env,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout_bytes, stderr_bytes = await proc.communicate()
    return SubprocessAuditResult(
        returncode=proc.returncode if proc.returncode is not None else 1,
        stdout=stdout_bytes.decode("utf-8", errors="replace"),
        stderr=stderr_bytes.decode("utf-8", errors="replace"),
    )


def run_pytest_subprocess(args: Sequence[str], cwd: Path = REPO_ROOT) -> SubprocessAuditResult:
    """Execute ``python -m pytest`` in a fresh child interpreter and return its output.

    :param args: Command-line arguments forwarded to ``python -m pytest``.
    :type args: Sequence[str]
    :param cwd: Working directory for the child process (defaults to repository root).
    :type cwd: Path
    :return: Captured ``SubprocessAuditResult``.
    :rtype: SubprocessAuditResult
    """
    return asyncio.run(_exec_pytest_subprocess(args, cwd))
