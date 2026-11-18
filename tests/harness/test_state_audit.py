"""Unit and subprocess integration tests for the global state mutation audit plugin and marker."""

import functools
import logging
from dataclasses import replace
from importlib.machinery import ModuleSpec
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any

import pytest

from tests.helpers.paths_and_constants import (
    REPO_ROOT,
    is_marker_explicitly_selected,
    preserve_logging_state,
)
from tests.helpers.state_audit import (
    SPECS_DIR,
    SYSTEM_TEMP_ROOT,
    StateSnapshot,
    diff_state_snapshots,
    fingerprint_value,
    format_leak_report,
    is_normal_imported_module,
    run_pytest_subprocess,
    snapshot_loggers,
    snapshot_module_vars_and_caches,
    snapshot_scoped_fixtures,
    take_state_snapshot,
)


def _make_module_with_spec(name: str, origin: str | None) -> ModuleType:
    """Create a synthetic ``ModuleType`` populated with a ``ModuleSpec`` and optional ``__file__``.

    :param name: Module name.
    :type name: str
    :param origin: File path string for ``__spec__.origin`` and ``__file__``, or ``None``.
    :type origin: str | None
    :return: Configured ``ModuleType`` instance.
    :rtype: ModuleType
    """
    mod = ModuleType(name)
    mod.__spec__ = ModuleSpec(name=name, loader=None, origin=origin)
    if origin is not None:
        mod.__file__ = origin
    return mod


@pytest.mark.parametrize(
    ("markexpr", "expected"),
    [
        pytest.param("", False, id="empty"),
        pytest.param("   ", False, id="whitespace-only"),
        pytest.param("not requires_spark and not requires_ceph and not docs", False, id="unrelated-exclusions"),
        pytest.param("not state_audit", False, id="direct-negation"),
        pytest.param("not docs and not state_audit", False, id="combined-negation"),
        pytest.param("not (docs or state_audit)", False, id="grouped-negation"),
        pytest.param("state_audit_other", False, id="identifier-boundary-mismatch"),
        pytest.param("state_audit and", False, id="invalid-expression"),
        pytest.param("state_audit", True, id="exact-selection"),
        pytest.param("state_audit and not slow_test", True, id="selected-with-exclusion"),
        pytest.param("docs or state_audit", True, id="selected-in-disjunction"),
    ],
)
def test_is_marker_explicitly_selected_evaluates_expressions(markexpr: str, expected: bool) -> None:
    """Verify ``is_marker_explicitly_selected`` only returns ``True`` when ``state_audit`` is positively selected."""
    assert is_marker_explicitly_selected(markexpr, "state_audit") is expected


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        pytest.param(None, ("NoneType", None), id="none"),
        pytest.param(True, ("bool", True), id="bool"),
        pytest.param(42, ("int", 42), id="int"),
        pytest.param("abc", ("str", "abc"), id="str"),
        pytest.param(b"xyz", ("bytes", b"xyz"), id="bytes"),
        pytest.param(Path("/a/b"), ("Path", str(Path("/a/b"))), id="path"),
        pytest.param([1, 2], ("list", "[1, 2]"), id="list"),
        pytest.param({"k": 1}, ("dict", "{'k': 1}"), id="dict"),
        pytest.param({"b", "a"}, ("set", ("'a'", "'b'")), id="set-sorted"),
        pytest.param(frozenset({2, 1}), ("frozenset", ("1", "2")), id="frozenset-sorted"),
    ],
)
def test_fingerprint_value_primitives_and_containers(value: object, expected: tuple[Any, ...]) -> None:
    """Verify ``fingerprint_value`` produces deterministic tuples across scalars, paths, and containers."""
    assert fingerprint_value(value) == expected


def test_fingerprint_value_custom_object_uses_identity() -> None:
    """Verify ``fingerprint_value`` falls back to qualified type name and object identity for custom objects."""
    sentinel = SimpleNamespace(flag=True)
    assert fingerprint_value(sentinel) == ("obj", "SimpleNamespace", id(sentinel))


@pytest.mark.parametrize(
    ("name", "mod", "expected"),
    [
        pytest.param("missing_pkg", None, True, id="none-sentinel"),
        pytest.param("_openssl", ModuleType("_openssl"), True, id="native-specless"),
        pytest.param("cryptography.hazmat", ModuleType("cryptography.hazmat"), True, id="cryptography-prefix"),
        pytest.param("specs.pkg", ModuleType("specs.pkg"), True, id="specs-package-prefix"),
        pytest.param("schema", _make_module_with_spec("schema", str(REPO_ROOT / "src" / "x.py")), False, id="schema"),
        pytest.param(
            "_fixture_mod",
            _make_module_with_spec("_fixture_mod", str(REPO_ROOT / "src" / "x.py")),
            False,
            id="fixture-prefix",
        ),
        pytest.param("dynamic_no_spec", ModuleType("dynamic_no_spec"), False, id="specless-dynamic"),
        pytest.param("builtin_mod", _make_module_with_spec("builtin_mod", None), True, id="fileless-spec"),
        pytest.param(
            "spec_fixture",
            _make_module_with_spec("spec_fixture", str(SPECS_DIR / "fixture_schema.py")),
            False,
            id="specs-dir-file",
        ),
        pytest.param(
            "temp_mod",
            _make_module_with_spec("temp_mod", str(SYSTEM_TEMP_ROOT / "generated.py")),
            False,
            id="system-temp-file",
        ),
        pytest.param(
            "cdm_data_loaders.sample",
            _make_module_with_spec("cdm_data_loaders.sample", str(REPO_ROOT / "src" / "sample.py")),
            True,
            id="normal-repo-module",
        ),
    ],
)
def test_is_normal_imported_module_classifies_entries(name: str, mod: ModuleType | None, expected: bool) -> None:
    """Verify ``is_normal_imported_module`` distinguishes standard imports from synthetic/spec test modules."""
    assert is_normal_imported_module(name, mod) is expected


def test_snapshot_loggers_captures_configured_and_ignores_defaults() -> None:
    """Verify ``snapshot_loggers`` omits default loggers and captures configured logger state."""
    default_name = "cdm_state_audit_unit_default_logger"
    configured_name = "cdm_state_audit_unit_configured_logger"
    with preserve_logging_state():
        default_lg = logging.getLogger(default_name)
        default_lg.setLevel(logging.NOTSET)
        configured_lg = logging.getLogger(configured_name)
        configured_lg.setLevel(logging.WARNING)
        configured_lg.propagate = False
        snap = snapshot_loggers()
    assert "<root>" in snap
    assert default_name not in snap
    assert snap[configured_name] == (logging.WARNING, False, False, (), ())


def test_snapshot_module_vars_and_caches_extracts_globals_and_lru_sizes() -> None:
    """Verify ``snapshot_module_vars_and_caches`` records mutable globals and ``lru_cache`` sizes on repo modules."""
    tracked_mod = ModuleType("cdm_data_loaders.fake_tracked")

    @functools.lru_cache(maxsize=8)
    def cached_fn(x: int) -> int:
        return x * 2

    cached_fn.__module__ = "cdm_data_loaders.fake_tracked"
    cached_fn.__qualname__ = "cached_fn"

    def plain_fn() -> int:
        return 1

    cached_fn(3)
    vars(tracked_mod).update(
        {
            "REGISTRY": {"alpha": 1},
            "cached_fn": cached_fn,
            "plain_fn": plain_fn,
            "SubMod": ModuleType("sub"),
            "SampleType": int,
        }
    )
    reexport_mod = ModuleType("tests.fake_reexporter")
    vars(reexport_mod)["cached_fn"] = cached_fn
    untracked_mod = ModuleType("urllib.fake_untracked")
    vars(untracked_mod)["IGNORED"] = [1, 2, 3]

    vars_snap, caches_snap = snapshot_module_vars_and_caches(
        {
            "cdm_data_loaders.fake_tracked": tracked_mod,
            "tests.fake_reexporter": reexport_mod,
            "urllib.fake_untracked": untracked_mod,
            "none_entry": None,
        }
    )
    assert vars_snap == {"cdm_data_loaders.fake_tracked.REGISTRY": ("dict", "{'alpha': 1}")}
    assert caches_snap == {"cdm_data_loaders.fake_tracked.cached_fn": 1}


@pytest.mark.parametrize(
    ("item", "expected"),
    [
        pytest.param(None, {}, id="none-item"),
        pytest.param(SimpleNamespace(), {}, id="missing-fixtureinfo"),
        pytest.param(
            SimpleNamespace(
                _fixtureinfo=SimpleNamespace(
                    name2fixturedefs={
                        "session_dict": [
                            SimpleNamespace(scope="session", cached_result=({"k": "v"}, 0, None), baseid="tests"),
                        ],
                        "func_dict": [
                            SimpleNamespace(scope="function", cached_result=({"f": 1}, 0, None), baseid="tests"),
                        ],
                        "uncached_list": [
                            SimpleNamespace(scope="module", cached_result=None, baseid="tests"),
                        ],
                        "session_str": [
                            SimpleNamespace(scope="session", cached_result=("immutable", 0, None), baseid="tests"),
                        ],
                    }
                )
            ),
            {"tests::session_dict[session]": ("dict", "{'k': 'v'}")},
            id="mixed-scopes-and-mutability",
        ),
    ],
)
def test_snapshot_scoped_fixtures_filters_by_scope_and_mutability(
    item: SimpleNamespace | None,
    expected: dict[str, tuple[Any, ...]],
) -> None:
    """Verify ``snapshot_scoped_fixtures`` captures only cached non-function-scoped mutable containers."""
    assert snapshot_scoped_fixtures(item) == expected  # type: ignore[arg-type]


def test_take_state_snapshot_self_diff_is_empty() -> None:
    """Verify ``take_state_snapshot`` produces a clean zero-diff snapshot when state is unchanged."""
    snap = take_state_snapshot()
    assert diff_state_snapshots(snap, snap) == {}


def _baseline_snapshot() -> StateSnapshot:
    """Construct a minimal deterministic ``StateSnapshot`` for unit-testing ``diff_state_snapshots``.

    :return: Baseline ``StateSnapshot`` instance.
    :rtype: StateSnapshot
    """
    existing_mod = _make_module_with_spec("cdm_data_loaders.existing", str(REPO_ROOT / "src" / "existing.py"))
    return StateSnapshot(
        cwd="/repo",
        environ={"EXISTING": "1", "KEEP": "same"},
        sys_argv=["pytest"],
        sys_path=["/repo/src", "/repo"],
        sys_modules={"cdm_data_loaders.existing": existing_mod, "_pytest.temp": ModuleType("_pytest.temp")},
        warnings_filters=[("default", None, Warning, None, 0)],
        importlib_resources_files_id=100,
        boto3_default_session_set=False,
        s3_client_cached=False,
        dlt_pipeline_active=False,
        cdm_logger_applied_key=None,
        logging_disable_level=0,
        loggers={"<root>": (logging.WARNING, True, False, (), ())},
        jsonschema_validators=frozenset({"draft2020-12"}),
        jsonschema_meta_schemas=frozenset({"https://json-schema.org/draft/2020-12/schema"}),
        module_vars={
            "cdm_data_loaders.mod.STATE": ("dict", "{}"),
            "cdm_data_loaders.mod.TO_DELETE": ("int", 1),
            "cdm_data_loaders.utils.logging.applied_key": ("NoneType", None),
        },
        caches={"cdm_data_loaders.mod.lookup": 0},
        scoped_fixtures={"tests::shared[module]": ("dict", "{'a': 1}")},
    )


@pytest.mark.parametrize(
    ("mutator_kwargs", "expected_key"),
    [
        pytest.param({"cwd": "/other"}, "cwd", id="cwd"),
        pytest.param({"sys_argv": ["prog", "--flag"]}, "sys_argv", id="sys-argv"),
        pytest.param({"sys_path": ["/extra", "/repo/src", "/repo"]}, "sys_path", id="sys-path-runtime"),
        pytest.param({"warnings_filters": []}, "warnings_filters", id="warnings-filters"),
        pytest.param({"importlib_resources_files_id": 999}, "importlib_resources_files", id="importlib-resources"),
        pytest.param({"boto3_default_session_set": True}, "boto3_default_session", id="boto3-session"),
        pytest.param({"s3_client_cached": True}, "s3_client_singleton", id="s3-singleton"),
        pytest.param({"dlt_pipeline_active": True}, "dlt_pipeline_active", id="dlt-pipeline"),
        pytest.param({"cdm_logger_applied_key": ("INFO", None)}, "cdm_logger_applied_key", id="cdm-logger-key"),
        pytest.param({"logging_disable_level": logging.CRITICAL}, "logging_disable_level", id="logging-disable"),
        pytest.param(
            {"environ": {"KEEP": "changed", "ADDED": "2"}},
            "environ",
            id="environ-added-removed-changed",
        ),
        pytest.param(
            {"loggers": {"<root>": (logging.DEBUG, True, False, (), ())}},
            "loggers",
            id="loggers",
        ),
        pytest.param(
            {"jsonschema_validators": frozenset({"draft2020-12", "urn:custom"})},
            "jsonschema_registry",
            id="jsonschema-registry",
        ),
        pytest.param(
            {"scoped_fixtures": {"tests::shared[module]": ("dict", "{'a': 1, 'leaked': 2}")}},
            "scoped_fixtures_mutated_in_place",
            id="scoped-fixture-mutation",
        ),
    ],
)
def test_diff_state_snapshots_detects_each_state_category(
    mutator_kwargs: dict[str, Any],
    expected_key: str,
) -> None:
    """Verify ``diff_state_snapshots`` reports mutations for each tracked global state category."""
    before = _baseline_snapshot()
    after = replace(before, **mutator_kwargs)
    diffs = diff_state_snapshots(before, after)
    assert expected_key in diffs


@pytest.mark.parametrize("scenario", ["clean", "leaky"], ids=["clean-collect", "leaky-after"])
def test_diff_state_snapshots_module_vars_caches_and_sys_modules_rules(scenario: str) -> None:
    """Verify ``diff_state_snapshots`` handles synthetic modules, collection targets, and pure memo caches."""
    before = _baseline_snapshot()
    collected_target = (SYSTEM_TEMP_ROOT / "pkg" / "test_target.py").resolve()
    if scenario == "clean":
        collected_mod = _make_module_with_spec("pkg.test_target", str(collected_target))
        ancestor_init = _make_module_with_spec("pkg", str(SYSTEM_TEMP_ROOT / "pkg" / "__init__.py"))
        after = replace(
            before,
            sys_path=[str(collected_target.parent), *before.sys_path],
            sys_modules={
                "cdm_data_loaders.existing": before.sys_modules["cdm_data_loaders.existing"],
                "pkg": ancestor_init,
                "pkg.test_target": collected_mod,
            },
            module_vars={
                "cdm_data_loaders.mod.STATE": ("dict", "{}"),
                "cdm_data_loaders.mod.TO_DELETE": ("int", 1),
                "cdm_data_loaders.utils.logging.applied_key": ("tuple", "changed-dedicated-ignored-here"),
            },
            caches={
                "cdm_data_loaders.mod.lookup": 0,
                "cdm_data_loaders.converters.core.metadata.metadata_keys_for": 5,
            },
        )
    else:
        synthetic_mod = ModuleType("leaked_dynamic")
        replacement_mod = _make_module_with_spec("cdm_data_loaders.existing", str(REPO_ROOT / "src" / "existing.py"))
        after = replace(
            before,
            sys_path=["/unrelated/dir", *before.sys_path],
            sys_modules={
                "cdm_data_loaders.existing": replacement_mod,
                "leaked_dynamic": synthetic_mod,
            },
            module_vars={"cdm_data_loaders.mod.STATE": ("dict", "{'dirty': True}")},
            caches={"cdm_data_loaders.mod.lookup": 2},
        )

    diffs = diff_state_snapshots(before, after, collected_module_path=collected_target)
    if scenario == "clean":
        assert diffs == {}
    else:
        assert "sys_path" in diffs
        assert diffs["sys_modules"] == {
            "added_synthetic_or_fixture": ["leaked_dynamic"],
            "replaced": ["cdm_data_loaders.existing"],
        }
        assert diffs["module_vars"] == [
            "cdm_data_loaders.mod.STATE",
            "cdm_data_loaders.mod.TO_DELETE (deleted)",
        ]
        assert diffs["caches"] == {"cdm_data_loaders.mod.lookup": {"before": 0, "after": 2}}


@pytest.mark.parametrize(
    ("collection_leaks", "test_leaks", "expected_substrings"),
    [
        pytest.param([], [], (), id="empty"),
        pytest.param(
            [("tests/test_collect.py", {"cwd": {"before": "/a", "after": "/b"}})],
            [("tests/test_run.py::test_x", {"s3_client_singleton": {"before": False, "after": True}})],
            (
                "(collection=1, tests=1):",
                "Collection leak in tests/test_collect.py:",
                "Test leak in tests/test_run.py::test_x:",
            ),
            id="populated",
        ),
    ],
)
def test_format_leak_report_empty_and_populated(
    collection_leaks: list[tuple[str, dict[str, Any]]],
    test_leaks: list[tuple[str, dict[str, Any]]],
    expected_substrings: tuple[str, ...],
) -> None:
    """Verify ``format_leak_report`` returns ``""`` when clean and formats collection and test leaks otherwise."""
    report = format_leak_report(collection_leaks, test_leaks)
    if not expected_substrings:
        assert report == ""
    for expected in expected_substrings:
        assert expected in report


@pytest.mark.parametrize(
    ("extra_args", "expected_snippets"),
    [
        pytest.param(
            ["-rs"],
            ("1 skipped", "need -m state_audit option to run global state mutation audit"),
            id="skipped-by-default",
        ),
        pytest.param(
            ["-m", "state_audit"],
            ("1 passed",),
            id="runs-when-selected",
        ),
    ],
)
def test_state_audit_marker_skipped_by_default_and_runs_when_selected(
    tmp_path: Path,
    extra_args: list[str],
    expected_snippets: tuple[str, ...],
) -> None:
    """Verify ``@pytest.mark.state_audit`` tests skip unless ``-m state_audit`` is explicitly provided."""
    sample = tmp_path / "test_marker_opt_in.py"
    sample.write_text(
        "import pytest\n\n@pytest.mark.state_audit\ndef test_opt_in_only() -> None:\n    assert True\n",
        encoding="utf-8",
    )
    res = run_pytest_subprocess(
        [
            "-p",
            "tests.conftest",
            *extra_args,
            "-o",
            "addopts=",
            "-p",
            "no:randomly",
            "-p",
            "no:cov",
            str(sample),
        ]
    )
    assert res.returncode == 0
    for snippet in expected_snippets:
        assert snippet in res.stdout


@pytest.mark.parametrize(
    ("filename", "source_text", "expected_returncode", "expected_snippets"),
    [
        pytest.param(
            "test_clean_audit.py",
            "def test_clean_case() -> None:\n    assert 1 + 1 == 2\n",
            0,
            ("[state-audit] Passed: 0 unreverted global state mutations across 1 test(s).",),
            id="clean-passes",
        ),
        pytest.param(
            "test_leaky_audit.py",
            (
                "import os\n"
                "import warnings\n"
                "import pytest\n\n"
                "os.environ['COLLECTION_LEAK_ENV'] = '1'\n\n"
                "@pytest.fixture(scope='module')\n"
                "def shared_box() -> dict[str, int]:\n"
                "    return {'seed': 1}\n\n"
                "def test_prime_fixture(shared_box: dict[str, int]) -> None:\n"
                "    assert shared_box == {'seed': 1}\n\n"
                "def test_mutate_shared_fixture_and_warnings(shared_box: dict[str, int]) -> None:\n"
                "    shared_box['leaked'] = 2\n"
                "    warnings.filterwarnings('ignore', message='runtime_leak_marker')\n\n"
                "def test_subsequent_consumer(shared_box: dict[str, int]) -> None:\n"
                "    assert 'seed' in shared_box\n"
            ),
            int(pytest.ExitCode.TESTS_FAILED),
            (
                "[state-audit] Detected unreverted global state mutations (collection=1, tests=1):",
                '"scoped_fixtures_mutated_in_place"',
                '"warnings_filters"',
            ),
            id="leaky-fails",
        ),
    ],
)
def test_state_audit_cli_flag_passes_clean_and_fails_on_leaks(
    tmp_path: Path,
    filename: str,
    source_text: str,
    expected_returncode: int,
    expected_snippets: tuple[str, ...],
) -> None:
    """Verify ``--state-audit`` exits 0 on clean tests and exits 1 with detailed diffs on unreverted leaks."""
    target_file = tmp_path / filename
    target_file.write_text(source_text, encoding="utf-8")
    res = run_pytest_subprocess(
        [
            "-p",
            "tests.conftest",
            "--state-audit",
            "-o",
            "addopts=",
            "-p",
            "no:randomly",
            "-p",
            "no:cov",
            str(target_file),
        ]
    )
    assert res.returncode == expected_returncode, res.stdout + res.stderr
    for snippet in expected_snippets:
        assert snippet in res.stdout
