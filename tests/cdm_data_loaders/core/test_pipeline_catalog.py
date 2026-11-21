"""Unit tests for ``cdm_data_loaders.core.pipeline_catalog``."""

import re
import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace
from unittest.mock import patch

import pytest
from pydantic_settings import SettingsConfigDict

from cdm_data_loaders.core import pipeline_catalog
from cdm_data_loaders.core.cli import CliRunner
from cdm_data_loaders.core.pipeline_catalog import (
    PACKAGE_DIST_NAME,
    PipelineSpec,
    _build_module_to_cli_map,
    _resolve_callable_spec,
    _resolve_runner_spec,
    _spec_from_module,
    discover_pipeline_targets,
    load_project_scripts,
    resolve_pipeline,
)
from cdm_data_loaders.pipelines.jsonlines.extract import (
    cli as jsonl_extract_cli,
)
from cdm_data_loaders.pipelines.jsonlines.extract import (
    run_jsonlines_pipeline,
)
from cdm_data_loaders.pipelines.jsonlines.settings import JsonlExtractSettings


def test_load_project_scripts_from_explicit_toml(tmp_path: Path) -> None:
    """Read and sort [project.scripts] entries from an explicit pyproject.toml file."""
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text(
        '[project.scripts]\nzebra = "pkg.z:cli"\nalpha = "pkg.a:cli"\n',
        encoding="utf-8",
    )
    assert load_project_scripts(pyproject) == {
        "alpha": "pkg.a:cli",
        "zebra": "pkg.z:cli",
    }
    assert discover_pipeline_targets(pyproject) == {
        "alpha": ("alpha", "pkg.a"),
        "zebra": ("zebra", "pkg.z"),
    }


def test_load_project_scripts_falls_back_to_entry_points(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Fall back to console_scripts entry points when pyproject.toml is absent."""
    missing_toml = tmp_path / "missing_pyproject.toml"
    monkeypatch.setattr(pipeline_catalog, "DEFAULT_PYPROJECT_PATH", missing_toml)

    fake_eps = [
        SimpleNamespace(
            name="xsv",
            value="cdm_data_loaders.pipelines.xsv.pipeline:cli",
            dist=SimpleNamespace(name=PACKAGE_DIST_NAME),
        ),
        SimpleNamespace(
            name="foreign_tool",
            value="other_pkg.cli:main",
            dist=SimpleNamespace(name="other-pkg"),
        ),
        SimpleNamespace(
            name="orphan_tool",
            value="orphan.cli:main",
            dist=None,
        ),
    ]
    with patch.object(pipeline_catalog, "entry_points", return_value=fake_eps):
        scripts = load_project_scripts()

    assert scripts == {"xsv": "cdm_data_loaders.pipelines.xsv.pipeline:cli"}


@pytest.mark.parametrize(
    "target_shape",
    ["short_name", "unregistered_cli_runner", "module_target"],
    ids=["short_name", "unregistered_cli_runner", "module_target"],
)
def test_resolve_pipeline_short_name_and_unregistered_runner(
    tmp_path: Path,
    target_shape: str,
) -> None:
    """Resolve short pipeline names, unregistered CliRunner instances, and plain pipeline modules."""
    if target_shape == "short_name":
        target: str | CliRunner[JsonlExtractSettings, object] | ModuleType = "jsonlines.extract"
        pyproject_path: Path | None = None
        expected_spec = PipelineSpec(
            cli_command="jsonlines_extract",
            module_path="cdm_data_loaders.pipelines.jsonlines.extract",
            settings_cls=JsonlExtractSettings,
            pipeline_fn=run_jsonlines_pipeline,
            cli_callable=jsonl_extract_cli,
        )
    elif target_shape == "unregistered_cli_runner":
        pyproject_path = tmp_path / "pyproject.toml"
        pyproject_path.write_text(
            '[project.scripts]\nalpha = "cdm_data_loaders.pipelines.xsv.pipeline:cli"\n',
            encoding="utf-8",
        )
        custom_runner = CliRunner(JsonlExtractSettings, run_jsonlines_pipeline)
        target = custom_runner
        expected_spec = PipelineSpec(
            cli_command="jsonlines_extract",
            module_path="cdm_data_loaders.pipelines.jsonlines.extract",
            settings_cls=JsonlExtractSettings,
            pipeline_fn=run_jsonlines_pipeline,
            cli_callable=custom_runner,
        )
    else:
        pyproject_path = tmp_path / "pyproject.toml"
        pyproject_path.write_text(
            '[project.scripts]\nalpha = "cdm_data_loaders.pipelines.xsv.pipeline:cli"\n',
            encoding="utf-8",
        )
        plain_mod = ModuleType("cdm_data_loaders.pipelines.plain_custom")
        plain_mod.Settings = JsonlExtractSettings  # type: ignore[attr-defined]
        plain_mod.run_pipeline = run_jsonlines_pipeline  # type: ignore[attr-defined]
        target = plain_mod
        expected_spec = PipelineSpec(
            cli_command="plain_custom",
            module_path="cdm_data_loaders.pipelines.plain_custom",
            settings_cls=JsonlExtractSettings,
            pipeline_fn=run_jsonlines_pipeline,
            cli_callable=None,
        )

    assert resolve_pipeline(target, pyproject_path=pyproject_path) == expected_spec


@pytest.mark.parametrize(
    ("script_spec", "expected_cli_command"),
    [
        pytest.param(
            "cdm_data_loaders.pipelines.jsonlines.extract:cli",
            "jsonlines_extract",
            id="registered-with-attr",
        ),
        pytest.param(
            "cdm_data_loaders.pipelines.jsonlines.extract",
            "jsonlines_extract",
            id="registered-without-attr",
        ),
        pytest.param(
            "cdm_data_loaders.pipelines.jsonlines.extract:cli",
            "custom_extract",
            id="registered-custom-command",
        ),
    ],
)
def test_resolve_callable_spec_registered_module(
    script_spec: str,
    expected_cli_command: str,
) -> None:
    """Resolve a callable whose module is registered in module_to_cli."""
    module_path = "cdm_data_loaders.pipelines.jsonlines.extract"
    scripts = {expected_cli_command: script_spec}
    module_to_cli = {module_path: expected_cli_command}
    expected_spec = PipelineSpec(
        cli_command=expected_cli_command,
        module_path=module_path,
        settings_cls=JsonlExtractSettings,
        pipeline_fn=run_jsonlines_pipeline,
        cli_callable=jsonl_extract_cli,
    )

    assert _resolve_callable_spec(run_jsonlines_pipeline, scripts, module_to_cli) == expected_spec


def test_resolve_callable_spec_imports_unloaded_module(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Import the callable's module when it is not yet in sys.modules."""
    module_path = "cdm_data_loaders.pipelines.jsonlines.extract"
    original_mod = sys.modules.get(module_path)
    monkeypatch.delitem(sys.modules, module_path, raising=False)
    scripts = {"jsonlines_extract": f"{module_path}:cli"}
    module_to_cli = {module_path: "jsonlines_extract"}

    result = _resolve_callable_spec(run_jsonlines_pipeline, scripts, module_to_cli)
    reloaded_mod = sys.modules[module_path]

    assert reloaded_mod is not original_mod
    assert result == PipelineSpec(
        cli_command="jsonlines_extract",
        module_path=module_path,
        settings_cls=reloaded_mod.JsonlExtractSettings,
        pipeline_fn=reloaded_mod.run_jsonlines_pipeline,
        cli_callable=reloaded_mod.cli,
    )


def test_resolve_callable_spec_unregistered_module_cli_runner(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Fall back to module.cli when module_path is unregistered and cli.action_fn matches."""
    mod_name = "tests_synthetic_unregistered_runner_mod"

    def action_fn(_settings: JsonlExtractSettings) -> None:
        return None

    action_fn.__module__ = mod_name
    runner = CliRunner(JsonlExtractSettings, action_fn)
    mod = ModuleType(mod_name)
    mod.cli = runner  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, mod_name, mod)

    result = _resolve_callable_spec(action_fn, scripts={}, module_to_cli={})

    assert result == PipelineSpec(
        cli_command="jsonlines_extract",
        module_path=mod_name,
        settings_cls=JsonlExtractSettings,
        pipeline_fn=action_fn,
        cli_callable=runner,
    )


@pytest.mark.parametrize(
    "case",
    ["no_module", "no_cli_attr", "non_runner_cli_attr", "different_action_fn"],
    ids=["no_module", "no_cli_attr", "non_runner_cli_attr", "different_action_fn"],
)
def test_resolve_callable_spec_rejects_unresolvable_callable(
    monkeypatch: pytest.MonkeyPatch,
    case: str,
) -> None:
    """Raise ValueError with callable and module context when a callable cannot be resolved."""
    if case == "no_module":

        def no_mod_fn(_settings: JsonlExtractSettings) -> None:
            return None

        no_mod_fn.__module__ = ""
        fn: object = no_mod_fn
        expected_mod = ""
    else:
        mod_name = f"tests_synthetic_unresolvable_{case}"

        def target_fn(_settings: JsonlExtractSettings) -> None:
            return None

        def other_fn(_settings: JsonlExtractSettings) -> None:
            return None

        target_fn.__module__ = mod_name
        mod = ModuleType(mod_name)
        if case == "non_runner_cli_attr":
            mod.cli = other_fn  # type: ignore[attr-defined]
        elif case == "different_action_fn":
            mod.cli = CliRunner(JsonlExtractSettings, other_fn)  # type: ignore[attr-defined]
        monkeypatch.setitem(sys.modules, mod_name, mod)
        fn = target_fn
        expected_mod = mod_name

    expected_msg = f"Cannot resolve pipeline from callable {fn!r} (module {expected_mod!r})"
    with pytest.raises(ValueError, match=re.escape(expected_msg)):
        _resolve_callable_spec(fn, scripts={}, module_to_cli={})  # type: ignore[arg-type]


def test_load_project_scripts_defaults_to_repo_pyproject() -> None:
    """Read repository pyproject.toml when pyproject_path is None."""
    default_scripts = load_project_scripts()
    assert "xsv" in default_scripts


def test_load_project_scripts_returns_empty_when_scripts_missing(tmp_path: Path) -> None:
    """Return an empty mapping when [project.scripts] is absent from pyproject.toml."""
    empty_toml = tmp_path / "empty.toml"
    empty_toml.write_text("[project]\nname = 'demo'\n", encoding="utf-8")

    assert load_project_scripts(empty_toml) == {}
    assert discover_pipeline_targets(empty_toml) == {}


def test_build_module_to_cli_map_prefers_cli_attr_and_handles_no_colon() -> None:
    """Prefer :cli over alphabetically earlier aliases and handle specs without a colon."""
    scripts = {
        "aaa_alias": "pkg.mod:other",
        "zzz_primary": "pkg.mod:cli",
        "bare": "pkg.bare",
    }
    assert _build_module_to_cli_map(scripts) == {
        "pkg.mod": "zzz_primary",
        "pkg.bare": "bare",
    }


@pytest.mark.parametrize(
    "case",
    [
        pytest.param("explicit_runner", id="explicit-runner-attr"),
        pytest.param("prefixed_plain_callable", id="command-prefix-plain-callable"),
        pytest.param("non_callable_attr", id="non-callable-attr"),
    ],
)
def test_spec_from_module_resolves_cli_attribute_variants(case: str) -> None:
    """Resolve explicit attr_name, <cmd>_cli fallback, and non-callable attributes on a module."""
    mod = ModuleType("tests_synthetic_spec_mod")
    runner = CliRunner(JsonlExtractSettings, run_jsonlines_pipeline)

    def plain_cli() -> None:
        return None

    mod.custom_cli = runner  # type: ignore[attr-defined]
    mod.plain_cli = plain_cli  # type: ignore[attr-defined]
    mod.not_callable = 42  # type: ignore[attr-defined]
    mod.Settings = JsonlExtractSettings  # type: ignore[attr-defined]
    mod.run_pipeline = run_jsonlines_pipeline  # type: ignore[attr-defined]

    if case == "explicit_runner":
        result = _spec_from_module("ignored", mod, attr_name="custom_cli")
        expected = PipelineSpec(
            cli_command="ignored",
            module_path="tests_synthetic_spec_mod",
            settings_cls=JsonlExtractSettings,
            pipeline_fn=run_jsonlines_pipeline,
            cli_callable=runner,
        )
    elif case == "prefixed_plain_callable":
        result = _spec_from_module("plain", mod)
        expected = PipelineSpec(
            cli_command="plain",
            module_path="tests_synthetic_spec_mod",
            settings_cls=JsonlExtractSettings,
            pipeline_fn=run_jsonlines_pipeline,
            cli_callable=plain_cli,
        )
    else:
        result = _spec_from_module("other", mod, attr_name="not_callable")
        expected = PipelineSpec(
            cli_command="other",
            module_path="tests_synthetic_spec_mod",
            settings_cls=JsonlExtractSettings,
            pipeline_fn=run_jsonlines_pipeline,
            cli_callable=None,
        )

    assert result == expected


def test_resolve_runner_spec_falls_back_to_module_stem_without_cli_prog_name() -> None:
    """Derive cli_command from the action module stem when cli_prog_name is None."""

    class NoProgSettings(JsonlExtractSettings):
        model_config = SettingsConfigDict(**{**JsonlExtractSettings.model_config, "cli_prog_name": None})

    def dummy_action(_s: NoProgSettings) -> None:
        return None

    dummy_action.__module__ = "pkg.sub.my_pipe"
    no_prog_runner = CliRunner(NoProgSettings, dummy_action)

    result = _resolve_runner_spec(no_prog_runner, scripts={}, module_to_cli={})

    assert result == PipelineSpec(
        cli_command="my_pipe",
        module_path="pkg.sub.my_pipe",
        settings_cls=NoProgSettings,
        pipeline_fn=dummy_action,
        cli_callable=no_prog_runner,
    )


@pytest.mark.parametrize(
    "target_case",
    [
        pytest.param("registered_runner", id="registered-cli-runner"),
        pytest.param("registered_module", id="registered-module"),
        pytest.param("unregistered_module_with_cli", id="unregistered-module-with-cli"),
        pytest.param("registered_callable", id="registered-callable"),
    ],
)
def test_resolve_pipeline_registered_and_fallback_targets(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    target_case: str,
) -> None:
    """Resolve registered CliRunner, module, callable, and cli-bearing unregistered module targets."""
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text(
        '[project.scripts]\njsonlines_extract = "cdm_data_loaders.pipelines.jsonlines.extract:cli"\n',
        encoding="utf-8",
    )
    expected_module_path = "cdm_data_loaders.pipelines.jsonlines.extract"

    if target_case == "registered_runner":
        target: object = jsonl_extract_cli
    elif target_case == "registered_module":
        target = sys.modules[expected_module_path]
    elif target_case == "registered_callable":
        target = run_jsonlines_pipeline
    else:
        mod_with_cli = ModuleType("cdm_data_loaders.pipelines.custom.entry")
        mod_with_cli.cli = jsonl_extract_cli  # type: ignore[attr-defined]
        monkeypatch.setitem(sys.modules, mod_with_cli.__name__, mod_with_cli)
        target = mod_with_cli
        expected_module_path = mod_with_cli.__name__

    result = resolve_pipeline(target, pyproject_path=pyproject)  # type: ignore[arg-type]

    assert result == PipelineSpec(
        cli_command="jsonlines_extract",
        module_path=expected_module_path,
        settings_cls=JsonlExtractSettings,
        pipeline_fn=run_jsonlines_pipeline,
        cli_callable=jsonl_extract_cli,
    )


def test_resolve_pipeline_returns_pipeline_spec_unchanged() -> None:
    """Return an existing PipelineSpec instance unchanged."""
    spec = PipelineSpec(cli_command="demo", module_path="pkg.demo")
    assert resolve_pipeline(spec) is spec


@pytest.mark.parametrize(
    "target",
    [
        pytest.param("  jsonlines_extract  ", id="whitespace-padded"),
        pytest.param("cdm_data_loaders.pipelines.jsonlines_extract", id="prefixed-command"),
        pytest.param("cdm_data_loaders.pipelines.jsonlines.extract", id="full-module-path"),
    ],
)
def test_resolve_pipeline_resolves_string_variants(target: str) -> None:
    """Resolve whitespace-padded, package-prefixed, and full module path string targets."""
    result = resolve_pipeline(target)
    assert result.cli_command == "jsonlines_extract"


@pytest.mark.parametrize(
    ("target", "expected_match"),
    [
        pytest.param("nope", r"Unknown pipeline 'nope'\. Known pipelines: alpha, beta", id="unknown-string"),
        pytest.param(123, r"Unknown pipeline 123\. Known pipelines: alpha, beta", id="unsupported-type"),
    ],
)
def test_resolve_pipeline_fail_unknown_target(
    tmp_path: Path,
    target: object,
    expected_match: str,
) -> None:
    """Raise ValueError listing known pipelines for unknown or unsupported targets."""
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text(
        '[project.scripts]\nbeta = "pkg.b:cli"\nalpha = "pkg.a:cli"\n',
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match=expected_match):
        resolve_pipeline(target, pyproject_path=pyproject)  # type: ignore[arg-type]
