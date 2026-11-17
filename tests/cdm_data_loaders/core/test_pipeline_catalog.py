"""Unit tests for ``cdm_data_loaders.core.pipeline_catalog``."""

from pathlib import Path
from types import ModuleType, SimpleNamespace
from unittest.mock import patch

import pytest

from cdm_data_loaders.core import pipeline_catalog
from cdm_data_loaders.core.cli import CliRunner
from cdm_data_loaders.core.pipeline_catalog import (
    PACKAGE_DIST_NAME,
    PipelineSpec,
    discover_pipeline_targets,
    load_project_scripts,
    resolve_pipeline,
)
from cdm_data_loaders.pipelines.jsonlines.extract_pipeline import (
    cli as jsonl_extract_cli,
)
from cdm_data_loaders.pipelines.jsonlines.extract_pipeline import (
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


def test_resolve_pipeline_short_name_and_unregistered_runner(tmp_path: Path) -> None:
    """Resolve short pipeline names and unregistered CliRunner instances."""
    spec = resolve_pipeline("jsonlines.extract_pipeline")
    assert spec == PipelineSpec(
        cli_command="jsonlines_extract",
        module_path="cdm_data_loaders.pipelines.jsonlines.extract_pipeline",
        settings_cls=JsonlExtractSettings,
        pipeline_fn=run_jsonlines_pipeline,
        cli_callable=jsonl_extract_cli,
    )

    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text('[project.scripts]\nalpha = "cdm_data_loaders.pipelines.xsv.pipeline:cli"\n', encoding="utf-8")
    custom_runner = CliRunner(JsonlExtractSettings, run_jsonlines_pipeline)
    unregistered_spec = resolve_pipeline(custom_runner, pyproject_path=pyproject)
    assert unregistered_spec.cli_command == "jsonlines_extract"
    assert unregistered_spec.settings_cls is JsonlExtractSettings
    assert unregistered_spec.pipeline_fn is run_jsonlines_pipeline

    plain_mod = ModuleType("cdm_data_loaders.pipelines.plain_custom")
    plain_mod.Settings = JsonlExtractSettings  # type: ignore[attr-defined]
    plain_mod.run_pipeline = run_jsonlines_pipeline  # type: ignore[attr-defined]
    plain_spec = resolve_pipeline(plain_mod, pyproject_path=pyproject)
    assert plain_spec.cli_command == "plain_custom"
    assert plain_spec.cli_callable is None
