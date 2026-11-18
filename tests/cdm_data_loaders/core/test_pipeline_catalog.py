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
